//! EXPLAIN-based query cost estimation for pre-check rejection.
//!
//! Provides runtime cost-based rejection of queries that would be too
//! expensive, using PostgreSQL's `EXPLAIN (FORMAT JSON)` output.
//!
//! # Modes
//! - **Off**: No EXPLAIN pre-check
//! - **Precheck**: Run EXPLAIN on cache-miss for queries with expand depth ≥ threshold
//! - **Enforce**: Always run EXPLAIN and enforce cost thresholds
//!
//! # Caching
//! EXPLAIN results are cached by `AST_shape_hash + rls_signature` with configurable TTL.
//! This avoids repeated EXPLAIN calls for the same query shape.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Configuration for EXPLAIN pre-check behavior.
#[derive(Debug, Clone)]
pub struct ExplainConfig {
    /// Operating mode for EXPLAIN pre-check.
    pub mode: ExplainMode,

    /// Run EXPLAIN for queries with expand_depth >= this value.
    /// Default: 3 (queries joining 3+ tables get pre-checked).
    pub depth_threshold: usize,

    /// Reject if PostgreSQL's estimated total cost exceeds this.
    /// Default: 100,000 (unitless PostgreSQL planner cost).
    pub max_total_cost: f64,

    /// Reject if PostgreSQL estimates more rows than this.
    /// Default: 1,000,000 rows.
    pub max_plan_rows: u64,

    /// TTL for cached EXPLAIN results.
    /// Default: 5 minutes.
    pub cache_ttl: Duration,
}

impl Default for ExplainConfig {
    fn default() -> Self {
        Self {
            mode: ExplainMode::Precheck,
            depth_threshold: 3,
            max_total_cost: 100_000.0,
            max_plan_rows: 1_000_000,
            cache_ttl: Duration::from_secs(300),
        }
    }
}

/// Operating mode for EXPLAIN pre-check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExplainMode {
    /// No EXPLAIN pre-check — fastest, no protection.
    Off,
    /// Run EXPLAIN on cache-miss for queries above depth threshold.
    /// Recommended default for production.
    Precheck,
    /// Always run EXPLAIN and enforce — strictest, slight latency cost.
    /// Recommended for staging or high-security tenants.
    Enforce,
}

/// Result of an EXPLAIN pre-check.
#[derive(Debug, Clone)]
pub struct ExplainEstimate {
    /// PostgreSQL's estimated total cost (arbitrary units).
    pub total_cost: f64,
    /// PostgreSQL's estimated number of rows returned.
    pub plan_rows: u64,
}

/// Cached EXPLAIN result with TTL and row-estimate snapshot.
struct CachedEstimate {
    estimate: ExplainEstimate,
    cached_at: Instant,
    /// Row estimate snapshot at cache time, for drift detection.
    plan_rows: u64,
}

/// In-memory cache for EXPLAIN estimates, keyed by AST shape hash.
pub struct ExplainCache {
    entries: Mutex<HashMap<u64, CachedEstimate>>,
    ttl: Duration,
    /// Maximum number of cached entries to prevent OOM from shape explosion
    max_entries: usize,
}

impl ExplainCache {
    /// Create a new EXPLAIN cache with the given TTL.
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            ttl,
            max_entries: 10_000,
        }
    }

    /// Get a cached estimate if it exists, hasn't expired, and row-estimate
    /// hasn't drifted beyond 50%.
    ///
    /// `current_reltuples` is the current `pg_class.reltuples` for the primary
    /// table. If provided and the cached plan_rows have drifted >50% from
    /// the current estimate, the entry is considered stale (data skew).
    pub fn get(&self, shape_hash: u64, current_reltuples: Option<u64>) -> Option<ExplainEstimate> {
        let entries = self.entries.lock().ok()?;
        let entry = entries.get(&shape_hash)?;
        if entry.cached_at.elapsed() < self.ttl {
            // Row-estimate drift check: invalidate if BOTH conditions met:
            // 1. Relative change > 50% (data skew)
            // 2. Absolute delta > 10,000 rows (prevents small table thrash)
            if let Some(current) = current_reltuples
                && entry.plan_rows > 0
            {
                let cached = entry.plan_rows as f64;
                let drift = ((current as f64) - cached).abs() / cached;
                let abs_delta = (current as i64 - entry.plan_rows as i64).unsigned_abs();
                if drift > 0.5 && abs_delta > 10_000 {
                    return None; // Stale — significant data skew detected
                }
            }
            Some(entry.estimate.clone())
        } else {
            None
        }
    }

    /// Store an estimate in the cache.
    pub fn insert(&self, shape_hash: u64, estimate: ExplainEstimate) {
        if let Ok(mut entries) = self.entries.lock() {
            // Evict expired entries when approaching capacity
            if entries.len() >= self.max_entries / 2 {
                let ttl = self.ttl;
                entries.retain(|_, v| v.cached_at.elapsed() < ttl);
            }
            // Hard cap: if still at capacity after eviction, skip insert
            if entries.len() >= self.max_entries {
                return;
            }
            entries.insert(shape_hash, CachedEstimate {
                plan_rows: estimate.plan_rows,
                estimate,
                cached_at: Instant::now(),
            });
        }
    }

    /// Number of cached entries (for metrics).
    pub fn len(&self) -> usize {
        self.entries.lock().map(|e| e.len()).unwrap_or(0)
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Parse `EXPLAIN (FORMAT JSON)` output to extract cost estimates.
///
/// Uses lightweight string parsing to avoid adding serde_json as a
/// dependency to the pg driver crate. The EXPLAIN JSON format is stable:
/// ```json
/// [{"Plan": {"Total Cost": 1234.56, "Plan Rows": 5000, ...}}]
/// ```
pub fn parse_explain_json(json_str: &str) -> Option<ExplainEstimate> {
    let total_cost = extract_json_number(json_str, "Total Cost")?;
    let plan_rows = extract_json_number(json_str, "Plan Rows")? as u64;

    Some(ExplainEstimate {
        total_cost,
        plan_rows,
    })
}

/// Extract a numeric value after `"key":` from a JSON string.
fn extract_json_number(json: &str, key: &str) -> Option<f64> {
    let pattern = format!("\"{}\":", key);
    let start = json.find(&pattern)?;
    let after_key = &json[start + pattern.len()..];

    // Skip whitespace
    let trimmed = after_key.trim_start();

    // Parse the number (may be integer or float)
    let end = trimmed.find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-' && c != 'e' && c != 'E' && c != '+')?;
    let num_str = &trimmed[..end];
    num_str.parse::<f64>().ok()
}

/// Decision from the EXPLAIN pre-check.
#[derive(Debug)]
pub enum ExplainDecision {
    /// Query is allowed to proceed.
    Allow,
    /// Query is rejected with an explanation.
    Reject {
        /// PostgreSQL's estimated total cost for the query.
        total_cost: f64,
        /// PostgreSQL's estimated row count.
        plan_rows: u64,
        /// Configured maximum cost threshold.
        max_cost: f64,
        /// Configured maximum row threshold.
        max_rows: u64,
    },
    /// EXPLAIN was skipped (mode=Off or below depth threshold).
    Skipped,
}

impl ExplainDecision {
    /// Returns true if the query should be rejected.
    pub fn is_rejected(&self) -> bool {
        matches!(self, ExplainDecision::Reject { .. })
    }

    /// Human-readable rejection message for API responses.
    pub fn rejection_message(&self) -> Option<String> {
        match self {
            ExplainDecision::Reject { total_cost, plan_rows, max_cost, max_rows } => {
                Some(format!(
                    "Query rejected: estimated cost {:.0} exceeds limit {:.0}, \
                     or estimated rows {} exceeds limit {}. \
                     Try narrowing your filters, reducing ?expand depth, or using pagination.",
                    total_cost, max_cost, plan_rows, max_rows
                ))
            }
            _ => None,
        }
    }

    /// Machine-readable rejection detail for structured API error responses.
    ///
    /// Returns `None` for `Allow` and `Skipped` decisions.
    /// Client SDKs can use this to programmatically react to cost rejections.
    pub fn rejection_detail(&self) -> Option<ExplainRejectionDetail> {
        match self {
            ExplainDecision::Reject { total_cost, plan_rows, max_cost, max_rows } => {
                Some(ExplainRejectionDetail {
                    estimated_cost: *total_cost,
                    cost_limit: *max_cost,
                    estimated_rows: *plan_rows,
                    row_limit: *max_rows,
                    suggestions: vec![
                        "Add WHERE clauses to narrow the result set".to_string(),
                        "Reduce ?expand depth (deep JOINs multiply cost)".to_string(),
                        "Use ?limit and ?offset for pagination".to_string(),
                        "Add indexes on frequently filtered columns".to_string(),
                    ],
                })
            }
            _ => None,
        }
    }
}

/// Structured rejection detail for EXPLAIN cost guard violations.
#[derive(Debug, Clone)]
pub struct ExplainRejectionDetail {
    /// PostgreSQL's estimated total cost for the query.
    pub estimated_cost: f64,
    /// Configured maximum cost threshold.
    pub cost_limit: f64,
    /// PostgreSQL's estimated row count.
    pub estimated_rows: u64,
    /// Configured maximum row threshold.
    pub row_limit: u64,
    /// Actionable suggestions to bring the query under limits.
    pub suggestions: Vec<String>,
}

/// Check an EXPLAIN estimate against configured thresholds.
pub fn check_estimate(estimate: &ExplainEstimate, config: &ExplainConfig) -> ExplainDecision {
    if estimate.total_cost > config.max_total_cost || estimate.plan_rows > config.max_plan_rows {
        ExplainDecision::Reject {
            total_cost: estimate.total_cost,
            plan_rows: estimate.plan_rows,
            max_cost: config.max_total_cost,
            max_rows: config.max_plan_rows,
        }
    } else {
        ExplainDecision::Allow
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_explain_json() {
        let json = r#"[{"Plan": {"Node Type": "Seq Scan", "Total Cost": 1234.56, "Plan Rows": 5000, "Plan Width": 100}}]"#;
        let est = parse_explain_json(json).unwrap();
        assert!((est.total_cost - 1234.56).abs() < 0.01);
        assert_eq!(est.plan_rows, 5000);
    }

    #[test]
    fn test_parse_explain_json_nested_join() {
        let json = r#"[{"Plan": {"Node Type": "Hash Join", "Total Cost": 250000.0, "Plan Rows": 2000000, "Plan Width": 200}}]"#;
        let est = parse_explain_json(json).unwrap();
        assert!((est.total_cost - 250000.0).abs() < 0.01);
        assert_eq!(est.plan_rows, 2_000_000);
    }

    #[test]
    fn test_parse_explain_json_invalid() {
        assert!(parse_explain_json("not json").is_none());
        assert!(parse_explain_json("{}").is_none());
        assert!(parse_explain_json("[]").is_none());
    }

    #[test]
    fn test_check_estimate_allow() {
        let config = ExplainConfig::default();
        let est = ExplainEstimate { total_cost: 100.0, plan_rows: 500 };
        let decision = check_estimate(&est, &config);
        assert!(!decision.is_rejected());
    }

    #[test]
    fn test_check_estimate_reject_cost() {
        let config = ExplainConfig::default();
        let est = ExplainEstimate { total_cost: 200_000.0, plan_rows: 500 };
        let decision = check_estimate(&est, &config);
        assert!(decision.is_rejected());
        assert!(decision.rejection_message().unwrap().contains("200000"));
    }

    #[test]
    fn test_check_estimate_reject_rows() {
        let config = ExplainConfig::default();
        let est = ExplainEstimate { total_cost: 50.0, plan_rows: 5_000_000 };
        let decision = check_estimate(&est, &config);
        assert!(decision.is_rejected());
    }

    #[test]
    fn test_cache_basic() {
        let cache = ExplainCache::new(Duration::from_secs(60));
        assert!(cache.is_empty());

        cache.insert(42, ExplainEstimate { total_cost: 100.0, plan_rows: 50 });
        assert_eq!(cache.len(), 1);

        let cached = cache.get(42, None).unwrap();
        assert!((cached.total_cost - 100.0).abs() < 0.01);
        assert_eq!(cached.plan_rows, 50);

        // Miss for unknown key
        assert!(cache.get(99, None).is_none());
    }

    #[test]
    fn test_cache_expiry() {
        let cache = ExplainCache::new(Duration::from_millis(1));
        cache.insert(1, ExplainEstimate { total_cost: 100.0, plan_rows: 50 });

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(5));
        assert!(cache.get(1, None).is_none());
    }

    #[test]
    fn test_cache_drift_invalidation() {
        let cache = ExplainCache::new(Duration::from_secs(60));

        // ── Small dataset: relative drift alone should NOT invalidate ──
        cache.insert(1, ExplainEstimate { total_cost: 50.0, plan_rows: 1000 });

        // No reltuples — pure TTL, should hit
        assert!(cache.get(1, None).is_some());

        // Same estimate — no drift, should hit
        assert!(cache.get(1, Some(1000)).is_some());

        // 60% relative drift but only 600 absolute — below 10k floor, should STILL hit
        assert!(cache.get(1, Some(1600)).is_some(), "small table should not thrash");

        // 60% shrinkage but only 600 absolute — should STILL hit
        assert!(cache.get(1, Some(400)).is_some(), "small shrinkage should not thrash");

        // ── Large dataset: BOTH relative AND absolute thresholds exceeded ──
        cache.insert(3, ExplainEstimate { total_cost: 500.0, plan_rows: 50_000 });

        // 70% drift + 35k absolute (both above threshold) — should miss
        assert!(cache.get(3, Some(85_000)).is_none(), "large drift should invalidate");

        // 40% drift + 20k absolute (relative below 50%) — should STILL hit
        assert!(cache.get(3, Some(70_000)).is_some(), "moderate drift should not invalidate");

        // 60% shrinkage + 30k absolute (both above threshold) — should miss
        assert!(cache.get(3, Some(20_000)).is_none(), "large shrinkage should invalidate");

        // Edge: plan_rows = 0 in cache — skip drift check entirely
        cache.insert(2, ExplainEstimate { total_cost: 10.0, plan_rows: 0 });
        assert!(cache.get(2, Some(999_999)).is_some());
    }

    #[test]
    fn test_explain_mode_default() {
        let config = ExplainConfig::default();
        assert_eq!(config.mode, ExplainMode::Precheck);
        assert_eq!(config.depth_threshold, 3);
        assert!((config.max_total_cost - 100_000.0).abs() < 0.01);
        assert_eq!(config.max_plan_rows, 1_000_000);
    }
}
