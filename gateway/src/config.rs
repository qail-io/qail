//! Gateway configuration

use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

/// Main gateway configuration
#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    /// Database connection URL
    pub database_url: String,
    
    /// Path to schema file (optional)
    pub schema_path: Option<String>,
    
    /// Path to policies file (optional)
    pub policy_path: Option<String>,
    
    /// Server bind address
    pub bind_address: String,
    
    /// Enable CORS
    #[serde(default = "default_true")]
    pub cors_enabled: bool,

    /// Allowed CORS origins. Empty = allow all (backward compatible).
    /// Example: ["https://app.qail.io", "https://staging.qail.io"]
    #[serde(default)]
    pub cors_allowed_origins: Vec<String>,

    /// SECURITY (M1): When true, reject startup if `cors_allowed_origins` is empty.
    /// Forces explicit origin allowlist for production deployments.
    #[serde(default)]
    pub cors_strict: bool,

    /// SECURITY (E7): Root directory for config files (schema, policy, events).
    /// Paths outside this root are rejected. Default: current working directory.
    #[serde(default)]
    pub config_root: Option<String>,

    /// SECURITY (M4): Optional bearer token to protect internal endpoints
    /// (`/metrics`, `/health/internal`). When set, requests must include
    /// `Authorization: Bearer <admin_token>`.
    #[serde(default)]
    pub admin_token: Option<String>,
    
    /// Enable query caching
    #[serde(default = "default_true")]
    pub cache_enabled: bool,
    
    /// Maximum cache entries
    #[serde(default = "default_cache_max")]
    pub cache_max_entries: usize,
    
    /// Cache TTL in seconds
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_seconds: u64,
    
    /// Path to event triggers config file (optional)
    pub events_path: Option<String>,

    /// Rate limiter: requests per second per IP
    #[serde(default = "default_rate_limit_rate")]
    pub rate_limit_rate: f64,

    /// Rate limiter: maximum burst capacity
    #[serde(default = "default_rate_limit_burst")]
    pub rate_limit_burst: u32,

    /// Maximum number of `?expand=` relations per request (default: 4).
    /// Prevents query explosion from unbounded LEFT JOINs.
    #[serde(default = "default_max_expand_depth")]
    pub max_expand_depth: usize,

    /// Statement timeout in milliseconds (default: 30000 = 30s).
    /// Applied to every RLS-scoped connection. Prevents runaway queries.
    #[serde(default = "default_statement_timeout_ms")]
    pub statement_timeout_ms: u32,

    /// Maximum rows returned per query (default: 10000).
    /// A guardrail against accidental full table scans.
    #[serde(default = "default_max_result_rows")]
    pub max_result_rows: usize,

    /// EXPLAIN pre-check mode: "off", "precheck" (default), or "enforce".
    #[serde(default = "default_explain_mode")]
    pub explain_mode: String,

    /// EXPLAIN: reject if estimated cost exceeds this (default: 100,000).
    #[serde(default = "default_explain_max_cost")]
    pub explain_max_cost: f64,

    /// EXPLAIN: reject if estimated rows exceed this (default: 1,000,000).
    #[serde(default = "default_explain_max_rows")]
    pub explain_max_rows: u64,

    /// EXPLAIN: only pre-check queries with expand depth ≥ this (default: 3).
    #[serde(default = "default_explain_depth_threshold")]
    pub explain_depth_threshold: usize,

    /// EXPLAIN cache TTL in seconds (default: 300 = 5 min).
    #[serde(default = "default_explain_cache_ttl")]
    pub explain_cache_ttl_secs: u64,

    /// Maximum concurrent queries per tenant (default: 10).
    /// Prevents a single tenant from monopolising the connection pool.
    #[serde(default = "default_max_concurrent_queries")]
    pub max_concurrent_queries: usize,

    /// Maximum tracked tenants in the concurrency limiter (default: 10,000).
    /// Prevents memory exhaustion from forged tenant IDs.
    #[serde(default = "default_max_tenants")]
    pub max_tenants: usize,

    /// Idle timeout for tenant semaphore entries in seconds (default: 300).
    /// Entries unused for this long are evicted by the background sweeper.
    #[serde(default = "default_tenant_idle_timeout")]
    pub tenant_idle_timeout_secs: u64,

    /// Maximum queries per batch request (default: 100).
    /// Prevents resource exhaustion from oversized /batch payloads.
    #[serde(default = "default_max_batch_queries")]
    pub max_batch_queries: usize,

    /// Query complexity guard: maximum nesting depth (CTEs + set ops + source queries).
    /// Default: 5.
    #[serde(default = "default_max_query_depth")]
    pub max_query_depth: usize,

    /// Query complexity guard: maximum number of filter conditions. Default: 20.
    #[serde(default = "default_max_query_filters")]
    pub max_query_filters: usize,

    /// Query complexity guard: maximum number of JOIN operations. Default: 10.
    #[serde(default = "default_max_query_joins")]
    pub max_query_joins: usize,

    /// Optional Qdrant configuration for vector operations.
    #[serde(default)]
    pub qdrant: Option<qail_core::config::QdrantConfig>,

    /// Tenant boundary column name (default: "operator_id").
    /// Tables using a different partition key (e.g., "tenant_id") can override this.
    #[serde(default = "default_tenant_column")]
    pub tenant_column: String,

    /// Path to query allow-list file (one pattern per line). Optional.
    /// When set, only pre-approved query patterns are executed.
    #[serde(default)]
    pub allow_list_path: Option<String>,

    /// Per-role guard overrides. Roles not listed use global defaults.
    ///
    /// Example TOML:
    /// ```toml
    /// [gateway.overrides.reporting]
    /// max_result_rows = 100000
    /// statement_timeout_ms = 120000
    /// ```
    #[serde(default)]
    pub role_overrides: HashMap<String, GuardOverrides>,
}

/// Per-role limit overrides. All fields optional — omitted fields
/// fall back to the global default.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct GuardOverrides {
    pub max_result_rows: Option<usize>,
    pub statement_timeout_ms: Option<u32>,
    pub explain_max_cost: Option<f64>,
    pub explain_max_rows: Option<u64>,
    pub max_expand_depth: Option<usize>,
}

/// Resolved limits for a specific request, after applying role overrides.
#[derive(Debug, Clone)]
pub struct EffectiveLimits {
    pub max_result_rows: usize,
    pub statement_timeout_ms: u32,
    pub explain_max_cost: f64,
    pub explain_max_rows: u64,
    pub max_expand_depth: usize,
}

fn default_true() -> bool { true }
fn default_cache_max() -> usize { 1000 }
fn default_cache_ttl() -> u64 { 60 }
fn default_rate_limit_rate() -> f64 { 100.0 }
fn default_rate_limit_burst() -> u32 { 200 }
fn default_max_expand_depth() -> usize { 4 }
fn default_statement_timeout_ms() -> u32 { 30_000 }
fn default_max_result_rows() -> usize { 10_000 }
fn default_explain_mode() -> String { "precheck".to_string() }
fn default_explain_max_cost() -> f64 { 100_000.0 }
fn default_explain_max_rows() -> u64 { 1_000_000 }
fn default_explain_depth_threshold() -> usize { 3 }
fn default_explain_cache_ttl() -> u64 { 300 }
fn default_max_concurrent_queries() -> usize { 10 }
fn default_max_tenants() -> usize { 10_000 }
fn default_tenant_idle_timeout() -> u64 { 300 }
fn default_max_batch_queries() -> usize { 100 }
fn default_max_query_depth() -> usize { 5 }
fn default_max_query_filters() -> usize { 20 }
fn default_max_query_joins() -> usize { 10 }
fn default_tenant_column() -> String { "operator_id".to_string() }

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost/qail".to_string(),
            schema_path: None,
            policy_path: None,
            bind_address: "0.0.0.0:8080".to_string(),
            cors_enabled: true,
            cors_allowed_origins: Vec::new(),
            cors_strict: false,
            config_root: None,
            admin_token: None,
            cache_enabled: true,
            cache_max_entries: 1000,
            cache_ttl_seconds: 60,
            events_path: None,
            rate_limit_rate: 100.0,
            rate_limit_burst: 200,
            max_expand_depth: 4,
            statement_timeout_ms: 30_000,
            max_result_rows: 10_000,
            explain_mode: "precheck".to_string(),
            explain_max_cost: 100_000.0,
            explain_max_rows: 1_000_000,
            explain_depth_threshold: 3,
            explain_cache_ttl_secs: 300,
            max_concurrent_queries: 10,
            max_tenants: 10_000,
            tenant_idle_timeout_secs: 300,
            max_batch_queries: 100,
            max_query_depth: 5,
            max_query_filters: 20,
            max_query_joins: 10,
            qdrant: None,
            tenant_column: "operator_id".to_string(),
            allow_list_path: None,
            role_overrides: HashMap::new(),
        }
    }
}

/// SECURITY (E7): Validate that a config file path does not escape the allowed root.
///
/// Canonicalizes the path (resolves `..`, symlinks) and verifies it starts with
/// the `config_root`. Returns the canonicalized path on success.
pub fn validate_config_path(
    path: &str,
    config_root: Option<&str>,
) -> Result<std::path::PathBuf, String> {
    let canonical = std::path::Path::new(path)
        .canonicalize()
        .map_err(|e| format!("Config path '{}' cannot be resolved: {}", path, e))?;

    if let Some(root) = config_root {
        let root_canonical = std::path::Path::new(root)
            .canonicalize()
            .map_err(|e| format!("Config root '{}' cannot be resolved: {}", root, e))?;

        if !canonical.starts_with(&root_canonical) {
            return Err(format!(
                "Config path '{}' escapes config_root '{}' (resolved to '{}')",
                path,
                root,
                canonical.display()
            ));
        }
    }

    Ok(canonical)
}

impl GatewayConfig {
    /// Get cache configuration
    pub fn cache_config(&self) -> crate::cache::CacheConfig {
        crate::cache::CacheConfig {
            enabled: self.cache_enabled,
            max_entries: self.cache_max_entries,
            ttl: Duration::from_secs(self.cache_ttl_seconds),
        }
    }

    /// Build EXPLAIN pre-check config from gateway settings.
    pub fn explain_config(&self) -> qail_pg::explain::ExplainConfig {
        use qail_pg::explain::{ExplainConfig, ExplainMode};

        let mode = match self.explain_mode.as_str() {
            "off" => ExplainMode::Off,
            "enforce" => ExplainMode::Enforce,
            _ => ExplainMode::Precheck,
        };

        ExplainConfig {
            mode,
            depth_threshold: self.explain_depth_threshold,
            max_total_cost: self.explain_max_cost,
            max_plan_rows: self.explain_max_rows,
            cache_ttl: Duration::from_secs(self.explain_cache_ttl_secs),
        }
    }

    /// Resolve effective limits for a given user role.
    ///
    /// Checks `role_overrides` for the role; any unset fields fall back
    /// to the global defaults. Returns a flat `EffectiveLimits` struct
    /// that handlers can use without further lookups.
    pub fn effective_limits(&self, role: &str) -> EffectiveLimits {
        let overrides = self.role_overrides.get(role);
        EffectiveLimits {
            max_result_rows: overrides
                .and_then(|o| o.max_result_rows)
                .unwrap_or(self.max_result_rows),
            statement_timeout_ms: overrides
                .and_then(|o| o.statement_timeout_ms)
                .unwrap_or(self.statement_timeout_ms),
            explain_max_cost: overrides
                .and_then(|o| o.explain_max_cost)
                .unwrap_or(self.explain_max_cost),
            explain_max_rows: overrides
                .and_then(|o| o.explain_max_rows)
                .unwrap_or(self.explain_max_rows),
            max_expand_depth: overrides
                .and_then(|o| o.max_expand_depth)
                .unwrap_or(self.max_expand_depth),
        }
    }

    /// Create gateway config from centralized `QailConfig`.
    ///
    /// Maps `[postgres]`, `[gateway]`, and `[project]` sections.
    pub fn from_qail_config(qail: &qail_core::config::QailConfig) -> Self {
        let (bind_address, cors_enabled, policy_path, cache_enabled, cache_max_entries, cache_ttl_seconds) =
            if let Some(ref gw) = qail.gateway {
                let (ce, cme, cts) = if let Some(ref cache) = gw.cache {
                    (cache.enabled, cache.max_entries, cache.ttl_secs)
                } else {
                    (true, 1000, 60)
                };
                (gw.bind.clone(), gw.cors, gw.policy.clone(), ce, cme, cts)
            } else {
                ("0.0.0.0:8080".to_string(), true, None, true, 1000, 60)
            };

        Self {
            database_url: qail.postgres.url.clone(),
            schema_path: qail.project.schema.clone(),
            policy_path,
            bind_address,
            cors_enabled,
            cors_allowed_origins: qail.gateway.as_ref()
                .and_then(|gw| gw.cors_allowed_origins.clone())
                .unwrap_or_default(),
            cors_strict: false,
            config_root: None,
            admin_token: None,
            cache_enabled,
            cache_max_entries,
            cache_ttl_seconds,
            events_path: None,
            rate_limit_rate: 100.0,
            rate_limit_burst: 200,
            max_expand_depth: qail.gateway.as_ref()
                .map(|gw| gw.max_expand_depth)
                .unwrap_or(4),
            statement_timeout_ms: 30_000,
            max_result_rows: 10_000,
            explain_mode: "precheck".to_string(),
            explain_max_cost: 100_000.0,
            explain_max_rows: 1_000_000,
            explain_depth_threshold: 3,
            explain_cache_ttl_secs: 300,
            max_concurrent_queries: 10,
            max_tenants: 10_000,
            tenant_idle_timeout_secs: 300,
            max_batch_queries: 100,
            max_query_depth: 5,
            max_query_filters: 20,
            max_query_joins: 10,
            qdrant: qail.qdrant.clone(),
            tenant_column: "operator_id".to_string(),
            allow_list_path: None,
            role_overrides: HashMap::new(),
        }
    }
}

impl GatewayConfig {
    /// Create a new configuration builder
    pub fn builder() -> GatewayConfigBuilder {
        GatewayConfigBuilder::default()
    }
}

/// Builder for GatewayConfig
#[derive(Debug, Default)]
pub struct GatewayConfigBuilder {
    config: GatewayConfig,
}

impl GatewayConfigBuilder {
    /// Set the database URL
    pub fn database(mut self, url: impl Into<String>) -> Self {
        self.config.database_url = url.into();
        self
    }
    
    /// Set the schema path
    pub fn schema(mut self, path: impl Into<String>) -> Self {
        self.config.schema_path = Some(path.into());
        self
    }
    
    /// Set the policy path
    pub fn policy(mut self, path: impl Into<String>) -> Self {
        self.config.policy_path = Some(path.into());
        self
    }
    
    /// Set the bind address
    pub fn bind(mut self, addr: impl Into<String>) -> Self {
        self.config.bind_address = addr.into();
        self
    }
    
    /// Build the configuration
    pub fn build(self) -> GatewayConfig {
        self.config
    }
}
