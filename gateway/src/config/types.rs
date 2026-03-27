use serde::Deserialize;
use std::collections::HashMap;

use super::defaults::*;

#[derive(Debug, Clone, Deserialize)]
pub struct GatewayQdrantConfig {
    #[serde(default = "default_qdrant_url")]
    pub url: String,
    pub grpc: Option<String>,
    #[serde(default = "default_qdrant_max_connections")]
    pub max_connections: usize,
    #[serde(default)]
    pub tls: Option<bool>,
}

impl GatewayQdrantConfig {
    pub fn to_core_config(&self) -> qail_core::config::QdrantConfig {
        qail_core::config::QdrantConfig {
            url: self.url.clone(),
            grpc: self.grpc.clone(),
            max_connections: self.max_connections,
            tls: self.tls,
        }
    }
}

impl From<qail_core::config::QdrantConfig> for GatewayQdrantConfig {
    fn from(value: qail_core::config::QdrantConfig) -> Self {
        Self {
            url: value.url,
            grpc: value.grpc,
            max_connections: value.max_connections,
            tls: value.tls,
        }
    }
}

fn default_qdrant_url() -> String {
    "http://localhost:6333".to_string()
}

fn default_qdrant_max_connections() -> usize {
    10
}

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
    /// Example: `["https://app.qail.io", "https://staging.qail.io"]`
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

    /// SECURITY: Require authentication for all request paths by default.
    /// Set to `false` only for explicitly public/demo deployments.
    #[serde(default = "default_require_auth")]
    pub require_auth: bool,

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

    /// Lock timeout in milliseconds (default: 5000 = 5s).
    /// Prevents queries from waiting indefinitely for row/table locks.
    #[serde(default = "default_lock_timeout_ms")]
    pub lock_timeout_ms: u32,

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

    /// Maximum global number of requests allowed to wait for a DB connection.
    /// Requests above this cap are shed immediately with 503.
    /// Default: 2048.
    #[serde(default = "default_db_max_waiters_global")]
    pub db_max_waiters_global: usize,

    /// Maximum number of waiting DB acquires allowed per tenant+user key.
    /// Requests above this cap are shed immediately with 503.
    /// Default: 64.
    #[serde(default = "default_db_max_waiters_per_tenant")]
    pub db_max_waiters_per_tenant: usize,

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
    pub qdrant: Option<GatewayQdrantConfig>,

    /// Tenant boundary column name (default: "tenant_id").
    #[serde(default = "default_tenant_column")]
    pub tenant_column: String,

    /// Tables exempt from the tenant boundary guard.
    /// Cross-tenant by design — e.g., resellers see other tenants' pricing
    /// via active contracts. RLS still applies; the guard simply won't treat
    /// cross-tenant rows as a violation for these tables.
    #[serde(default)]
    pub tenant_guard_exempt_tables: Vec<String>,

    /// Path to query allow-list file (one pattern per line). Optional.
    /// When set, only pre-approved query patterns are executed.
    #[serde(default)]
    pub allow_list_path: Option<String>,

    /// SECURITY: Require the query allow-list to be enabled for `/qail/binary` endpoint.
    /// When true (default), binary AST requests are rejected unless an allow-list is loaded.
    /// This prevents untrusted binary AST from bypassing query restrictions.
    #[serde(default = "default_true")]
    pub binary_requires_allow_list: bool,

    /// Require schema-qualified RPC function names (`schema.function`).
    #[serde(default)]
    pub rpc_require_schema_qualified: bool,

    /// Path to RPC allow-list file (one function per line). Optional.
    /// Entries are matched case-insensitively against normalized function names.
    #[serde(default)]
    pub rpc_allowlist_path: Option<String>,

    /// Validate named RPC args against PostgreSQL function signatures.
    /// Requires schema-qualified function names.
    #[serde(default)]
    pub rpc_signature_check: bool,

    /// Maximum request body size in bytes (default: 1MB).
    /// Rejects payloads exceeding this limit with 413 Payload Too Large.
    #[serde(default = "default_max_request_body_bytes")]
    pub max_request_body_bytes: usize,

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

    /// SECURITY: Enforce fail-closed production checks at startup.
    /// Refuses boot unless JWT/JWKS, explicit CORS origins, admin token,
    /// and query/RPC allow-lists are all configured.
    #[serde(default)]
    pub production_strict: bool,

    /// Allowed JWT algorithms for token validation.
    /// Empty = auto-detect from token header (legacy/dev mode).
    /// Recommended production: `["RS256"]` or `["ES256"]`.
    #[serde(default)]
    pub jwt_allowed_algorithms: Vec<String>,

    /// Post-auth tenant rate limiter: requests per second per tenant+user.
    #[serde(default = "default_tenant_rate_limit_rate")]
    pub tenant_rate_limit_rate: f64,

    /// Post-auth tenant rate limiter: maximum burst capacity.
    #[serde(default = "default_tenant_rate_limit_burst")]
    pub tenant_rate_limit_burst: u32,

    /// PostgreSQL TLS mode: "disable", "prefer", "require" (default: "prefer").
    /// Applied to the connection pool. URL `?sslmode=` overrides this.
    #[serde(default = "default_pg_sslmode")]
    pub pg_sslmode: String,

    /// PostgreSQL SCRAM channel binding: "disable", "prefer", "require" (default: "prefer").
    /// Applied to the connection pool. URL `?channel_binding=` overrides this.
    #[serde(default = "default_pg_channel_binding")]
    pub pg_channel_binding: String,

    /// Tables to block from auto-REST endpoint generation.
    /// Blocked tables will not have any CRUD routes, cannot be referenced
    /// via `?expand=`, and cannot appear as nested route targets.
    /// Use this to hide sensitive tables (e.g., `users`) from the HTTP API.
    #[serde(default)]
    pub blocked_tables: Vec<String>,

    /// Tables to allow for auto-REST endpoint generation (whitelist mode).
    /// When set, ONLY these tables are exposed — all others are blocked.
    /// Takes precedence over `blocked_tables`.
    #[serde(default)]
    pub allowed_tables: Vec<String>,

    /// Transaction session idle timeout in seconds (default: 30).
    /// Sessions idle beyond this are rolled back and released.
    #[serde(default = "default_txn_session_timeout")]
    pub txn_session_timeout_secs: u64,

    /// Maximum concurrent transaction sessions (default: 0 = pool_size / 2).
    /// Prevents transaction starvation of the connection pool.
    #[serde(default)]
    pub txn_max_sessions: usize,

    /// Maximum wall-clock lifetime of a transaction session in seconds.
    /// Sessions older than this are terminated on next use.
    /// Default: 900 (15 minutes).
    #[serde(default = "default_txn_max_lifetime_secs")]
    pub txn_max_lifetime_secs: u64,

    /// Maximum number of statements allowed per transaction session.
    /// Includes `/txn/query` and `/txn/savepoint`.
    /// Default: 1000.
    #[serde(default = "default_txn_max_statements_per_session")]
    pub txn_max_statements_per_session: usize,
}

/// Per-role limit overrides. All fields optional — omitted fields
/// fall back to the global default.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct GuardOverrides {
    /// Override for `max_result_rows`.
    pub max_result_rows: Option<usize>,
    /// Override for `statement_timeout_ms`.
    pub statement_timeout_ms: Option<u32>,
    /// Override for `lock_timeout_ms`.
    pub lock_timeout_ms: Option<u32>,
    /// Override for `explain_max_cost`.
    pub explain_max_cost: Option<f64>,
    /// Override for `explain_max_rows`.
    pub explain_max_rows: Option<u64>,
    /// Override for `max_expand_depth`.
    pub max_expand_depth: Option<usize>,
}

/// Resolved limits for a specific request, after applying role overrides.
#[derive(Debug, Clone)]
pub struct EffectiveLimits {
    /// Maximum rows returned per query.
    pub max_result_rows: usize,
    /// Statement timeout in milliseconds.
    pub statement_timeout_ms: u32,
    /// Lock timeout in milliseconds.
    pub lock_timeout_ms: u32,
    /// EXPLAIN max cost threshold.
    pub explain_max_cost: f64,
    /// EXPLAIN max row estimate threshold.
    pub explain_max_rows: u64,
    /// Maximum expand (JOIN) depth.
    pub max_expand_depth: usize,
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
