use super::GatewayConfig;
use std::collections::HashMap;

pub(super) fn default_true() -> bool {
    true
}

pub(super) fn default_cache_max() -> usize {
    1000
}

pub(super) fn default_cache_ttl() -> u64 {
    60
}

pub(super) fn default_rate_limit_rate() -> f64 {
    100.0
}

pub(super) fn default_rate_limit_burst() -> u32 {
    200
}

pub(super) fn default_max_expand_depth() -> usize {
    4
}

pub(super) fn default_statement_timeout_ms() -> u32 {
    30_000
}

pub(super) fn default_lock_timeout_ms() -> u32 {
    5_000
}

pub(super) fn default_max_request_body_bytes() -> usize {
    1_048_576
}

pub(super) fn default_max_result_rows() -> usize {
    10_000
}

pub(super) fn default_explain_mode() -> String {
    "precheck".to_string()
}

pub(super) fn default_explain_max_cost() -> f64 {
    100_000.0
}

pub(super) fn default_explain_max_rows() -> u64 {
    1_000_000
}

pub(super) fn default_explain_depth_threshold() -> usize {
    3
}

pub(super) fn default_explain_cache_ttl() -> u64 {
    300
}

pub(super) fn default_max_concurrent_queries() -> usize {
    10
}

pub(super) fn default_max_tenants() -> usize {
    10_000
}

pub(super) fn default_db_max_waiters_global() -> usize {
    2048
}

pub(super) fn default_db_max_waiters_per_tenant() -> usize {
    64
}

pub(super) fn default_tenant_idle_timeout() -> u64 {
    300
}

pub(super) fn default_max_batch_queries() -> usize {
    100
}

pub(super) fn default_max_query_depth() -> usize {
    5
}

pub(super) fn default_max_query_filters() -> usize {
    20
}

pub(super) fn default_max_query_joins() -> usize {
    10
}

pub(super) fn default_tenant_column() -> String {
    "operator_id".to_string()
}

pub(super) fn default_tenant_rate_limit_rate() -> f64 {
    50.0
}

pub(super) fn default_tenant_rate_limit_burst() -> u32 {
    100
}

pub(super) fn default_pg_sslmode() -> String {
    "prefer".to_string()
}

pub(super) fn default_pg_channel_binding() -> String {
    "prefer".to_string()
}

pub(super) fn default_txn_session_timeout() -> u64 {
    30
}

pub(super) fn default_txn_max_lifetime_secs() -> u64 {
    900
}

pub(super) fn default_txn_max_statements_per_session() -> usize {
    1000
}

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
            lock_timeout_ms: 5_000,
            max_result_rows: 10_000,
            explain_mode: "precheck".to_string(),
            explain_max_cost: 100_000.0,
            explain_max_rows: 1_000_000,
            explain_depth_threshold: 3,
            explain_cache_ttl_secs: 300,
            max_concurrent_queries: 10,
            max_tenants: 10_000,
            db_max_waiters_global: 2048,
            db_max_waiters_per_tenant: 64,
            tenant_idle_timeout_secs: 300,
            max_batch_queries: 100,
            max_query_depth: 5,
            max_query_filters: 20,
            max_query_joins: 10,
            qdrant: None,
            tenant_column: "operator_id".to_string(),
            tenant_guard_exempt_tables: Vec::new(),
            allow_list_path: None,
            binary_requires_allow_list: true,
            rpc_require_schema_qualified: false,
            rpc_allowlist_path: None,
            rpc_signature_check: false,
            max_request_body_bytes: 1_048_576,
            role_overrides: HashMap::new(),
            production_strict: false,
            jwt_allowed_algorithms: Vec::new(),
            tenant_rate_limit_rate: 50.0,
            tenant_rate_limit_burst: 100,
            pg_sslmode: "prefer".to_string(),
            pg_channel_binding: "prefer".to_string(),
            blocked_tables: Vec::new(),
            allowed_tables: Vec::new(),
            txn_session_timeout_secs: 30,
            txn_max_sessions: 0,
            txn_max_lifetime_secs: 900,
            txn_max_statements_per_session: 1000,
        }
    }
}
