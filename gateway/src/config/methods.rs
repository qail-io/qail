use super::{EffectiveLimits, GatewayConfig, GatewayConfigBuilder};
use std::collections::HashMap;
use std::time::Duration;

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
            lock_timeout_ms: overrides
                .and_then(|o| o.lock_timeout_ms)
                .unwrap_or(self.lock_timeout_ms),
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
        let (
            bind_address,
            cors_enabled,
            policy_path,
            cache_enabled,
            cache_max_entries,
            cache_ttl_seconds,
        ) = if let Some(ref gw) = qail.gateway {
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
            cors_allowed_origins: qail
                .gateway
                .as_ref()
                .and_then(|gw| gw.cors_allowed_origins.clone())
                .unwrap_or_default(),
            cors_strict: false,
            config_root: None,
            require_auth: true,
            admin_token: None,
            ws_allow_query_token: false,
            cache_enabled,
            cache_max_entries,
            cache_ttl_seconds,
            events_path: None,
            rate_limit_rate: 100.0,
            rate_limit_burst: 200,
            max_expand_depth: qail
                .gateway
                .as_ref()
                .map(|gw| gw.max_expand_depth)
                .unwrap_or(4),
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
            qdrant: qail.qdrant.clone().map(Into::into),
            tenant_column: "tenant_id".to_string(),
            tenant_guard_exempt_tables: Vec::new(),
            allow_list_path: None,
            binary_requires_allow_list: true,
            rpc_require_schema_qualified: true,
            rpc_allowlist_path: None,
            rpc_signature_check: true,
            max_request_body_bytes: 1_048_576,
            role_overrides: HashMap::new(),
            production_strict: false,
            jwt_allowed_algorithms: Vec::new(),
            tenant_rate_limit_rate: 50.0,
            tenant_rate_limit_burst: 100,
            pg_sslmode: "prefer".to_string(),
            pg_channel_binding: "prefer".to_string(),
            blocked_tables: qail
                .gateway
                .as_ref()
                .and_then(|gw| gw.blocked_tables.clone())
                .unwrap_or_default(),
            allowed_tables: qail
                .gateway
                .as_ref()
                .and_then(|gw| gw.allowed_tables.clone())
                .unwrap_or_default(),
            txn_session_timeout_secs: 30,
            txn_max_sessions: 0,
            txn_max_lifetime_secs: 900,
            txn_max_statements_per_session: 1000,
        }
    }
}

impl GatewayConfig {
    /// Create a new configuration builder
    pub fn builder() -> GatewayConfigBuilder {
        GatewayConfigBuilder::default()
    }
}
