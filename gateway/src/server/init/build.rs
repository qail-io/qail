use std::sync::Arc;

use super::super::database_url::load_rpc_allow_list;
use super::helpers::{
    build_pool, load_event_engine, load_policy_engine, load_schema_registry,
    load_user_operator_map, verify_schema_drift,
};
use super::{Gateway, StartupState};
use crate::error::GatewayError;
use crate::middleware::RateLimiter;

impl Gateway {
    pub(crate) async fn build_startup_state(&self) -> Result<StartupState, GatewayError> {
        let policy_engine = load_policy_engine(&self.config)?;
        let schema = load_schema_registry(&self.config)?;

        let cache_config = self.config.cache_config();
        tracing::info!(
            "Query cache: enabled={}, max_entries={}, ttl={}s",
            cache_config.enabled,
            cache_config.max_entries,
            cache_config.ttl.as_secs()
        );
        let cache = crate::cache::QueryCache::new(cache_config);

        let pool = build_pool(&self.config).await?;
        verify_schema_drift(
            &pool,
            &schema,
            self.config.statement_timeout_ms,
            self.config.lock_timeout_ms,
        )
        .await?;

        let event_engine = load_event_engine(&self.config)?;

        let rate_limiter =
            RateLimiter::new(self.config.rate_limit_rate, self.config.rate_limit_burst);
        tracing::info!(
            "Rate limiter: {:.0} req/s, burst={}",
            self.config.rate_limit_rate,
            self.config.rate_limit_burst
        );

        let tenant_rate_limiter = RateLimiter::new(
            self.config.tenant_rate_limit_rate,
            self.config.tenant_rate_limit_burst,
        );
        tracing::info!(
            "Tenant rate limiter: {:.0} req/s/tenant, burst={}",
            self.config.tenant_rate_limit_rate,
            self.config.tenant_rate_limit_burst
        );

        let explain_config = self.config.explain_config();
        let explain_cache = qail_pg::explain::ExplainCache::new(explain_config.cache_ttl);
        tracing::info!(
            "EXPLAIN pre-check: mode={:?}, depth_threshold={}, max_cost={:.0}, max_rows={}",
            explain_config.mode,
            explain_config.depth_threshold,
            explain_config.max_total_cost,
            explain_config.max_plan_rows
        );

        let tenant_semaphore = Arc::new(crate::concurrency::TenantSemaphore::with_limits(
            self.config.max_concurrent_queries,
            self.config.max_tenants,
            std::time::Duration::from_secs(self.config.tenant_idle_timeout_secs),
        ));
        tenant_semaphore.start_sweeper();
        tracing::info!(
            "Tenant concurrency: {} queries/tenant, max {} tenants, {}s idle timeout",
            self.config.max_concurrent_queries,
            self.config.max_tenants,
            self.config.tenant_idle_timeout_secs
        );

        let db_backpressure = Arc::new(crate::db_backpressure::DbBackpressure::new(
            self.config.db_max_waiters_global,
            self.config.db_max_waiters_per_tenant,
            self.config.max_tenants,
        ));
        tracing::info!(
            "DB acquire backpressure: global_waiters={}, per_tenant_waiters={}, tracked_tenants_cap={}",
            self.config.db_max_waiters_global,
            self.config.db_max_waiters_per_tenant,
            self.config.max_tenants
        );

        let user_operator_map = load_user_operator_map(
            &pool,
            "startup_user_map",
            self.config.statement_timeout_ms,
            self.config.lock_timeout_ms,
        )
        .await?;

        #[cfg(feature = "qdrant")]
        let qdrant_pool = if let Some(ref qdrant_config) = self.config.qdrant {
            let pool_config = qail_qdrant::PoolConfig::from_qail_config_ref(qdrant_config);
            let pool = qail_qdrant::QdrantPool::new(pool_config)
                .await
                .map_err(|e| GatewayError::Database(format!("Qdrant pool init failed: {}", e)))?;
            tracing::info!(
                "Qdrant pool: max {} connections, tls={}, host={}",
                pool.max_connections(),
                qdrant_config.tls.unwrap_or(false),
                qdrant_config.grpc.as_deref().unwrap_or(&qdrant_config.url),
            );
            Some(pool)
        } else {
            tracing::info!("Qdrant: not configured (no [qdrant] section)");
            None
        };

        let prometheus_handle = Arc::new(crate::metrics::init_metrics());

        let complexity_guard = crate::middleware::QueryComplexityGuard::new(
            self.config.max_query_depth,
            self.config.max_query_filters,
            self.config.max_query_joins,
        );
        tracing::info!(
            "Complexity guard: max_depth={}, max_filters={}, max_joins={}",
            self.config.max_query_depth,
            self.config.max_query_filters,
            self.config.max_query_joins
        );

        let mut allow_list = crate::middleware::QueryAllowList::new();
        if let Some(ref path) = self.config.allow_list_path {
            let canonical =
                crate::config::validate_config_path(path, self.config.config_root.as_deref())
                    .map_err(GatewayError::Config)?;
            allow_list
                .load_from_file(canonical.to_str().unwrap_or(path))
                .map_err(|e| {
                    GatewayError::Config(format!(
                        "Failed to load allow-list from '{}': {}",
                        path, e
                    ))
                })?;
            tracing::info!(
                "Query allow-list: {} patterns loaded from '{}'",
                allow_list.len(),
                path
            );
        }

        let rpc_allow_list = if let Some(ref path) = self.config.rpc_allowlist_path {
            let canonical =
                crate::config::validate_config_path(path, self.config.config_root.as_deref())
                    .map_err(GatewayError::Config)?;
            let set = load_rpc_allow_list(&canonical)?;
            tracing::info!(
                "RPC allow-list: {} entries loaded from '{}'",
                set.len(),
                canonical.display()
            );
            Some(set)
        } else {
            None
        };

        tracing::info!("Tenant column: '{}'", self.config.tenant_column);

        let jwks_store = if let Some(store) = crate::jwks::JwksKeyStore::from_env() {
            match store.initial_fetch().await {
                Ok(n) => {
                    tracing::info!("JWKS: loaded {} keys from endpoint", n);
                    store.start_refresh_task();
                    Some(store)
                }
                Err(e) => {
                    tracing::warn!("JWKS: initial fetch failed (non-fatal): {}", e);
                    Some(store)
                }
            }
        } else {
            None
        };

        let jwt_allowed_algorithms =
            crate::auth::parse_allowed_algorithms(&self.config.jwt_allowed_algorithms)?;

        let txn_max_sessions = if self.config.txn_max_sessions > 0 {
            self.config.txn_max_sessions
        } else {
            std::cmp::max(pool.max_connections() / 2, 2)
        };
        let transaction_manager = Arc::new(crate::transaction::TransactionSessionManager::new(
            txn_max_sessions,
            self.config.txn_session_timeout_secs,
            self.config.txn_max_lifetime_secs,
            self.config.txn_max_statements_per_session,
        ));
        tracing::info!(
            "Transaction sessions: max_sessions={}, idle_timeout={}s, max_lifetime={}s, max_statements={}",
            txn_max_sessions,
            self.config.txn_session_timeout_secs,
            self.config.txn_max_lifetime_secs,
            self.config.txn_max_statements_per_session
        );
        crate::transaction::spawn_reaper(Arc::clone(&transaction_manager));

        Ok(StartupState {
            policy_engine,
            event_engine,
            schema,
            cache,
            pool,
            rate_limiter,
            tenant_rate_limiter,
            explain_cache,
            explain_config,
            tenant_semaphore,
            db_backpressure,
            user_operator_map,
            #[cfg(feature = "qdrant")]
            qdrant_pool,
            prometheus_handle,
            complexity_guard,
            allow_list,
            rpc_allow_list,
            jwks_store,
            jwt_allowed_algorithms,
            blocked_tables: self.config.blocked_tables.iter().cloned().collect(),
            allowed_tables: self.config.allowed_tables.iter().cloned().collect(),
            transaction_manager,
        })
    }
}
