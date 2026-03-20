use std::collections::HashSet;
use std::sync::Arc;

use super::{GatewayError, GatewayState};
use crate::cache::QueryCache;
use crate::config::GatewayConfig;
use crate::middleware::RateLimiter;
use qail_pg::PgPool;

mod helpers;

impl GatewayState {
    /// Create a `GatewayState` with an externally-provided pool (embedded mode).
    ///
    /// Use this when embedding the gateway router inside another Axum application
    /// (e.g., workers) that already has its own `PgPool`. The gateway will share
    /// the provided pool instead of creating a new one.
    ///
    /// This skips dev-mode safety and production-strict checks — the host app
    /// is responsible for those.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let pool = PgPool::connect(config).await?;
    /// let gw_config = GatewayConfig::default();
    /// let gw_state = GatewayState::new_embedded(pool, gw_config).await?;
    /// let gw_router = qail_gateway::create_router(Arc::new(gw_state), &[]);
    /// ```
    pub async fn new_embedded(pool: PgPool, config: GatewayConfig) -> Result<Self, GatewayError> {
        tracing::info!("Initializing QAIL Gateway (embedded mode)...");

        let policy_engine = helpers::load_policy_engine(&config)?;
        let schema = helpers::load_schema_registry(&config)?;
        helpers::verify_schema_drift(
            &pool,
            &schema,
            config.statement_timeout_ms,
            config.lock_timeout_ms,
        )
        .await?;

        let cache = QueryCache::new(config.cache_config());
        let event_engine = helpers::load_event_engine(&config)?;

        let rate_limiter = RateLimiter::new(config.rate_limit_rate, config.rate_limit_burst);
        let tenant_rate_limiter = RateLimiter::new(
            config.tenant_rate_limit_rate,
            config.tenant_rate_limit_burst,
        );

        let explain_cfg = config.explain_config();
        let explain_cache = qail_pg::explain::ExplainCache::new(explain_cfg.cache_ttl);

        let tenant_semaphore = Arc::new(crate::concurrency::TenantSemaphore::with_limits(
            config.max_concurrent_queries,
            config.max_tenants,
            std::time::Duration::from_secs(config.tenant_idle_timeout_secs),
        ));
        tenant_semaphore.start_sweeper();

        let db_backpressure = Arc::new(crate::db_backpressure::DbBackpressure::new(
            config.db_max_waiters_global,
            config.db_max_waiters_per_tenant,
            config.max_tenants,
        ));

        let user_tenant_map = helpers::load_user_tenant_map(
            &pool,
            config.statement_timeout_ms,
            config.lock_timeout_ms,
        )
        .await?;

        #[cfg(feature = "qdrant")]
        let qdrant_pool = if let Some(ref qdrant_config) = config.qdrant {
            let core_qdrant = qdrant_config.to_core_config();
            let pool_config = qail_qdrant::PoolConfig::from_qail_config_ref(&core_qdrant);
            let pool = qail_qdrant::QdrantPool::new(pool_config)
                .await
                .map_err(|e| GatewayError::Database(format!("Qdrant pool init failed: {}", e)))?;
            Some(pool)
        } else {
            None
        };

        let prometheus_handle = helpers::build_prometheus_handle()?;

        let complexity_guard = crate::middleware::QueryComplexityGuard::new(
            config.max_query_depth,
            config.max_query_filters,
            config.max_query_joins,
        );

        let allow_list = helpers::load_allow_list(&config)?;
        let rpc_allow_list = helpers::load_rpc_allow_list_from_config(&config)?;
        let jwks_store = helpers::load_jwks_store().await;

        let jwt_allowed_algorithms =
            crate::auth::parse_allowed_algorithms(&config.jwt_allowed_algorithms)?;

        let blocked_tables: HashSet<String> = config.blocked_tables.iter().cloned().collect();
        let allowed_tables: HashSet<String> = config.allowed_tables.iter().cloned().collect();

        let txn_max = if config.txn_max_sessions > 0 {
            config.txn_max_sessions
        } else {
            std::cmp::max(pool.max_connections() / 2, 2)
        };
        let txn_manager = Arc::new(crate::transaction::TransactionSessionManager::new(
            txn_max,
            config.txn_session_timeout_secs,
            config.txn_max_lifetime_secs,
            config.txn_max_statements_per_session,
        ));
        tracing::info!(
            "Embedded transaction sessions: max_sessions={}, idle_timeout={}s, max_lifetime={}s, max_statements={}",
            txn_max,
            config.txn_session_timeout_secs,
            config.txn_max_lifetime_secs,
            config.txn_max_statements_per_session
        );
        crate::transaction::spawn_reaper(Arc::clone(&txn_manager));

        let state = GatewayState {
            pool,
            policy_engine,
            event_engine,
            schema,
            cache,
            config,
            rate_limiter,
            tenant_rate_limiter,
            explain_cache,
            explain_config: explain_cfg,
            tenant_semaphore,
            db_backpressure,
            user_tenant_map,
            #[cfg(feature = "qdrant")]
            qdrant_pool,
            prometheus_handle,
            complexity_guard,
            allow_list,
            rpc_allow_list,
            rpc_signature_cache: moka::sync::Cache::builder()
                .max_capacity(512)
                .time_to_live(std::time::Duration::from_secs(300))
                .build(),
            parse_cache: moka::sync::Cache::builder()
                .max_capacity(1024)
                .time_to_live(std::time::Duration::from_secs(300))
                .build(),
            idempotency_store: crate::idempotency::IdempotencyStore::production(),
            jwks_store,
            jwt_allowed_algorithms,
            blocked_tables,
            allowed_tables,
            transaction_manager: txn_manager,
        };

        if !state.allowed_tables.is_empty() {
            tracing::info!(
                "SECURITY: allowlist mode — {} table(s) allowed for auto-REST: {}",
                state.allowed_tables.len(),
                state
                    .allowed_tables
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        } else if !state.blocked_tables.is_empty() {
            tracing::info!(
                "SECURITY: {} table(s) blocked from auto-REST: {}",
                state.blocked_tables.len(),
                state
                    .blocked_tables
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        tracing::info!("Gateway (embedded) initialized");

        Ok(state)
    }
}
