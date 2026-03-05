use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use jsonwebtoken::Algorithm;
use tokio::sync::RwLock;

use super::{Gateway, GatewayState};
use crate::cache::QueryCache;
use crate::config::GatewayConfig;
use crate::event::EventTriggerEngine;
use crate::middleware::RateLimiter;
use crate::policy::PolicyEngine;
use crate::schema::SchemaRegistry;
use qail_pg::PgPool;

mod build;
mod helpers;

pub(crate) struct StartupState {
    policy_engine: PolicyEngine,
    event_engine: EventTriggerEngine,
    schema: SchemaRegistry,
    cache: QueryCache,
    pool: PgPool,
    rate_limiter: Arc<RateLimiter>,
    tenant_rate_limiter: Arc<RateLimiter>,
    explain_cache: qail_pg::explain::ExplainCache,
    explain_config: qail_pg::explain::ExplainConfig,
    tenant_semaphore: Arc<crate::concurrency::TenantSemaphore>,
    db_backpressure: Arc<crate::db_backpressure::DbBackpressure>,
    user_operator_map: Arc<RwLock<HashMap<String, String>>>,
    #[cfg(feature = "qdrant")]
    qdrant_pool: Option<qail_qdrant::QdrantPool>,
    prometheus_handle: Arc<metrics_exporter_prometheus::PrometheusHandle>,
    complexity_guard: crate::middleware::QueryComplexityGuard,
    allow_list: crate::middleware::QueryAllowList,
    rpc_allow_list: Option<HashSet<String>>,
    jwks_store: Option<crate::jwks::JwksKeyStore>,
    jwt_allowed_algorithms: Vec<Algorithm>,
    blocked_tables: HashSet<String>,
    allowed_tables: HashSet<String>,
    transaction_manager: Arc<crate::transaction::TransactionSessionManager>,
}

impl StartupState {
    pub(crate) fn into_gateway_state(self, config: GatewayConfig) -> GatewayState {
        GatewayState {
            pool: self.pool,
            policy_engine: self.policy_engine,
            event_engine: self.event_engine,
            schema: self.schema,
            cache: self.cache,
            config,
            rate_limiter: self.rate_limiter,
            tenant_rate_limiter: self.tenant_rate_limiter,
            explain_cache: self.explain_cache,
            explain_config: self.explain_config,
            tenant_semaphore: self.tenant_semaphore,
            db_backpressure: self.db_backpressure,
            user_operator_map: self.user_operator_map,
            #[cfg(feature = "qdrant")]
            qdrant_pool: self.qdrant_pool,
            prometheus_handle: self.prometheus_handle,
            complexity_guard: self.complexity_guard,
            allow_list: self.allow_list,
            rpc_allow_list: self.rpc_allow_list,
            rpc_signature_cache: moka::sync::Cache::builder()
                .max_capacity(512)
                .time_to_live(std::time::Duration::from_secs(300))
                .build(),
            parse_cache: moka::sync::Cache::builder()
                .max_capacity(1024)
                .time_to_live(std::time::Duration::from_secs(300))
                .build(),
            idempotency_store: crate::idempotency::IdempotencyStore::production(),
            jwks_store: self.jwks_store,
            jwt_allowed_algorithms: self.jwt_allowed_algorithms,
            blocked_tables: self.blocked_tables,
            allowed_tables: self.allowed_tables,
            transaction_manager: self.transaction_manager,
        }
    }
}

impl Gateway {
    pub(crate) fn log_table_exposure_mode(&self) {
        if !self.config.allowed_tables.is_empty() {
            tracing::info!(
                "SECURITY: allowlist mode — {} table(s) allowed for auto-REST: {}",
                self.config.allowed_tables.len(),
                self.config.allowed_tables.join(", ")
            );
        } else if !self.config.blocked_tables.is_empty() {
            tracing::info!(
                "SECURITY: {} table(s) blocked from auto-REST: {}",
                self.config.blocked_tables.len(),
                self.config.blocked_tables.join(", ")
            );
        }
    }
}
