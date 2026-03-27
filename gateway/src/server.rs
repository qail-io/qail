//! Gateway server implementation
//!
//! Main entry point for running the QAIL Gateway.

use axum::routing::MethodRouter;
use jsonwebtoken::Algorithm;
use metrics_exporter_prometheus::PrometheusHandle;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

mod builder;
mod database_url;
mod embedded;
mod init;
mod runtime;
mod security;

pub use builder::GatewayBuilder;

use crate::cache::QueryCache;
use crate::config::GatewayConfig;
use crate::error::GatewayError;
use crate::event::EventTriggerEngine;
use crate::middleware::RateLimiter;
use crate::policy::PolicyEngine;
use crate::schema::SchemaRegistry;
use qail_pg::PgPool;

/// Cached callable signature metadata for one PostgreSQL function overload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcCallableSignature {
    /// Number of input arguments (`pronargs`).
    pub total_args: usize,
    /// Number of trailing input arguments with defaults (`pronargdefaults`).
    pub default_args: usize,
    /// Whether the final input argument is variadic.
    pub variadic: bool,
    /// Input argument names (normalized lowercase), aligned to input argument order.
    /// Unnamed arguments are represented as `None`.
    pub arg_names: Vec<Option<String>>,
    /// Input argument PostgreSQL type names, aligned to input argument order.
    pub arg_types: Vec<String>,
    /// Identity argument text from `pg_get_function_identity_arguments`.
    pub identity_args: String,
    /// Result type text from `pg_get_function_result`.
    pub result_type: String,
}

impl RpcCallableSignature {
    /// Number of required input arguments.
    pub fn required_args(&self) -> usize {
        self.total_args.saturating_sub(self.default_args)
    }
}

/// Shared state for the gateway
pub struct GatewayState {
    /// PostgreSQL connection pool.
    pub pool: PgPool,
    /// Row-level security policy engine.
    pub policy_engine: PolicyEngine,
    /// Webhook event trigger engine.
    pub event_engine: EventTriggerEngine,
    /// Loaded table schema registry.
    pub schema: SchemaRegistry,
    /// In-memory query cache.
    pub cache: QueryCache,
    /// Gateway configuration.
    pub config: GatewayConfig,
    /// Per-IP rate limiter.
    pub rate_limiter: Arc<RateLimiter>,
    /// Per-tenant post-auth rate limiter (keyed by tenant_id:user_id).
    pub tenant_rate_limiter: Arc<RateLimiter>,
    /// EXPLAIN cost estimate cache.
    pub explain_cache: qail_pg::explain::ExplainCache,
    /// EXPLAIN pre-check configuration.
    pub explain_config: qail_pg::explain::ExplainConfig,
    /// Per-tenant concurrency semaphore.
    pub tenant_semaphore: Arc<crate::concurrency::TenantSemaphore>,
    /// DB acquire backpressure guard (global + per-tenant waiting caps).
    pub db_backpressure: Arc<crate::db_backpressure::DbBackpressure>,
    /// Cache mapping user_id → tenant_id for JWTs that lack tenant scope.
    /// Loaded at startup from users (`tenant_id`).
    pub user_tenant_map: Arc<RwLock<HashMap<String, String>>>,
    /// Optional Qdrant connection pool for vector operations.
    #[cfg(feature = "qdrant")]
    pub qdrant_pool: Option<qail_qdrant::QdrantPool>,
    /// Prometheus metrics handle for rendering /metrics endpoint
    pub prometheus_handle: Arc<PrometheusHandle>,
    /// Query complexity guard — limits depth, filters, and joins per query.
    pub complexity_guard: crate::middleware::QueryComplexityGuard,
    /// Query allow-list — when enabled, only pre-approved patterns are executed.
    pub allow_list: crate::middleware::QueryAllowList,
    /// RPC allow-list — optional set of approved function names.
    pub rpc_allow_list: Option<HashSet<String>>,
    /// RPC signature cache — normalized function name to callable overload metadata.
    pub rpc_signature_cache: moka::sync::Cache<String, Arc<Vec<RpcCallableSignature>>>,
    /// DSL parse cache — exact query text → parsed Qail AST.
    /// Skips re-parsing for repeated identical queries.
    pub parse_cache: moka::sync::Cache<String, qail_core::ast::Qail>,
    /// Idempotency store — caches mutation responses by Idempotency-Key header.
    pub idempotency_store: crate::idempotency::IdempotencyStore,
    /// JWKS key store — caches JWT public keys from a JWKS endpoint (Phase 6a).
    pub jwks_store: Option<crate::jwks::JwksKeyStore>,
    /// Allowed JWT algorithms parsed from configuration.
    pub jwt_allowed_algorithms: Vec<Algorithm>,
    /// Tables blocked from auto-REST endpoint generation.
    /// Checked at route generation and as a runtime handler guard.
    pub blocked_tables: HashSet<String>,
    /// Tables allowed for auto-REST (whitelist mode). When non-empty,
    /// ONLY these tables are exposed — all others are implicitly blocked.
    /// Takes precedence over `blocked_tables`.
    pub allowed_tables: HashSet<String>,
    /// Transaction session manager for multi-statement transactions.
    pub transaction_manager: Arc<crate::transaction::TransactionSessionManager>,
}

/// The QAIL Gateway server
pub struct Gateway {
    config: GatewayConfig,
    state: Option<Arc<GatewayState>>,
    custom_routes: Vec<(String, MethodRouter<Arc<GatewayState>>)>,
}

impl Gateway {
    /// Create a new gateway with the given configuration
    pub fn new(config: GatewayConfig) -> Self {
        Self {
            config,
            state: None,
            custom_routes: Vec::new(),
        }
    }

    /// Create a gateway builder
    pub fn builder() -> GatewayBuilder {
        GatewayBuilder::default()
    }

    /// Initialize the gateway (load schema, policies, connect to DB)
    pub async fn init(&mut self) -> Result<(), GatewayError> {
        tracing::info!("Initializing QAIL Gateway...");

        // SECURITY (M2): Block startup if dev-mode is enabled with unsafe config.
        self.check_dev_mode_safety()?;

        // SECURITY (P0-1): Block startup if production_strict=true and controls are missing.
        self.check_production_strict()?;
        let init = self.build_startup_state().await?;
        self.state = Some(Arc::new(init.into_gateway_state(self.config.clone())));
        self.log_table_exposure_mode();
        tracing::info!("Gateway initialized");
        Ok(())
    }
}
