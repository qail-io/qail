//! Gateway server implementation
//!
//! Main entry point for running the QAIL Gateway.

use std::collections::HashMap;
use std::sync::Arc;
use metrics_exporter_prometheus::PrometheusHandle;
use axum::routing::MethodRouter;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use url::Url;

use crate::cache::QueryCache;
use crate::config::GatewayConfig;
use crate::error::GatewayError;
use crate::event::EventTriggerEngine;
use crate::middleware::RateLimiter;
use crate::policy::PolicyEngine;
use crate::router::create_router;
use crate::schema::SchemaRegistry;

use qail_pg::{PgPool, PoolConfig};

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
    /// EXPLAIN cost estimate cache.
    pub explain_cache: qail_pg::explain::ExplainCache,
    /// EXPLAIN pre-check configuration.
    pub explain_config: qail_pg::explain::ExplainConfig,
    /// Per-tenant concurrency semaphore.
    pub tenant_semaphore: Arc<crate::concurrency::TenantSemaphore>,
    /// Cache mapping user_id → operator_id for JWT tokens that lack operator_id.
    /// Loaded at startup from the users table.
    pub user_operator_map: Arc<RwLock<HashMap<String, String>>>,
    /// Optional Qdrant connection pool for vector operations.
    #[cfg(feature = "qdrant")]
    pub qdrant_pool: Option<qail_qdrant::QdrantPool>,
    /// Prometheus metrics handle for rendering /metrics endpoint
    pub prometheus_handle: Arc<PrometheusHandle>,
    /// Query complexity guard — limits depth, filters, and joins per query.
    pub complexity_guard: crate::middleware::QueryComplexityGuard,
    /// Query allow-list — when enabled, only pre-approved patterns are executed.
    pub allow_list: crate::middleware::QueryAllowList,
    /// DSL parse cache — hash(query_text) → parsed Qail AST.
    /// Skips re-parsing for repeated identical queries.
    pub parse_cache: moka::sync::Cache<u64, qail_core::ast::Qail>,
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
        
        // Load policies
        let mut policy_engine = PolicyEngine::new();
        if let Some(ref policy_path) = self.config.policy_path {
            // SECURITY (E7): Validate path before reading
            crate::config::validate_config_path(policy_path, self.config.config_root.as_deref())
                .map_err(GatewayError::Config)?;
            tracing::info!("Loading policies from: {}", policy_path);
            policy_engine.load_from_file(policy_path)?;
        }
        
        // Load schema
        let mut schema = SchemaRegistry::new();
        if let Some(ref schema_path) = self.config.schema_path {
            // SECURITY (E7): Validate path before reading
            crate::config::validate_config_path(schema_path, self.config.config_root.as_deref())
                .map_err(GatewayError::Config)?;
            tracing::info!("Loading schema from: {}", schema_path);
            schema.load_from_file(schema_path)?;
        }
        
        // Initialize cache
        let cache_config = self.config.cache_config();
        tracing::info!(
            "Query cache: enabled={}, max_entries={}, ttl={}s",
            cache_config.enabled,
            cache_config.max_entries,
            cache_config.ttl.as_secs()
        );
        let cache = QueryCache::new(cache_config);
        
        // Create connection pool
        tracing::info!("Creating connection pool...");
        let mut pool_config = parse_database_url(&self.config.database_url)?;

        // Allow env var overrides for pool sizing (takes precedence over URL query params)
        if let Ok(min) = std::env::var("POOL_MIN_CONNECTIONS") {
            if let Ok(n) = min.parse() {
                pool_config = pool_config.min_connections(n);
            }
        }
        if let Ok(max) = std::env::var("POOL_MAX_CONNECTIONS") {
            if let Ok(n) = max.parse() {
                pool_config = pool_config.max_connections(n);
            }
        }

        let pool = PgPool::connect(pool_config)
            .await
            .map_err(|e| GatewayError::Database(e.to_string()))?;
        
        let stats = pool.stats().await;
        tracing::info!("Connection pool: {} idle, {} max", stats.idle, stats.max_size);
        
        // Schema drift verification: cross-check .qail tables against database
        if !schema.table_names().is_empty() {
            tracing::info!("Verifying schema against database...");
            let mut conn = pool.acquire_system().await
                .map_err(|e| GatewayError::Database(format!("Schema verification connection failed: {}", e)))?;
            
            // Query information_schema for all public tables
            let cmd = qail_core::ast::Qail::get("information_schema.tables")
                .columns(["table_name"])
                .eq("table_schema", qail_core::ast::Value::String("public".into()));
            
            match conn.fetch_all_uncached(&cmd).await {
                Ok(rows) => {
                    let db_tables: std::collections::HashSet<String> = rows.iter()
                        .filter_map(|row| row.get_string(0))
                        .collect();
                    
                    let mut missing = Vec::new();
                    for table in schema.table_names() {
                        if !db_tables.contains(table) {
                            missing.push(table.to_string());
                        }
                    }
                    
                    if !missing.is_empty() {
                        let msg = format!(
                            "Schema drift detected! {} table(s) defined in .qail but missing from database: {}",
                            missing.len(),
                            missing.join(", ")
                        );
                        tracing::error!("{}", msg);
                        return Err(GatewayError::Config(msg));
                    }
                    
                    tracing::info!(
                        "Schema verified: {} tables match ({} in DB)",
                        schema.table_names().len(),
                        db_tables.len()
                    );
                }
                Err(e) => {
                    tracing::warn!("Schema verification skipped (query failed): {}", e);
                    // Non-fatal: allow startup even if introspection fails
                }
            }
        }
        
        // Load event triggers
        let mut event_engine = EventTriggerEngine::new();
        if let Some(ref events_path) = self.config.events_path {
            // SECURITY (E7): Validate path before reading
            crate::config::validate_config_path(events_path, self.config.config_root.as_deref())
                .map_err(GatewayError::Config)?;
            tracing::info!("Loading event triggers from: {}", events_path);
            event_engine.load_from_file(events_path)
                .map_err(GatewayError::Config)?;
        }
        
        // Initialize rate limiter
        let rate_limiter = RateLimiter::new(
            self.config.rate_limit_rate,
            self.config.rate_limit_burst,
        );
        tracing::info!(
            "Rate limiter: {:.0} req/s, burst={}",
            self.config.rate_limit_rate,
            self.config.rate_limit_burst
        );
        
        let explain_cfg = self.config.explain_config();
        let explain_cache = qail_pg::explain::ExplainCache::new(explain_cfg.cache_ttl);
        tracing::info!(
            "EXPLAIN pre-check: mode={:?}, depth_threshold={}, max_cost={:.0}, max_rows={}",
            explain_cfg.mode, explain_cfg.depth_threshold,
            explain_cfg.max_total_cost, explain_cfg.max_plan_rows
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

        // Load user → operator_id mapping for JWT resolution
        let user_operator_map = Arc::new(RwLock::new(HashMap::new()));
        {
            let token = qail_core::rls::SuperAdminToken::for_system_process("startup_user_map");
            let rls = qail_core::rls::RlsContext::super_admin(token);
            let mut conn = pool.acquire_with_rls(rls).await
                .map_err(|e| GatewayError::Database(format!("User lookup connection failed: {}", e)))?;
            let cmd = qail_core::ast::Qail::get("users")
                .columns(["id", "operator_id"])
                .limit(10_000);
            match conn.fetch_all_uncached(&cmd).await {
                Ok(rows) => {
                    let mut map = user_operator_map.write().await;
                    for row in &rows {
                        if let (Some(uid), Some(oid)) = (row.get_string(0), row.get_string(1)) {
                            if !oid.is_empty() {
                                map.insert(uid, oid);
                            }
                        }
                    }
                    tracing::info!("Loaded {} user→operator mappings for JWT resolution", map.len());
                }
                Err(e) => {
                    tracing::warn!("Could not load user→operator map (non-fatal): {}", e);
                }
            }
        }

        // Initialize Qdrant pool (optional — only if config has [qdrant])
        #[cfg(feature = "qdrant")]
        let qdrant_pool = if let Some(ref qdrant_config) = self.config.qdrant {
            let pool_config = qail_qdrant::PoolConfig::from_qail_config_ref(qdrant_config);
            let pool = qail_qdrant::QdrantPool::new(pool_config).await
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

        // Initialize Prometheus metrics recorder
        let prometheus_handle = Arc::new(crate::metrics::init_metrics());

        // Initialize query complexity guard
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

        // Initialize query allow-list (optional)
        let mut allow_list = crate::middleware::QueryAllowList::new();
        if let Some(ref path) = self.config.allow_list_path {
            allow_list.load_from_file(path).map_err(|e| {
                GatewayError::Config(format!("Failed to load allow-list from '{}': {}", path, e))
            })?;
            tracing::info!("Query allow-list: {} patterns loaded from '{}'", allow_list.len(), path);
        }

        tracing::info!("Tenant column: '{}'", self.config.tenant_column);

        self.state = Some(Arc::new(GatewayState {
            pool,
            policy_engine,
            event_engine,
            schema,
            cache,
            config: self.config.clone(),
            rate_limiter,
            explain_cache,
            explain_config: explain_cfg,
            tenant_semaphore,
            user_operator_map,
            #[cfg(feature = "qdrant")]
            qdrant_pool,
            prometheus_handle,
            complexity_guard,
            allow_list,
            parse_cache: moka::sync::Cache::builder()
                .max_capacity(1024)
                .time_to_live(std::time::Duration::from_secs(300))
                .build(),
        }));
        
        tracing::info!("Gateway initialized");
        Ok(())
    }
    
    /// SECURITY (M2): Refuse to start if dev-mode is enabled with unsafe config.
    ///
    /// Prevents operators from accidentally running header-based auth on a
    /// public interface. Two conditions trigger the guard:
    /// 1. `QAIL_DEV_MODE=true` + bind address is NOT `127.0.0.1` or `localhost`
    /// 2. `QAIL_DEV_MODE=true` + `JWT_SECRET` is not set
    fn check_dev_mode_safety(&self) -> Result<(), GatewayError> {
        let dev_mode = std::env::var("QAIL_DEV_MODE")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        if !dev_mode {
            return Ok(());
        }

        let bind = &self.config.bind_address;
        let is_local = bind.starts_with("127.0.0.1")
            || bind.starts_with("localhost")
            || bind.starts_with("[::1]");

        if !is_local {
            return Err(GatewayError::Config(format!(
                "SECURITY: QAIL_DEV_MODE=true but bind_address='{}' is not localhost. \
                 Dev mode allows unauthenticated header-based auth. \
                 Either set bind_address to 127.0.0.1 or unset QAIL_DEV_MODE.",
                bind
            )));
        }

        let jwt_set = std::env::var("JWT_SECRET").is_ok();
        if !jwt_set {
            tracing::warn!(
                "QAIL_DEV_MODE=true and JWT_SECRET is not set. \
                 Header-based auth is active on {}. Do NOT expose this port publicly.",
                bind
            );
        }

        Ok(())
    }

    /// Start serving requests
    /// 
    /// # Errors
    /// Returns error if server fails to start
    pub async fn serve(&self) -> Result<(), GatewayError> {
        let state = self.state.as_ref()
            .ok_or_else(|| GatewayError::Config("Gateway not initialized. Call init() first.".to_string()))?;
        
        let router = create_router(Arc::clone(state), &self.custom_routes);
        
        let addr = &self.config.bind_address;
        tracing::info!("🚀 QAIL Gateway starting on {}", addr);
        tracing::info!("   POST /qail     - Execute QAIL queries");
        tracing::info!("   GET  /health   - Health check");
        tracing::info!("   GET  /api/*    - Auto-REST endpoints");
        if !self.custom_routes.is_empty() {
            tracing::info!("   {} custom handler(s)", self.custom_routes.len());
        }
        
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| GatewayError::Config(format!("Failed to bind to {}: {}", addr, e)))?;

        // Context for background metrics task
        let state_metrics = Arc::clone(state);
        tokio::spawn(async move {
            loop {
                // Use PgPool native methods
                let active = state_metrics.pool.active_count();
                let max = state_metrics.pool.max_connections();
                let idle = state_metrics.pool.idle_count().await;
                
                crate::metrics::record_pool_stats(active, idle, max);
                
                // Collect process memory (RSS) from /proc/self/stat
                // Field 24 (index 23) is RSS in pages. Assumes 4KB pages.
                if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
                    if let Some(rss_pages) = stat.split_whitespace().nth(23).and_then(|s| s.parse::<u64>().ok()) {
                         metrics::gauge!("process_resident_memory_bytes").set((rss_pages * 4096) as f64);
                    }
                }
                
                if let Ok(stat) = std::fs::read_to_string("/proc/self/stat") {
                    if let Some(rss_pages) = stat.split_whitespace().nth(23).and_then(|s| s.parse::<u64>().ok()) {
                         metrics::gauge!("process_resident_memory_bytes").set((rss_pages * 4096) as f64);
                    }
                }

                // Poll cache stats
                let cache_stats = state_metrics.cache.stats();
                metrics::gauge!("qail_cache_entries").set(cache_stats.entries as f64);
                metrics::gauge!("qail_cache_weighted_bytes").set(cache_stats.weighted_size as f64);
                
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });
        
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| GatewayError::Internal(e.into()))?;
        
        // In-flight requests have drained — close pool for deterministic cleanup
        tracing::info!("In-flight requests drained. Closing connection pool...");
        state.pool.close().await;
        tracing::info!("Gateway shutdown complete.");
        
        Ok(())
    }
}

/// Wait for a shutdown signal (SIGTERM or Ctrl+C).
///
/// Used with `axum::serve().with_graceful_shutdown()` to implement
/// the correct shutdown sequence:
/// 1. Stop accepting new connections
/// 2. Wait for in-flight requests to complete  
/// 3. Return control to caller for pool cleanup
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("Received Ctrl+C, starting graceful shutdown..."),
        _ = terminate => tracing::info!("Received SIGTERM, starting graceful shutdown..."),
    }
}

/// Parse a database URL into PoolConfig
fn parse_database_url(url_str: &str) -> Result<PoolConfig, GatewayError> {
    use percent_encoding::percent_decode_str;
    
    let url = Url::parse(url_str)
        .map_err(|e| GatewayError::Config(format!("Invalid database URL: {}", e)))?;
    
    let host = url.host_str()
        .ok_or_else(|| GatewayError::Config("Missing host in database URL".to_string()))?;
    
    let port = url.port().unwrap_or(5432);
    
    // Url::username() returns percent-encoded string — decode it
    let user = if url.username().is_empty() {
        "postgres".to_string()
    } else {
        percent_decode_str(url.username())
            .decode_utf8()
            .map_err(|e| GatewayError::Config(format!("Invalid UTF-8 in username: {}", e)))?
            .into_owned()
    };
    
    let database = url.path().trim_start_matches('/');
    if database.is_empty() {
        return Err(GatewayError::Config("Missing database name in URL".to_string()));
    }
    
    let mut config = PoolConfig::new(host, port, &user, database);
    
    // Url::password() returns percent-encoded string — decode it
    // Critical: passwords with '=' get encoded as '%3D' by the url crate,
    // but SCRAM-SHA-256 needs the raw password bytes for PBKDF2.
    if let Some(password) = url.password() {
        let decoded = percent_decode_str(password)
            .decode_utf8()
            .map_err(|e| GatewayError::Config(format!("Invalid UTF-8 in password: {}", e)))?;
        config = config.password(&decoded);
    }
    
    // Parse query params for pool settings
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "max_connections" => {
                if let Ok(n) = value.parse() {
                    config = config.max_connections(n);
                }
            }
            "min_connections" => {
                if let Ok(n) = value.parse() {
                    config = config.min_connections(n);
                }
            }
            _ => {}
        }
    }
    
    Ok(config)
}

/// Builder for the Gateway
pub struct GatewayBuilder {
    config: GatewayConfig,
    custom_routes: Vec<(String, MethodRouter<Arc<GatewayState>>)>,
}

impl GatewayBuilder {
    /// Create a new builder with default config
    pub fn new() -> Self {
        Self {
            config: GatewayConfig::default(),
            custom_routes: Vec::new(),
        }
    }

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
    
    /// Override rate limiter settings (requests per second, burst capacity)
    pub fn rate_limit(mut self, rate: f64, burst: u32) -> Self {
        self.config.rate_limit_rate = rate;
        self.config.rate_limit_burst = burst;
        self
    }
    
    /// Register a custom handler that overrides or extends auto-REST routes.
    ///
    /// Custom handlers merge AFTER auto-REST, so they take precedence.
    ///
    /// # Example
    /// ```ignore
    /// use axum::routing::post;
    ///
    /// let gateway = Gateway::builder()
    ///     .database("postgres://...")
    ///     .schema("schema.qail")
    ///     .extend("/api/orders/:id/pay", post(custom_payment_handler))
    ///     .extend("/api/reports/daily", get(daily_report_handler))
    ///     .build_and_init()
    ///     .await?;
    /// ```
    pub fn extend(
        mut self,
        path: impl Into<String>,
        handler: MethodRouter<Arc<GatewayState>>,
    ) -> Self {
        self.custom_routes.push((path.into(), handler));
        self
    }

    /// Build the gateway
    pub fn build(self) -> Gateway {
        let mut gw = Gateway::new(self.config);
        gw.custom_routes = self.custom_routes;
        gw
    }
    
    /// Build and initialize the gateway
    /// 
    /// # Errors
    /// Returns error if initialization fails
    pub async fn build_and_init(self) -> Result<Gateway, GatewayError> {
        let mut gateway = self.build();
        gateway.init().await?;
        Ok(gateway)
    }
}

impl Default for GatewayBuilder {
    fn default() -> Self {
        Self::new()
    }
}
