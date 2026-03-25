//! Pool lifecycle: PgPoolInner, PgPool core (connect, maintain, close),
//! hot statement pre-prepare, and connection creation.

use super::ScopedPoolFuture;
use super::churn::{
    PoolStats, decrement_active_count_saturating, pool_churn_record_destroy,
    pool_churn_remaining_open, record_pool_connection_destroy,
};
use super::config::PoolConfig;
use super::connection::PooledConn;
use super::connection::PooledConnection;
use super::gss::*;
use crate::driver::{
    AstPipelineMode, AutoCountPath, AutoCountPlan, ConnectOptions, PgConnection, PgError, PgResult,
    is_ignorable_session_message, unexpected_backend_message,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

/// Maximum number of hot statements to track globally.
pub(super) const MAX_HOT_STATEMENTS: usize = 32;

/// Inner pool state (shared across clones).
pub(super) struct PgPoolInner {
    pub(super) config: PoolConfig,
    pub(super) connections: Mutex<Vec<PooledConn>>,
    pub(super) semaphore: Semaphore,
    pub(super) closed: AtomicBool,
    pub(super) active_count: AtomicUsize,
    pub(super) total_created: AtomicUsize,
    pub(super) leaked_cleanup_inflight: AtomicUsize,
    /// Global registry of frequently-used prepared statements.
    /// Maps sql_hash → (stmt_name, sql_text).
    /// New connections pre-prepare these on checkout for instant cache hits.
    pub(super) hot_statements: std::sync::RwLock<std::collections::HashMap<u64, (String, String)>>,
}

pub(super) fn handle_hot_preprepare_message(
    msg: &crate::protocol::BackendMessage,
    parse_complete_count: &mut usize,
    error: &mut Option<PgError>,
) -> PgResult<bool> {
    match msg {
        crate::protocol::BackendMessage::ParseComplete => {
            *parse_complete_count += 1;
            Ok(false)
        }
        crate::protocol::BackendMessage::ErrorResponse(err) => {
            if error.is_none() {
                *error = Some(PgError::QueryServer(err.clone().into()));
            }
            Ok(false)
        }
        crate::protocol::BackendMessage::ReadyForQuery(_) => Ok(true),
        msg if is_ignorable_session_message(msg) => Ok(false),
        other => Err(unexpected_backend_message("pool hot pre-prepare", other)),
    }
}

impl PgPoolInner {
    pub(super) async fn return_connection(&self, conn: PgConnection, created_at: Instant) {
        decrement_active_count_saturating(&self.active_count);

        if conn.is_io_desynced() {
            tracing::warn!(
                host = %self.config.host,
                port = self.config.port,
                user = %self.config.user,
                db = %self.config.database,
                "pool_return_desynced: dropping connection due to prior I/O/protocol desync"
            );
            record_pool_connection_destroy("pool_desynced_drop");
            self.semaphore.add_permits(1);
            pool_churn_record_destroy(&self.config, "return_desynced");
            return;
        }

        if self.closed.load(Ordering::Relaxed) {
            record_pool_connection_destroy("pool_closed_drop");
            self.semaphore.add_permits(1);
            return;
        }

        let mut connections = self.connections.lock().await;
        if connections.len() < self.config.max_connections {
            connections.push(PooledConn {
                conn,
                created_at,
                last_used: Instant::now(),
            });
        } else {
            record_pool_connection_destroy("pool_overflow_drop");
        }

        self.semaphore.add_permits(1);
    }

    /// Get a healthy connection from the pool, or None if pool is empty.
    async fn get_healthy_connection(&self) -> Option<PooledConn> {
        let mut connections = self.connections.lock().await;

        while let Some(pooled) = connections.pop() {
            if pooled.last_used.elapsed() > self.config.idle_timeout {
                tracing::debug!(
                    idle_secs = pooled.last_used.elapsed().as_secs(),
                    timeout_secs = self.config.idle_timeout.as_secs(),
                    "pool_checkout_evict: connection exceeded idle timeout"
                );
                record_pool_connection_destroy("idle_timeout_evict");
                continue;
            }

            if let Some(max_life) = self.config.max_lifetime
                && pooled.created_at.elapsed() > max_life
            {
                tracing::debug!(
                    age_secs = pooled.created_at.elapsed().as_secs(),
                    max_lifetime_secs = max_life.as_secs(),
                    "pool_checkout_evict: connection exceeded max lifetime"
                );
                record_pool_connection_destroy("max_lifetime_evict");
                continue;
            }

            return Some(pooled);
        }

        None
    }
}

/// # Example
/// ```ignore
/// let config = PoolConfig::new("localhost", 5432, "user", "db")
///     .password("secret")
///     .max_connections(20);
/// let pool = PgPool::connect(config).await?;
/// // Get a connection from the pool
/// let mut conn = pool.acquire_raw().await?;
/// conn.simple_query("SELECT 1").await?;
/// ```
#[derive(Clone)]
pub struct PgPool {
    pub(super) inner: Arc<PgPoolInner>,
}

impl PgPool {
    /// Create a pool from `qail.toml` (loads and parses automatically).
    ///
    /// # Example
    /// ```ignore
    /// let pool = PgPool::from_config().await?;
    /// ```
    pub async fn from_config() -> PgResult<Self> {
        let qail = qail_core::config::QailConfig::load()
            .map_err(|e| PgError::Connection(format!("Config error: {}", e)))?;
        let config = PoolConfig::from_qail_config(&qail)?;
        Self::connect(config).await
    }

    /// Create a new connection pool.
    pub async fn connect(config: PoolConfig) -> PgResult<Self> {
        validate_pool_config(&config)?;

        // Semaphore starts with max_connections permits
        let semaphore = Semaphore::new(config.max_connections);

        let mut initial_connections = Vec::new();
        for _ in 0..config.min_connections {
            let conn = Self::create_connection(&config).await?;
            initial_connections.push(PooledConn {
                conn,
                created_at: Instant::now(),
                last_used: Instant::now(),
            });
        }

        let initial_count = initial_connections.len();

        let inner = Arc::new(PgPoolInner {
            config,
            connections: Mutex::new(initial_connections),
            semaphore,
            closed: AtomicBool::new(false),
            active_count: AtomicUsize::new(0),
            total_created: AtomicUsize::new(initial_count),
            leaked_cleanup_inflight: AtomicUsize::new(0),
            hot_statements: std::sync::RwLock::new(std::collections::HashMap::new()),
        });

        Ok(Self { inner })
    }

    /// Acquire a raw connection from the pool (crate-internal only).
    ///
    /// # Safety (not `unsafe` in the Rust sense, but security-critical)
    ///
    /// This returns a connection with **no RLS context**. All tenant data
    /// queries on this connection will bypass row-level security.
    ///
    /// **Safe usage**: Pair with `fetch_all_with_rls()` for pipelined
    /// RLS+query execution (single roundtrip). Or use `acquire_with_rls()`
    /// / `acquire_with_rls_timeout()` for the 2-roundtrip path.
    ///
    /// **Unsafe usage**: Running queries directly on a raw connection
    /// without RLS context. Every call site MUST include a `// SAFETY:`
    /// comment explaining why raw acquisition is justified.
    pub async fn acquire_raw(&self) -> PgResult<PooledConnection> {
        if self.inner.closed.load(Ordering::Relaxed) {
            return Err(PgError::PoolClosed);
        }

        if let Some(remaining) = pool_churn_remaining_open(&self.inner.config) {
            metrics::counter!("qail_pg_pool_churn_circuit_reject_total").increment(1);
            tracing::warn!(
                host = %self.inner.config.host,
                port = self.inner.config.port,
                user = %self.inner.config.user,
                db = %self.inner.config.database,
                remaining_ms = remaining.as_millis() as u64,
                "pool_connection_churn_circuit_open"
            );
            return Err(PgError::PoolExhausted {
                max: self.inner.config.max_connections,
            });
        }

        // Wait for available slot with timeout
        let acquire_timeout = self.inner.config.acquire_timeout;
        let permit =
            match tokio::time::timeout(acquire_timeout, self.inner.semaphore.acquire()).await {
                Ok(permit) => permit.map_err(|_| PgError::PoolClosed)?,
                Err(_) => {
                    metrics::counter!("qail_pg_pool_acquire_timeouts_total").increment(1);
                    return Err(PgError::Timeout(format!(
                        "pool acquire after {}s ({} max connections)",
                        acquire_timeout.as_secs(),
                        self.inner.config.max_connections
                    )));
                }
            };

        if self.inner.closed.load(Ordering::Relaxed) {
            return Err(PgError::PoolClosed);
        }

        // Try to get existing healthy connection
        let (mut conn, mut created_at) =
            if let Some(pooled) = self.inner.get_healthy_connection().await {
                (pooled.conn, pooled.created_at)
            } else {
                let conn = Self::create_connection(&self.inner.config).await?;
                self.inner.total_created.fetch_add(1, Ordering::Relaxed);
                (conn, Instant::now())
            };

        if self.inner.config.test_on_acquire
            && let Err(e) = execute_simple_with_timeout(
                &mut conn,
                "SELECT 1",
                self.inner.config.connect_timeout,
                "pool checkout health check",
            )
            .await
        {
            tracing::warn!(
                host = %self.inner.config.host,
                port = self.inner.config.port,
                user = %self.inner.config.user,
                db = %self.inner.config.database,
                error = %e,
                "pool_health_check_failed: checkout probe failed, creating replacement connection"
            );
            pool_churn_record_destroy(&self.inner.config, "health_check_failed");
            conn = Self::create_connection(&self.inner.config).await?;
            self.inner.total_created.fetch_add(1, Ordering::Relaxed);
            created_at = Instant::now();
        }

        // Pre-prepare hot statements that this connection doesn't have yet.
        // Collect data synchronously (guard dropped before async work).
        let missing: Vec<(u64, String, String)> = {
            if let Ok(hot) = self.inner.hot_statements.read() {
                hot.iter()
                    .filter(|(hash, _)| !conn.stmt_cache.contains(hash))
                    .map(|(hash, (name, sql))| (*hash, name.clone(), sql.clone()))
                    .collect()
            } else {
                Vec::new()
            }
        }; // RwLockReadGuard dropped here — safe across .await

        if !missing.is_empty() {
            use crate::protocol::PgEncoder;
            let mut buf = bytes::BytesMut::new();
            for (_, name, sql) in &missing {
                let parse_msg = PgEncoder::try_encode_parse(name, sql, &[])?;
                buf.extend_from_slice(&parse_msg);
            }
            PgEncoder::encode_sync_to(&mut buf);
            let preprepare_timeout = self.inner.config.connect_timeout;
            let preprepare_result: PgResult<()> = match tokio::time::timeout(
                preprepare_timeout,
                async {
                    conn.send_bytes(&buf).await?;
                    // Drain responses and fail closed on any parse error.
                    let mut parse_complete_count = 0usize;
                    let mut parse_error: Option<PgError> = None;
                    loop {
                        let msg = conn.recv().await?;
                        if handle_hot_preprepare_message(
                            &msg,
                            &mut parse_complete_count,
                            &mut parse_error,
                        )? {
                            if let Some(err) = parse_error {
                                return Err(err);
                            }
                            if parse_complete_count != missing.len() {
                                return Err(PgError::Protocol(format!(
                                    "hot pre-prepare completed with {} ParseComplete messages (expected {})",
                                    parse_complete_count,
                                    missing.len()
                                )));
                            }
                            break;
                        }
                    }
                    Ok::<(), PgError>(())
                },
            )
            .await
            {
                Ok(res) => res,
                Err(_) => Err(PgError::Timeout(format!(
                    "hot statement pre-prepare timeout after {:?} (pool config connect_timeout)",
                    preprepare_timeout
                ))),
            };

            if let Err(e) = preprepare_result {
                tracing::warn!(
                    host = %self.inner.config.host,
                    port = self.inner.config.port,
                    user = %self.inner.config.user,
                    db = %self.inner.config.database,
                    timeout_ms = preprepare_timeout.as_millis() as u64,
                    error = %e,
                    "pool_hot_prepare_failed: replacing connection to avoid handing out uncertain protocol state"
                );
                pool_churn_record_destroy(&self.inner.config, "hot_prepare_failed");
                conn = Self::create_connection(&self.inner.config).await?;
                self.inner.total_created.fetch_add(1, Ordering::Relaxed);
                created_at = Instant::now();
            } else {
                // Register in local cache
                for (hash, name, sql) in &missing {
                    conn.stmt_cache.put(*hash, name.clone());
                    conn.prepared_statements.insert(name.clone(), sql.clone());
                }
            }
        }

        self.inner.active_count.fetch_add(1, Ordering::Relaxed);
        // Permit is intentionally detached here; returned by `release()` / pool return.
        permit.forget();

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.inner.clone(),
            rls_dirty: false,
            created_at,
        })
    }

    /// Acquire a connection with RLS context pre-configured.
    ///
    /// Sets PostgreSQL session variables for tenant isolation before
    /// returning the connection. When the connection is dropped, it
    /// automatically clears the RLS context before returning to the pool.
    ///
    /// # Example
    /// ```ignore
    /// use qail_core::rls::RlsContext;
    ///
    /// let mut conn = pool.acquire_with_rls(
    ///     RlsContext::tenant("550e8400-e29b-41d4-a716-446655440000")
    /// ).await?;
    /// // All queries through `conn` are now scoped to this tenant
    /// ```
    pub async fn acquire_with_rls(
        &self,
        ctx: qail_core::rls::RlsContext,
    ) -> PgResult<PooledConnection> {
        // SAFETY: RLS context is set immediately below via context_to_sql().
        let mut conn = self.acquire_raw().await?;

        // Set RLS context on the raw connection
        let sql = crate::driver::rls::context_to_sql(&ctx);
        let pg_conn = conn.get_mut()?;
        if let Err(e) = execute_simple_with_timeout(
            pg_conn,
            &sql,
            self.inner.config.connect_timeout,
            "pool acquire_with_rls setup",
        )
        .await
        {
            // Attempt recovery ROLLBACK to salvage the connection rather than
            // letting Drop destroy it (which wastes a TCP connection).
            if let Ok(pg_conn) = conn.get_mut() {
                let _ = pg_conn.execute_simple("ROLLBACK").await;
            }
            conn.release().await;
            return Err(e);
        }

        // Mark dirty so Drop resets context before pool return
        conn.rls_dirty = true;

        Ok(conn)
    }

    /// Scoped connection helper that guarantees `release()` after closure execution.
    ///
    /// Prefer this over manual `acquire_with_rls()` in normal request handlers.
    pub async fn with_rls<T, F>(&self, ctx: qail_core::rls::RlsContext, f: F) -> PgResult<T>
    where
        F: for<'a> FnOnce(&'a mut PooledConnection) -> ScopedPoolFuture<'a, T>,
    {
        let mut conn = self.acquire_with_rls(ctx).await?;
        let out = f(&mut conn).await;
        conn.release().await;
        out
    }

    /// Scoped helper for system-level operations (`RlsContext::empty()`).
    pub async fn with_system<T, F>(&self, f: F) -> PgResult<T>
    where
        F: for<'a> FnOnce(&'a mut PooledConnection) -> ScopedPoolFuture<'a, T>,
    {
        self.with_rls(qail_core::rls::RlsContext::empty(), f).await
    }

    /// Scoped helper for global/platform row access (`tenant_id IS NULL`).
    pub async fn with_global<T, F>(&self, f: F) -> PgResult<T>
    where
        F: for<'a> FnOnce(&'a mut PooledConnection) -> ScopedPoolFuture<'a, T>,
    {
        self.with_rls(qail_core::rls::RlsContext::global(), f).await
    }

    /// Scoped helper for single-tenant access.
    pub async fn with_tenant<T, F>(&self, tenant_id: &str, f: F) -> PgResult<T>
    where
        F: for<'a> FnOnce(&'a mut PooledConnection) -> ScopedPoolFuture<'a, T>,
    {
        self.with_rls(qail_core::rls::RlsContext::tenant(tenant_id), f)
            .await
    }

    /// Acquire a connection with RLS context AND statement timeout.
    ///
    /// Like `acquire_with_rls()`, but also sets `statement_timeout` to prevent
    /// runaway queries from holding pool connections indefinitely.
    pub async fn acquire_with_rls_timeout(
        &self,
        ctx: qail_core::rls::RlsContext,
        timeout_ms: u32,
    ) -> PgResult<PooledConnection> {
        // SAFETY: RLS context + timeout set immediately below via context_to_sql_with_timeout().
        let mut conn = self.acquire_raw().await?;

        // Set RLS context + statement_timeout atomically
        let sql = crate::driver::rls::context_to_sql_with_timeout(&ctx, timeout_ms);
        let pg_conn = conn.get_mut()?;
        if let Err(e) = execute_simple_with_timeout(
            pg_conn,
            &sql,
            self.inner.config.connect_timeout,
            "pool acquire_with_rls_timeout setup",
        )
        .await
        {
            if let Ok(pg_conn) = conn.get_mut() {
                let _ = pg_conn.execute_simple("ROLLBACK").await;
            }
            conn.release().await;
            return Err(e);
        }

        // Mark dirty so Drop resets context + timeout before pool return
        conn.rls_dirty = true;

        Ok(conn)
    }

    /// Scoped connection helper that guarantees `release()` after closure execution.
    pub async fn with_rls_timeout<T, F>(
        &self,
        ctx: qail_core::rls::RlsContext,
        timeout_ms: u32,
        f: F,
    ) -> PgResult<T>
    where
        F: for<'a> FnOnce(&'a mut PooledConnection) -> ScopedPoolFuture<'a, T>,
    {
        let mut conn = self.acquire_with_rls_timeout(ctx, timeout_ms).await?;
        let out = f(&mut conn).await;
        conn.release().await;
        out
    }

    /// Acquire a connection with RLS context, statement timeout, AND lock timeout.
    ///
    /// Like `acquire_with_rls_timeout()`, but also sets `lock_timeout` to prevent
    /// queries from blocking indefinitely on row/table locks.
    /// When `lock_timeout_ms` is 0, the lock_timeout clause is omitted.
    pub async fn acquire_with_rls_timeouts(
        &self,
        ctx: qail_core::rls::RlsContext,
        statement_timeout_ms: u32,
        lock_timeout_ms: u32,
    ) -> PgResult<PooledConnection> {
        // SAFETY: RLS context + timeouts set immediately below via context_to_sql_with_timeouts().
        let mut conn = self.acquire_raw().await?;

        let sql = crate::driver::rls::context_to_sql_with_timeouts(
            &ctx,
            statement_timeout_ms,
            lock_timeout_ms,
        );
        let pg_conn = conn.get_mut()?;
        if let Err(e) = execute_simple_with_timeout(
            pg_conn,
            &sql,
            self.inner.config.connect_timeout,
            "pool acquire_with_rls_timeouts setup",
        )
        .await
        {
            if let Ok(pg_conn) = conn.get_mut() {
                let _ = pg_conn.execute_simple("ROLLBACK").await;
            }
            conn.release().await;
            return Err(e);
        }

        conn.rls_dirty = true;

        Ok(conn)
    }

    /// Scoped connection helper that guarantees `release()` after closure execution.
    pub async fn with_rls_timeouts<T, F>(
        &self,
        ctx: qail_core::rls::RlsContext,
        statement_timeout_ms: u32,
        lock_timeout_ms: u32,
        f: F,
    ) -> PgResult<T>
    where
        F: for<'a> FnOnce(&'a mut PooledConnection) -> ScopedPoolFuture<'a, T>,
    {
        let mut conn = self
            .acquire_with_rls_timeouts(ctx, statement_timeout_ms, lock_timeout_ms)
            .await?;
        let out = f(&mut conn).await;
        conn.release().await;
        out
    }

    /// Acquire a connection for system-level operations (no tenant context).
    ///
    /// Sets RLS session variables to maximally restrictive values:
    /// - `app.current_tenant_id = ''`
    /// - `app.current_agent_id = ''`  
    /// - `app.is_super_admin = false`
    ///
    /// Use this for startup introspection, migrations, and health checks
    /// that must not operate within any tenant scope.
    pub async fn acquire_system(&self) -> PgResult<PooledConnection> {
        let ctx = qail_core::rls::RlsContext::empty();
        self.acquire_with_rls(ctx).await
    }

    /// Acquire a connection scoped to global/platform rows.
    ///
    /// Shorthand for `acquire_with_rls(RlsContext::global())`.
    /// Use this for shared reference data (for example: currencies, ports,
    /// vessel types) stored as `tenant_id IS NULL`.
    pub async fn acquire_global(&self) -> PgResult<PooledConnection> {
        self.acquire_with_rls(qail_core::rls::RlsContext::global())
            .await
    }

    /// Acquire a connection scoped to a specific tenant.
    ///
    /// Shorthand for `acquire_with_rls(RlsContext::tenant(tenant_id))`.
    /// Use this when you already know the tenant UUID and want a
    /// tenant-scoped connection in a single call.
    ///
    /// # Example
    /// ```ignore
    /// let mut conn = pool.acquire_for_tenant("550e8400-...").await?;
    /// // All queries through `conn` are now scoped to this tenant
    /// ```
    pub async fn acquire_for_tenant(&self, tenant_id: &str) -> PgResult<PooledConnection> {
        self.acquire_with_rls(qail_core::rls::RlsContext::tenant(tenant_id))
            .await
    }

    /// Acquire a connection with branch context pre-configured.
    ///
    /// Sets PostgreSQL session variable `app.branch_id` for data virtualization.
    /// When the connection is dropped, it automatically clears the branch context.
    ///
    /// # Example
    /// ```ignore
    /// use qail_core::branch::BranchContext;
    ///
    /// let ctx = BranchContext::branch("feature-auth");
    /// let mut conn = pool.acquire_with_branch(&ctx).await?;
    /// // All queries through `conn` are now branch-aware
    /// ```
    pub async fn acquire_with_branch(
        &self,
        ctx: &qail_core::branch::BranchContext,
    ) -> PgResult<PooledConnection> {
        // SAFETY: Branch context is set immediately below via branch_context_sql().
        let mut conn = self.acquire_raw().await?;

        if let Some(branch_name) = ctx.branch_name() {
            let sql = crate::driver::branch_sql::branch_context_sql(branch_name);
            let pg_conn = conn.get_mut()?;
            if let Err(e) = execute_simple_with_timeout(
                pg_conn,
                &sql,
                self.inner.config.connect_timeout,
                "pool acquire_with_branch setup",
            )
            .await
            {
                if let Ok(pg_conn) = conn.get_mut() {
                    let _ = pg_conn.execute_simple("ROLLBACK").await;
                }
                conn.release().await;
                return Err(e);
            }
            conn.rls_dirty = true; // Reuse dirty flag for auto-reset
        }

        Ok(conn)
    }

    /// Get the current number of idle connections.
    pub async fn idle_count(&self) -> usize {
        self.inner.connections.lock().await.len()
    }

    /// Get the number of connections currently in use.
    pub fn active_count(&self) -> usize {
        self.inner.active_count.load(Ordering::Relaxed)
    }

    /// Get the maximum number of connections.
    pub fn max_connections(&self) -> usize {
        self.inner.config.max_connections
    }

    /// Plan auto count strategy for a given batch length.
    pub fn plan_auto_count(&self, batch_len: usize) -> AutoCountPlan {
        AutoCountPlan::for_pool(
            batch_len,
            self.inner.config.max_connections,
            self.inner.semaphore.available_permits(),
        )
    }

    /// Execute commands with runtime auto strategy and return count + plan.
    pub async fn execute_count_auto_with_plan(
        &self,
        cmds: &[qail_core::ast::Qail],
    ) -> PgResult<(usize, AutoCountPlan)> {
        let plan = self.plan_auto_count(cmds.len());

        let completed = match plan.path {
            AutoCountPath::SingleCached => {
                if cmds.is_empty() {
                    0
                } else {
                    let mut conn = self.acquire_system().await?;
                    let run_result = conn.fetch_all_cached(&cmds[0]).await;
                    conn.release().await;
                    let _ = run_result?;
                    1
                }
            }
            AutoCountPath::PipelineOneShot | AutoCountPath::PipelineCached => {
                let mode = if matches!(plan.path, AutoCountPath::PipelineOneShot) {
                    AstPipelineMode::OneShot
                } else {
                    AstPipelineMode::Cached
                };

                let mut pooled = self.acquire_system().await?;
                let run_result = {
                    let conn = pooled.get_mut()?;
                    conn.pipeline_execute_count_ast_with_mode(cmds, mode).await
                };
                pooled.release().await;
                run_result?
            }
            AutoCountPath::PoolParallel => {
                if cmds.is_empty() {
                    0
                } else {
                    let all_cmds = Arc::new(cmds.to_vec());
                    let mut tasks: JoinSet<PgResult<usize>> = JoinSet::new();

                    for worker in 0..plan.workers {
                        let start = worker * plan.chunk_size;
                        if start >= all_cmds.len() {
                            break;
                        }
                        let end = (start + plan.chunk_size).min(all_cmds.len());
                        let pool = self.clone();
                        let all_cmds = Arc::clone(&all_cmds);

                        tasks.spawn(async move {
                            let mut pooled = pool.acquire_system().await?;
                            let run_result = {
                                let conn = pooled.get_mut()?;
                                conn.pipeline_execute_count_ast_with_mode(
                                    &all_cmds[start..end],
                                    AstPipelineMode::Auto,
                                )
                                .await
                            };
                            pooled.release().await;
                            run_result
                        });
                    }

                    let mut total = 0usize;
                    while let Some(joined) = tasks.join_next().await {
                        match joined {
                            Ok(Ok(count)) => {
                                total += count;
                            }
                            Ok(Err(err)) => return Err(err),
                            Err(err) => {
                                return Err(PgError::Connection(format!(
                                    "auto pool worker join failed: {err}"
                                )));
                            }
                        }
                    }
                    total
                }
            }
        };

        Ok((completed, plan))
    }

    /// Execute commands with runtime auto strategy.
    #[inline]
    pub async fn execute_count_auto(&self, cmds: &[qail_core::ast::Qail]) -> PgResult<usize> {
        let (completed, _plan) = self.execute_count_auto_with_plan(cmds).await?;
        Ok(completed)
    }

    /// Get comprehensive pool statistics.
    pub async fn stats(&self) -> PoolStats {
        let idle = self.inner.connections.lock().await.len();
        let active = self.inner.active_count.load(Ordering::Relaxed);
        let used_slots = self
            .inner
            .config
            .max_connections
            .saturating_sub(self.inner.semaphore.available_permits());
        PoolStats {
            active,
            idle,
            pending: used_slots.saturating_sub(active),
            max_size: self.inner.config.max_connections,
            total_created: self.inner.total_created.load(Ordering::Relaxed),
        }
    }

    /// Check if the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Relaxed)
    }

    /// Close the pool gracefully.
    ///
    /// Rejects new acquires immediately, then waits up to `acquire_timeout`
    /// for in-flight connections to be released before dropping idle
    /// connections. Connections released after closure are destroyed by
    /// `return_connection` and not returned to the idle queue.
    pub async fn close(&self) {
        self.close_graceful(self.inner.config.acquire_timeout).await;
    }

    /// Close the pool gracefully with an explicit drain timeout.
    pub async fn close_graceful(&self, drain_timeout: Duration) {
        self.inner.closed.store(true, Ordering::Relaxed);
        // Wake blocked acquires immediately so shutdown doesn't wait on acquire_timeout.
        self.inner.semaphore.close();

        let deadline = Instant::now() + drain_timeout;
        loop {
            let active = self.inner.active_count.load(Ordering::Relaxed);
            if active == 0 {
                break;
            }
            if Instant::now() >= deadline {
                tracing::warn!(
                    active_connections = active,
                    timeout_ms = drain_timeout.as_millis() as u64,
                    "pool_close_drain_timeout: forcing idle cleanup while active connections remain"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        let mut connections = self.inner.connections.lock().await;
        let dropped_idle = connections.len();
        connections.clear();
        tracing::info!(
            dropped_idle_connections = dropped_idle,
            active_connections = self.inner.active_count.load(Ordering::Relaxed),
            "pool_closed"
        );
    }

    /// Create a new connection using the pool configuration.
    async fn create_connection(config: &PoolConfig) -> PgResult<PgConnection> {
        if !config.auth_settings.has_any_password_method()
            && config.mtls.is_none()
            && config.password.is_some()
        {
            return Err(PgError::Auth(
                "Invalid PoolConfig: all password auth methods are disabled".to_string(),
            ));
        }

        let options = ConnectOptions {
            tls_mode: config.tls_mode,
            gss_enc_mode: config.gss_enc_mode,
            tls_ca_cert_pem: config.tls_ca_cert_pem.clone(),
            mtls: config.mtls.clone(),
            gss_token_provider: config.gss_token_provider,
            gss_token_provider_ex: config.gss_token_provider_ex.clone(),
            auth: config.auth_settings,
            startup_params: Vec::new(),
        };

        if let Some(remaining) = gss_circuit_remaining_open(config) {
            metrics::counter!("qail_pg_gss_circuit_open_total").increment(1);
            tracing::warn!(
                host = %config.host,
                port = config.port,
                user = %config.user,
                db = %config.database,
                remaining_ms = remaining.as_millis() as u64,
                "gss_connect_circuit_open"
            );
            return Err(PgError::Connection(format!(
                "GSS connection circuit is open; retry after {:?}",
                remaining
            )));
        }

        let mut attempt = 0usize;
        loop {
            let connect_result = tokio::time::timeout(
                config.connect_timeout,
                PgConnection::connect_with_options(
                    &config.host,
                    config.port,
                    &config.user,
                    &config.database,
                    config.password.as_deref(),
                    options.clone(),
                ),
            )
            .await;

            let connect_result = match connect_result {
                Ok(result) => result,
                Err(_) => Err(PgError::Timeout(format!(
                    "connect timeout after {:?} (pool config connect_timeout)",
                    config.connect_timeout
                ))),
            };

            match connect_result {
                Ok(conn) => {
                    metrics::counter!("qail_pg_pool_connect_success_total").increment(1);
                    gss_circuit_record_success(config);
                    return Ok(conn);
                }
                Err(err) if should_retry_gss_connect_error(config, attempt, &err) => {
                    metrics::counter!("qail_pg_gss_connect_retries_total").increment(1);
                    gss_circuit_record_failure(config);
                    let delay = gss_retry_delay(config.gss_retry_base_delay, attempt);
                    tracing::warn!(
                        host = %config.host,
                        port = config.port,
                        user = %config.user,
                        db = %config.database,
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis() as u64,
                        error = %err,
                        "gss_connect_retry"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(err) => {
                    metrics::counter!("qail_pg_pool_connect_failures_total").increment(1);
                    if should_track_gss_circuit_error(config, &err) {
                        metrics::counter!("qail_pg_gss_connect_failures_total").increment(1);
                        gss_circuit_record_failure(config);
                    }
                    return Err(err);
                }
            }
        }
    }

    /// Run one maintenance cycle: evict stale idle connections and backfill
    /// to `min_connections`. Called periodically by `spawn_pool_maintenance`.
    pub async fn maintain(&self) {
        if self.inner.closed.load(Ordering::Relaxed) {
            return;
        }

        // Phase 1: Evict idle and expired connections from the pool.
        let evicted = {
            let mut connections = self.inner.connections.lock().await;
            let before = connections.len();
            connections.retain(|pooled| {
                if pooled.last_used.elapsed() > self.inner.config.idle_timeout {
                    record_pool_connection_destroy("idle_sweep_evict");
                    return false;
                }
                if let Some(max_life) = self.inner.config.max_lifetime
                    && pooled.created_at.elapsed() > max_life
                {
                    record_pool_connection_destroy("lifetime_sweep_evict");
                    return false;
                }
                true
            });
            before - connections.len()
        };

        if evicted > 0 {
            tracing::debug!(evicted, "pool_maintenance: evicted stale idle connections");
        }

        // Phase 2: Backfill to min_connections if below threshold.
        let min = self.inner.config.min_connections;
        if min == 0 {
            return;
        }

        let idle_count = self.inner.connections.lock().await.len();
        if idle_count >= min {
            return;
        }

        let deficit = min - idle_count;
        let mut created = 0usize;
        for _ in 0..deficit {
            match Self::create_connection(&self.inner.config).await {
                Ok(conn) => {
                    self.inner.total_created.fetch_add(1, Ordering::Relaxed);
                    let mut connections = self.inner.connections.lock().await;
                    if connections.len() < self.inner.config.max_connections {
                        connections.push(PooledConn {
                            conn,
                            created_at: Instant::now(),
                            last_used: Instant::now(),
                        });
                        created += 1;
                    } else {
                        // Pool filled by concurrent acquires; stop backfill.
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "pool_maintenance: backfill connection failed");
                    break; // Transient failure — retry next cycle.
                }
            }
        }

        if created > 0 {
            tracing::debug!(
                created,
                min_connections = min,
                "pool_maintenance: backfilled idle connections"
            );
        }
    }
}

/// Spawn a background task that periodically maintains pool health.
///
/// Runs every `idle_timeout / 2` (min 5s): evicts stale idle connections and
/// backfills to `min_connections`. Call once after `PgPool::connect`.
pub fn spawn_pool_maintenance(pool: PgPool) {
    let interval_secs = std::cmp::max(pool.inner.config.idle_timeout.as_secs() / 2, 5);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;
            if pool.is_closed() {
                break;
            }
            pool.maintain().await;
        }
    });
}

pub(super) fn validate_pool_config(config: &PoolConfig) -> PgResult<()> {
    if config.max_connections == 0 {
        return Err(PgError::Connection(
            "Invalid PoolConfig: max_connections must be >= 1".to_string(),
        ));
    }
    if config.min_connections > config.max_connections {
        return Err(PgError::Connection(format!(
            "Invalid PoolConfig: min_connections ({}) must be <= max_connections ({})",
            config.min_connections, config.max_connections
        )));
    }
    if config.acquire_timeout.is_zero() {
        return Err(PgError::Connection(
            "Invalid PoolConfig: acquire_timeout must be > 0".to_string(),
        ));
    }
    if config.connect_timeout.is_zero() {
        return Err(PgError::Connection(
            "Invalid PoolConfig: connect_timeout must be > 0".to_string(),
        ));
    }
    if config.leaked_cleanup_queue == 0 {
        return Err(PgError::Connection(
            "Invalid PoolConfig: leaked_cleanup_queue must be >= 1".to_string(),
        ));
    }
    Ok(())
}

pub(super) async fn execute_simple_with_timeout(
    conn: &mut PgConnection,
    sql: &str,
    timeout: Duration,
    operation: &str,
) -> PgResult<()> {
    match tokio::time::timeout(timeout, conn.execute_simple(sql)).await {
        Ok(result) => result,
        Err(_) => {
            conn.mark_io_desynced();
            Err(PgError::Timeout(format!(
                "{} timeout after {:?} (pool config connect_timeout)",
                operation, timeout
            )))
        }
    }
}
