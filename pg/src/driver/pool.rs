//! PostgreSQL Connection Pool
//!
//! Provides connection pooling for efficient resource management.
//! Connections are reused across queries to avoid reconnection overhead.

use super::{PgConnection, PgError, PgResult};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};

/// Configuration for a PostgreSQL connection pool.
///
/// Use the builder pattern to customise settings:
///
/// ```ignore
/// use std::time::Duration;
/// use qail_pg::driver::pool::PoolConfig;
/// let config = PoolConfig::new("localhost", 5432, "app", "mydb")
///     .password("secret")
///     .max_connections(20)
///     .acquire_timeout(Duration::from_secs(5));
/// ```
#[derive(Clone)]
pub struct PoolConfig {
    /// PostgreSQL server hostname or IP address.
    pub host: String,
    /// PostgreSQL server port (default: 5432).
    pub port: u16,
    /// Database role / user name.
    pub user: String,
    /// Target database name.
    pub database: String,
    /// Optional password for authentication.
    pub password: Option<String>,
    /// Hard upper limit on simultaneous connections (default: 10).
    pub max_connections: usize,
    /// Minimum idle connections kept warm in the pool (default: 1).
    pub min_connections: usize,
    /// Close idle connections after this duration (default: 10 min).
    pub idle_timeout: Duration,
    /// Maximum time to wait when acquiring a connection (default: 30s).
    pub acquire_timeout: Duration,
    /// TCP connect timeout for new connections (default: 10s).
    pub connect_timeout: Duration,
    /// Optional maximum lifetime of any connection in the pool.
    pub max_lifetime: Option<Duration>,
    /// When `true`, run a health check (`SELECT 1`) before handing out a connection.
    pub test_on_acquire: bool,
}

impl PoolConfig {
    /// Create a new pool configuration with sensible defaults.
    ///
    /// # Arguments
    ///
    /// * `host` — PostgreSQL server hostname or IP.
    /// * `port` — TCP port (typically 5432).
    /// * `user` — PostgreSQL role name.
    /// * `database` — Target database name.
    pub fn new(host: &str, port: u16, user: &str, database: &str) -> Self {
        Self {
            host: host.to_string(),
            port,
            user: user.to_string(),
            database: database.to_string(),
            password: None,
            max_connections: 10,
            min_connections: 1,
            idle_timeout: Duration::from_secs(600), // 10 minutes
            acquire_timeout: Duration::from_secs(30), // 30 seconds
            connect_timeout: Duration::from_secs(10), // 10 seconds
            max_lifetime: None,                      // No limit by default
            test_on_acquire: false,                  // Disabled by default for performance
        }
    }

    /// Set password for authentication.
    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    /// Set maximum simultaneous connections.
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Set minimum idle connections.
    pub fn min_connections(mut self, min: usize) -> Self {
        self.min_connections = min;
        self
    }

    /// Set idle timeout (connections idle longer than this are closed).
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Set acquire timeout (max wait time when getting a connection).
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set connect timeout (max time to establish new connection).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set maximum lifetime of a connection before recycling.
    pub fn max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = Some(lifetime);
        self
    }

    /// Enable connection validation on acquire.
    pub fn test_on_acquire(mut self, enabled: bool) -> Self {
        self.test_on_acquire = enabled;
        self
    }

    /// Create a `PoolConfig` from a centralized `QailConfig`.
    ///
    /// Parses `postgres.url` for host/port/user/database/password
    /// and applies pool tuning from `[postgres]` section.
    pub fn from_qail_config(qail: &qail_core::config::QailConfig) -> PgResult<Self> {
        let pg = &qail.postgres;
        let (host, port, user, database, password) = parse_pg_url(&pg.url)?;

        let mut config = PoolConfig::new(&host, port, &user, &database)
            .max_connections(pg.max_connections)
            .min_connections(pg.min_connections)
            .idle_timeout(Duration::from_secs(pg.idle_timeout_secs))
            .acquire_timeout(Duration::from_secs(pg.acquire_timeout_secs))
            .connect_timeout(Duration::from_secs(pg.connect_timeout_secs))
            .test_on_acquire(pg.test_on_acquire);

        if let Some(ref pw) = password {
            config = config.password(pw);
        }

        Ok(config)
    }
}

/// Parse a postgres URL into (host, port, user, database, password).
fn parse_pg_url(url: &str) -> PgResult<(String, u16, String, String, Option<String>)> {
    let url = url.trim_start_matches("postgres://").trim_start_matches("postgresql://");

    let (credentials, host_part) = if url.contains('@') {
        let mut parts = url.splitn(2, '@');
        let creds = parts.next().unwrap_or("");
        let host = parts.next().unwrap_or("localhost/postgres");
        (Some(creds), host)
    } else {
        (None, url)
    };

    let (host_port, database) = if host_part.contains('/') {
        let mut parts = host_part.splitn(2, '/');
        (parts.next().unwrap_or("localhost"), parts.next().unwrap_or("postgres").to_string())
    } else {
        (host_part, "postgres".to_string())
    };

    let (host, port) = if host_port.contains(':') {
        let mut parts = host_port.split(':');
        let h = parts.next().unwrap_or("localhost").to_string();
        let p = parts.next().and_then(|s| s.parse().ok()).unwrap_or(5432u16);
        (h, p)
    } else {
        (host_port.to_string(), 5432u16)
    };

    let (user, password) = if let Some(creds) = credentials {
        if creds.contains(':') {
            let mut parts = creds.splitn(2, ':');
            let u = parts.next().unwrap_or("postgres").to_string();
            let p = parts.next().map(|s| s.to_string());
            (u, p)
        } else {
            (creds.to_string(), None)
        }
    } else {
        ("postgres".to_string(), None)
    };

    Ok((host, port, user, database, password))
}

/// Pool statistics for monitoring.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Connections currently checked out by callers.
    pub active: usize,
    /// Connections idle in the pool, ready for reuse.
    pub idle: usize,
    /// Callers waiting for a connection.
    pub pending: usize,
    /// Maximum connections configured
    pub max_size: usize,
    /// Cumulative connections created since pool startup.
    pub total_created: usize,
}

/// A pooled connection with creation timestamp for idle tracking.
struct PooledConn {
    conn: PgConnection,
    created_at: Instant,
    last_used: Instant,
}

/// A pooled connection that returns to the pool when dropped.
///
/// When `rls_dirty` is true (set by `acquire_with_rls`), the connection
/// will automatically reset RLS session variables before returning to
/// the pool. This prevents cross-tenant data leakage.
pub struct PooledConnection {
    conn: Option<PgConnection>,
    pool: Arc<PgPoolInner>,
    rls_dirty: bool,
}

impl PooledConnection {
    /// Get a reference to the underlying connection, returning an error
    /// if the connection has already been released.
    fn conn_ref(&self) -> PgResult<&PgConnection> {
        self.conn.as_ref().ok_or_else(|| PgError::Connection(
            "Connection already released back to pool".into()
        ))
    }

    /// Get a mutable reference to the underlying connection, returning an error
    /// if the connection has already been released.
    fn conn_mut(&mut self) -> PgResult<&mut PgConnection> {
        self.conn.as_mut().ok_or_else(|| PgError::Connection(
            "Connection already released back to pool".into()
        ))
    }

    /// Get a mutable reference to the underlying connection.
    /// Panics if the connection has been released (use `conn_mut()` for fallible access).
    pub fn get_mut(&mut self) -> &mut PgConnection {
        // SAFETY: Connection is always Some while PooledConnection is in use.
        // Only becomes None after release() or Drop, after which no methods should be called.
        self.conn
            .as_mut()
            .expect("Connection should always be present")
    }

    /// Get a token to cancel the currently running query.
    pub fn cancel_token(&self) -> PgResult<crate::driver::CancelToken> {
        let conn = self.conn_ref()?;
        let (process_id, secret_key) = conn.get_cancel_key();
        Ok(crate::driver::CancelToken {
            host: self.pool.config.host.clone(),
            port: self.pool.config.port,
            process_id,
            secret_key,
        })
    }

    /// Deterministic connection cleanup and pool return.
    ///
    /// This is the **correct** way to return a connection to the pool.
    /// COMMITs the transaction (which auto-resets transaction-local RLS
    /// session variables) and returns the connection to the pool with
    /// prepared statement caches intact.
    ///
    /// If cleanup fails, the connection is destroyed (not returned to pool).
    ///
    /// # Usage
    /// ```ignore
    /// let mut conn = pool.acquire_with_rls(ctx).await?;
    /// let result = conn.fetch_all_cached(&cmd).await;
    /// conn.release().await; // COMMIT + return to pool
    /// result
    /// ```
    pub async fn release(mut self) {
        if let Some(mut conn) = self.conn.take() {
            // COMMIT the transaction opened by acquire_with_rls.
            // Transaction-local set_config values auto-reset on COMMIT,
            // so no explicit RLS cleanup is needed.
            // Prepared statements survive — they are NOT transaction-scoped.
            if let Err(e) = conn.execute_simple(super::rls::reset_sql()).await {
                eprintln!(
                    "[CRITICAL] pool_release_failed: COMMIT failed — \
                     dropping connection to prevent state leak: {}",
                    e
                );
                return; // Connection destroyed — not returned to pool
            }

            self.pool.return_connection(conn).await;
        }
    }

    /// Execute a QAIL command and fetch all rows (UNCACHED).
    /// Returns rows with column metadata for JSON serialization.
    pub async fn fetch_all_uncached(&mut self, cmd: &qail_core::ast::Qail) -> PgResult<Vec<super::PgRow>> {
        use crate::protocol::AstEncoder;
        use super::ColumnInfo;

        let conn = self.conn_mut()?;

        let wire_bytes = AstEncoder::encode_cmd_reuse(
            cmd,
            &mut conn.sql_buf,
            &mut conn.params_buf,
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

        conn.send_bytes(&wire_bytes).await?;

        let mut rows: Vec<super::PgRow> = Vec::new();
        let mut column_info: Option<Arc<ColumnInfo>> = None;
        let mut error: Option<PgError> = None;

        loop {
            let msg = conn.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    column_info = Some(Arc::new(ColumnInfo::from_fields(&fields)));
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(super::PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::Query(err.message));
                    }
                }
                _ => {}
            }
        }
    }

    /// Execute a QAIL command and fetch all rows (FAST VERSION).
    /// Uses native AST-to-wire encoding and optimized recv_with_data_fast.
    /// Skips column metadata for maximum speed.
    pub async fn fetch_all_fast(&mut self, cmd: &qail_core::ast::Qail) -> PgResult<Vec<super::PgRow>> {
        use crate::protocol::AstEncoder;

        let conn = self.conn_mut()?;

        AstEncoder::encode_cmd_reuse_into(
            cmd,
            &mut conn.sql_buf,
            &mut conn.params_buf,
            &mut conn.write_buf,
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

        conn.flush_write_buf().await?;

        let mut rows: Vec<super::PgRow> = Vec::with_capacity(32);
        let mut error: Option<PgError> = None;

        loop {
            let res = conn.recv_with_data_fast().await;
            match res {
                Ok((msg_type, data)) => {
                    match msg_type {
                        b'D' => {
                            if error.is_none() && let Some(columns) = data {
                                rows.push(super::PgRow {
                                    columns,
                                    column_info: None,
                                });
                            }
                        }
                        b'Z' => {
                            if let Some(err) = error {
                                return Err(err);
                            }
                            return Ok(rows);
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    if error.is_none() {
                        error = Some(e);
                    }
                }
            }
        }
    }

    /// Execute a QAIL command and fetch all rows (CACHED).
    /// Uses prepared statement caching: Parse+Describe on first call,
    /// then Bind+Execute only on subsequent calls with the same SQL shape.
    /// This matches PostgREST's behavior for fair benchmarks.
    pub async fn fetch_all_cached(&mut self, cmd: &qail_core::ast::Qail) -> PgResult<Vec<super::PgRow>> {
        use super::ColumnInfo;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let conn = self.conn.as_mut().ok_or_else(|| PgError::Connection(
            "Connection already released back to pool".into()
        ))?;

        conn.sql_buf.clear();
        conn.params_buf.clear();

        // Encode SQL + params to reusable buffers
        match cmd.action {
            qail_core::ast::Action::Get | qail_core::ast::Action::With => {
                crate::protocol::ast_encoder::dml::encode_select(cmd, &mut conn.sql_buf, &mut conn.params_buf).ok();
            }
            qail_core::ast::Action::Add => {
                crate::protocol::ast_encoder::dml::encode_insert(cmd, &mut conn.sql_buf, &mut conn.params_buf).ok();
            }
            qail_core::ast::Action::Set => {
                crate::protocol::ast_encoder::dml::encode_update(cmd, &mut conn.sql_buf, &mut conn.params_buf).ok();
            }
            qail_core::ast::Action::Del => {
                crate::protocol::ast_encoder::dml::encode_delete(cmd, &mut conn.sql_buf, &mut conn.params_buf).ok();
            }
            _ => {
                // Fallback: unsupported actions go through uncached path
                return self.fetch_all_uncached(cmd).await;
            }
        }

        let mut hasher = DefaultHasher::new();
        conn.sql_buf.hash(&mut hasher);
        let sql_hash = hasher.finish();

        let is_cache_miss = !conn.stmt_cache.contains(&sql_hash);

        conn.write_buf.clear();

        let stmt_name = if let Some(name) = conn.stmt_cache.get(&sql_hash) {
            name.clone()
        } else {
            let name = format!("qail_{:x}", sql_hash);

            conn.evict_prepared_if_full();

            let sql_str = std::str::from_utf8(&conn.sql_buf).unwrap_or("");

            use crate::protocol::PgEncoder;
            let parse_msg = PgEncoder::encode_parse(&name, sql_str, &[]);
            let describe_msg = PgEncoder::encode_describe(false, &name);
            conn.write_buf.extend_from_slice(&parse_msg);
            conn.write_buf.extend_from_slice(&describe_msg);

            conn.stmt_cache.put(sql_hash, name.clone());
            conn.prepared_statements.insert(name.clone(), sql_str.to_string());

            // Register in global hot-statement registry for cross-connection sharing
            if let Ok(mut hot) = self.pool.hot_statements.write()
                && hot.len() < MAX_HOT_STATEMENTS
            {
                hot.insert(sql_hash, (name.clone(), sql_str.to_string()));
            }

            name
        };

        use crate::protocol::PgEncoder;
        PgEncoder::encode_bind_to(&mut conn.write_buf, &stmt_name, &conn.params_buf)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut conn.write_buf);
        PgEncoder::encode_sync_to(&mut conn.write_buf);

        conn.flush_write_buf().await?;

        let cached_column_info = conn.column_info_cache.get(&sql_hash).cloned();

        let mut rows: Vec<super::PgRow> = Vec::with_capacity(32);
        let mut column_info: Option<Arc<ColumnInfo>> = cached_column_info;
        let mut error: Option<PgError> = None;

        loop {
            let msg = conn.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::ParameterDescription(_) => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    let info = Arc::new(ColumnInfo::from_fields(&fields));
                    if is_cache_miss {
                        conn.column_info_cache.insert(sql_hash, info.clone());
                    }
                    column_info = Some(info);
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(super::PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::Query(err.message));
                    }
                }
                _ => {}
            }
        }
    }

    /// Execute a QAIL command with RLS context in a SINGLE roundtrip.
    ///
    /// Pipelines the RLS setup (BEGIN + set_config) and the query
    /// (Parse/Bind/Execute/Sync) into one `write_all` syscall.
    /// PG processes messages in order, so the BEGIN + set_config
    /// completes before the query executes — security is preserved.
    ///
    /// Wire layout:
    /// ```text
    /// [SimpleQuery: "BEGIN; SET LOCAL...; SELECT set_config(...)"]
    /// [Parse (if cache miss)]
    /// [Describe (if cache miss)]
    /// [Bind]
    /// [Execute]
    /// [Sync]
    /// ```
    ///
    /// Response processing: consume 2× ReadyForQuery (SimpleQuery + Sync).
    pub async fn fetch_all_with_rls(
        &mut self,
        cmd: &qail_core::ast::Qail,
        rls_sql: &str,
    ) -> PgResult<Vec<super::PgRow>> {
        use super::ColumnInfo;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let conn = self.conn.as_mut().ok_or_else(|| PgError::Connection(
            "Connection already released back to pool".into()
        ))?;

        conn.sql_buf.clear();
        conn.params_buf.clear();

        // Encode SQL + params to reusable buffers
        if cmd.is_raw_sql() {
            // Raw SQL pass-through: write verbatim, RLS context already set above
            conn.sql_buf.clear();
            conn.params_buf.clear();
            conn.sql_buf.extend_from_slice(cmd.table.as_bytes());
        } else {
        match cmd.action {
            qail_core::ast::Action::Get | qail_core::ast::Action::With => {
                crate::protocol::ast_encoder::dml::encode_select(cmd, &mut conn.sql_buf, &mut conn.params_buf).ok();
            }
            qail_core::ast::Action::Add => {
                crate::protocol::ast_encoder::dml::encode_insert(cmd, &mut conn.sql_buf, &mut conn.params_buf).ok();
            }
            qail_core::ast::Action::Set => {
                crate::protocol::ast_encoder::dml::encode_update(cmd, &mut conn.sql_buf, &mut conn.params_buf).ok();
            }
            qail_core::ast::Action::Del => {
                crate::protocol::ast_encoder::dml::encode_delete(cmd, &mut conn.sql_buf, &mut conn.params_buf).ok();
            }
            _ => {
                // Fallback: RLS setup must happen synchronously for unsupported actions
                conn.execute_simple(rls_sql).await?;
                self.rls_dirty = true;
                return self.fetch_all_uncached(cmd).await;
            }
        }
        }

        let mut hasher = DefaultHasher::new();
        conn.sql_buf.hash(&mut hasher);
        let sql_hash = hasher.finish();

        let is_cache_miss = !conn.stmt_cache.contains(&sql_hash);

        conn.write_buf.clear();

        // ── Prepend RLS Simple Query message ─────────────────────────
        // This is the key optimization: RLS setup bytes go first in the
        // same buffer as the query messages.
        let rls_msg = crate::protocol::PgEncoder::encode_query_string(rls_sql);
        conn.write_buf.extend_from_slice(&rls_msg);

        // ── Then append the query messages (same as fetch_all_cached) ──
        let stmt_name = if let Some(name) = conn.stmt_cache.get(&sql_hash) {
            name.clone()
        } else {
            let name = format!("qail_{:x}", sql_hash);

            conn.evict_prepared_if_full();

            let sql_str = std::str::from_utf8(&conn.sql_buf).unwrap_or("");

            use crate::protocol::PgEncoder;
            let parse_msg = PgEncoder::encode_parse(&name, sql_str, &[]);
            let describe_msg = PgEncoder::encode_describe(false, &name);
            conn.write_buf.extend_from_slice(&parse_msg);
            conn.write_buf.extend_from_slice(&describe_msg);

            conn.stmt_cache.put(sql_hash, name.clone());
            conn.prepared_statements.insert(name.clone(), sql_str.to_string());

            if let Ok(mut hot) = self.pool.hot_statements.write()
                && hot.len() < MAX_HOT_STATEMENTS
            {
                hot.insert(sql_hash, (name.clone(), sql_str.to_string()));
            }

            name
        };

        use crate::protocol::PgEncoder;
        PgEncoder::encode_bind_to(&mut conn.write_buf, &stmt_name, &conn.params_buf)
            .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut conn.write_buf);
        PgEncoder::encode_sync_to(&mut conn.write_buf);

        // ── Single write_all for RLS + Query ────────────────────────
        conn.flush_write_buf().await?;

        // Mark connection as RLS-dirty (needs COMMIT on release)
        self.rls_dirty = true;

        // ── Phase 1: Consume Simple Query responses (RLS setup) ─────
        // Simple Query produces: CommandComplete × N, then ReadyForQuery.
        // set_config results and BEGIN/SET LOCAL responses are all here.
        let mut rls_error: Option<PgError> = None;
        loop {
            let msg = conn.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    // RLS setup done — break to Extended Query phase
                    if let Some(err) = rls_error {
                        return Err(err);
                    }
                    break;
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if rls_error.is_none() {
                        rls_error = Some(PgError::Query(err.message));
                    }
                }
                // CommandComplete, DataRow (from set_config), RowDescription — ignore
                _ => {}
            }
        }

        // ── Phase 2: Consume Extended Query responses (actual data) ──
        let cached_column_info = conn.column_info_cache.get(&sql_hash).cloned();

        let mut rows: Vec<super::PgRow> = Vec::with_capacity(32);
        let mut column_info: Option<std::sync::Arc<ColumnInfo>> = cached_column_info;
        let mut error: Option<PgError> = None;

        loop {
            let msg = conn.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::ParameterDescription(_) => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    let info = std::sync::Arc::new(ColumnInfo::from_fields(&fields));
                    if is_cache_miss {
                        conn.column_info_cache.insert(sql_hash, info.clone());
                    }
                    column_info = Some(info);
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(super::PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::Query(err.message));
                    }
                }
                _ => {}
            }
        }
    }

    /// Execute multiple QAIL commands in a single PG pipeline round-trip.
    ///
    /// Sends all queries as Parse+Bind+Execute in one write, receives all
    /// responses in one read. Returns raw column data per query per row.
    ///
    /// This is the fastest path for batch operations — amortizes TCP
    /// overhead across N queries into a single syscall pair.
    pub async fn pipeline_ast(
        &mut self,
        cmds: &[qail_core::ast::Qail],
    ) -> PgResult<Vec<Vec<Vec<Option<Vec<u8>>>>>> {
        let conn = self.conn_mut()?;
        conn.pipeline_ast(cmds).await
    }

    /// Run `EXPLAIN (FORMAT JSON)` on a Qail command and return cost estimates.
    ///
    /// Uses `simple_query` under the hood — no additional round-trips beyond
    /// the single EXPLAIN statement. Returns `None` if parsing fails or
    /// the EXPLAIN output is unexpected.
    pub async fn explain_estimate(
        &mut self,
        cmd: &qail_core::ast::Qail,
    ) -> PgResult<Option<super::explain::ExplainEstimate>> {
        use qail_core::transpiler::ToSql;

        let sql = cmd.to_sql();
        let explain_sql = format!("EXPLAIN (FORMAT JSON) {}", sql);

        let rows = self.simple_query(&explain_sql).await?;

        // PostgreSQL returns the JSON plan as a single text column across one or more rows
        let mut json_output = String::new();
        for row in &rows {
            if let Some(Some(val)) = row.columns.first()
                && let Ok(text) = std::str::from_utf8(val)
            {
                json_output.push_str(text);
            }
        }

        Ok(super::explain::parse_explain_json(&json_output))
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if self.conn.is_some() {
            // Safety net: connection was NOT released via `release()`.
            // This happens when:
            //   - Handler panicked
            //   - Early return without calling release()
            //   - Missed release() call (programming error)
            //
            // We DESTROY the connection (don't return to pool) to prevent
            // dirty session state from being reused. This costs a pool slot
            // but guarantees no cross-tenant leakage.
            //
            // The `conn` field is dropped here, closing the TCP socket.
            eprintln!(
                "[WARN] pool_connection_leaked: PooledConnection dropped without release() — \
                 connection destroyed to prevent state leak (rls_dirty={}). \
                 Use conn.release().await for deterministic cleanup.",
                self.rls_dirty
            );
            // Decrement active count so pool can create a replacement
            self.pool.active_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

impl std::ops::Deref for PooledConnection {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        // SAFETY: Connection is always Some while PooledConnection is alive and in use.
        // Only becomes None after release() consumes self, or during Drop.
        self.conn
            .as_ref()
            .expect("PooledConnection::deref called after release — this is a bug")
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: Connection is always Some while PooledConnection is alive and in use.
        // Only becomes None after release() consumes self, or during Drop.
        self.conn
            .as_mut()
            .expect("PooledConnection::deref_mut called after release — this is a bug")
    }
}

/// Maximum number of hot statements to track globally.
const MAX_HOT_STATEMENTS: usize = 32;

/// Inner pool state (shared across clones).
struct PgPoolInner {
    config: PoolConfig,
    connections: Mutex<Vec<PooledConn>>,
    semaphore: Semaphore,
    closed: AtomicBool,
    active_count: AtomicUsize,
    total_created: AtomicUsize,
    /// Global registry of frequently-used prepared statements.
    /// Maps sql_hash → (stmt_name, sql_text).
    /// New connections pre-prepare these on checkout for instant cache hits.
    hot_statements: std::sync::RwLock<std::collections::HashMap<u64, (String, String)>>,
}

impl PgPoolInner {
    async fn return_connection(&self, conn: PgConnection) {

        self.active_count.fetch_sub(1, Ordering::Relaxed);
        

        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        
        let mut connections = self.connections.lock().await;
        if connections.len() < self.config.max_connections {
            connections.push(PooledConn {
                conn,
                created_at: Instant::now(),
                last_used: Instant::now(),
            });
        }

        self.semaphore.add_permits(1);
    }

    /// Get a healthy connection from the pool, or None if pool is empty.
    async fn get_healthy_connection(&self) -> Option<PgConnection> {
        let mut connections = self.connections.lock().await;

        while let Some(pooled) = connections.pop() {
            if pooled.last_used.elapsed() > self.config.idle_timeout {
                // Connection is stale, drop it
                continue;
            }

            if let Some(max_life) = self.config.max_lifetime
                && pooled.created_at.elapsed() > max_life
            {
                // Connection exceeded max lifetime, recycle it
                continue;
            }

            return Some(pooled.conn);
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
    inner: Arc<PgPoolInner>,
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

        // Wait for available slot with timeout
        let acquire_timeout = self.inner.config.acquire_timeout;
        let permit = tokio::time::timeout(acquire_timeout, self.inner.semaphore.acquire())
            .await
            .map_err(|_| {
                PgError::Timeout(format!(
                    "pool acquire after {}s ({} max connections)",
                    acquire_timeout.as_secs(),
                    self.inner.config.max_connections
                ))
            })?
            .map_err(|_| PgError::PoolClosed)?;
        permit.forget();

        // Try to get existing healthy connection
        let mut conn = if let Some(conn) = self.inner.get_healthy_connection().await {
            conn
        } else {
            let conn = Self::create_connection(&self.inner.config).await?;
            self.inner.total_created.fetch_add(1, Ordering::Relaxed);
            conn
        };

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
                let parse_msg = PgEncoder::encode_parse(name, sql, &[]);
                buf.extend_from_slice(&parse_msg);
            }
            PgEncoder::encode_sync_to(&mut buf);
            if conn.send_bytes(&buf).await.is_ok() {
                // Drain responses (ParseComplete + ReadyForQuery)
                loop {
                    match conn.recv().await {
                        Ok(crate::protocol::BackendMessage::ReadyForQuery(_)) => break,
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
                // Register in local cache
                for (hash, name, sql) in &missing {
                    conn.stmt_cache.put(*hash, name.clone());
                    conn.prepared_statements.insert(name.clone(), sql.clone());
                }
            }
        }

        self.inner.active_count.fetch_add(1, Ordering::Relaxed);

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.inner.clone(),
            rls_dirty: false,
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
    ///     RlsContext::operator("550e8400-e29b-41d4-a716-446655440000")
    /// ).await?;
    /// // All queries through `conn` are now scoped to this operator
    /// ```
    pub async fn acquire_with_rls(
        &self,
        ctx: qail_core::rls::RlsContext,
    ) -> PgResult<PooledConnection> {
        // SAFETY: RLS context is set immediately below via context_to_sql().
        let mut conn = self.acquire_raw().await?;

        // Set RLS context on the raw connection
        let sql = super::rls::context_to_sql(&ctx);
        let pg_conn = conn.get_mut();
        pg_conn.execute_simple(&sql).await?;

        // Mark dirty so Drop resets context before pool return
        conn.rls_dirty = true;

        Ok(conn)
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
        let sql = super::rls::context_to_sql_with_timeout(&ctx, timeout_ms);
        let pg_conn = conn.get_mut();
        pg_conn.execute_simple(&sql).await?;

        // Mark dirty so Drop resets context + timeout before pool return
        conn.rls_dirty = true;

        Ok(conn)
    }

    /// Acquire a connection for system-level operations (no tenant context).
    ///
    /// Sets RLS session variables to maximally restrictive values:
    /// - `app.current_operator_id = ''`
    /// - `app.current_agent_id = ''`  
    /// - `app.is_super_admin = false`
    ///
    /// Use this for startup introspection, migrations, and health checks
    /// that must not operate within any tenant scope.
    pub async fn acquire_system(&self) -> PgResult<PooledConnection> {
        let ctx = qail_core::rls::RlsContext::empty();
        self.acquire_with_rls(ctx).await
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
        self.acquire_with_rls(qail_core::rls::RlsContext::tenant(tenant_id)).await
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
            let sql = super::branch_sql::branch_context_sql(branch_name);
            let pg_conn = conn.get_mut();
            pg_conn.execute_simple(&sql).await?;
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

    /// Get comprehensive pool statistics.
    pub async fn stats(&self) -> PoolStats {
        let idle = self.inner.connections.lock().await.len();
        PoolStats {
            active: self.inner.active_count.load(Ordering::Relaxed),
            idle,
            pending: self.inner.config.max_connections
                - self.inner.semaphore.available_permits()
                - self.active_count(),
            max_size: self.inner.config.max_connections,
            total_created: self.inner.total_created.load(Ordering::Relaxed),
        }
    }

    /// Check if the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Relaxed)
    }

    /// Close the pool gracefully.
    pub async fn close(&self) {
        self.inner.closed.store(true, Ordering::Relaxed);

        let mut connections = self.inner.connections.lock().await;
        connections.clear();
    }

    /// Create a new connection using the pool configuration.
    async fn create_connection(config: &PoolConfig) -> PgResult<PgConnection> {
        match &config.password {
            Some(password) => {
                PgConnection::connect_with_password(
                    &config.host,
                    config.port,
                    &config.user,
                    &config.database,
                    Some(password),
                )
                .await
            }
            None => {
                PgConnection::connect(&config.host, config.port, &config.user, &config.database)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config() {
        let config = PoolConfig::new("localhost", 5432, "user", "testdb")
            .password("secret123")
            .max_connections(20)
            .min_connections(5);

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.user, "user");
        assert_eq!(config.database, "testdb");
        assert_eq!(config.password, Some("secret123".to_string()));
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.min_connections, 5);
    }

    #[test]
    fn test_pool_config_defaults() {
        let config = PoolConfig::new("localhost", 5432, "user", "testdb");
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.min_connections, 1);
        assert_eq!(config.idle_timeout, Duration::from_secs(600));
        assert_eq!(config.acquire_timeout, Duration::from_secs(30));
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert!(config.password.is_none());
    }

    #[test]
    fn test_pool_config_builder_chaining() {
        let config = PoolConfig::new("db.example.com", 5433, "admin", "prod")
            .password("p@ss")
            .max_connections(50)
            .min_connections(10)
            .idle_timeout(Duration::from_secs(300))
            .acquire_timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(3))
            .max_lifetime(Duration::from_secs(3600))
            .test_on_acquire(false);

        assert_eq!(config.host, "db.example.com");
        assert_eq!(config.port, 5433);
        assert_eq!(config.max_connections, 50);
        assert_eq!(config.min_connections, 10);
        assert_eq!(config.idle_timeout, Duration::from_secs(300));
        assert_eq!(config.acquire_timeout, Duration::from_secs(5));
        assert_eq!(config.connect_timeout, Duration::from_secs(3));
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(3600)));
        assert!(!config.test_on_acquire);
    }

    #[test]
    fn test_timeout_error_display() {
        let err = PgError::Timeout("pool acquire after 30s (10 max connections)".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Timeout"));
        assert!(msg.contains("30s"));
        assert!(msg.contains("10 max connections"));
    }

    #[test]
    fn test_pool_closed_error_display() {
        let err = PgError::PoolClosed;
        assert_eq!(err.to_string(), "Connection pool is closed");
    }

    #[test]
    fn test_pool_exhausted_error_display() {
        let err = PgError::PoolExhausted { max: 20 };
        let msg = err.to_string();
        assert!(msg.contains("exhausted"));
        assert!(msg.contains("20"));
    }

    #[test]
    fn test_io_error_source_chaining() {
        use std::error::Error;
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "peer reset");
        let pg_err = PgError::Io(io_err);
        // source() should return the inner io::Error
        let source = pg_err.source().expect("Io variant should have source");
        assert!(source.to_string().contains("peer reset"));
    }

    #[test]
    fn test_non_io_errors_have_no_source() {
        use std::error::Error;
        assert!(PgError::Connection("test".into()).source().is_none());
        assert!(PgError::Query("test".into()).source().is_none());
        assert!(PgError::Timeout("test".into()).source().is_none());
        assert!(PgError::PoolClosed.source().is_none());
        assert!(PgError::NoRows.source().is_none());
    }

    #[test]
    fn test_io_error_from_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        let pg_err: PgError = io_err.into();
        assert!(matches!(pg_err, PgError::Io(_)));
        assert!(pg_err.to_string().contains("broken"));
    }

    #[test]
    fn test_error_variants_are_distinct() {
        // Ensure we can match on each variant for programmatic error handling
        let errors: Vec<PgError> = vec![
            PgError::Connection("conn".into()),
            PgError::Protocol("proto".into()),
            PgError::Auth("auth".into()),
            PgError::Query("query".into()),
            PgError::NoRows,
            PgError::Io(std::io::Error::new(std::io::ErrorKind::Other, "io")),
            PgError::Encode("enc".into()),
            PgError::Timeout("timeout".into()),
            PgError::PoolExhausted { max: 10 },
            PgError::PoolClosed,
        ];
        // All 10 variants produce non-empty display strings
        for err in &errors {
            assert!(!err.to_string().is_empty());
        }
        assert_eq!(errors.len(), 10);
    }
}

