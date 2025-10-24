//! PostgreSQL Connection Pool
//!
//! Provides connection pooling for efficient resource management.
//! Connections are reused across queries to avoid reconnection overhead.

use super::{PgConnection, PgError, PgResult};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};

#[derive(Clone)]
pub struct PoolConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub password: Option<String>,
    pub max_connections: usize,
    pub min_connections: usize,
    pub idle_timeout: Duration,
    pub acquire_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_lifetime: Option<Duration>,
    pub test_on_acquire: bool,
}

impl PoolConfig {
    /// Create a new pool configuration with sensible defaults.
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
    pub active: usize,
    pub idle: usize,
    pub pending: usize,
    /// Maximum connections configured
    pub max_size: usize,
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
    /// Get a mutable reference to the underlying connection.
    pub fn get_mut(&mut self) -> &mut PgConnection {
        self.conn
            .as_mut()
            .expect("Connection should always be present")
    }

    /// Get a token to cancel the currently running query.
    pub fn cancel_token(&self) -> crate::driver::CancelToken {
        let (process_id, secret_key) = self.conn.as_ref().expect("Connection missing").get_cancel_key();
        crate::driver::CancelToken {
            host: self.pool.config.host.clone(),
            port: self.pool.config.port,
            process_id,
            secret_key,
        }
    }

    /// Execute a QAIL command and fetch all rows (UNCACHED).
    /// Returns rows with column metadata for JSON serialization.
    pub async fn fetch_all_uncached(&mut self, cmd: &qail_core::ast::Qail) -> PgResult<Vec<super::PgRow>> {
        use crate::protocol::AstEncoder;
        use super::ColumnInfo;

        let conn = self.conn.as_mut().expect("Connection should always be present");

        let wire_bytes = AstEncoder::encode_cmd_reuse(
            cmd,
            &mut conn.sql_buf,
            &mut conn.params_buf,
        );

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
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let pool = self.pool.clone();
            let rls_dirty = self.rls_dirty;
            tokio::spawn(async move {
                if rls_dirty {
                    // Reset RLS session variables before returning to pool.
                    // This prevents the next acquire() from inheriting
                    // a stale tenant context from a different request.
                    let mut conn = conn;
                    let _ = conn.execute_simple(super::rls::reset_sql()).await;
                    pool.return_connection(conn).await;
                } else {
                    pool.return_connection(conn).await;
                }
            });
        }
    }
}

impl std::ops::Deref for PooledConnection {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        self.conn
            .as_ref()
            .expect("Connection should always be present")
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn
            .as_mut()
            .expect("Connection should always be present")
    }
}

/// Inner pool state (shared across clones).
struct PgPoolInner {
    config: PoolConfig,
    connections: Mutex<Vec<PooledConn>>,
    semaphore: Semaphore,
    closed: AtomicBool,
    active_count: AtomicUsize,
    total_created: AtomicUsize,
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
/// let mut conn = pool.acquire().await?;
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
        });

        Ok(Self { inner })
    }

    /// Acquire a connection from the pool.
    pub async fn acquire(&self) -> PgResult<PooledConnection> {
        if self.inner.closed.load(Ordering::Relaxed) {
            return Err(PgError::Connection("Pool is closed".to_string()));
        }

        // Wait for available slot with timeout
        let acquire_timeout = self.inner.config.acquire_timeout;
        let permit = tokio::time::timeout(acquire_timeout, self.inner.semaphore.acquire())
            .await
            .map_err(|_| {
                PgError::Connection(format!(
                    "Timed out waiting for connection ({}s)",
                    acquire_timeout.as_secs()
                ))
            })?
            .map_err(|_| PgError::Connection("Pool closed".to_string()))?;
        permit.forget();

        // Try to get existing healthy connection
        let conn = if let Some(conn) = self.inner.get_healthy_connection().await {
            conn
        } else {
            let conn = Self::create_connection(&self.inner.config).await?;
            self.inner.total_created.fetch_add(1, Ordering::Relaxed);
            conn
        };


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
        let mut conn = self.acquire().await?;

        // Set RLS context on the raw connection
        let sql = super::rls::context_to_sql(&ctx);
        let pg_conn = conn.get_mut();
        pg_conn.execute_simple(&sql).await?;

        // Mark dirty so Drop resets context before pool return
        conn.rls_dirty = true;

        Ok(conn)
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
        let mut conn = self.acquire().await?;

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
}
