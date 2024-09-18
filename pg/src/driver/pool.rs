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
pub struct PooledConnection {
    conn: Option<PgConnection>,
    pool: Arc<PgPoolInner>,
}

impl PooledConnection {
    /// Get a mutable reference to the underlying connection.
    pub fn get_mut(&mut self) -> &mut PgConnection {
        self.conn
            .as_mut()
            .expect("Connection should always be present")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let pool = self.pool.clone();
            tokio::spawn(async move {
                pool.return_connection(conn).await;
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
        })
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
