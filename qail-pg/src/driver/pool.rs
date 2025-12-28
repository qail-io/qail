//! PostgreSQL Connection Pool
//!
//! Provides connection pooling for efficient resource management.
//! Connections are reused across queries to avoid reconnection overhead.

use super::{PgConnection, PgError, PgResult};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};

/// Connection pool configuration.
#[derive(Clone)]
pub struct PoolConfig {
    /// Host address
    pub host: String,
    /// Port number
    pub port: u16,
    /// Username
    pub user: String,
    /// Database name
    pub database: String,
    /// Password (optional for trust mode)
    pub password: Option<String>,
    /// Maximum number of connections
    pub max_connections: usize,
    /// Minimum number of idle connections to maintain
    pub min_connections: usize,
    /// Maximum time a connection can be idle before being closed
    pub idle_timeout: Duration,
    /// Maximum time to wait for a connection from the pool
    pub acquire_timeout: Duration,
    /// Maximum time to wait when establishing a new connection
    pub connect_timeout: Duration,
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
        }
    }

    /// Set password for authentication.
    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    /// Set maximum connections.
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
}

/// A pooled connection with creation timestamp for idle tracking.
struct PooledConn {
    conn: PgConnection,
    #[allow(dead_code)]
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
            // Return connection to pool
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
}

impl PgPoolInner {
    async fn return_connection(&self, conn: PgConnection) {
        let mut connections = self.connections.lock().await;
        if connections.len() < self.config.max_connections {
            connections.push(PooledConn {
                conn,
                created_at: Instant::now(),
                last_used: Instant::now(),
            });
        }
        // Connection dropped if pool is full
        self.semaphore.add_permits(1);
    }

    /// Get a healthy connection from the pool, or None if pool is empty.
    async fn get_healthy_connection(&self) -> Option<PgConnection> {
        let mut connections = self.connections.lock().await;

        while let Some(pooled) = connections.pop() {
            // Check if connection is too old (idle timeout)
            if pooled.last_used.elapsed() > self.config.idle_timeout {
                // Connection is stale, drop it
                continue;
            }

            // Return the connection
            return Some(pooled.conn);
        }

        None
    }
}

/// PostgreSQL connection pool.
///
/// # Example
/// ```ignore
/// let config = PoolConfig::new("localhost", 5432, "user", "db")
///     .password("secret")
///     .max_connections(20);
///
/// let pool = PgPool::connect(config).await?;
///
/// // Get a connection from the pool
/// let mut conn = pool.acquire().await?;
/// conn.simple_query("SELECT 1").await?;
/// // Connection automatically returned when dropped
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

        // Create initial connections (they go to idle pool)
        let mut initial_connections = Vec::new();
        for _ in 0..config.min_connections {
            let conn = Self::create_connection(&config).await?;
            initial_connections.push(PooledConn {
                conn,
                created_at: Instant::now(),
                last_used: Instant::now(),
            });
        }

        // NOTE: Don't acquire permits for initial connections!
        // They are idle (available), not in-use.
        // Permits are only consumed when acquire() is called.

        let inner = Arc::new(PgPoolInner {
            config,
            connections: Mutex::new(initial_connections),
            semaphore,
        });

        Ok(Self { inner })
    }

    /// Acquire a connection from the pool.
    ///
    /// Waits if all connections are in use (up to acquire_timeout).
    /// Stale connections (idle > idle_timeout) are automatically discarded.
    /// Connection is automatically returned when dropped.
    pub async fn acquire(&self) -> PgResult<PooledConnection> {
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
            // Create new connection
            Self::create_connection(&self.inner.config).await?
        };

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.inner.clone(),
        })
    }

    /// Get the current number of idle connections.
    pub async fn idle_count(&self) -> usize {
        self.inner.connections.lock().await.len()
    }

    /// Get the maximum number of connections.
    pub fn max_connections(&self) -> usize {
        self.inner.config.max_connections
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
