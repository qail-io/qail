//! PostgreSQL Connection Pool
//!
//! Provides connection pooling for efficient resource management.
//! Connections are reused across queries to avoid reconnection overhead.

use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use super::{PgConnection, PgResult, PgError};

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
}

impl PoolConfig {
    /// Create a new pool configuration.
    pub fn new(host: &str, port: u16, user: &str, database: &str) -> Self {
        Self {
            host: host.to_string(),
            port,
            user: user.to_string(),
            database: database.to_string(),
            password: None,
            max_connections: 10,
            min_connections: 1,
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
}

/// A pooled connection that returns to the pool when dropped.
pub struct PooledConnection {
    conn: Option<PgConnection>,
    pool: Arc<PgPoolInner>,
}

impl PooledConnection {
    /// Get a mutable reference to the underlying connection.
    pub fn get_mut(&mut self) -> &mut PgConnection {
        self.conn.as_mut().expect("Connection should always be present")
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
        self.conn.as_ref().expect("Connection should always be present")
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.conn.as_mut().expect("Connection should always be present")
    }
}

/// Inner pool state (shared across clones).
struct PgPoolInner {
    config: PoolConfig,
    connections: Mutex<Vec<PgConnection>>,
    semaphore: Semaphore,
}

impl PgPoolInner {
    async fn return_connection(&self, conn: PgConnection) {
        let mut connections = self.connections.lock().await;
        if connections.len() < self.config.max_connections {
            connections.push(conn);
        }
        // Connection dropped if pool is full
        self.semaphore.add_permits(1);
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
            initial_connections.push(conn);
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
    /// Waits if all connections are in use.
    /// Connection is automatically returned when dropped.
    pub async fn acquire(&self) -> PgResult<PooledConnection> {
        // Wait for available slot
        let permit = self.inner.semaphore.acquire().await
            .map_err(|_| PgError::Connection("Pool closed".to_string()))?;
        permit.forget();

        // Try to get existing connection
        let mut connections = self.inner.connections.lock().await;
        let conn = if let Some(conn) = connections.pop() {
            conn
        } else {
            drop(connections); // Release lock before connecting
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
                ).await
            }
            None => {
                PgConnection::connect(
                    &config.host,
                    config.port,
                    &config.user,
                    &config.database,
                ).await
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
