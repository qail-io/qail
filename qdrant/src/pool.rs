//! Connection pool for Qdrant gRPC driver.
//!
//! Provides efficient connection pooling with semaphore-based concurrency limiting.

use crate::error::{QdrantError, QdrantResult};
use crate::driver::QdrantDriver;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of concurrent connections.
    pub max_connections: usize,
    /// Host to connect to.
    pub host: String,
    /// gRPC port (default 6334).
    pub port: u16,
}

impl PoolConfig {
    /// Create a new pool configuration.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            max_connections: 10,
            host: host.into(),
            port,
        }
    }

    /// Set maximum connections.
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            host: "localhost".to_string(),
            port: 6334,
        }
    }
}

/// Connection pool for Qdrant gRPC driver.
///
/// Uses a semaphore to limit concurrent connections. Each connection
/// is independent and can be used concurrently.
///
/// # Example
/// ```ignore
/// use qail_qdrant::{QdrantPool, PoolConfig};
///
/// let pool = QdrantPool::new(
///     PoolConfig::new("localhost", 6334).max_connections(20)
/// ).await?;
///
/// // Get a connection from the pool
/// let mut conn = pool.get().await?;
/// let results = conn.search("products", &embedding, 10, None).await?;
/// ```
#[derive(Clone)]
pub struct QdrantPool {
    config: Arc<PoolConfig>,
    semaphore: Arc<Semaphore>,
}

impl QdrantPool {
    /// Create a new connection pool.
    pub async fn new(config: PoolConfig) -> QdrantResult<Self> {
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(config.max_connections)),
            config: Arc::new(config),
        })
    }

    /// Get a connection from the pool.
    ///
    /// This acquires a permit from the semaphore, limiting concurrency.
    /// The connection is created lazily when acquired.
    pub async fn get(&self) -> QdrantResult<PooledConnection> {
        let permit = self.semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| QdrantError::Connection(e.to_string()))?;
        
        // Create a new connection (lazy)
        let driver = QdrantDriver::connect(&self.config.host, self.config.port).await?;
        
        Ok(PooledConnection {
            driver,
            _permit: permit,
        })
    }

    /// Number of available permits (connections not in use).
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Maximum number of connections.
    pub fn max_connections(&self) -> usize {
        self.config.max_connections
    }
}

/// A pooled connection that releases back to the pool on drop.
pub struct PooledConnection {
    driver: QdrantDriver,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl std::ops::Deref for PooledConnection {
    type Target = QdrantDriver;

    fn deref(&self) -> &Self::Target {
        &self.driver
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.driver
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_builder() {
        let config = PoolConfig::new("localhost", 6334)
            .max_connections(20);
        
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 6334);
    }

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 6334);
    }
}
