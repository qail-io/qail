//! Connection pool for Qdrant driver.
//!
//! Provides a simple connection pool that manages multiple HTTP clients
//! with keep-alive for better performance under concurrent load.

use crate::error::{QdrantError, QdrantResult};
use crate::QdrantDriver;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of concurrent connections.
    pub max_connections: usize,
    /// Connection timeout in seconds.
    pub connect_timeout_secs: u64,
    /// Request timeout in seconds.
    pub request_timeout_secs: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            connect_timeout_secs: 5,
            request_timeout_secs: 30,
        }
    }
}

/// Connection pool for Qdrant.
///
/// Uses a semaphore to limit concurrent connections and shares
/// an underlying reqwest Client with connection pooling.
///
/// # Example
/// ```ignore
/// use qail_qdrant::pool::{QdrantPool, PoolConfig};
///
/// let pool = QdrantPool::connect("localhost", 6333, PoolConfig::default()).await?;
///
/// // Use the pool (it will manage concurrency)
/// let driver = pool.get().await?;
/// let results = driver.search(&cmd).await?;
/// ```
pub struct QdrantPool {
    driver: Arc<QdrantDriver>,
    semaphore: Arc<Semaphore>,
    #[allow(dead_code)]
    config: PoolConfig,
}

impl QdrantPool {
    /// Create a new connection pool.
    pub async fn connect(host: &str, port: u16, config: PoolConfig) -> QdrantResult<Self> {
        let driver = QdrantDriver::connect(host, port).await?;
        
        Ok(Self {
            driver: Arc::new(driver),
            semaphore: Arc::new(Semaphore::new(config.max_connections)),
            config,
        })
    }

    /// Connect with address string.
    pub async fn connect_addr(addr: &str, config: PoolConfig) -> QdrantResult<Self> {
        let parts: Vec<&str> = addr.split(':').collect();
        let host = parts.first().unwrap_or(&"localhost");
        let port: u16 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(6333);
        Self::connect(host, port, config).await
    }

    /// Get a connection from the pool.
    ///
    /// This acquires a permit from the semaphore, limiting concurrency.
    /// The permit is released when the guard is dropped.
    pub async fn get(&self) -> QdrantResult<PooledConnection<'_>> {
        let permit = self.semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| QdrantError::Connection(e.to_string()))?;
        
        Ok(PooledConnection {
            driver: &self.driver,
            _permit: permit,
        })
    }

    /// Get the underlying driver directly (ignores pool limits).
    ///
    /// Use this for single-threaded scenarios or when you manage
    /// concurrency yourself.
    pub fn driver(&self) -> &QdrantDriver {
        &self.driver
    }

    /// Number of available permits (connections not in use).
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }
}

/// A pooled connection that releases back to the pool on drop.
pub struct PooledConnection<'a> {
    driver: &'a QdrantDriver,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl<'a> std::ops::Deref for PooledConnection<'a> {
    type Target = QdrantDriver;

    fn deref(&self) -> &Self::Target {
        self.driver
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.connect_timeout_secs, 5);
        assert_eq!(config.request_timeout_secs, 30);
    }
}
