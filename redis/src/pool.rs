//! Connection pooling for Redis.
//!
//! Manages a pool of RedisDriver connections for concurrent access.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use tokio::sync::{Mutex, Semaphore};

use crate::driver::RedisDriver;
use crate::error::{RedisError, RedisResult};

/// Pool configuration.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of connections.
    pub max_connections: usize,
    /// Redis host.
    pub host: String,
    /// Redis port.
    pub port: u16,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            host: "127.0.0.1".to_string(),
            port: 6379,
        }
    }
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

    /// Set max connections.
    pub fn max_connections(mut self, n: usize) -> Self {
        self.max_connections = n;
        self
    }
}

/// Redis connection pool.
pub struct RedisPool {
    config: PoolConfig,
    connections: Arc<Mutex<VecDeque<RedisDriver>>>,
    semaphore: Arc<Semaphore>,
}

impl RedisPool {
    /// Create a new connection pool.
    pub fn new(config: PoolConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_connections));
        Self {
            config,
            connections: Arc::new(Mutex::new(VecDeque::new())),
            semaphore,
        }
    }

    /// Get a connection from the pool.
    pub async fn get(&self) -> RedisResult<PooledConnection> {
        // Acquire permit
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| RedisError::Pool("Failed to acquire pool permit".into()))?;

        // Try to get existing connection
        let driver = {
            let mut conns = self.connections.lock().await;
            conns.pop_front()
        };

        let driver = match driver {
            Some(d) => d,
            None => {
                // Create new connection
                RedisDriver::connect(&self.config.host, self.config.port).await?
            }
        };

        Ok(PooledConnection {
            driver: Some(driver),
            pool: self.connections.clone(),
            _permit: permit,
        })
    }
}

/// A pooled connection that returns to the pool on drop.
pub struct PooledConnection {
    driver: Option<RedisDriver>,
    pool: Arc<Mutex<VecDeque<RedisDriver>>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Deref for PooledConnection {
    type Target = RedisDriver;

    fn deref(&self) -> &Self::Target {
        self.driver.as_ref().unwrap()
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.driver.as_mut().unwrap()
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(driver) = self.driver.take() {
            let pool = self.pool.clone();
            tokio::spawn(async move {
                let mut conns = pool.lock().await;
                conns.push_back(driver);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 6379);
    }
}
