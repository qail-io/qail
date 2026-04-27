//! Connection pool for Qdrant gRPC driver.
//!
//! Maintains a pool of idle connections and reuses them across requests.
//! Limits concurrency via semaphore and returns connections to the idle
//! queue on drop.

use crate::driver::QdrantDriver;
use crate::error::{QdrantError, QdrantResult};
use http::Uri;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

/// Configuration for the connection pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of concurrent connections.
    pub max_connections: usize,
    /// Host to connect to.
    pub host: String,
    /// gRPC port (default 6334).
    pub port: u16,
    /// Whether to use TLS (rustls).
    pub tls: bool,
}

impl PoolConfig {
    /// Create a new pool configuration.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            max_connections: 10,
            host: host.into(),
            port,
            tls: false,
        }
    }

    /// Enable TLS for connections.
    pub fn tls(mut self, enabled: bool) -> Self {
        self.tls = enabled;
        self
    }

    /// Set maximum connections.
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Create config from centralized `QailConfig`.
    ///
    /// Reads `[qdrant]` section; returns `None` if section is absent.
    /// Uses gRPC endpoint (port 6334) by default.
    pub fn from_qail_config(qail: &qail_core::config::QailConfig) -> Option<Self> {
        let qdrant = qail.qdrant.as_ref()?;
        Some(Self::from_qail_config_ref(qdrant))
    }

    /// Create config directly from a `&QdrantConfig` reference.
    ///
    /// Used by the gateway which already has the config extracted.
    pub fn from_qail_config_ref(qdrant: &qail_core::config::QdrantConfig) -> Self {
        let use_tls = qdrant
            .tls
            .unwrap_or_else(|| qdrant.url.starts_with("https://"));

        let (host, mut port) = if let Some(ref grpc) = qdrant.grpc {
            parse_endpoint_host_port(grpc, 6334)
        } else {
            parse_endpoint_host_port(&qdrant.url, 6334)
        };
        if qdrant.grpc.is_none() && port == 6333 {
            // `qdrant.url` is commonly an HTTP endpoint; when grpc is omitted,
            // avoid accidentally dialing REST port 6333 with the gRPC client.
            port = 6334;
        }

        Self {
            max_connections: qdrant.max_connections,
            host,
            port,
            tls: use_tls,
        }
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            host: "localhost".to_string(),
            port: 6334,
            tls: false,
        }
    }
}

/// Shared pool internals behind an Arc for cheap cloning.
struct PoolInner {
    config: PoolConfig,
    /// Idle connections ready for reuse.
    idle: Mutex<Vec<QdrantDriver>>,
    /// Semaphore limiting total simultaneous connections.
    semaphore: Semaphore,
}

/// Connection pool for Qdrant gRPC driver.
///
/// Maintains a pool of idle connections and reuses them instead of
/// creating a new TCP+H2 handshake per request. Connections are
/// returned to the idle queue when the `PooledConnection` is dropped.
///
/// # Example
/// ```ignore
/// use qail_qdrant::{QdrantPool, PoolConfig};
///
/// let pool = QdrantPool::new(
///     PoolConfig::new("localhost", 6334).max_connections(20)
/// ).await?;
///
/// // Get a connection from the pool (reuses idle connections)
/// let mut conn = pool.get().await?;
/// let results = conn.search("products", &embedding, 10, None).await?;
/// // conn is returned to the pool when dropped
/// ```
#[derive(Clone)]
pub struct QdrantPool {
    inner: Arc<PoolInner>,
}

impl QdrantPool {
    /// Create a new connection pool.
    pub async fn new(config: PoolConfig) -> QdrantResult<Self> {
        let max = config.max_connections;
        Ok(Self {
            inner: Arc::new(PoolInner {
                config,
                idle: Mutex::new(Vec::with_capacity(max)),
                semaphore: Semaphore::new(max),
            }),
        })
    }

    /// Get a connection from the pool.
    ///
    /// Returns an idle connection if available, otherwise creates a new one.
    /// The semaphore limits total connections to `max_connections`.
    pub async fn get(&self) -> QdrantResult<PooledConnection> {
        let permit = self
            .inner
            .semaphore
            .acquire()
            .await
            .map_err(|e| QdrantError::Connection(format!("Semaphore closed: {}", e)))?;

        // Try to take an idle connection
        let driver = {
            let mut idle = self.inner.idle.lock().await;
            idle.pop()
        };

        let driver = match driver {
            Some(d) => d,
            None => {
                // No idle connection — create a new one
                if self.inner.config.tls {
                    QdrantDriver::connect_tls(&self.inner.config.host, self.inner.config.port)
                        .await?
                } else {
                    QdrantDriver::connect(&self.inner.config.host, self.inner.config.port).await?
                }
            }
        };

        // Forget the permit — we'll manually add it back in PooledConnection::drop
        permit.forget();

        Ok(PooledConnection {
            driver: Some(driver),
            pool: Arc::clone(&self.inner),
        })
    }

    /// Number of idle connections waiting for reuse.
    pub async fn idle_count(&self) -> usize {
        self.inner.idle.lock().await.len()
    }

    /// Number of available permits (connections not in use).
    pub fn available(&self) -> usize {
        self.inner.semaphore.available_permits()
    }

    /// Maximum number of connections.
    pub fn max_connections(&self) -> usize {
        self.inner.config.max_connections
    }
}

/// A pooled connection that returns itself to the idle queue on drop.
pub struct PooledConnection {
    driver: Option<QdrantDriver>,
    pool: Arc<PoolInner>,
}

impl std::ops::Deref for PooledConnection {
    type Target = QdrantDriver;

    fn deref(&self) -> &Self::Target {
        match self.driver.as_ref() {
            Some(driver) => driver,
            None => unreachable!("driver taken after drop"),
        }
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self.driver.as_mut() {
            Some(driver) => driver,
            None => unreachable!("driver taken after drop"),
        }
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(driver) = self.driver.take() {
            if let Ok(mut idle) = self.pool.idle.try_lock()
                && idle.len() < self.pool.config.max_connections
            {
                idle.push(driver);
            }
            // Always release a permit, even if we couldn't return to idle.
            self.pool.semaphore.add_permits(1);
        }
    }
}

fn parse_endpoint_host_port(input: &str, default_port: u16) -> (String, u16) {
    let raw = input.trim();
    if raw.is_empty() {
        return ("localhost".to_string(), default_port);
    }

    if raw.contains("://")
        && let Ok(uri) = raw.parse::<Uri>()
    {
        let host = uri.host().unwrap_or("localhost").to_string();
        let port = uri.port_u16().unwrap_or(default_port);
        return (host, port);
    }

    if raw.starts_with('[')
        && let Some(end) = raw.find(']')
    {
        let host = &raw[1..end];
        let port = raw[end + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse().ok())
            .unwrap_or(default_port);
        return (host.to_string(), port);
    }

    if let Some((host, port_str)) = raw.rsplit_once(':')
        && !host.is_empty()
        && let Ok(port) = port_str.parse::<u16>()
    {
        return (host.to_string(), port);
    }

    (raw.to_string(), default_port)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_builder() {
        let config = PoolConfig::new("localhost", 6334).max_connections(20);

        assert_eq!(config.max_connections, 20);
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 6334);
        assert!(!config.tls);

        // With TLS
        let tls_config = PoolConfig::new("cloud.qdrant.io", 6334).tls(true);
        assert!(tls_config.tls);
    }

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 6334);
    }

    #[test]
    fn test_parse_endpoint_host_port() {
        assert_eq!(
            parse_endpoint_host_port("localhost:6334", 6334),
            ("localhost".to_string(), 6334)
        );
        assert_eq!(
            parse_endpoint_host_port("https://cloud.qdrant.io:443", 6334),
            ("cloud.qdrant.io".to_string(), 443)
        );
        assert_eq!(
            parse_endpoint_host_port("[::1]:6334", 6334),
            ("::1".to_string(), 6334)
        );
        assert_eq!(
            parse_endpoint_host_port("qdrant.internal", 6334),
            ("qdrant.internal".to_string(), 6334)
        );
    }

    #[test]
    fn test_from_qail_config_ref_defaults_grpc_port_when_url_uses_rest_port() {
        let cfg = qail_core::config::QdrantConfig {
            url: "http://localhost:6333".to_string(),
            grpc: None,
            max_connections: 7,
            tls: None,
        };
        let pool = PoolConfig::from_qail_config_ref(&cfg);
        assert_eq!(pool.host, "localhost");
        assert_eq!(pool.port, 6334);
        assert_eq!(pool.max_connections, 7);
        assert!(!pool.tls);
    }
}
