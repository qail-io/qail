//! Gateway configuration

use serde::Deserialize;
use std::time::Duration;

/// Main gateway configuration
#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    /// Database connection URL
    pub database_url: String,
    
    /// Path to schema file (optional)
    pub schema_path: Option<String>,
    
    /// Path to policies file (optional)
    pub policy_path: Option<String>,
    
    /// Server bind address
    pub bind_address: String,
    
    /// Enable CORS
    #[serde(default = "default_true")]
    pub cors_enabled: bool,
    
    /// Enable query caching
    #[serde(default = "default_true")]
    pub cache_enabled: bool,
    
    /// Maximum cache entries
    #[serde(default = "default_cache_max")]
    pub cache_max_entries: usize,
    
    /// Cache TTL in seconds
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_seconds: u64,
}

fn default_true() -> bool { true }
fn default_cache_max() -> usize { 1000 }
fn default_cache_ttl() -> u64 { 60 }

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost/qail".to_string(),
            schema_path: None,
            policy_path: None,
            bind_address: "0.0.0.0:8080".to_string(),
            cors_enabled: true,
            cache_enabled: true,
            cache_max_entries: 1000,
            cache_ttl_seconds: 60,
        }
    }
}

impl GatewayConfig {
    /// Get cache configuration
    pub fn cache_config(&self) -> crate::cache::CacheConfig {
        crate::cache::CacheConfig {
            enabled: self.cache_enabled,
            max_entries: self.cache_max_entries,
            ttl: Duration::from_secs(self.cache_ttl_seconds),
        }
    }

    /// Create gateway config from centralized `QailConfig`.
    ///
    /// Maps `[postgres]`, `[gateway]`, and `[project]` sections.
    pub fn from_qail_config(qail: &qail_core::config::QailConfig) -> Self {
        let (bind_address, cors_enabled, policy_path, cache_enabled, cache_max_entries, cache_ttl_seconds) =
            if let Some(ref gw) = qail.gateway {
                let (ce, cme, cts) = if let Some(ref cache) = gw.cache {
                    (cache.enabled, cache.max_entries, cache.ttl_secs)
                } else {
                    (true, 1000, 60)
                };
                (gw.bind.clone(), gw.cors, gw.policy.clone(), ce, cme, cts)
            } else {
                ("0.0.0.0:8080".to_string(), true, None, true, 1000, 60)
            };

        Self {
            database_url: qail.postgres.url.clone(),
            schema_path: qail.project.schema.clone(),
            policy_path,
            bind_address,
            cors_enabled,
            cache_enabled,
            cache_max_entries,
            cache_ttl_seconds,
        }
    }
}

impl GatewayConfig {
    /// Create a new configuration builder
    pub fn builder() -> GatewayConfigBuilder {
        GatewayConfigBuilder::default()
    }
}

/// Builder for GatewayConfig
#[derive(Debug, Default)]
pub struct GatewayConfigBuilder {
    config: GatewayConfig,
}

impl GatewayConfigBuilder {
    /// Set the database URL
    pub fn database(mut self, url: impl Into<String>) -> Self {
        self.config.database_url = url.into();
        self
    }
    
    /// Set the schema path
    pub fn schema(mut self, path: impl Into<String>) -> Self {
        self.config.schema_path = Some(path.into());
        self
    }
    
    /// Set the policy path
    pub fn policy(mut self, path: impl Into<String>) -> Self {
        self.config.policy_path = Some(path.into());
        self
    }
    
    /// Set the bind address
    pub fn bind(mut self, addr: impl Into<String>) -> Self {
        self.config.bind_address = addr.into();
        self
    }
    
    /// Build the configuration
    pub fn build(self) -> GatewayConfig {
        self.config
    }
}
