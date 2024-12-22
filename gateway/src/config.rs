//! Gateway configuration

use serde::Deserialize;

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
}

fn default_true() -> bool { true }

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost/qail".to_string(),
            schema_path: None,
            policy_path: None,
            bind_address: "0.0.0.0:8080".to_string(),
            cors_enabled: true,
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
