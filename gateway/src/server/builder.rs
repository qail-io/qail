use std::sync::Arc;

use axum::routing::MethodRouter;

use super::{Gateway, GatewayState};
use crate::config::GatewayConfig;
use crate::error::GatewayError;

/// Builder for the Gateway
pub struct GatewayBuilder {
    config: GatewayConfig,
    custom_routes: Vec<(String, MethodRouter<Arc<GatewayState>>)>,
}

impl GatewayBuilder {
    /// Create a new builder with default config
    pub fn new() -> Self {
        Self {
            config: GatewayConfig::default(),
            custom_routes: Vec::new(),
        }
    }

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

    /// Override rate limiter settings (requests per second, burst capacity)
    pub fn rate_limit(mut self, rate: f64, burst: u32) -> Self {
        self.config.rate_limit_rate = rate;
        self.config.rate_limit_burst = burst;
        self
    }

    /// Register a custom handler that overrides or extends auto-REST routes.
    ///
    /// Custom handlers merge AFTER auto-REST, so they take precedence.
    ///
    /// # Example
    /// ```ignore
    /// use axum::routing::post;
    ///
    /// let gateway = Gateway::builder()
    ///     .database("postgres://...")
    ///     .schema("schema.qail")
    ///     .extend("/api/orders/:id/pay", post(custom_payment_handler))
    ///     .extend("/api/reports/daily", get(daily_report_handler))
    ///     .build_and_init()
    ///     .await?;
    /// ```
    pub fn extend(
        mut self,
        path: impl Into<String>,
        handler: MethodRouter<Arc<GatewayState>>,
    ) -> Self {
        self.custom_routes.push((path.into(), handler));
        self
    }

    /// Build the gateway
    pub fn build(self) -> Gateway {
        let mut gw = Gateway::new(self.config);
        gw.custom_routes = self.custom_routes;
        gw
    }

    /// Build and initialize the gateway
    ///
    /// # Errors
    /// Returns error if initialization fails
    pub async fn build_and_init(self) -> Result<Gateway, GatewayError> {
        let mut gateway = self.build();
        gateway.init().await?;
        Ok(gateway)
    }
}

impl Default for GatewayBuilder {
    fn default() -> Self {
        Self::new()
    }
}
