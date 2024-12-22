//! Gateway error types

use thiserror::Error;

/// Main error type for the gateway
#[derive(Debug, Error)]
pub enum GatewayError {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),
    
    /// Schema loading error
    #[error("Failed to load schema: {0}")]
    Schema(String),
    
    /// Policy loading error
    #[error("Failed to load policy: {0}")]
    Policy(String),
    
    /// Database connection error
    #[error("Database error: {0}")]
    Database(String),
    
    /// Authentication error
    #[error("Authentication failed: {0}")]
    Auth(String),
    
    /// Authorization error (policy violation)
    #[error("Access denied: {0}")]
    AccessDenied(String),
    
    /// Query validation error
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
    
    /// Internal server error
    #[error("Internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl GatewayError {
    /// Get HTTP status code for this error
    pub fn status_code(&self) -> u16 {
        match self {
            Self::Config(_) => 500,
            Self::Schema(_) => 500,
            Self::Policy(_) => 500,
            Self::Database(_) => 503,
            Self::Auth(_) => 401,
            Self::AccessDenied(_) => 403,
            Self::InvalidQuery(_) => 400,
            Self::Internal(_) => 500,
        }
    }
}
