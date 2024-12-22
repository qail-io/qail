//! Authentication middleware
//!
//! Handles JWT validation and user context extraction.

use crate::error::GatewayError;
use axum::http::HeaderMap;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// JWT claims structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject (user ID)
    pub sub: String,
    pub exp: usize,
    /// Role
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// User context extracted from authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthContext {
    /// User ID
    pub user_id: String,
    
    /// User role (for policy evaluation)
    pub role: String,
    
    /// Tenant/organization ID (for multi-tenancy)
    #[serde(default)]
    pub tenant_id: Option<String>,
    
    /// Additional claims from JWT
    #[serde(default)]
    pub claims: HashMap<String, serde_json::Value>,
}

impl AuthContext {
    /// Create an anonymous context (for public queries)
    pub fn anonymous() -> Self {
        Self {
            user_id: "anonymous".to_string(),
            role: "anonymous".to_string(),
            tenant_id: None,
            claims: HashMap::new(),
        }
    }
    
    /// Check if user has a specific role
    pub fn has_role(&self, role: &str) -> bool {
        self.role == role
    }
    
    /// Check if user is authenticated (not anonymous)
    pub fn is_authenticated(&self) -> bool {
        self.user_id != "anonymous"
    }
}

/// JWT configuration
#[derive(Debug, Clone)]
pub struct JwtConfig {
    /// Secret key for HS256
    pub secret: Option<String>,
    /// Public key for RS256 (PEM format)
    pub public_key: Option<String>,
    /// Algorithm (HS256, RS256, etc.)
    pub algorithm: Algorithm,
    /// Issuer to validate
    pub issuer: Option<String>,
    /// Audience to validate
    pub audience: Option<String>,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            secret: None,
            public_key: None,
            algorithm: Algorithm::HS256,
            issuer: None,
            audience: None,
        }
    }
}

/// Validate a JWT token and extract auth context
pub fn validate_jwt(token: &str, config: &JwtConfig) -> Result<AuthContext, GatewayError> {
    let decoding_key = match config.algorithm {
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 => {
            let secret = config.secret.as_ref()
                .ok_or_else(|| GatewayError::Auth("JWT secret not configured".to_string()))?;
            DecodingKey::from_secret(secret.as_bytes())
        }
        Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512 => {
            let key = config.public_key.as_ref()
                .ok_or_else(|| GatewayError::Auth("JWT public key not configured".to_string()))?;
            DecodingKey::from_rsa_pem(key.as_bytes())
                .map_err(|e| GatewayError::Auth(format!("Invalid RSA key: {}", e)))?
        }
        _ => return Err(GatewayError::Auth("Unsupported JWT algorithm".to_string())),
    };
    
    let mut validation = Validation::new(config.algorithm);
    
    if let Some(ref issuer) = config.issuer {
        validation.set_issuer(&[issuer]);
    }
    if let Some(ref audience) = config.audience {
        validation.set_audience(&[audience]);
    }
    
    let token_data = decode::<JwtClaims>(token, &decoding_key, &validation)
        .map_err(|e| GatewayError::Auth(format!("Invalid token: {}", e)))?;
    
    let claims = token_data.claims;
    
    Ok(AuthContext {
        user_id: claims.sub,
        role: claims.role.unwrap_or_else(|| "user".to_string()),
        tenant_id: claims.tenant_id,
        claims: claims.extra,
    })
}

/// Extract auth context from request headers
/// 
/// Priority:
/// 1. Authorization: Bearer <jwt> (if JWT_SECRET is set)
/// 2. X-User-ID / X-User-Role headers (for dev/testing)
pub fn extract_auth_from_headers(headers: &HeaderMap) -> AuthContext {
    // Try JWT first
    if let Some(auth_header) = headers.get("authorization") {
        if let Ok(value) = auth_header.to_str() {
            if let Some(token) = value.strip_prefix("Bearer ") {
                // Check if JWT is configured via env
                if let Ok(secret) = std::env::var("JWT_SECRET") {
                    let config = JwtConfig {
                        secret: Some(secret),
                        algorithm: Algorithm::HS256,
                        ..Default::default()
                    };
                    
                    match validate_jwt(token, &config) {
                        Ok(auth) => {
                            tracing::debug!("JWT validated: user={}", auth.user_id);
                            return auth;
                        }
                        Err(e) => {
                            tracing::warn!("JWT validation failed: {}", e);
                            // Fall through to header-based auth
                        }
                    }
                }
            }
        }
    }
    
    // Header-based auth (for development/testing)
    let user_id = headers
        .get("x-user-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    
    let role = headers
        .get("x-user-role")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("anonymous")
        .to_string();
    
    let tenant_id = headers
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    
    AuthContext {
        user_id,
        role,
        tenant_id,
        claims: HashMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    
    #[test]
    fn test_jwt_validation() {
        let secret = "test-secret-key-12345";
        let claims = JwtClaims {
            sub: "user123".to_string(),
            exp: (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize,
            role: Some("admin".to_string()),
            tenant_id: Some("tenant1".to_string()),
            extra: HashMap::new(),
        };
        
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        ).unwrap();
        
        let config = JwtConfig {
            secret: Some(secret.to_string()),
            algorithm: Algorithm::HS256,
            ..Default::default()
        };
        
        let auth = validate_jwt(&token, &config).unwrap();
        assert_eq!(auth.user_id, "user123");
        assert_eq!(auth.role, "admin");
        assert_eq!(auth.tenant_id, Some("tenant1".to_string()));
    }
}
