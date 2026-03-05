//! Authentication middleware
//!
//! Handles JWT validation and user context extraction.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

mod context;
mod extract;
mod jwt;
mod policy;

pub use extract::{
    extract_auth_for_state, extract_auth_from_headers, extract_auth_from_headers_with_jwks,
};
pub use jwt::{JwtConfig, parse_allowed_algorithms, validate_jwt};
pub use policy::{authenticate_request, ensure_request_auth, ensure_tenant_rate_limit};

/// JWT claims structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Accepts both standard JWT "sub" and engine-style "user_id"
    #[serde(alias = "user_id")]
    pub sub: String,
    /// Token expiration time (Unix timestamp).
    pub exp: usize,
    /// User role (e.g. `"admin"`, `"operator"`).
    #[serde(default)]
    pub role: Option<String>,
    /// Tenant / operator ID embedded in the token.
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// Engine-style: operator_id directly in JWT claims
    #[serde(default)]
    pub operator_id: Option<String>,
    /// Additional claims not captured by named fields.
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// User context extracted from authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthContext {
    /// Authenticated user identifier.
    pub user_id: String,
    /// User role string.
    pub role: String,
    /// Tenant / operator ID (may be resolved after JWT decode).
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// Extra JWT claims passed through for downstream use.
    #[serde(default)]
    pub claims: HashMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests;
