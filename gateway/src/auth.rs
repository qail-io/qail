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
    /// Tenant ID embedded in the token.
    #[serde(default)]
    pub tenant_id: Option<String>,
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
    /// Tenant ID (may be resolved after JWT decode).
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// Extra JWT claims passed through for downstream use.
    #[serde(default)]
    pub claims: HashMap<String, serde_json::Value>,
}

pub(crate) fn canonical_json_value(value: &serde_json::Value) -> String {
    let mut out = String::new();
    write_canonical_json_value(value, &mut out);
    out
}

fn write_canonical_json_value(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => match serde_json::to_string(value) {
            Ok(serialized) => out.push_str(&serialized),
            Err(_) => out.push_str("null"),
        },
        serde_json::Value::Array(items) => {
            out.push('[');
            for (idx, item) in items.iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                write_canonical_json_value(item, out);
            }
            out.push(']');
        }
        serde_json::Value::Object(map) => {
            out.push('{');
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort_unstable();
            for (idx, key) in keys.into_iter().enumerate() {
                if idx > 0 {
                    out.push(',');
                }
                match serde_json::to_string(key) {
                    Ok(serialized) => out.push_str(&serialized),
                    Err(_) => out.push_str("\"\""),
                }
                out.push(':');
                if let Some(value) = map.get(key) {
                    write_canonical_json_value(value, out);
                } else {
                    out.push_str("null");
                }
            }
            out.push('}');
        }
    }
}

#[cfg(test)]
mod tests;
