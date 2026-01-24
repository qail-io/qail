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

impl AuthContext {
    /// Create an unauthenticated anonymous context.
    pub fn anonymous() -> Self {
        Self {
            user_id: "anonymous".to_string(),
            role: "anonymous".to_string(),
            tenant_id: None,
            claims: HashMap::new(),
        }
    }
    
    /// Check whether the user holds the given role.
    pub fn has_role(&self, role: &str) -> bool {
        self.role == role
    }
    
    /// Returns `true` if the context represents a real (non-anonymous) user.
    pub fn is_authenticated(&self) -> bool {
        !self.user_id.is_empty() && self.user_id != "anonymous"
    }

    /// Resolve tenant_id from the user→operator cache when the JWT doesn't include it.
    ///
    /// Engine-style JWTs only contain `user_id` and `role` — the `operator_id`
    /// must be looked up from the database. This method checks the startup-loaded
    /// cache and fills in `tenant_id` if missing.
    pub async fn enrich_with_operator_map(
        &mut self,
        map: &tokio::sync::RwLock<std::collections::HashMap<String, String>>,
    ) {
        if self.tenant_id.is_none() && self.is_authenticated() {
            let guard = map.read().await;
            if let Some(operator_id) = guard.get(&self.user_id) {
                self.tenant_id = Some(operator_id.clone());
            }
        }
    }

    /// Convert gateway AuthContext to PgDriver's RlsContext for Postgres-native RLS.
    ///
    /// Mapping:
    /// - `tenant_id` → `operator_id`
    /// - `claims["agent_id"]` → `agent_id`
    /// - `role == "super_admin"` → `is_super_admin`
    pub fn to_rls_context(&self) -> qail_core::rls::RlsContext {
        // Only the platform-level "administrator" role bypasses RLS.
        // Tenant-scoped roles (operator, super_admin) use operator_id filtering.
        let is_super_admin = matches!(
            self.role.as_str(),
            "administrator" | "Administrator"
        );

        // Audit log: super_admin activation is a high-privilege event
        if is_super_admin {
            tracing::warn!(
                user_id = %self.user_id,
                tenant_id = ?self.tenant_id,
                event = "super_admin_rls_bypass",
                "SUPER_ADMIN access activated — RLS bypass enabled"
            );
            let token = qail_core::rls::SuperAdminToken::for_auth("admin_rls_bypass");
            return qail_core::rls::RlsContext::super_admin(token);
        }

        let operator_id = self.tenant_id.clone().unwrap_or_default();
        let agent_id = self
            .claims
            .get("agent_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if !agent_id.is_empty() && !operator_id.is_empty() {
            qail_core::rls::RlsContext::operator_and_agent(&operator_id, &agent_id)
        } else if !agent_id.is_empty() {
            qail_core::rls::RlsContext::agent(&agent_id)
        } else {
            qail_core::rls::RlsContext::operator(&operator_id)
        }
    }
}

/// JWT validation configuration.
#[derive(Debug, Clone)]
pub struct JwtConfig {
    /// HMAC shared secret (for HS256/HS384/HS512).
    pub secret: Option<String>,
    /// RSA/EC public key in PEM format (for RS*/ES*).
    pub public_key: Option<String>,
    /// Signing algorithm (default: HS256).
    pub algorithm: Algorithm,
    /// Expected `iss` claim (if set, tokens without it are rejected).
    pub issuer: Option<String>,
    /// Expected `aud` claim.
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

/// Decode and validate a JWT token, returning an [`AuthContext`] on success.
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
    
    // Resolve tenant_id: prefer explicit tenant_id, then operator_id from claims,
    // then check extra claims for operator_id (engine puts it in flattened extra)
    let tenant_id = claims.tenant_id
        .or(claims.operator_id)
        .or_else(|| {
            claims.extra.get("operator_id")
                .and_then(|v| v.as_str())
                .map(String::from)
        });

    Ok(AuthContext {
        user_id: claims.sub,
        role: claims.role.unwrap_or_else(|| "user".to_string()),
        tenant_id,
        claims: claims.extra,
    })
}

/// Extract auth context from request headers
/// 
/// Priority:
/// 1. `Authorization: Bearer <jwt>` (if JWT_SECRET is set)
/// 2. X-User-ID / X-User-Role headers (dev mode only)
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
                        }
                    }
                }
            }
        }
    }
    
    // Header-based auth (for development/testing ONLY)
    // SECURITY: This path allows arbitrary role spoofing via request headers.
    // In production, QAIL_DEV_MODE must NOT be set.
    let dev_mode = std::env::var("QAIL_DEV_MODE")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if !dev_mode {
        // Production: unauthenticated requests get anonymous context
        return AuthContext::anonymous();
    }

    tracing::warn!("DEV MODE: using header-based auth (X-User-ID/X-User-Role)");

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
            operator_id: None,
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

    #[test]
    fn test_engine_style_jwt() {
        // Engine JWT uses "user_id" instead of "sub" and may not have tenant_id
        let secret = "test-secret-key-12345";
        let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        
        // Simulate engine JWT payload: { "user_id": "...", "role": "SuperAdmin", "email": "..." }
        let payload = serde_json::json!({
            "user_id": "4fcc89a7-0753-4b8d-8457-71619533dbd8",
            "email": "scootsuperadmin@qail.io",
            "role": "SuperAdmin",
            "exp": exp,
            "iat": exp - 86400,
        });
        
        let token = encode(
            &Header::default(),
            &payload,
            &EncodingKey::from_secret(secret.as_bytes()),
        ).unwrap();
        
        let config = JwtConfig {
            secret: Some(secret.to_string()),
            algorithm: Algorithm::HS256,
            ..Default::default()
        };
        
        let auth = validate_jwt(&token, &config).unwrap();
        assert_eq!(auth.user_id, "4fcc89a7-0753-4b8d-8457-71619533dbd8");
        assert_eq!(auth.role, "SuperAdmin");
        // No tenant_id in engine JWT — will be resolved via user_operator_map
        assert_eq!(auth.tenant_id, None);
    }

    #[test]
    fn test_administrator_bypasses_rls() {
        // Platform-level "administrator" role bypasses RLS
        let auth = AuthContext {
            user_id: "master-user".to_string(),
            role: "administrator".to_string(),
            tenant_id: None,
            claims: HashMap::new(),
        };
        let rls = auth.to_rls_context();
        assert!(rls.bypasses_rls(), "administrator role should bypass RLS");
    }

    #[test]
    fn test_administrator_pascal_case_bypasses_rls() {
        let auth = AuthContext {
            user_id: "master-user".to_string(),
            role: "Administrator".to_string(),
            tenant_id: None,
            claims: HashMap::new(),
        };
        let rls = auth.to_rls_context();
        assert!(rls.bypasses_rls(), "Administrator (PascalCase) role should bypass RLS");
    }

    #[test]
    fn test_super_admin_does_not_bypass_rls() {
        // Tenant-level super_admin should NOT bypass RLS
        let auth = AuthContext {
            user_id: "scoot-user".to_string(),
            role: "super_admin".to_string(),
            tenant_id: Some("operator-123".to_string()),
            claims: HashMap::new(),
        };
        let rls = auth.to_rls_context();
        assert!(!rls.bypasses_rls(), "super_admin should NOT bypass RLS");
        assert_eq!(rls.operator_id, "operator-123");
    }

    #[test]
    fn test_operator_role_does_not_bypass_rls() {
        let auth = AuthContext {
            user_id: "test-user-001".to_string(),
            role: "operator".to_string(),
            tenant_id: Some("op-test-001".to_string()),
            claims: HashMap::new(),
        };
        let rls = auth.to_rls_context();
        assert!(!rls.bypasses_rls(), "operator role should NOT bypass RLS");
        assert_eq!(rls.operator_id, "op-test-001");
    }

    // ══════════════════════════════════════════════════════════════════
    // RED-TEAM: JWT Edge Cases (#6 from adversarial checklist)
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn redteam_jwt_empty_sub_field() {
        let secret = "test-secret-key-12345";
        let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        let payload = serde_json::json!({
            "sub": "",
            "exp": exp,
        });
        let token = encode(&Header::default(), &payload, &EncodingKey::from_secret(secret.as_bytes())).unwrap();
        let config = JwtConfig { secret: Some(secret.to_string()), algorithm: Algorithm::HS256, ..Default::default() };
        let auth = validate_jwt(&token, &config).unwrap();
        assert_eq!(auth.user_id, "");
        // FIXED: empty sub now correctly fails is_authenticated().
        // Previously this was a documented finding where "" != "anonymous" passed.
        assert!(!auth.is_authenticated(), "Empty sub must not pass is_authenticated");
    }

    #[test]
    fn redteam_jwt_integer_tenant_id() {
        // tenant_id: 42 (integer instead of string) — must not silently coerce
        let secret = "test-secret-key-12345";
        let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        let payload = serde_json::json!({
            "sub": "user-1",
            "exp": exp,
            "tenant_id": 42,
        });
        let token = encode(&Header::default(), &payload, &EncodingKey::from_secret(secret.as_bytes())).unwrap();
        let config = JwtConfig { secret: Some(secret.to_string()), algorithm: Algorithm::HS256, ..Default::default() };
        let result = validate_jwt(&token, &config);
        // STRONG: serde rejects the entire JWT — integer tenant_id causes parse error
        assert!(result.is_err(), "Integer tenant_id must cause JWT parse failure (not silent coercion)");
    }

    #[test]
    fn redteam_jwt_is_super_admin_claim_no_rls_bypass() {
        // Attacker injects is_super_admin=true in JWT extra claims
        let auth = AuthContext {
            user_id: "attacker".to_string(),
            role: "user".to_string(),
            tenant_id: Some("tenant-x".to_string()),
            claims: {
                let mut m = HashMap::new();
                m.insert("is_super_admin".to_string(), serde_json::json!(true));
                m
            },
        };
        let rls = auth.to_rls_context();
        assert!(!rls.bypasses_rls(), "JWT is_super_admin claim must NOT grant RLS bypass");
    }

    #[test]
    fn redteam_jwt_operator_id_resolution() {
        // Engine JWT with operator_id claim
        let secret = "test-secret-key-12345";
        let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
        let payload = serde_json::json!({
            "sub": "user-abc",
            "exp": exp,
            "operator_id": "op-123",
        });
        let token = encode(&Header::default(), &payload, &EncodingKey::from_secret(secret.as_bytes())).unwrap();
        let config = JwtConfig { secret: Some(secret.to_string()), algorithm: Algorithm::HS256, ..Default::default() };
        let auth = validate_jwt(&token, &config).unwrap();
        assert_eq!(auth.tenant_id, Some("op-123".to_string()), "operator_id claim must resolve to tenant_id");
    }

    #[tokio::test]
    async fn redteam_enrich_fills_missing_tenant() {
        let mut auth = AuthContext {
            user_id: "user-abc".to_string(),
            role: "user".to_string(),
            tenant_id: None,
            claims: HashMap::new(),
        };
        let map = tokio::sync::RwLock::new({
            let mut m = std::collections::HashMap::new();
            m.insert("user-abc".to_string(), "operator-xyz".to_string());
            m
        });
        auth.enrich_with_operator_map(&map).await;
        assert_eq!(auth.tenant_id, Some("operator-xyz".to_string()));
    }

    #[tokio::test]
    async fn redteam_enrich_does_not_overwrite_existing_tenant() {
        let mut auth = AuthContext {
            user_id: "user-abc".to_string(),
            role: "user".to_string(),
            tenant_id: Some("already-set".to_string()),
            claims: HashMap::new(),
        };
        let map = tokio::sync::RwLock::new({
            let mut m = std::collections::HashMap::new();
            m.insert("user-abc".to_string(), "operator-xyz".to_string());
            m
        });
        auth.enrich_with_operator_map(&map).await;
        assert_eq!(auth.tenant_id, Some("already-set".to_string()), "Must not overwrite existing tenant_id");
    }

    #[tokio::test]
    async fn redteam_enrich_skips_anonymous() {
        let mut auth = AuthContext {
            user_id: "anonymous".to_string(),
            role: "anon".to_string(),
            tenant_id: None,
            claims: HashMap::new(),
        };
        let map = tokio::sync::RwLock::new({
            let mut m = std::collections::HashMap::new();
            m.insert("anonymous".to_string(), "should-not-see".to_string());
            m
        });
        auth.enrich_with_operator_map(&map).await;
        assert_eq!(auth.tenant_id, None, "Anonymous users must not get tenant_id enrichment");
    }

    #[test]
    fn redteam_finance_admin_does_not_bypass_rls() {
        let auth = AuthContext {
            user_id: "finance-user".to_string(),
            role: "FinanceAdmin".to_string(),
            tenant_id: Some("op-123".to_string()),
            claims: HashMap::new(),
        };
        let rls = auth.to_rls_context();
        assert!(!rls.bypasses_rls(), "FinanceAdmin must NOT bypass RLS");
    }
}
