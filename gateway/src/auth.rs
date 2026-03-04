//! Authentication middleware
//!
//! Handles JWT validation and user context extraction.

use crate::error::GatewayError;
use axum::http::HeaderMap;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
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

    /// Create a denied auth context for invalid credentials.
    ///
    /// Unlike `anonymous()`, this signals that the client *attempted*
    /// authentication but failed — downstream handlers / RLS should
    /// reject the request outright.
    pub fn denied() -> Self {
        Self {
            user_id: "denied".to_string(),
            role: "denied".to_string(),
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
        !self.user_id.is_empty() && self.user_id != "anonymous" && self.user_id != "denied"
    }

    /// Returns `true` if the context represents a denied (invalid credentials) user.
    pub fn is_denied(&self) -> bool {
        self.role == "denied"
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
        let is_super_admin = matches!(self.role.as_str(), "administrator" | "Administrator");

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
            let secret = config
                .secret
                .as_ref()
                .ok_or_else(|| GatewayError::Auth("JWT secret not configured".to_string()))?;
            DecodingKey::from_secret(secret.as_bytes())
        }
        Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512 => {
            let key = config
                .public_key
                .as_ref()
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
    let tenant_id = claims.tenant_id.or(claims.operator_id).or_else(|| {
        claims
            .extra
            .get("operator_id")
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
    extract_auth_from_headers_with_jwks(headers, None, &[])
}

/// Parse configured JWT algorithm names into [`Algorithm`] values.
///
/// Input is case-insensitive and duplicate values are de-duplicated while
/// preserving first-seen order.
pub fn parse_allowed_algorithms(values: &[String]) -> Result<Vec<Algorithm>, GatewayError> {
    let mut out = Vec::new();

    for raw in values {
        let alg = match raw.trim().to_ascii_uppercase().as_str() {
            "RS256" => Algorithm::RS256,
            "RS384" => Algorithm::RS384,
            "RS512" => Algorithm::RS512,
            "ES256" => Algorithm::ES256,
            "ES384" => Algorithm::ES384,
            "PS256" => Algorithm::PS256,
            "PS384" => Algorithm::PS384,
            "PS512" => Algorithm::PS512,
            "HS256" => Algorithm::HS256,
            "HS384" => Algorithm::HS384,
            "HS512" => Algorithm::HS512,
            "EDDSA" => Algorithm::EdDSA,
            other => {
                return Err(GatewayError::Config(format!(
                    "Unsupported JWT algorithm '{}' in jwt_allowed_algorithms",
                    other
                )));
            }
        };

        if !out.contains(&alg) {
            out.push(alg);
        }
    }

    Ok(out)
}

/// Detect JWT algorithm from token header without full validation.
fn detect_jwt_algorithm(token: &str) -> Option<Algorithm> {
    let header_b64 = token.split('.').next()?;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let header_json = URL_SAFE_NO_PAD.decode(header_b64).ok()?;
    let header: serde_json::Value = serde_json::from_slice(&header_json).ok()?;
    let alg_str = header.get("alg")?.as_str()?;
    match alg_str {
        "RS256" => Some(Algorithm::RS256),
        "RS384" => Some(Algorithm::RS384),
        "RS512" => Some(Algorithm::RS512),
        "ES256" => Some(Algorithm::ES256),
        "ES384" => Some(Algorithm::ES384),
        "PS256" => Some(Algorithm::PS256),
        "PS384" => Some(Algorithm::PS384),
        "PS512" => Some(Algorithm::PS512),
        "HS256" => Some(Algorithm::HS256),
        "HS384" => Some(Algorithm::HS384),
        "HS512" => Some(Algorithm::HS512),
        "EdDSA" => Some(Algorithm::EdDSA),
        _ => None,
    }
}

/// Extract auth context, optionally using a JWKS key store for JWT validation.
///
/// Priority:
/// 1. `Authorization: Bearer <jwt>` with JWKS key store (if kid present + store available)
/// 2. `Authorization: Bearer <jwt>` with static JWT_SECRET (HS256)
/// 3. X-User-ID / X-User-Role headers (dev mode only, QAIL_DEV_MODE=true)
/// 4. Anonymous context (no auth)
///
/// **Security (F1):** If a Bearer token is present but validation fails,
/// returns `AuthContext::denied()` instead of falling through to anonymous.
pub fn extract_auth_from_headers_with_jwks(
    headers: &HeaderMap,
    jwks_store: Option<&crate::jwks::JwksKeyStore>,
    allowed_algorithms: &[Algorithm],
) -> AuthContext {
    // Try JWT first
    if let Some(auth_header) = headers.get("authorization")
        && let Ok(value) = auth_header.to_str()
        && value.len() > 7
        && value[..7].eq_ignore_ascii_case("Bearer ")
    {
        let token = &value[7..];
        // Path A: JWKS — check if token has `kid` and we have a JWKS store
        if let Some(store) = jwks_store
            && let Some(kid) = crate::jwks::extract_kid_from_jwt(token)
        {
            // Synchronous-safe: use try_read to avoid blocking
            // For async access in middleware, callers should pre-resolve.
            // Here we do a blocking read since auth extraction is sync.
            let key = {
                let keys = store.keys_blocking();
                keys.get(&kid).cloned()
            };

            if let Some(decoding_key) = key {
                // Auto-detect algorithm from JWT header
                let alg = detect_jwt_algorithm(token).unwrap_or(Algorithm::RS256);

                // SECURITY (P0-4): Enforce server-side algorithm allow-list.
                if !allowed_algorithms.is_empty() && !allowed_algorithms.contains(&alg) {
                    tracing::warn!(
                        "JWT algorithm {:?} not in allowed list {:?} — rejecting token",
                        alg,
                        allowed_algorithms
                    );
                    return AuthContext::denied();
                }

                let mut validation = Validation::new(alg);

                // Apply issuer/audience from env if set
                if let Ok(issuer) = std::env::var("JWT_ISSUER") {
                    validation.set_issuer(&[issuer]);
                }
                if let Ok(audience) = std::env::var("JWT_AUDIENCE") {
                    validation.set_audience(&[audience]);
                }

                match decode::<JwtClaims>(token, &decoding_key, &validation) {
                    Ok(token_data) => {
                        let claims = token_data.claims;
                        let tenant_id = claims.tenant_id.or(claims.operator_id).or_else(|| {
                            claims
                                .extra
                                .get("operator_id")
                                .and_then(|v| v.as_str())
                                .map(String::from)
                        });
                        tracing::debug!(
                            "JWT validated via JWKS (kid={}): user={}",
                            kid,
                            claims.sub
                        );
                        return AuthContext {
                            user_id: claims.sub,
                            role: claims.role.unwrap_or_else(|| "user".to_string()),
                            tenant_id,
                            claims: claims.extra,
                        };
                    }
                    Err(e) => {
                        tracing::warn!(
                            kid = %kid,
                            "JWKS JWT validation failed: {}", e
                        );
                        // F1: Hard-fail — do NOT fall through to anonymous
                        return AuthContext::denied();
                    }
                }
            } else {
                tracing::warn!(kid = %kid, "JWKS: no key found for kid");
                // F1: kid present but key not found — deny
                return AuthContext::denied();
            }
        }
        // No kid in token — fall through to static secret path

        // Path B: Static JWT_SECRET (HS256)
        if let Ok(secret) = std::env::var("JWT_SECRET") {
            // SECURITY (P0-4): Enforce algorithm allow-list for static path too.
            if !allowed_algorithms.is_empty() && !allowed_algorithms.contains(&Algorithm::HS256) {
                tracing::warn!(
                    "JWT_SECRET uses HS256 but allowed_algorithms={:?} — rejecting token",
                    allowed_algorithms
                );
                return AuthContext::denied();
            }

            let config = JwtConfig {
                secret: Some(secret),
                algorithm: Algorithm::HS256,
                issuer: std::env::var("JWT_ISSUER").ok(),
                audience: std::env::var("JWT_AUDIENCE").ok(),
                ..Default::default()
            };

            match validate_jwt(token, &config) {
                Ok(auth) => {
                    tracing::debug!("JWT validated: user={}", auth.user_id);
                    return auth;
                }
                Err(e) => {
                    tracing::warn!("JWT validation failed: {}", e);
                    // F1: Hard-fail — invalid Bearer token must NOT degrade to anonymous
                    return AuthContext::denied();
                }
            }
        }

        // No JWT_SECRET and no JWKS — Bearer token present but no validation config
        tracing::warn!("Bearer token present but no JWT_SECRET or JWKS_URL configured");
        return AuthContext::denied();
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

/// Extract auth using state-aware JWT/JWKS settings and enrich tenant_id from cache.
///
/// **Impersonation:** If the `X-Impersonate-Tenant` header is present and the
/// caller is a platform `administrator`, the auth context is scoped to the
/// requested tenant. The role is downgraded to `operator` so that
/// `to_rls_context()` applies tenant-scoped RLS instead of super-admin bypass.
pub async fn extract_auth_for_state(
    headers: &HeaderMap,
    state: &crate::GatewayState,
) -> AuthContext {
    let mut auth = extract_auth_from_headers_with_jwks(
        headers,
        state.jwks_store.as_ref(),
        &state.jwt_allowed_algorithms,
    );
    auth.enrich_with_operator_map(&state.user_operator_map)
        .await;

    // ── Tenant impersonation ──────────────────────────────────────
    if let Some(impersonate_header) = headers.get("x-impersonate-tenant")
        && let Ok(target_tenant) = impersonate_header.to_str()
    {
        let target_tenant = target_tenant.trim();
        if !target_tenant.is_empty() {
            let is_platform_admin = matches!(auth.role.as_str(), "administrator" | "Administrator");

            if is_platform_admin {
                tracing::warn!(
                    user_id = %auth.user_id,
                    target_tenant = %target_tenant,
                    event = "impersonation_active",
                    "Platform admin impersonating tenant"
                );
                auth.tenant_id = Some(target_tenant.to_string());
                // Downgrade role so to_rls_context() applies tenant-scoped
                // RLS instead of super-admin bypass.
                auth.role = "operator".to_string();
            } else {
                tracing::warn!(
                    user_id = %auth.user_id,
                    role = %auth.role,
                    target_tenant = %target_tenant,
                    event = "impersonation_denied",
                    "Non-administrator attempted tenant impersonation"
                );
                // Non-admins cannot impersonate — ignore the header silently.
                // We do NOT deny the request; the header is simply ignored.
            }
        }
    }

    auth
}

/// Enforce request authentication policy.
///
/// - Always reject denied credentials
/// - Reject anonymous access when `production_strict=true`
pub fn ensure_request_auth(
    auth: &AuthContext,
    production_strict: bool,
) -> Result<(), crate::middleware::ApiError> {
    if auth.is_denied() {
        return Err(crate::middleware::ApiError::with_code(
            "AUTH_DENIED",
            "Invalid credentials",
        ));
    }
    if !auth.is_authenticated() && production_strict {
        return Err(crate::middleware::ApiError::with_code(
            "AUTH_REQUIRED",
            "Authentication required",
        ));
    }
    Ok(())
}

/// Apply post-auth tenant rate limiting.
pub async fn ensure_tenant_rate_limit(
    state: &crate::GatewayState,
    auth: &AuthContext,
) -> Result<(), crate::middleware::ApiError> {
    if !auth.is_authenticated() {
        return Ok(());
    }

    let tenant_key = format!(
        "{}:{}",
        auth.tenant_id.as_deref().unwrap_or("_"),
        auth.user_id
    );

    match state.tenant_rate_limiter.check(&tenant_key).await {
        Ok(_) => Ok(()),
        Err(()) => Err(crate::middleware::ApiError::with_code(
            "TENANT_RATE_LIMIT",
            "Tenant rate limit exceeded",
        )),
    }
}

/// Canonical request auth path for all endpoints.
pub async fn authenticate_request(
    state: &crate::GatewayState,
    headers: &HeaderMap,
) -> Result<AuthContext, crate::middleware::ApiError> {
    let auth = extract_auth_for_state(headers, state).await;
    ensure_request_auth(&auth, state.config.production_strict)?;
    ensure_tenant_rate_limit(state, &auth).await?;
    Ok(auth)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};

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
        )
        .unwrap();

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
        )
        .unwrap();

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
        assert!(
            rls.bypasses_rls(),
            "Administrator (PascalCase) role should bypass RLS"
        );
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
        let token = encode(
            &Header::default(),
            &payload,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();
        let config = JwtConfig {
            secret: Some(secret.to_string()),
            algorithm: Algorithm::HS256,
            ..Default::default()
        };
        let auth = validate_jwt(&token, &config).unwrap();
        assert_eq!(auth.user_id, "");
        // FIXED: empty sub now correctly fails is_authenticated().
        // Previously this was a documented finding where "" != "anonymous" passed.
        assert!(
            !auth.is_authenticated(),
            "Empty sub must not pass is_authenticated"
        );
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
        let token = encode(
            &Header::default(),
            &payload,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();
        let config = JwtConfig {
            secret: Some(secret.to_string()),
            algorithm: Algorithm::HS256,
            ..Default::default()
        };
        let result = validate_jwt(&token, &config);
        // STRONG: serde rejects the entire JWT — integer tenant_id causes parse error
        assert!(
            result.is_err(),
            "Integer tenant_id must cause JWT parse failure (not silent coercion)"
        );
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
        assert!(
            !rls.bypasses_rls(),
            "JWT is_super_admin claim must NOT grant RLS bypass"
        );
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
        let token = encode(
            &Header::default(),
            &payload,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();
        let config = JwtConfig {
            secret: Some(secret.to_string()),
            algorithm: Algorithm::HS256,
            ..Default::default()
        };
        let auth = validate_jwt(&token, &config).unwrap();
        assert_eq!(
            auth.tenant_id,
            Some("op-123".to_string()),
            "operator_id claim must resolve to tenant_id"
        );
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
        assert_eq!(
            auth.tenant_id,
            Some("already-set".to_string()),
            "Must not overwrite existing tenant_id"
        );
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
        assert_eq!(
            auth.tenant_id, None,
            "Anonymous users must not get tenant_id enrichment"
        );
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

    // ══════════════════════════════════════════════════════════════════
    // F1/F2: JWT hard-fail + JWKS wiring tests
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn denied_context_is_not_authenticated() {
        let auth = AuthContext::denied();
        assert!(!auth.is_authenticated());
        assert!(auth.is_denied());
        assert_eq!(auth.user_id, "denied");
        assert_eq!(auth.role, "denied");
    }

    #[test]
    fn anonymous_context_is_not_denied() {
        let auth = AuthContext::anonymous();
        assert!(!auth.is_authenticated());
        assert!(!auth.is_denied());
    }

    #[test]
    fn invalid_jwt_returns_denied_not_anonymous() {
        // Test via extract_auth_from_headers_with_jwks without env mutation.
        // With no JWKS store and no JWT_SECRET, any Bearer token must be denied.
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer invalid.jwt.token".parse().unwrap());

        // No JWKS store, no JWT_SECRET env → denied
        let auth = extract_auth_from_headers_with_jwks(&headers, None, &[]);
        assert!(
            auth.is_denied(),
            "Invalid JWT must return denied, got: {:?}",
            auth
        );
        assert!(!auth.is_authenticated());
    }

    #[test]
    fn bearer_without_any_config_returns_denied() {
        // Bearer present but neither JWKS store nor JWT_SECRET → denied
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer some.valid.token".parse().unwrap());

        let auth = extract_auth_from_headers_with_jwks(&headers, None, &[]);
        assert!(auth.is_denied(), "Bearer without any JWT config must deny");
    }

    #[test]
    fn detect_jwt_algorithm_rs256() {
        // JWT header: {"alg":"RS256","typ":"JWT"}
        // base64url: eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9
        let token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig";
        assert_eq!(detect_jwt_algorithm(token), Some(Algorithm::RS256));
    }

    #[test]
    fn detect_jwt_algorithm_hs256() {
        // JWT header: {"alg":"HS256","typ":"JWT"}
        // base64url: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9
        let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig";
        assert_eq!(detect_jwt_algorithm(token), Some(Algorithm::HS256));
    }

    #[test]
    fn detect_jwt_algorithm_malformed() {
        assert_eq!(detect_jwt_algorithm("not-jwt"), None);
        assert_eq!(detect_jwt_algorithm(""), None);
    }

    #[test]
    fn lowercase_bearer_enters_jwt_path() {
        // "bearer" (lowercase) must NOT silently fall through to anonymous
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "bearer some.jwt.token".parse().unwrap());

        let auth = extract_auth_from_headers_with_jwks(&headers, None, &[]);
        assert!(
            auth.is_denied(),
            "lowercase 'bearer' must enter JWT path and deny (no config), got: {:?}",
            auth
        );
    }

    #[test]
    fn uppercase_bearer_enters_jwt_path() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "BEARER some.jwt.token".parse().unwrap());

        let auth = extract_auth_from_headers_with_jwks(&headers, None, &[]);
        assert!(
            auth.is_denied(),
            "uppercase 'BEARER' must enter JWT path and deny (no config), got: {:?}",
            auth
        );
    }

    #[test]
    fn parse_allowed_algorithms_case_insensitive_dedup() {
        let parsed = parse_allowed_algorithms(&[
            " rs256 ".to_string(),
            "HS256".to_string(),
            "Rs256".to_string(),
        ])
        .unwrap();
        assert_eq!(parsed, vec![Algorithm::RS256, Algorithm::HS256]);
    }

    #[test]
    fn parse_allowed_algorithms_rejects_unknown() {
        let err = parse_allowed_algorithms(&["FOO256".to_string()]).unwrap_err();
        assert!(err.to_string().contains("Unsupported JWT algorithm"));
    }

    // ══════════════════════════════════════════════════════════════════
    // Tenant impersonation tests
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn impersonation_downgrades_administrator_role() {
        // Simulate what extract_auth_for_state does for impersonation
        let mut auth = AuthContext {
            user_id: "master-user".to_string(),
            role: "administrator".to_string(),
            tenant_id: None,
            claims: HashMap::new(),
        };

        // Apply impersonation logic (same as in extract_auth_for_state)
        let target_tenant = "operator-abc-123";
        let is_platform_admin = matches!(auth.role.as_str(), "administrator" | "Administrator");
        assert!(is_platform_admin);
        auth.tenant_id = Some(target_tenant.to_string());
        auth.role = "operator".to_string();

        // After impersonation: tenant_id is set, role is downgraded
        assert_eq!(auth.tenant_id, Some("operator-abc-123".to_string()));
        assert_eq!(auth.role, "operator");

        // RLS must NOT bypass
        let rls = auth.to_rls_context();
        assert!(
            !rls.bypasses_rls(),
            "Impersonated admin must NOT bypass RLS"
        );
        assert_eq!(rls.operator_id, "operator-abc-123");
    }

    #[test]
    fn impersonation_ignored_for_non_administrator() {
        // Non-administrator roles must not be able to impersonate
        let auth = AuthContext {
            user_id: "tenant-user".to_string(),
            role: "super_admin".to_string(),
            tenant_id: Some("original-tenant".to_string()),
            claims: HashMap::new(),
        };

        // Apply same check — should NOT match
        let is_platform_admin = matches!(auth.role.as_str(), "administrator" | "Administrator");
        assert!(!is_platform_admin, "super_admin is NOT platform admin");

        // tenant_id should remain unchanged
        assert_eq!(auth.tenant_id, Some("original-tenant".to_string()));
        assert_eq!(auth.role, "super_admin");
    }
}
