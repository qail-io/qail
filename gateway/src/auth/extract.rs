use super::{AuthContext, JwtClaims, JwtConfig, validate_jwt};
use axum::http::HeaderMap;
use jsonwebtoken::{Algorithm, Validation, decode};
use std::collections::HashMap;

/// Extract auth context from request headers
///
/// Priority:
/// 1. `Authorization: Bearer <jwt>` (if JWT_SECRET is set)
/// 2. X-User-ID / X-User-Role headers (dev mode only)
pub fn extract_auth_from_headers(headers: &HeaderMap) -> AuthContext {
    extract_auth_from_headers_with_jwks(headers, None, &[])
}

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
                let alg = super::jwt::detect_jwt_algorithm(token).unwrap_or(Algorithm::RS256);

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
                        let mut extra_claims = claims.extra;
                        extra_claims.insert(
                            "exp".to_string(),
                            serde_json::Value::from(claims.exp as u64),
                        );
                        tracing::debug!(
                            "JWT validated via JWKS (kid={}): user={}",
                            kid,
                            claims.sub
                        );
                        return AuthContext {
                            user_id: claims.sub,
                            role: claims.role.unwrap_or_else(|| "user".to_string()),
                            tenant_id: claims.tenant_id,
                            claims: extra_claims,
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

    let mut claims = HashMap::new();
    // Preserve existing dev-mode ergonomics while production JWT auth remains
    // fail-closed via explicit platform-admin claims.
    if role.eq_ignore_ascii_case("administrator") && tenant_id.as_deref().is_none_or(str::is_empty)
    {
        claims.insert("platform_admin".to_string(), serde_json::Value::Bool(true));
    }

    AuthContext {
        user_id,
        role,
        tenant_id,
        claims,
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
    auth.enrich_with_tenant_map(&state.user_tenant_map).await;

    // SECURITY: do not infer platform admin from a missing tenant map entry.
    if auth.role.eq_ignore_ascii_case("administrator")
        && auth.tenant_id.as_deref().is_none_or(str::is_empty)
        && !auth.has_platform_admin_claim()
    {
        tracing::warn!(
            user_id = %auth.user_id,
            role = %auth.role,
            event = "administrator_without_platform_claim_denied",
            "Rejecting administrator auth without explicit platform-admin claim"
        );
        return AuthContext::denied();
    }

    if let Some(impersonate_header) = headers.get("x-impersonate-tenant")
        && let Ok(target_tenant) = impersonate_header.to_str()
    {
        let target_tenant = target_tenant.trim();
        if !target_tenant.is_empty() {
            if auth.is_platform_admin() {
                tracing::warn!(
                    user_id = %auth.user_id,
                    target_tenant = %target_tenant,
                    event = "impersonation_active",
                    "Platform admin impersonating tenant"
                );
                auth.tenant_id = Some(target_tenant.to_string());
                auth.role = "operator".to_string();
            } else {
                tracing::warn!(
                    user_id = %auth.user_id,
                    role = %auth.role,
                    target_tenant = %target_tenant,
                    event = "impersonation_denied",
                    "Non-administrator attempted tenant impersonation"
                );
            }
        }
    }

    auth
}
