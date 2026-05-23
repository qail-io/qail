use super::AuthContext;
use axum::http::HeaderMap;

/// Enforce request authentication policy.
///
/// - Always reject denied credentials
/// - Reject anonymous access when `require_auth=true`
pub fn ensure_request_auth(
    auth: &AuthContext,
    require_auth: bool,
) -> Result<(), crate::middleware::ApiError> {
    if auth.is_denied() {
        return Err(crate::middleware::ApiError::with_code(
            "AUTH_DENIED",
            "Invalid credentials",
        ));
    }
    if !auth.is_authenticated() && require_auth {
        return Err(crate::middleware::ApiError::with_code(
            "AUTH_REQUIRED",
            "Authentication required",
        ));
    }
    if auth.is_authenticated()
        && !auth.is_platform_admin()
        && auth.tenant_id.as_deref().is_none_or(str::is_empty)
    {
        return Err(crate::middleware::ApiError::with_code(
            "AUTH_TENANT_REQUIRED",
            "Authenticated requests require a tenant scope",
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
    let auth = super::extract_auth_for_state(headers, state).await;
    ensure_request_auth(
        &auth,
        state.config.require_auth || state.config.production_strict,
    )?;
    Ok(auth)
}
