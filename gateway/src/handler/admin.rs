//! Administrative handlers: health checks and internal operations.

use axum::{extract::State, response::Json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{HealthCheckPublic, HealthResponse};
use crate::GatewayState;

fn constant_time_eq(expected: &str, provided: &str) -> bool {
    if expected.len() != provided.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in expected.as_bytes().iter().zip(provided.as_bytes()) {
        diff |= a ^ b;
    }
    diff == 0
}

fn require_admin_token(
    state: &GatewayState,
    headers: &axum::http::HeaderMap,
    disabled_message: &'static str,
) -> Result<(), axum::response::Response> {
    use axum::response::IntoResponse;

    let Some(expected) = state.config.admin_token.as_deref() else {
        return Err((axum::http::StatusCode::FORBIDDEN, disabled_message).into_response());
    };

    let provided = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    match provided {
        Some(token) if constant_time_eq(expected, token) => Ok(()),
        _ => Err((
            axum::http::StatusCode::UNAUTHORIZED,
            "Unauthorized: admin_token required",
        )
            .into_response()),
    }
}

enum CacheAdminAccess {
    AdminToken,
    PlatformUser(String),
}

async fn require_cache_admin_access(
    state: &GatewayState,
    headers: &axum::http::HeaderMap,
) -> Result<CacheAdminAccess, axum::response::Response> {
    use axum::response::IntoResponse;

    // Allow static admin token when configured.
    if let Some(expected) = state.config.admin_token.as_deref() {
        let provided = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if let Some(token) = provided
            && constant_time_eq(expected, token)
        {
            return Ok(CacheAdminAccess::AdminToken);
        }
    }

    // Also allow platform-admin JWT so Deck/Workers can call this endpoint
    // without requiring an extra shared secret.
    let auth = crate::auth::authenticate_request(state, headers)
        .await
        .map_err(|e| e.into_response())?;

    if !auth.is_platform_admin() {
        return Err((
            axum::http::StatusCode::FORBIDDEN,
            "Platform administrator access required for cache invalidation",
        )
            .into_response());
    }

    Ok(CacheAdminAccess::PlatformUser(auth.user_id))
}

/// Public health check endpoint — returns status and version.
pub async fn health_check() -> Json<HealthCheckPublic> {
    Json(HealthCheckPublic {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Swagger UI — serves interactive API documentation.
///
/// Loads swagger-ui from CDN with SRI integrity checks and points it at
/// the gateway's own `/api/_openapi` endpoint.
///
/// **Security:** Disabled when `production_strict=true`. In strict mode,
/// returns 403 — use direct API calls instead.
pub async fn swagger_ui(State(state): State<Arc<GatewayState>>) -> axum::response::Response {
    use axum::response::IntoResponse;

    if state.config.production_strict {
        return (
            axum::http::StatusCode::FORBIDDEN,
            "Swagger UI is disabled in production_strict mode",
        )
            .into_response();
    }

    axum::response::Html(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>QAIL Gateway — API Documentation</title>
    <link rel="stylesheet"
          href="https://unpkg.com/swagger-ui-dist@5.18.2/swagger-ui.css"
          integrity="sha384-SF1aEBgAer1S7fZoSzh4mONLj3E0XPsmFSfBPjjr14mCsWcq43gg9DMvz21fkPl"
          crossorigin="anonymous">
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5.18.2/swagger-ui-bundle.js"
            integrity="sha384-dMxm7RrsoJqZPHQ+b98s7sHVMnlJNMOF7nOPFGH2kEMnnKaWcg/ddiHoCP2WAkH"
            crossorigin="anonymous"></script>
    <script>
    SwaggerUIBundle({
        url: '/api/_openapi',
        dom_id: '#swagger-ui',
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
        layout: 'BaseLayout',
        deepLinking: true,
        defaultModelsExpandDepth: 1,
        docExpansion: 'list',
    });
    </script>
    <style>
        body { margin: 0; padding: 0; }
        .swagger-ui .topbar { display: none; }
        .swagger-ui .info .title { font-size: 2em; font-weight: 700; }
    </style>
</body>
</html>"#
            .to_string(),
    )
    .into_response()
}

/// Internal health check — includes pool stats and tenant guard metrics.
///
/// SECURITY (M4): When `admin_token` is configured, requires
/// `Authorization: Bearer <token>` to prevent leaking operational details.
pub async fn health_check_internal(
    State(state): State<Arc<GatewayState>>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    if let Err(response) = require_admin_token(
        state.as_ref(),
        &headers,
        "Internal endpoint disabled: configure admin_token to enable /health/internal",
    ) {
        return response;
    }

    let stats = state.pool.stats().await;
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        pool_active: stats.active,
        pool_idle: stats.idle,
        tenant_guard: crate::tenant_guard::metrics_snapshot(),
    })
    .into_response()
}

#[derive(Debug, Deserialize)]
pub struct CacheInvalidateRequest {
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub table: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CacheInvalidateResponse {
    pub status: String,
    pub scope: String,
    pub table: Option<String>,
    pub invalidated_entries: usize,
}

fn is_safe_identifier(input: &str) -> bool {
    !input.is_empty()
        && input
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

/// Internal cache invalidation endpoint.
///
/// SECURITY: protected by `admin_token` or platform-admin JWT.
/// Body:
/// - `{ "scope": "all" }` for global cache flush
/// - `{ "table": "harbors" }` for table-scoped invalidation
pub async fn cache_invalidate_internal(
    State(state): State<Arc<GatewayState>>,
    headers: axum::http::HeaderMap,
    axum::extract::Json(payload): axum::extract::Json<CacheInvalidateRequest>,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let access = match require_cache_admin_access(state.as_ref(), &headers).await {
        Ok(access) => access,
        Err(response) => {
            return response;
        }
    };

    let access_actor = match &access {
        CacheAdminAccess::AdminToken => "admin_token".to_string(),
        CacheAdminAccess::PlatformUser(user_id) => format!("platform_user:{user_id}"),
    };

    let scope = payload
        .scope
        .as_deref()
        .map(str::trim)
        .map(|v| v.to_ascii_lowercase());

    let (scope_label, table_label, invalidated_entries) = if scope.as_deref() == Some("all") {
        let count = state.cache.invalidate_all();
        tracing::info!(
            actor = %access_actor,
            scope = "all",
            invalidated_entries = count,
            "gateway cache invalidated"
        );
        ("all".to_string(), None, count)
    } else if let Some(raw_table) = payload.table.as_deref().map(str::trim) {
        if !is_safe_identifier(raw_table) {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                "Invalid table identifier",
            )
                .into_response();
        }
        let normalized = raw_table.to_string();
        let count = state.cache.invalidate_table(&normalized);
        tracing::info!(
            actor = %access_actor,
            scope = "table",
            table = %normalized,
            invalidated_entries = count,
            "gateway cache invalidated"
        );
        ("table".to_string(), Some(normalized), count)
    } else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "Provide either {\"scope\":\"all\"} or {\"table\":\"...\"}",
        )
            .into_response();
    };

    Json(CacheInvalidateResponse {
        status: "ok".to_string(),
        scope: scope_label,
        table: table_label,
        invalidated_entries,
    })
    .into_response()
}
