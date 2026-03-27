//! Administrative handlers: health checks and Swagger UI.

use axum::{extract::State, response::Json};
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

    let Some(expected) = state.config.admin_token.as_deref() else {
        return (
            axum::http::StatusCode::FORBIDDEN,
            "Internal endpoint disabled: configure admin_token to enable /health/internal",
        )
            .into_response();
    };

    let provided = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match provided {
        Some(token) if constant_time_eq(expected, token) => {}
        _ => {
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                "Unauthorized: admin_token required",
            )
                .into_response();
        }
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
