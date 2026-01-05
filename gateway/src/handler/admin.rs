//! Administrative handlers: health checks and Swagger UI.

use axum::{extract::State, response::Json};
use std::sync::Arc;

use super::{HealthCheckPublic, HealthResponse};
use crate::GatewayState;

/// Public health check endpoint — returns status and version.
pub async fn health_check() -> Json<HealthCheckPublic> {
    Json(HealthCheckPublic {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Swagger UI — serves interactive API documentation.
///
/// Loads swagger-ui from CDN and points it at the gateway's own `/api/_openapi` endpoint.
/// No authentication required (the OpenAPI spec itself is auth-gated, but reading the
/// UI chrome is harmless).
pub async fn swagger_ui() -> axum::response::Html<String> {
    axum::response::Html(r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>QAIL Gateway — API Documentation</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script>
    SwaggerUIBundle({
        url: '/api/_openapi',
        dom_id: '#swagger-ui',
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
        layout: 'BaseLayout',
        deepLinking: true,
        defaultModelsExpandDepth: 1,
        docExpansion: 'list',
        requestInterceptor: function(req) {
            const token = localStorage.getItem('qail_token');
            if (token) {
                req.headers['Authorization'] = 'Bearer ' + token;
            }
            return req;
        },
    });
    </script>
    <style>
        body { margin: 0; padding: 0; }
        .swagger-ui .topbar { display: none; }
        .swagger-ui .info .title { font-size: 2em; font-weight: 700; }
    </style>
</body>
</html>"#.to_string())
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

    if let Some(ref expected) = state.config.admin_token {
        let provided = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        match provided {
            Some(token) if token == expected => {}
            _ => {
                return (
                    axum::http::StatusCode::UNAUTHORIZED,
                    "Unauthorized: admin_token required",
                )
                    .into_response();
            }
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
