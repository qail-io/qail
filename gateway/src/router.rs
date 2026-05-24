//! HTTP Router for QAIL Gateway
//!
//! Defines the axum router with all gateway endpoints.
//! Applies security hardening: CORS, security headers, body limits.

use axum::{
    Router,
    http::HeaderValue,
    middleware as axum_mw,
    routing::{MethodRouter, get, post},
};
use std::sync::Arc;
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    set_header::SetResponseHeaderLayer,
    trace::TraceLayer,
};

use crate::GatewayState;
use crate::handler::{
    execute_batch, execute_query, execute_query_binary, execute_query_export, execute_query_fast,
    health_check, health_check_internal, swagger_ui, txn_begin, txn_commit, txn_query,
    txn_rollback, txn_savepoint,
};
use crate::middleware::rate_limit_middleware;
use crate::rest::auto_rest_routes;
use crate::ws::ws_handler;

/// Create the main router for the gateway.
///
/// Custom routes are merged AFTER auto-REST, so they override auto-generated
/// CRUD endpoints for the same path.
pub fn create_router(
    state: Arc<GatewayState>,
    custom_routes: &[(String, MethodRouter<Arc<GatewayState>>)],
) -> Router {
    // ── CORS ─────────────────────────────────────────────────────────
    let cors = build_cors_layer(&state.config);

    // ── Tracing ──────────────────────────────────────────────────────
    let trace = TraceLayer::new_for_http();

    // ── Body size limit from config ─────────────────────────────────
    let max_body = state.config.max_request_body_bytes;

    // ── Auto-REST routes from schema ─────────────────────────────────
    let rest = auto_rest_routes(Arc::clone(&state));

    let mut router = Router::new()
        // Health check (public — minimal info)
        .route("/health", get(health_check))
        // Health check (internal — full metrics, restrict in production)
        .route("/health/internal", get(health_check_internal))
        // Prometheus metrics (outside rate limiting — Prometheus scraper must always succeed)
        .route("/metrics", get(crate::metrics::metrics_handler))
        // Swagger UI (interactive API docs)
        .route("/docs", get(swagger_ui))
        // Query endpoints (Qail AST protocol)
        .route("/qail", post(execute_query))
        .route("/qail/export", post(execute_query_export))
        .route("/qail/binary", post(execute_query_binary))
        .route("/qail/fast", post(execute_query_fast))
        .route("/qail/batch", post(execute_batch))
        // REST-friendly batch alias
        .route("/api/_batch", post(execute_batch))
        // WebSocket
        .route("/ws", get(ws_handler))
        // Transaction session endpoints
        .route("/txn/begin", post(txn_begin))
        .route("/txn/query", post(txn_query))
        .route("/txn/commit", post(txn_commit))
        .route("/txn/rollback", post(txn_rollback))
        .route("/txn/savepoint", post(txn_savepoint))
        // Merge auto-REST routes
        .merge(rest);

    // Merge custom routes (before with_state so types align)
    for (path, handler) in custom_routes {
        tracing::info!("  CUSTOM: {} → overrides auto-REST", path);
        router = router.route(path, handler.clone());
    }

    router
        // Middleware layers (order: bottom = outermost = first to run)
        .layer(axum_mw::from_fn_with_state(
            Arc::clone(&state),
            rate_limit_middleware,
        ))
        .layer(axum_mw::from_fn_with_state(
            Arc::clone(&state),
            crate::idempotency::idempotency_middleware,
        ))
        .layer(
            CompressionLayer::new()
                .compress_when(tower_http::compression::predicate::SizeAbove::new(1024)),
        )
        // ── Security Response Headers ────────────────────────────────
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::X_FRAME_OPTIONS,
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::REFERRER_POLICY,
            HeaderValue::from_static("strict-origin-when-cross-origin"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::HeaderName::from_static("permissions-policy"),
            HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::HeaderName::from_static("content-security-policy"),
            HeaderValue::from_static("default-src 'self'; script-src 'self' 'unsafe-inline' https://unpkg.com; style-src 'self' 'unsafe-inline' https://unpkg.com; img-src 'self' data:; connect-src 'self';"),
        ))
        // ── Stable API Version Header (Phase 4) ─────────────────────
        .layer(SetResponseHeaderLayer::if_not_present(
            axum::http::header::HeaderName::from_static("x-api-version"),
            HeaderValue::from_static(env!("CARGO_PKG_VERSION")),
        ))
        // ── Request Body Size Limit ──────────────────────────────────
        .layer(RequestBodyLimitLayer::new(max_body))
        .layer(trace)
        .layer(cors)
        .with_state(state)
}

/// Build CORS layer from gateway config.
///
/// - If `cors_allowed_origins` is non-empty → strict origin allowlist
/// - Otherwise → fail-closed (no allowed origins)
fn build_cors_layer(config: &crate::config::GatewayConfig) -> CorsLayer {
    if !config.cors_enabled {
        // CORS disabled — return restrictive layer (no Access-Control headers)
        return CorsLayer::new();
    }

    let base = CorsLayer::new().allow_methods(Any).allow_headers(Any);

    if config.cors_allowed_origins.is_empty() {
        tracing::error!(
            "SECURITY: cors_allowed_origins is empty. \
             Applying fail-closed CORS (no allowed origins). \
             Configure explicit CORS origins."
        );
        base
    } else {
        let mut origins = Vec::with_capacity(config.cors_allowed_origins.len());
        for origin in &config.cors_allowed_origins {
            match origin.parse::<HeaderValue>() {
                Ok(value) => origins.push(value),
                Err(e) => {
                    tracing::error!(
                        origin = %origin,
                        error = %e,
                        "SECURITY: invalid cors_allowed_origins entry. \
                         Applying fail-closed CORS (no allowed origins)."
                    );
                    return base;
                }
            }
        }

        if origins.is_empty() {
            tracing::error!(
                "SECURITY: none of cors_allowed_origins parsed. \
                 Applying fail-closed CORS (no allowed origins)."
            );
            base
        } else {
            tracing::info!(
                "CORS restricted to {} origin(s): {:?}",
                origins.len(),
                config.cors_allowed_origins
            );
            base.allow_origin(AllowOrigin::list(origins))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode, header};
    use tower::ServiceExt;

    async fn ok() -> &'static str {
        "ok"
    }

    async fn cors_preflight(config: crate::config::GatewayConfig) -> axum::response::Response {
        Router::new()
            .route("/", get(ok))
            .layer(build_cors_layer(&config))
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/")
                    .header(header::ORIGIN, "https://app.example")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should execute")
    }

    #[tokio::test]
    async fn cors_allows_configured_valid_origin() {
        let config = crate::config::GatewayConfig {
            cors_allowed_origins: vec!["https://app.example".to_string()],
            ..crate::config::GatewayConfig::default()
        };

        let response = cors_preflight(config).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|v| v.to_str().ok()),
            Some("https://app.example")
        );
    }

    #[tokio::test]
    async fn cors_fails_closed_when_any_configured_origin_is_invalid() {
        let config = crate::config::GatewayConfig {
            cors_allowed_origins: vec![
                "https://app.example".to_string(),
                "https://bad.example\r\nx: injected".to_string(),
            ],
            ..crate::config::GatewayConfig::default()
        };

        let response = cors_preflight(config).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .is_none(),
            "invalid CORS config must not leave the valid subset enabled"
        );
    }
}
