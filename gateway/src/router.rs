//! HTTP Router for QAIL Gateway
//!
//! Defines the axum router with all gateway endpoints.
//! Applies security hardening: CORS, security headers, body limits.

use axum::{
    http::HeaderValue,
    middleware as axum_mw,
    routing::{get, post, MethodRouter},
    Router,
};
use std::sync::Arc;
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowOrigin, Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    set_header::SetResponseHeaderLayer,
    trace::TraceLayer,
};

use crate::handler::{execute_batch, execute_query, execute_query_binary, execute_query_fast, health_check, health_check_internal, swagger_ui};
use crate::middleware::rate_limit_middleware;
use crate::rest::auto_rest_routes;
use crate::ws::ws_handler;
use crate::GatewayState;

/// Maximum request body size (2 MiB).
/// Prevents large-payload DoS attacks.
const MAX_BODY_SIZE: usize = 2 * 1024 * 1024;

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
        .route("/qail/binary", post(execute_query_binary))
        .route("/qail/fast", post(execute_query_fast))
        .route("/qail/batch", post(execute_batch))
        // REST-friendly batch alias
        .route("/api/_batch", post(execute_batch))
        // WebSocket
        .route("/ws", get(ws_handler))
        // Merge auto-REST routes
        .merge(rest);

    // Merge custom routes (before with_state so types align)
    for (path, handler) in custom_routes {
        tracing::info!("  CUSTOM: {} → overrides auto-REST", path);
        router = router.route(path, handler.clone());
    }

    router
        // Middleware layers (order: bottom = outermost = first to run)
        .layer(axum_mw::from_fn_with_state(Arc::clone(&state), rate_limit_middleware))
        .layer(CompressionLayer::new()
            .compress_when(tower_http::compression::predicate::SizeAbove::new(1024))
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
        // ── Request Body Size Limit ──────────────────────────────────
        .layer(RequestBodyLimitLayer::new(MAX_BODY_SIZE))
        .layer(trace)
        .layer(cors)
        .with_state(state)
}

/// Build CORS layer from gateway config.
///
/// - If `cors_allowed_origins` is non-empty → strict origin allowlist
/// - Otherwise → `allow_origin(Any)` (backward compatible)
fn build_cors_layer(config: &crate::config::GatewayConfig) -> CorsLayer {
    if !config.cors_enabled {
        // CORS disabled — return restrictive layer (no Access-Control headers)
        return CorsLayer::new();
    }

    let base = CorsLayer::new()
        .allow_methods(Any)
        .allow_headers(Any);

    if config.cors_allowed_origins.is_empty() {
        if config.cors_strict {
            // SECURITY (M1): In strict mode, refuse to start without explicit origins.
            panic!(
                "SECURITY: cors_strict=true but cors_allowed_origins is empty. \
                 Configure explicit CORS origins or set cors_strict=false."
            );
        }
        // Backward compatible: warn and allow all
        tracing::warn!(
            "CORS allows ANY origin (cors_allowed_origins is empty). \
             Set `cors_allowed_origins` for production deployments."
        );
        base.allow_origin(Any)
    } else {
        let origins: Vec<HeaderValue> = config
            .cors_allowed_origins
            .iter()
            .filter_map(|o| o.parse::<HeaderValue>().ok())
            .collect();

        if origins.is_empty() {
            tracing::warn!("cors_allowed_origins configured but none parsed — falling back to Any");
            base.allow_origin(Any)
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
