//! HTTP Router for QAIL Gateway
//!
//! Defines the axum router with all gateway endpoints.

use axum::{
    routing::{get, post, MethodRouter},
    Router,
};
use std::sync::Arc;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

use crate::handler::{execute_batch, execute_query, execute_query_binary, health_check};
use crate::rest::auto_rest_routes;
use crate::ws::ws_handler;
use crate::GatewayState;

/// Create the main router for the gateway.
///
/// Custom routes are merged AFTER auto-REST, so they override auto-generated
/// CRUD endpoints for the same path.
pub fn create_router(
    state: Arc<GatewayState>,
    custom_routes: &[(String, MethodRouter<Arc<GatewayState>>)],
) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    
    // Tracing layer for request logging
    let trace = TraceLayer::new_for_http();

    // Auto-REST routes from schema
    let rest = auto_rest_routes(Arc::clone(&state));
    
    let mut router = Router::new()
        // Health check
        .route("/health", get(health_check))
        // Query endpoints (Qail AST protocol)
        .route("/qail", post(execute_query))
        .route("/qail/binary", post(execute_query_binary))
        .route("/qail/batch", post(execute_batch))
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
        // Middleware layers
        .layer(trace)
        .layer(cors)
        .with_state(state)
}
