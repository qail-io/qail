//! HTTP Router for QAIL Gateway
//!
//! Defines the axum router with all gateway endpoints.

use axum::{
    Router,
    routing::{get, post},
};
use std::sync::Arc;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

use crate::handler::{execute_batch, execute_query, execute_query_binary, health_check};
use crate::ws::ws_handler;
use crate::GatewayState;

/// Create the main router for the gateway
pub fn create_router(state: Arc<GatewayState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    
    // Tracing layer for request logging
    let trace = TraceLayer::new_for_http();
    
    Router::new()
        // Health check
        .route("/health", get(health_check))
        // Query endpoints
        .route("/qail", post(execute_query))
        .route("/qail/binary", post(execute_query_binary))
        .route("/qail/batch", post(execute_batch))
        // WebSocket
        .route("/ws", get(ws_handler))
        // Middleware layers
        .layer(trace)
        .layer(cors)
        .with_state(state)
}
