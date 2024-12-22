//! Prometheus metrics module
//!
//! Exposes gateway metrics for monitoring.

use axum::{extract::State, response::IntoResponse};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::Arc;
use std::time::Instant;

/// Initialize Prometheus metrics recorder
pub fn init_metrics() -> PrometheusHandle {
    PrometheusBuilder::new()
        .install_recorder()
        .expect("Failed to install Prometheus recorder")
}

/// Metrics handler - returns Prometheus format metrics
pub async fn metrics_handler(
    State(handle): State<Arc<PrometheusHandle>>,
) -> impl IntoResponse {
    handle.render()
}

// Metric recording helpers

/// Record a query execution
pub fn record_query(table: &str, action: &str, duration_ms: f64, success: bool) {
    let labels = [
        ("table", table.to_string()),
        ("action", action.to_string()),
        ("status", if success { "success" } else { "error" }.to_string()),
    ];
    
    counter!("qail_queries_total", &labels).increment(1);
    histogram!("qail_query_duration_ms", &labels).record(duration_ms);
}

/// Record pool stats
pub fn record_pool_stats(active: usize, idle: usize, max: usize) {
    gauge!("qail_pool_active_connections").set(active as f64);
    gauge!("qail_pool_idle_connections").set(idle as f64);
    gauge!("qail_pool_max_connections").set(max as f64);
}

/// Record WebSocket connections
pub fn record_ws_connection(connected: bool) {
    if connected {
        counter!("qail_ws_connections_total").increment(1);
        gauge!("qail_ws_active_connections").increment(1.0);
    } else {
        gauge!("qail_ws_active_connections").decrement(1.0);
    }
}

/// Record batch query
pub fn record_batch(query_count: usize, success_count: usize, duration_ms: f64) {
    counter!("qail_batch_queries_total").increment(query_count as u64);
    counter!("qail_batch_success_total").increment(success_count as u64);
    histogram!("qail_batch_duration_ms").record(duration_ms);
}

/// Timer for measuring query duration
pub struct QueryTimer {
    start: Instant,
    table: String,
    action: String,
}

impl QueryTimer {
    pub fn new(table: &str, action: &str) -> Self {
        Self {
            start: Instant::now(),
            table: table.to_string(),
            action: action.to_string(),
        }
    }
    
    pub fn finish(self, success: bool) {
        let duration_ms = self.start.elapsed().as_secs_f64() * 1000.0;
        record_query(&self.table, &self.action, duration_ms, success);
    }
}
