use axum::{extract::State, response::IntoResponse};
use metrics::{counter, gauge, histogram};
use std::sync::Arc;
use std::time::Instant;

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

/// Metrics handler - returns Prometheus format metrics
///
/// SECURITY (M4): When `admin_token` is configured, requires
/// `Authorization: Bearer <token>` to prevent exposing internal metrics.
pub async fn metrics_handler(
    State(state): State<Arc<crate::GatewayState>>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let Some(expected) = state.config.admin_token.as_deref() else {
        return (
            axum::http::StatusCode::FORBIDDEN,
            "Metrics endpoint disabled: configure admin_token to enable /metrics",
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
    state.prometheus_handle.render().into_response()
}

/// Record a query execution.
///
/// # Arguments
///
/// * `table` - Target table name.
/// * `action` - CRUD action (`get`, `put`, `mod`, `del`).
/// * `duration_ms` - Query execution time in milliseconds.
/// * `success` - Whether the query succeeded.
pub fn record_query(table: &str, action: &str, duration_ms: f64, success: bool) {
    let labels = [
        ("table", table.to_string()),
        ("action", action.to_string()),
        (
            "status",
            if success { "success" } else { "error" }.to_string(),
        ),
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

/// Record cache statistics (call periodically or on each cache access).
///
/// # Arguments
///
/// * `hits` - Total cache hit count.
/// * `misses` - Total cache miss count.
/// * `entries` - Current number of cached entries.
/// * `weighted_bytes` - Estimated memory used by cache entries.
pub fn record_cache_stats(hits: u64, misses: u64, entries: usize, weighted_bytes: u64) {
    counter!("qail_cache_hits_total").absolute(hits);
    counter!("qail_cache_misses_total").absolute(misses);
    gauge!("qail_cache_entries").set(entries as f64);
    gauge!("qail_cache_weighted_bytes").set(weighted_bytes as f64);
}

/// Record a rate limiter rejection
pub fn record_rate_limited() {
    counter!("qail_rate_limited_total").increment(1);
}

/// Record an EXPLAIN cost rejection
pub fn record_explain_rejected(estimated_cost: f64, cost_limit: f64) {
    counter!("qail_explain_rejections_total").increment(1);
    gauge!("qail_last_rejected_cost").set(estimated_cost);
    gauge!("qail_explain_cost_limit").set(cost_limit);
}

/// Record a query complexity guard rejection
pub fn record_complexity_rejected() {
    counter!("qail_complexity_rejections_total").increment(1);
}

/// Record DB-acquire queue wait duration.
pub fn record_db_acquire_wait(wait_ms: f64, outcome: &str) {
    let labels = [("outcome", outcome.to_string())];
    histogram!("qail_db_acquire_wait_ms", &labels).record(wait_ms);
}

/// Record DB-acquire timeout.
pub fn record_db_acquire_timeout() {
    counter!("qail_db_acquire_timeouts_total").increment(1);
}

/// Record shed decision due to DB backpressure.
pub fn record_db_acquire_shed(scope: &str) {
    let labels = [("scope", scope.to_string())];
    counter!("qail_db_acquire_shed_total", &labels).increment(1);
}

/// Record current DB waiter depth.
pub fn record_db_waiters(global_waiters: usize, tracked_tenants: usize) {
    gauge!("qail_db_waiters_global").set(global_waiters as f64);
    gauge!("qail_db_waiters_tracked_tenants").set(tracked_tenants as f64);
}

/// Record an HTTP request (for request rate + latency panels)
pub fn record_http_request(method: &str, status: u16, duration_secs: f64) {
    let labels = [
        ("method", method.to_string()),
        ("status", status.to_string()),
    ];
    counter!("qail_http_requests_total", &labels).increment(1);
    histogram!("qail_http_request_duration_seconds", &labels).record(duration_secs);
}

/// Record a PostgreSQL error classified by SQLSTATE.
pub fn record_db_error(sqlstate: &str, class: &str) {
    let labels = [
        ("sqlstate", sqlstate.to_string()),
        ("class", class.to_string()),
    ];
    counter!("qail_db_errors_total", &labels).increment(1);
}

/// Record an RPC allow-list rejection.
pub fn record_rpc_allowlist_rejection() {
    counter!("qail_rpc_allowlist_rejections_total").increment(1);
}

/// Record an RPC signature cache hit.
pub fn record_rpc_signature_cache_hit() {
    counter!("qail_rpc_signature_cache_hits_total").increment(1);
}

/// Record an RPC signature cache miss.
pub fn record_rpc_signature_cache_miss() {
    counter!("qail_rpc_signature_cache_misses_total").increment(1);
}

/// Record when local signature matcher disagrees with PostgreSQL resolver.
pub fn record_rpc_signature_local_mismatch() {
    counter!("qail_rpc_signature_local_mismatch_total").increment(1);
}

/// Record an RPC signature contract rejection.
pub fn record_rpc_signature_rejection(reason: &str) {
    let labels = [("reason", reason.to_string())];
    counter!("qail_rpc_signature_rejections_total", &labels).increment(1);
}

/// Record RPC call latency/outcome.
pub fn record_rpc_call(duration_ms: f64, success: bool, result_format: &str) {
    let labels = [
        (
            "status",
            if success { "success" } else { "error" }.to_string(),
        ),
        ("result_format", result_format.to_string()),
    ];
    counter!("qail_rpc_calls_total", &labels).increment(1);
    histogram!("qail_rpc_duration_ms", &labels).record(duration_ms);
}

/// Record a fallback when binary RPC output could not be strongly decoded.
pub fn record_rpc_binary_decode_fallback() {
    counter!("qail_rpc_binary_decode_fallback_total").increment(1);
}

/// Timer for measuring query duration
pub struct QueryTimer {
    start: Instant,
    table: String,
    action: String,
}

impl QueryTimer {
    /// Start a new query timer for the given table and action.
    pub fn new(table: &str, action: &str) -> Self {
        Self {
            start: Instant::now(),
            table: table.to_string(),
            action: action.to_string(),
        }
    }

    /// Stop the timer and record the query duration metric.
    pub fn finish(self, success: bool) {
        let duration_ms = self.start.elapsed().as_secs_f64() * 1000.0;
        record_query(&self.table, &self.action, duration_ms, success);
    }
}
