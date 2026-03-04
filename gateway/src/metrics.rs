//! Prometheus metrics module
//!
//! Exposes gateway metrics for monitoring.

use axum::{extract::State, response::IntoResponse};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::Arc;
#[cfg(test)]
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;

#[cfg(test)]
static TEST_TXN_CREATED: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static TEST_TXN_EXPIRED: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static TEST_TXN_STATEMENT_LIMIT_HIT: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static TEST_TXN_FORCED_CAPACITY: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static TEST_TXN_FORCED_LIFETIME: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static TEST_TXN_FORCED_STATEMENT: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static TEST_TXN_FORCED_IDLE: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
static TEST_TXN_ACTIVE: AtomicI64 = AtomicI64::new(0);
#[cfg(test)]
static TEST_TXN_SERIAL: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TxnTestMetricsSnapshot {
    pub created: u64,
    pub expired: u64,
    pub statement_limit_hit: u64,
    pub forced_capacity: u64,
    pub forced_lifetime: u64,
    pub forced_statement: u64,
    pub forced_idle: u64,
    pub active: i64,
}

#[cfg(test)]
pub fn reset_txn_test_metrics() {
    TEST_TXN_CREATED.store(0, Ordering::Relaxed);
    TEST_TXN_EXPIRED.store(0, Ordering::Relaxed);
    TEST_TXN_STATEMENT_LIMIT_HIT.store(0, Ordering::Relaxed);
    TEST_TXN_FORCED_CAPACITY.store(0, Ordering::Relaxed);
    TEST_TXN_FORCED_LIFETIME.store(0, Ordering::Relaxed);
    TEST_TXN_FORCED_STATEMENT.store(0, Ordering::Relaxed);
    TEST_TXN_FORCED_IDLE.store(0, Ordering::Relaxed);
    TEST_TXN_ACTIVE.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub fn txn_test_metrics_snapshot() -> TxnTestMetricsSnapshot {
    TxnTestMetricsSnapshot {
        created: TEST_TXN_CREATED.load(Ordering::Relaxed),
        expired: TEST_TXN_EXPIRED.load(Ordering::Relaxed),
        statement_limit_hit: TEST_TXN_STATEMENT_LIMIT_HIT.load(Ordering::Relaxed),
        forced_capacity: TEST_TXN_FORCED_CAPACITY.load(Ordering::Relaxed),
        forced_lifetime: TEST_TXN_FORCED_LIFETIME.load(Ordering::Relaxed),
        forced_statement: TEST_TXN_FORCED_STATEMENT.load(Ordering::Relaxed),
        forced_idle: TEST_TXN_FORCED_IDLE.load(Ordering::Relaxed),
        active: TEST_TXN_ACTIVE.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
pub async fn txn_test_serial_guard() -> tokio::sync::OwnedSemaphorePermit {
    TEST_TXN_SERIAL
        .get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
        .clone()
        .acquire_owned()
        .await
        .expect("txn test serial semaphore should remain open")
}

/// Initialize Prometheus metrics recorder and return the handle for scraping.
///
/// `install_recorder()` calls `build_recorder()` + `set_global_recorder()`.
/// We MUST manually spawn an upkeep task to call `run_upkeep()` periodically,
/// otherwise histograms (latency) will not rotate buckets and will appear empty.
pub fn init_metrics() -> PrometheusHandle {
    let builder = match PrometheusBuilder::new()
        // Use standard latency buckets for HTTP requests (0.005s to 10s)
        // This forces histograms (buckets) instead of summaries (quantiles), matching Grafana queries.
        .set_buckets(&[
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ]) {
        Ok(builder) => builder,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Failed to configure Prometheus buckets; using exporter defaults"
            );
            PrometheusBuilder::new()
        }
    };
    let handle = match builder.install_recorder() {
        Ok(handle) => handle,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Failed to install Prometheus recorder; falling back to local handle"
            );
            let recorder = PrometheusBuilder::new().build_recorder();
            recorder.handle()
        }
    };

    // Spawn upkeep task — required for histograms to drain properly
    let upkeep_handle = handle.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            upkeep_handle.run_upkeep();
        }
    });

    // Seed all counters to zero so Prometheus reports them immediately.
    // Without this, Grafana shows "No data" instead of "0" for counters
    // that haven't been incremented yet.
    seed_zero_counters();

    handle
}

/// Register all known counter metrics with a zero value so Prometheus
/// always reports them (instead of "No data" in Grafana).
fn seed_zero_counters() {
    // Query performance
    counter!(
        "qail_queries_total",
        &[
            ("table", "seed".to_string()),
            ("action", "seed".to_string()),
            ("status", "seed".to_string()),
        ]
    )
    .absolute(0);

    // Cache
    counter!("qail_cache_hits_total").absolute(0);
    counter!("qail_cache_misses_total").absolute(0);

    // Rate limiter
    counter!("qail_rate_limited_total").absolute(0);

    // EXPLAIN rejections
    counter!("qail_explain_rejections_total").absolute(0);

    // Complexity guard rejections
    counter!("qail_complexity_rejections_total").absolute(0);

    // DB acquire backpressure / queueing
    counter!(
        "qail_db_acquire_shed_total",
        &[("scope", "seed".to_string())]
    )
    .absolute(0);
    counter!("qail_db_acquire_timeouts_total").absolute(0);
    gauge!("qail_db_waiters_global").set(0.0);
    gauge!("qail_db_waiters_tracked_tenants").set(0.0);

    // RPC hardening + execution
    counter!("qail_rpc_allowlist_rejections_total").absolute(0);
    counter!("qail_rpc_signature_cache_hits_total").absolute(0);
    counter!("qail_rpc_signature_cache_misses_total").absolute(0);
    counter!("qail_rpc_signature_local_mismatch_total").absolute(0);
    counter!(
        "qail_rpc_signature_rejections_total",
        &[("reason", "seed".to_string())]
    )
    .absolute(0);
    counter!(
        "qail_rpc_calls_total",
        &[
            ("status", "seed".to_string()),
            ("result_format", "seed".to_string()),
        ]
    )
    .absolute(0);
    counter!("qail_rpc_binary_decode_fallback_total").absolute(0);

    // PostgreSQL SQLSTATE-classified errors
    counter!(
        "qail_db_errors_total",
        &[
            ("sqlstate", "seed".to_string()),
            ("class", "seed".to_string()),
        ]
    )
    .absolute(0);

    // Idempotency replays
    counter!("qail_idempotency_hits_total").absolute(0);

    // Transaction sessions
    counter!("qail_txn_sessions_created_total").absolute(0);
    counter!("qail_txn_sessions_expired_total").absolute(0);
    counter!("qail_txn_sessions_statement_limit_hit_total").absolute(0);
    counter!(
        "qail_txn_sessions_closed_total",
        &[("outcome", "seed".to_string())]
    )
    .absolute(0);
    counter!(
        "qail_txn_forced_rollbacks_total",
        &[("reason", "seed".to_string())]
    )
    .absolute(0);
    gauge!("qail_txn_active_sessions").set(0.0);
}

/// Record an idempotency key cache hit (response replayed).
pub fn record_idempotency_hit() {
    counter!("qail_idempotency_hits_total").increment(1);
}

/// Record transaction session creation.
pub fn record_txn_session_created() {
    counter!("qail_txn_sessions_created_total").increment(1);
    #[cfg(test)]
    TEST_TXN_CREATED.fetch_add(1, Ordering::Relaxed);
}

/// Record transaction session expiration due to max lifetime.
pub fn record_txn_session_expired() {
    counter!("qail_txn_sessions_expired_total").increment(1);
    #[cfg(test)]
    TEST_TXN_EXPIRED.fetch_add(1, Ordering::Relaxed);
}

/// Record transaction session statement-limit enforcement.
pub fn record_txn_statement_limit_hit() {
    counter!("qail_txn_sessions_statement_limit_hit_total").increment(1);
    #[cfg(test)]
    TEST_TXN_STATEMENT_LIMIT_HIT.fetch_add(1, Ordering::Relaxed);
}

/// Record transaction session closure outcome.
pub fn record_txn_session_closed(outcome: &str) {
    let labels = [("outcome", outcome.to_string())];
    counter!("qail_txn_sessions_closed_total", &labels).increment(1);
}

/// Record a forced rollback with a reason label.
pub fn record_txn_forced_rollback(reason: &str) {
    let labels = [("reason", reason.to_string())];
    counter!("qail_txn_forced_rollbacks_total", &labels).increment(1);
    #[cfg(test)]
    match reason {
        "capacity_guard" => {
            TEST_TXN_FORCED_CAPACITY.fetch_add(1, Ordering::Relaxed);
        }
        "lifetime_limit" => {
            TEST_TXN_FORCED_LIFETIME.fetch_add(1, Ordering::Relaxed);
        }
        "statement_limit" => {
            TEST_TXN_FORCED_STATEMENT.fetch_add(1, Ordering::Relaxed);
        }
        "idle_timeout" => {
            TEST_TXN_FORCED_IDLE.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

/// Record the current number of active transaction sessions.
pub fn record_txn_active_sessions(active: usize) {
    gauge!("qail_txn_active_sessions").set(active as f64);
    #[cfg(test)]
    TEST_TXN_ACTIVE.store(active as i64, Ordering::Relaxed);
}

/// Metrics handler - returns Prometheus format metrics
///
/// SECURITY (M4): When `admin_token` is configured, requires
/// `Authorization: Bearer <token>` to prevent exposing internal metrics.
pub async fn metrics_handler(
    State(state): State<Arc<crate::GatewayState>>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
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
    state.prometheus_handle.render().into_response()
}

// Metric recording helpers

/// Record a query execution.
///
/// # Arguments
///
/// * `table` — Target table name.
/// * `action` — CRUD action (`get`, `put`, `mod`, `del`).
/// * `duration_ms` — Query execution time in milliseconds.
/// * `success` — Whether the query succeeded.
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
/// * `hits` — Total cache hit count.
/// * `misses` — Total cache miss count.
/// * `entries` — Current number of cached entries.
/// * `weighted_bytes` — Estimated memory used by cache entries.
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
