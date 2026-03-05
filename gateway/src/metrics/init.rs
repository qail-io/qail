use metrics::{counter, gauge};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

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

    // Spawn upkeep task - required for histograms to drain properly
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
