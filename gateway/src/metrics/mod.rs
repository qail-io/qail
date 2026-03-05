//! Prometheus metrics module
//!
//! Exposes gateway metrics for monitoring.

mod init;
mod records;
mod txn;

pub use init::init_metrics;
pub use records::{
    QueryTimer, metrics_handler, record_batch, record_cache_stats, record_complexity_rejected,
    record_db_acquire_shed, record_db_acquire_timeout, record_db_acquire_wait, record_db_error,
    record_db_waiters, record_explain_rejected, record_http_request, record_pool_stats,
    record_query, record_rate_limited, record_rpc_allowlist_rejection,
    record_rpc_binary_decode_fallback, record_rpc_call, record_rpc_signature_cache_hit,
    record_rpc_signature_cache_miss, record_rpc_signature_local_mismatch,
    record_rpc_signature_rejection, record_ws_connection,
};
#[cfg(test)]
pub use txn::{
    TxnTestMetricsSnapshot, reset_txn_test_metrics, txn_test_metrics_snapshot,
    txn_test_serial_guard,
};
pub use txn::{
    record_idempotency_hit, record_txn_active_sessions, record_txn_forced_rollback,
    record_txn_session_closed, record_txn_session_created, record_txn_session_expired,
    record_txn_statement_limit_hit,
};
