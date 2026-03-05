use metrics::{counter, gauge};
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

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
