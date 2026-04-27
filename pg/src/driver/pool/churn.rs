//! Pool statistics and connection churn circuit breaker.

use super::config::PoolConfig;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Pool statistics for monitoring.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Connections currently checked out by callers.
    pub active: usize,
    /// Connections idle in the pool, ready for reuse.
    pub idle: usize,
    /// Callers waiting for a connection.
    pub pending: usize,
    /// Maximum connections configured
    pub max_size: usize,
    /// Cumulative connections created since pool startup.
    pub total_created: usize,
}

pub(super) const POOL_CHURN_THRESHOLD: usize = 24;
const POOL_CHURN_WINDOW: Duration = Duration::from_secs(15);
const POOL_CHURN_COOLDOWN: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(super) struct PoolChurnState {
    window_started_at: Instant,
    failure_count: usize,
    open_until: Option<Instant>,
}

pub(super) fn pool_churn_registry() -> &'static std::sync::Mutex<HashMap<String, PoolChurnState>> {
    static REGISTRY: OnceLock<std::sync::Mutex<HashMap<String, PoolChurnState>>> = OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

pub(super) fn pool_churn_key(config: &PoolConfig) -> String {
    format!(
        "{}:{}:{}:{}",
        config.host, config.port, config.user, config.database
    )
}

pub(super) fn pool_churn_remaining_open(config: &PoolConfig) -> Option<Duration> {
    let now = Instant::now();
    let key = pool_churn_key(config);
    let Ok(mut registry) = pool_churn_registry().lock() else {
        return None;
    };
    let state = registry.get_mut(&key)?;
    let until = state.open_until?;
    if until > now {
        return Some(until.duration_since(now));
    }
    state.open_until = None;
    state.failure_count = 0;
    state.window_started_at = now;
    None
}

pub(super) fn record_pool_connection_destroy(reason: &'static str) {
    metrics::counter!("qail_pg_pool_connection_destroyed_total", "reason" => reason).increment(1);
}

pub(super) fn pool_churn_record_destroy(config: &PoolConfig, reason: &'static str) {
    record_pool_connection_destroy(reason);

    let now = Instant::now();
    let key = pool_churn_key(config);
    let Ok(mut registry) = pool_churn_registry().lock() else {
        return;
    };
    let state = registry.entry(key).or_insert_with(|| PoolChurnState {
        window_started_at: now,
        failure_count: 0,
        open_until: None,
    });

    if let Some(until) = state.open_until {
        if until > now {
            return;
        }
        state.open_until = None;
        state.failure_count = 0;
        state.window_started_at = now;
    }

    if now.duration_since(state.window_started_at) > POOL_CHURN_WINDOW {
        state.window_started_at = now;
        state.failure_count = 0;
    }

    state.failure_count += 1;
    if state.failure_count >= POOL_CHURN_THRESHOLD {
        metrics::counter!("qail_pg_pool_churn_circuit_open_total").increment(1);
        state.open_until = Some(now + POOL_CHURN_COOLDOWN);
        state.failure_count = 0;
        state.window_started_at = now;
        tracing::warn!(
            host = %config.host,
            port = config.port,
            user = %config.user,
            db = %config.database,
            threshold = POOL_CHURN_THRESHOLD as u64,
            cooldown_ms = POOL_CHURN_COOLDOWN.as_millis() as u64,
            "pool_connection_churn_circuit_opened"
        );
    }
}

pub(super) fn decrement_active_count_saturating(counter: &AtomicUsize) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        if current == 0 {
            return;
        }
        match counter.compare_exchange_weak(
            current,
            current - 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}
