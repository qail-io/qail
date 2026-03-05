//! GSS/Kerberos circuit breaker and retry logic.

use super::config::PoolConfig;
use crate::driver::PgError;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

pub(super) fn should_retry_gss_connect_error(
    config: &PoolConfig,
    attempt: usize,
    err: &PgError,
) -> bool {
    if attempt >= config.gss_connect_retries {
        return false;
    }

    if !is_gss_auth_enabled(config) {
        return false;
    }

    match err {
        PgError::Auth(msg) | PgError::Connection(msg) => is_transient_gss_message(msg),
        PgError::Timeout(_) => true,
        PgError::Io(io) => matches!(
            io.kind(),
            std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::WouldBlock
        ),
        _ => false,
    }
}

fn is_gss_auth_enabled(config: &PoolConfig) -> bool {
    config.gss_token_provider.is_some()
        || config.gss_token_provider_ex.is_some()
        || config.auth_settings.allow_kerberos_v5
        || config.auth_settings.allow_gssapi
        || config.auth_settings.allow_sspi
}

fn is_transient_gss_message(msg: &str) -> bool {
    let msg = msg.to_ascii_lowercase();
    [
        "temporary",
        "temporarily unavailable",
        "try again",
        "timed out",
        "timeout",
        "connection reset",
        "connection refused",
        "network is unreachable",
        "resource temporarily unavailable",
        "service unavailable",
    ]
    .iter()
    .any(|needle| msg.contains(needle))
}

pub(super) fn gss_retry_delay(base: Duration, attempt: usize) -> Duration {
    let factor = 1u32 << attempt.min(6);
    let delay = base.saturating_mul(factor).min(Duration::from_secs(5));
    let jitter_cap_ms = ((delay.as_millis() as u64) / 5).clamp(1, 250);
    let jitter_ms = pseudo_random_jitter_ms(jitter_cap_ms);
    delay.saturating_add(Duration::from_millis(jitter_ms))
}

fn pseudo_random_jitter_ms(max_inclusive: u64) -> u64 {
    if max_inclusive == 0 {
        return 0;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    nanos % (max_inclusive + 1)
}

#[derive(Debug, Clone)]
struct GssCircuitState {
    window_started_at: Instant,
    failure_count: usize,
    open_until: Option<Instant>,
}

fn gss_circuit_registry() -> &'static std::sync::Mutex<HashMap<String, GssCircuitState>> {
    static REGISTRY: OnceLock<std::sync::Mutex<HashMap<String, GssCircuitState>>> = OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn gss_circuit_key(config: &PoolConfig) -> String {
    format!(
        "{}:{}:{}:{}",
        config.host, config.port, config.user, config.database
    )
}

pub(super) fn gss_circuit_remaining_open(config: &PoolConfig) -> Option<Duration> {
    if !is_gss_auth_enabled(config)
        || config.gss_circuit_breaker_threshold == 0
        || config.gss_circuit_breaker_window.is_zero()
        || config.gss_circuit_breaker_cooldown.is_zero()
    {
        return None;
    }

    let now = Instant::now();
    let key = gss_circuit_key(config);
    let Ok(mut registry) = gss_circuit_registry().lock() else {
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

pub(super) fn should_track_gss_circuit_error(config: &PoolConfig, err: &PgError) -> bool {
    if !is_gss_auth_enabled(config) {
        return false;
    }
    matches!(
        err,
        PgError::Auth(_) | PgError::Connection(_) | PgError::Timeout(_) | PgError::Io(_)
    )
}

pub(super) fn gss_circuit_record_failure(config: &PoolConfig) {
    if !is_gss_auth_enabled(config)
        || config.gss_circuit_breaker_threshold == 0
        || config.gss_circuit_breaker_window.is_zero()
        || config.gss_circuit_breaker_cooldown.is_zero()
    {
        return;
    }

    let now = Instant::now();
    let key = gss_circuit_key(config);
    let Ok(mut registry) = gss_circuit_registry().lock() else {
        return;
    };
    let state = registry
        .entry(key.clone())
        .or_insert_with(|| GssCircuitState {
            window_started_at: now,
            failure_count: 0,
            open_until: None,
        });

    if now.duration_since(state.window_started_at) > config.gss_circuit_breaker_window {
        state.window_started_at = now;
        state.failure_count = 0;
        state.open_until = None;
    }

    state.failure_count += 1;
    if state.failure_count >= config.gss_circuit_breaker_threshold {
        metrics::counter!("qail_pg_gss_circuit_open_total").increment(1);
        state.open_until = Some(now + config.gss_circuit_breaker_cooldown);
        state.failure_count = 0;
        state.window_started_at = now;
        tracing::warn!(
            host = %config.host,
            port = config.port,
            user = %config.user,
            db = %config.database,
            threshold = config.gss_circuit_breaker_threshold,
            cooldown_ms = config.gss_circuit_breaker_cooldown.as_millis() as u64,
            "gss_connect_circuit_opened"
        );
    }
}

pub(super) fn gss_circuit_record_success(config: &PoolConfig) {
    if !is_gss_auth_enabled(config) {
        return;
    }
    let key = gss_circuit_key(config);
    if let Ok(mut registry) = gss_circuit_registry().lock() {
        registry.remove(&key);
    }
}
