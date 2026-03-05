use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Cached response for an idempotency key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CachedResponse {
    /// HTTP status code of the original response.
    pub status: u16,
    /// Response body bytes.
    pub body: Vec<u8>,
    /// Content-Type header value.
    pub content_type: String,
    /// SECURITY: Request fingerprint (method+path) — used to detect
    /// key reuse across different mutation routes.
    pub request_fingerprint: String,
}

/// In-memory idempotency store backed by moka cache.
#[derive(Debug)]
pub struct IdempotencyStore {
    /// Cache: compound key (tenant_scope + idempotency_key) → cached response.
    cache: moka::sync::Cache<String, CachedResponse>,
    /// SECURITY: In-flight keys currently being processed.
    /// Prevents concurrent duplicate execution of the same idempotency key.
    pub(crate) in_flight: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl IdempotencyStore {
    /// Create a new idempotency store.
    ///
    /// - `max_entries`: maximum number of cached responses (LRU eviction).
    /// - `ttl`: time-to-live for cached entries.
    pub(crate) fn new(max_entries: u64, ttl: Duration) -> Self {
        Self {
            cache: moka::sync::Cache::builder()
                .max_capacity(max_entries)
                .time_to_live(ttl)
                .build(),
            in_flight: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// Default production store: 100k entries, 24-hour TTL.
    pub(crate) fn production() -> Self {
        Self::new(100_000, Duration::from_secs(86400))
    }

    /// Build the composite cache key: `{tenant_scope}:{idempotency_key}`.
    pub(crate) fn cache_key(tenant_scope: &str, idempotency_key: &str) -> String {
        format!("{}:{}", tenant_scope, idempotency_key)
    }

    /// Look up a cached response by tenant scope + idempotency key.
    pub(crate) fn get(&self, tenant_scope: &str, idempotency_key: &str) -> Option<CachedResponse> {
        self.cache
            .get(&Self::cache_key(tenant_scope, idempotency_key))
    }

    /// Store a response in the idempotency cache.
    pub(crate) fn insert(
        &self,
        tenant_scope: &str,
        idempotency_key: &str,
        response: CachedResponse,
    ) {
        self.cache
            .insert(Self::cache_key(tenant_scope, idempotency_key), response);
    }

    /// Number of entries currently cached (for metrics).
    pub(crate) fn len(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Returns `true` if the cache is empty.
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub(crate) fn lock_in_flight_set(
    in_flight: &std::sync::Mutex<std::collections::HashSet<String>>,
) -> std::sync::MutexGuard<'_, std::collections::HashSet<String>> {
    match in_flight.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::error!(
                "Idempotency in-flight lock poisoned; recovering inner state to stay available"
            );
            poisoned.into_inner()
        }
    }
}
