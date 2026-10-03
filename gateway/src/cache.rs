//! Query Cache Module
//!
//! Production-grade in-memory cache backed by moka (Window-TinyLFU).
//! Only caches GET/SELECT queries; mutations invalidate relevant cache entries.
//!
//! # Design
//! - **TinyLFU eviction**: Frequency-aware eviction keeps hot entries, evicts cold ones.
//! - **TTL expiry**: Entries expire after configurable TTL (default 60s).
//! - **Memory-aware**: Weigher tracks byte size of values, not just entry count.
//! - **Table invalidation**: Mutations invalidate all cache entries for the affected table.
//! - **Thread-safe**: All operations are safe for concurrent access without external locking.

use moka::notification::RemovalCause;
use moka::sync::Cache;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of cached entries.
    pub max_entries: usize,
    /// Time-to-live for each cache entry.
    pub ttl: Duration,
    /// Toggle cache on/off.
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            ttl: Duration::from_secs(60),
            enabled: true,
        }
    }
}

/// Thread-safe query cache with TTL and TinyLFU eviction (moka-backed)
pub struct QueryCache {
    /// moka cache: query_key → JSON result
    entries: Cache<String, String>,
    /// Table → cache keys for invalidation
    table_keys: Arc<RwLock<HashMap<String, HashSet<String>>>>,
    enabled: bool,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl QueryCache {
    /// Create a new cache from configuration.
    pub fn new(config: CacheConfig) -> Self {
        let table_keys = Arc::new(RwLock::new(HashMap::<String, HashSet<String>>::new()));
        let evict_index = Arc::clone(&table_keys);
        let entries = Cache::builder()
            .max_capacity(config.max_entries as u64)
            .time_to_live(config.ttl)
            // Weight by byte size: 1 unit per byte of key + value.
            // This prevents a few large responses from dominating the cache.
            .weigher(|key: &String, val: &String| -> u32 {
                (key.len() + val.len()).min(u32::MAX as usize) as u32
            })
            .eviction_listener(move |key, _value, cause| {
                if matches!(
                    cause,
                    RemovalCause::Expired | RemovalCause::Explicit | RemovalCause::Size
                ) && let Ok(mut map) = evict_index.write()
                {
                    remove_key_from_table_index(&mut map, key.as_ref());
                }
            })
            .build();

        Self {
            entries,
            table_keys,
            enabled: config.enabled,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Returns whether the cache is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Look up a cached query result. Returns `None` on miss.
    pub fn get(&self, query: &str) -> Option<String> {
        if !self.enabled {
            return None;
        }

        let result = if let Some(result) = self.entries.get(query) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(result)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        };

        // Export to Prometheus
        crate::metrics::record_cache_stats(
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.entries.entry_count() as usize,
            self.entries.weighted_size(),
        );

        result
    }

    /// Insert a query result into the cache, associated with a table.
    ///
    /// # Arguments
    ///
    /// * `query` — SQL query string used as cache key.
    /// * `table` — Table name for invalidation tracking.
    /// * `result` — Serialized query result to cache.
    pub fn set(&self, query: &str, table: &str, result: String) {
        self.set_for_tables(query, std::slice::from_ref(&table), result);
    }

    /// Insert a query result into the cache, associated with every table it
    /// reads from. Invalidating any of those tables removes the entry.
    pub fn set_for_tables(&self, query: &str, tables: &[&str], result: String) {
        if !self.enabled {
            return;
        }

        let key = query.to_string();
        self.entries.insert(key.clone(), result);

        // Track which keys belong to each table for invalidation.
        if let Ok(mut map) = self.table_keys.write() {
            remove_key_from_table_index(&mut map, &key);
            for table in tables {
                if table.is_empty() {
                    continue;
                }
                map.entry((*table).to_string())
                    .or_default()
                    .insert(key.clone());
            }
        }
    }

    /// Invalidate all cache entries for a table
    pub fn invalidate_table(&self, table: &str) {
        let keys = self
            .table_keys
            .write()
            .ok()
            .and_then(|mut map| map.remove(table));

        if let Some(keys) = keys {
            let count = keys.len();
            for key in &keys {
                self.entries.invalidate(key);
            }
            tracing::debug!("Invalidated {} cache entries for table '{}'", count, table);
        }
    }

    /// Invalidate every cached query result.
    ///
    /// Used by mutation surfaces that do not currently declare precise table
    /// dependencies, such as RPC functions.
    pub fn invalidate_all(&self) {
        if !self.enabled {
            return;
        }

        self.entries.invalidate_all();
        if let Ok(mut map) = self.table_keys.write() {
            map.clear();
        }
        tracing::debug!("Invalidated all cache entries");
    }

    /// Return a snapshot of cache statistics.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entries: self.entries.entry_count() as usize,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            weighted_size: self.entries.weighted_size(),
        }
    }
}

/// SHA-256 over length-framed fields, for cache keys that decide which stored
/// result a request is served. A collision replays one query's rows for another.
pub(crate) struct CacheKeyDigest(sha2::Sha256);

impl CacheKeyDigest {
    pub(crate) fn new(domain: &str) -> Self {
        use sha2::Digest;
        let mut digest = Self(sha2::Sha256::new());
        digest.field(domain.as_bytes());
        digest
    }

    pub(crate) fn field(&mut self, bytes: &[u8]) {
        use sha2::Digest;
        self.0.update((bytes.len() as u64).to_le_bytes());
        self.0.update(bytes);
    }

    /// The text form (`Display`, wire text v1) and `to_sql()` drop clauses
    /// (INSERT values, ON CONFLICT, DISTINCT, set ops, row locks), and
    /// serde_json writes NaN and ±inf all as `null`. The derived `Debug`
    /// prints every field, floats exactly and strings escaped.
    pub(crate) fn qail(&mut self, cmd: &qail_core::ast::Qail) {
        self.field(format!("{cmd:?}").as_bytes());
    }

    pub(crate) fn finish_hex(self) -> String {
        use sha2::Digest;
        format!("{:x}", self.0.finalize())
    }

    /// First 64 bits, for caches whose key type is `u64`.
    pub(crate) fn finish_u64(self) -> u64 {
        use sha2::Digest;
        let digest = self.0.finalize();
        let mut prefix = [0_u8; 8];
        prefix.copy_from_slice(&digest[..8]);
        u64::from_le_bytes(prefix)
    }
}

fn remove_key_from_table_index(map: &mut HashMap<String, HashSet<String>>, key: &str) {
    map.retain(|_, keys| {
        keys.remove(key);
        !keys.is_empty()
    });
}

/// Snapshot of cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Number of live entries.
    pub entries: usize,
    /// Total cache hits.
    pub hits: u64,
    /// Total cache misses.
    pub misses: u64,
    /// Total weighted size of all entries (bytes of key + value)
    pub weighted_size: u64,
}

impl CacheStats {
    /// Hit rate as a percentage
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            (self.hits as f64 / total as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests;
