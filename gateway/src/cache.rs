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

use moka::sync::Cache;
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// Table → list of cache keys for invalidation
    table_keys: RwLock<HashMap<String, Vec<String>>>,
    enabled: bool,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl QueryCache {
    /// Create a new cache from configuration.
    pub fn new(config: CacheConfig) -> Self {
        let entries = Cache::builder()
            .max_capacity(config.max_entries as u64)
            .time_to_live(config.ttl)
            // Weight by byte size: 1 unit per byte of key + value.
            // This prevents a few large responses from dominating the cache.
            .weigher(|key: &String, val: &String| -> u32 {
                (key.len() + val.len()).min(u32::MAX as usize) as u32
            })
            .build();

        Self {
            entries,
            table_keys: RwLock::new(HashMap::new()),
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
        if !self.enabled {
            return;
        }

        let key = query.to_string();
        self.entries.insert(key.clone(), result);

        // Track which keys belong to which table for invalidation
        if let Ok(mut map) = self.table_keys.write() {
            map.entry(table.to_string()).or_default().push(key);
        }
    }

    /// Invalidate all cache entries for a table
    pub fn invalidate_table(&self, table: &str) {
        if let Ok(mut map) = self.table_keys.write()
            && let Some(keys) = map.remove(table)
        {
            let count = keys.len();
            for key in &keys {
                self.entries.invalidate(key);
            }
            tracing::debug!("Invalidated {} cache entries for table '{}'", count, table);
        }
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
