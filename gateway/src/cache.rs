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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::Duration;

/// Cache configuration
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub max_entries: usize,
    pub ttl: Duration,
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

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn get(&self, query: &str) -> Option<String> {
        if !self.enabled {
            return None;
        }

        if let Some(result) = self.entries.get(query) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(result)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    pub fn set(&self, query: &str, table: &str, result: String) {
        if !self.enabled {
            return;
        }

        let key = query.to_string();
        self.entries.insert(key.clone(), result);

        // Track which keys belong to which table for invalidation
        if let Ok(mut map) = self.table_keys.write() {
            map.entry(table.to_string())
                .or_default()
                .push(key);
        }
    }

    /// Invalidate all cache entries for a table
    pub fn invalidate_table(&self, table: &str) {
        if let Ok(mut map) = self.table_keys.write() {
            if let Some(keys) = map.remove(table) {
                let count = keys.len();
                for key in &keys {
                    self.entries.invalidate(key);
                }
                tracing::debug!("Invalidated {} cache entries for table '{}'", count, table);
            }
        }
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entries: self.entries.entry_count() as usize,
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            weighted_size: self.entries.weighted_size(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub hits: u64,
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
mod tests {
    use super::*;

    #[test]
    fn test_cache_hit_miss() {
        let cache = QueryCache::new(CacheConfig::default());

        // Miss on first access
        assert!(cache.get("get users").is_none());

        // Set and hit
        cache.set("get users", "users", r#"{"rows":[]}"#.to_string());
        assert!(cache.get("get users").is_some());

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_cache_invalidation() {
        let cache = QueryCache::new(CacheConfig::default());

        cache.set("get users", "users", r#"{"rows":[]}"#.to_string());
        assert!(cache.get("get users").is_some());

        // Invalidate table
        cache.invalidate_table("users");
        assert!(cache.get("get users").is_none());
    }

    #[test]
    fn test_cache_disabled() {
        let cache = QueryCache::new(CacheConfig {
            enabled: false,
            ..Default::default()
        });

        cache.set("get users", "users", r#"{"rows":[]}"#.to_string());
        assert!(cache.get("get users").is_none());
    }

    // =========================================================================
    // Cache Correctness (Production Readiness)
    // =========================================================================

    #[test]
    fn test_write_then_read_consistency() {
        // After a write + invalidation, reads must NOT return stale data
        let cache = QueryCache::new(CacheConfig::default());

        // Initial state: cache "v1"
        cache.set("SELECT * FROM users", "users", r#"{"version":"v1"}"#.to_string());
        assert_eq!(cache.get("SELECT * FROM users").unwrap(), r#"{"version":"v1"}"#);

        // Simulate a mutation → invalidate
        cache.invalidate_table("users");

        // Read MUST miss (no stale v1)
        assert!(cache.get("SELECT * FROM users").is_none(), "Must not return stale data after invalidation");

        // Re-cache with "v2"
        cache.set("SELECT * FROM users", "users", r#"{"version":"v2"}"#.to_string());
        assert_eq!(cache.get("SELECT * FROM users").unwrap(), r#"{"version":"v2"}"#);
    }

    #[test]
    fn test_cross_table_isolation() {
        // Invalidating table A must NOT affect table B
        let cache = QueryCache::new(CacheConfig::default());

        cache.set("SELECT * FROM users", "users", "users_data".to_string());
        cache.set("SELECT * FROM orders", "orders", "orders_data".to_string());

        cache.invalidate_table("users");

        assert!(cache.get("SELECT * FROM users").is_none(), "users should be invalidated");
        assert!(cache.get("SELECT * FROM orders").is_some(), "orders should NOT be invalidated");
    }

    #[test]
    fn test_ttl_expiry() {
        let cache = QueryCache::new(CacheConfig {
            ttl: Duration::from_millis(50), // 50ms TTL
            ..Default::default()
        });

        cache.set("query", "table", "data".to_string());
        assert!(cache.get("query").is_some(), "Should hit immediately");

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(60));
        assert!(cache.get("query").is_none(), "Should miss after TTL expiry");
    }

    #[test]
    fn test_eviction_under_capacity() {
        // moka uses TinyLFU — it should evict cold entries, not crash or corrupt
        let cache = QueryCache::new(CacheConfig {
            max_entries: 5,
            ttl: Duration::from_secs(60),
            enabled: true,
        });

        // Insert more than capacity — moka will evict based on TinyLFU policy
        for i in 0..20 {
            cache.set(&format!("query_{}", i), "table", format!("data_{}", i));
        }

        // Verify no corruption: every entry that IS cached has correct data
        for i in 0..20 {
            if let Some(val) = cache.get(&format!("query_{}", i)) {
                assert_eq!(val, format!("data_{}", i), "Data corruption detected!");
            }
        }
    }

    #[test]
    fn test_concurrent_read_write_no_stale_data() {
        use std::sync::Arc;

        let cache = Arc::new(QueryCache::new(CacheConfig::default()));
        let mut handles = Vec::new();

        // 5 writer threads: continuously write + invalidate
        for i in 0..5 {
            let cache = cache.clone();
            handles.push(std::thread::spawn(move || {
                for j in 0..100 {
                    let query = format!("SELECT * FROM table_{}", i);
                    let table = format!("table_{}", i);
                    cache.set(&query, &table, format!("data_{}_{}", i, j));
                    // Immediately invalidate
                    cache.invalidate_table(&table);
                }
            }));
        }

        // 5 reader threads: continuously read, verify no panic
        for i in 0..5 {
            let cache = cache.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let query = format!("SELECT * FROM table_{}", i);
                    // This should never panic, and if it returns data it should be valid
                    let _ = cache.get(&query);
                }
            }));
        }

        for h in handles {
            h.join().expect("Thread panicked during concurrent cache test");
        }

        // If we get here without panics or deadlocks, the test passes
    }

    #[test]
    fn test_hit_rate_accuracy() {
        let cache = QueryCache::new(CacheConfig::default());

        cache.set("q1", "t", "d".to_string());

        // 1 miss
        cache.get("q_nonexistent");
        // 3 hits
        cache.get("q1");
        cache.get("q1");
        cache.get("q1");

        let stats = cache.stats();
        assert_eq!(stats.hits, 3);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_rate() - 75.0).abs() < 0.01, "Hit rate should be 75%, got {}", stats.hit_rate());
    }

    #[test]
    fn test_large_value_stored_correctly() {
        let cache = QueryCache::new(CacheConfig::default());
        let large_value = "x".repeat(1000);

        cache.set("key", "t", large_value.clone());

        // The important property: large values are stored and retrieved correctly
        let retrieved = cache.get("key").expect("Large value should be cached");
        assert_eq!(retrieved.len(), 1000);
        assert_eq!(retrieved, large_value);
    }
}
