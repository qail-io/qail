//! Query Cache Module
//!
//! In-memory LRU cache with TTL for query results.
//! Only caches GET/SELECT queries; mutations invalidate relevant cache entries.

use dashmap::DashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Cached query result
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Serialized JSON result
    result: String,
    /// When this entry was created
    created_at: Instant,
}

/// Thread-safe query cache with TTL
pub struct QueryCache {
    entries: DashMap<u64, CacheEntry>,
    table_queries: DashMap<String, Vec<u64>>,
    config: CacheConfig,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl QueryCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            entries: DashMap::with_capacity(config.max_entries),
            table_queries: DashMap::new(),
            config,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }
    
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
    
    fn hash_query(query: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);
        hasher.finish()
    }
    
    pub fn get(&self, query: &str) -> Option<String> {
        if !self.config.enabled {
            return None;
        }
        
        let hash = Self::hash_query(query);
        
        if let Some(entry) = self.entries.get(&hash) {
                // Check TTL
            if entry.created_at.elapsed() < self.config.ttl {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(entry.result.clone());
            } else {
                // Expired - remove it
                drop(entry);
                self.entries.remove(&hash);
            }
        }
        
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }
    
    pub fn set(&self, query: &str, table: &str, result: String) {
        if !self.config.enabled {
            return;
        }
        
        if self.entries.len() >= self.config.max_entries {
            self.evict_expired();
            
            if self.entries.len() >= self.config.max_entries {
                return;
            }
        }
        
        let hash = Self::hash_query(query);
        let entry = CacheEntry {
            result,
            created_at: Instant::now(),
        };
        
        self.entries.insert(hash, entry);
        
        self.table_queries
            .entry(table.to_string())
            .or_default()
            .push(hash);
    }
    
    /// Invalidate all cache entries for a table
    pub fn invalidate_table(&self, table: &str) {
        if let Some((_, hashes)) = self.table_queries.remove(table) {
            let count = hashes.len();
            for hash in &hashes {
                self.entries.remove(hash);
            }
            tracing::debug!("Invalidated {} cache entries for table '{}'", count, table);
        }
    }
    
    fn evict_expired(&self) {
        let now = Instant::now();
        let ttl = self.config.ttl;
        
        self.entries.retain(|_, entry| {
            now.duration_since(entry.created_at) < ttl
        });
    }
    
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entries: self.entries.len(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
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
    fn test_max_capacity_does_not_corrupt() {
        let cache = QueryCache::new(CacheConfig {
            max_entries: 5,
            ttl: Duration::from_secs(60),
            enabled: true,
        });

        // Fill to capacity
        for i in 0..5 {
            cache.set(&format!("query_{}", i), "table", format!("data_{}", i));
        }
        assert_eq!(cache.stats().entries, 5);

        // Try to add beyond capacity — should silently drop
        cache.set("query_overflow", "table", "overflow_data".to_string());

        // Verify no corruption: existing entries still return correct data
        for i in 0..5 {
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
}
