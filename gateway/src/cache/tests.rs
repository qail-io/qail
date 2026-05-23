use super::*;

#[test]
fn test_cache_hit_miss() {
    let cache = QueryCache::new(CacheConfig::default());

    assert!(cache.get("get users").is_none());

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

#[test]
fn test_write_then_read_consistency() {
    let cache = QueryCache::new(CacheConfig::default());

    cache.set(
        "SELECT * FROM users",
        "users",
        r#"{"version":"v1"}"#.to_string(),
    );
    assert_eq!(
        cache.get("SELECT * FROM users").unwrap(),
        r#"{"version":"v1"}"#
    );

    cache.invalidate_table("users");
    assert!(
        cache.get("SELECT * FROM users").is_none(),
        "Must not return stale data after invalidation"
    );

    cache.set(
        "SELECT * FROM users",
        "users",
        r#"{"version":"v2"}"#.to_string(),
    );
    assert_eq!(
        cache.get("SELECT * FROM users").unwrap(),
        r#"{"version":"v2"}"#
    );
}

#[test]
fn test_cross_table_isolation() {
    let cache = QueryCache::new(CacheConfig::default());

    cache.set("SELECT * FROM users", "users", "users_data".to_string());
    cache.set("SELECT * FROM orders", "orders", "orders_data".to_string());

    cache.invalidate_table("users");

    assert!(
        cache.get("SELECT * FROM users").is_none(),
        "users should be invalidated"
    );
    assert!(
        cache.get("SELECT * FROM orders").is_some(),
        "orders should NOT be invalidated"
    );
}

#[test]
fn test_multi_table_entry_invalidates_from_any_table() {
    let cache = QueryCache::new(CacheConfig::default());

    cache.set_for_tables(
        "SELECT * FROM orders JOIN users",
        &["orders", "users"],
        "joined_data".to_string(),
    );
    assert!(cache.get("SELECT * FROM orders JOIN users").is_some());

    cache.invalidate_table("users");

    assert!(
        cache.get("SELECT * FROM orders JOIN users").is_none(),
        "joined cache entry should be invalidated when a related table changes"
    );
}

#[test]
fn test_invalidate_all_clears_entries_and_table_index() {
    let cache = QueryCache::new(CacheConfig::default());

    cache.set("SELECT * FROM users", "users", "users_data".to_string());
    cache.set("SELECT * FROM orders", "orders", "orders_data".to_string());

    cache.invalidate_all();

    assert!(cache.get("SELECT * FROM users").is_none());
    assert!(cache.get("SELECT * FROM orders").is_none());
    assert!(
        cache
            .table_keys
            .read()
            .expect("table index lock")
            .is_empty()
    );
}

#[test]
fn test_ttl_expiry() {
    let cache = QueryCache::new(CacheConfig {
        ttl: Duration::from_millis(50),
        ..Default::default()
    });

    cache.set("query", "table", "data".to_string());
    assert!(cache.get("query").is_some(), "Should hit immediately");

    std::thread::sleep(Duration::from_millis(60));
    assert!(cache.get("query").is_none(), "Should miss after TTL expiry");
}

#[test]
fn test_table_index_pruned_on_ttl_expiry() {
    let cache = QueryCache::new(CacheConfig {
        ttl: Duration::from_millis(20),
        ..Default::default()
    });

    cache.set("query", "users", "data".to_string());
    assert_eq!(
        cache
            .table_keys
            .read()
            .expect("table index lock")
            .get("users")
            .map(|keys| keys.len()),
        Some(1)
    );

    std::thread::sleep(Duration::from_millis(30));
    assert!(cache.get("query").is_none());
    cache.entries.run_pending_tasks();

    assert!(
        !cache
            .table_keys
            .read()
            .expect("table index lock")
            .contains_key("users"),
        "expired entries must be removed from the table invalidation index"
    );
}

#[test]
fn test_table_index_deduplicates_repeated_cache_key() {
    let cache = QueryCache::new(CacheConfig::default());

    for i in 0..5 {
        cache.set("SELECT * FROM users", "users", format!(r#"{{"v":{}}}"#, i));
    }

    assert_eq!(
        cache
            .table_keys
            .read()
            .expect("table index lock")
            .get("users")
            .map(|keys| keys.len()),
        Some(1)
    );
}

#[test]
fn test_eviction_under_capacity() {
    let cache = QueryCache::new(CacheConfig {
        max_entries: 5,
        ttl: Duration::from_secs(60),
        enabled: true,
    });

    for i in 0..20 {
        cache.set(&format!("query_{}", i), "table", format!("data_{}", i));
    }

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

    for i in 0..5 {
        let cache = Arc::clone(&cache);
        handles.push(std::thread::spawn(move || {
            for j in 0..100 {
                let query = format!("SELECT * FROM table_{}", i);
                let table = format!("table_{}", i);
                cache.set(&query, &table, format!("data_{}_{}", i, j));
                cache.invalidate_table(&table);
            }
        }));
    }

    for i in 0..5 {
        let cache = Arc::clone(&cache);
        handles.push(std::thread::spawn(move || {
            for _ in 0..100 {
                let query = format!("SELECT * FROM table_{}", i);
                let _ = cache.get(&query);
            }
        }));
    }

    for h in handles {
        h.join()
            .expect("Thread panicked during concurrent cache test");
    }
}

#[test]
fn test_hit_rate_accuracy() {
    let cache = QueryCache::new(CacheConfig::default());

    cache.set("q1", "t", "d".to_string());

    cache.get("q_nonexistent");
    cache.get("q1");
    cache.get("q1");
    cache.get("q1");

    let stats = cache.stats();
    assert_eq!(stats.hits, 3);
    assert_eq!(stats.misses, 1);
    assert!(
        (stats.hit_rate() - 75.0).abs() < 0.01,
        "Hit rate should be 75%, got {}",
        stats.hit_rate()
    );
}

#[test]
fn test_large_value_stored_correctly() {
    let cache = QueryCache::new(CacheConfig::default());
    let large_value = "x".repeat(1000);

    cache.set("key", "t", large_value.clone());

    let retrieved = cache.get("key").expect("Large value should be cached");
    assert_eq!(retrieved.len(), 1000);
    assert_eq!(retrieved, large_value);
}
