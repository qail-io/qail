//! Integration test for qail-redis driver with unified Qail API.
//!
//! Run with: cargo run --example integration_test
//!
//! Requires Redis running on localhost:6379:
//!   brew services start redis
//!   # or: docker run -d -p 6379:6379 redis:7-alpine

use qail_redis::{RedisDriver, RedisResult};

#[tokio::main]
async fn main() -> RedisResult<()> {
    println!("🔌 Connecting to Redis...");
    let mut driver = RedisDriver::connect("127.0.0.1", 6379).await?;

    // Test PING
    println!("📡 PING...");
    let pong = driver.ping().await?;
    assert!(pong, "PING should return PONG");
    println!("   ✅ PONG received");

    // Test SET/GET
    println!("📝 SET/GET...");
    let key = "qail:test:hello";
    driver.set(key, b"world").await?;
    let value = driver.get_str(key).await?;
    assert_eq!(value, Some("world".to_string()));
    println!("   ✅ SET/GET works");

    // Test SET with TTL
    println!("⏱️  SET with TTL...");
    let ttl_key = "qail:test:ttl";
    driver.set_ex(ttl_key, b"expires", 60).await?;
    let ttl = driver.ttl(ttl_key).await?;
    assert!(ttl > 0 && ttl <= 60, "TTL should be between 1-60");
    println!("   ✅ TTL is {} seconds", ttl);

    // Test INCR
    println!("🔢 INCR...");
    let counter_key = "qail:test:counter";
    driver.del(counter_key).await?; // Reset
    let val1 = driver.incr(counter_key).await?;
    let val2 = driver.incr(counter_key).await?;
    let val3 = driver.incr(counter_key).await?;
    assert_eq!(val1, 1);
    assert_eq!(val2, 2);
    assert_eq!(val3, 3);
    println!("   ✅ INCR works: 1 → 2 → 3");

    // Test EXISTS
    println!("🔍 EXISTS...");
    let exists = driver.exists(key).await?;
    assert!(exists, "Key should exist");
    let not_exists = driver.exists("qail:nonexistent").await?;
    assert!(!not_exists, "Key should not exist");
    println!("   ✅ EXISTS works");

    // Test DEL
    println!("🗑️  DEL...");
    let deleted = driver.del(key).await?;
    assert_eq!(deleted, 1);
    let after_del = driver.get(key).await?;
    assert!(after_del.is_none(), "Key should be deleted");
    println!("   ✅ DEL works");

    // Cleanup
    driver.del(ttl_key).await?;
    driver.del(counter_key).await?;

    println!("\n🎉 All integration tests passed!");
    println!("   Postgres stores facts, Qdrant stores meaning, Redis stores time — QAIL decides.");

    Ok(())
}
