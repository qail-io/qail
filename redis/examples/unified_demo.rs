//! QAIL Unified Driver Demo
//!
//! Demonstrates the QAIL vision with unified Qail AST across all databases:
//! "Postgres stores facts, Qdrant stores meaning, Redis stores time — QAIL decides."
//!
//! ## Cargo.toml options (pick what you need):
//! ```toml
//! # Facts (PostgreSQL) - 353k q/s, 4% faster than libpq
//! qail-pg = "0.14"
//!
//! # Meaning (Qdrant) - zero-copy gRPC, matches official client
//! qail-qdrant = "0.14"
//!
//! # Time (Redis) - RESP3, unified Qail AST
//! qail-redis = "0.14"
//! ```
//!
//! Run: cargo run -p qail-redis --example unified_demo

use qail_core::prelude::*;
use qail_redis::{RedisDriver, RedisExt, RedisResult};

#[tokio::main]
async fn main() -> RedisResult<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("  QAIL: The Decision Layer (Unified AST)");
    println!("  Postgres stores facts, Qdrant stores meaning, Redis stores time");
    println!("═══════════════════════════════════════════════════════════════\n");

    // =========================================================================
    // REDIS: "Time" - Ephemeral state, caching, sessions
    // =========================================================================
    println!("🕐 REDIS (Time) - Connecting...");
    let mut redis = RedisDriver::connect("127.0.0.1", 6379).await?;
    println!("   ✅ Connected\n");

    // Session management using unified Qail AST
    println!("   📌 Session Management (Qail AST):");
    let session_id = "session:user:12345";
    let session_data = r#"{"user_id":12345,"role":"admin"}"#;
    
    // Unified Qail API with .redis_ex() extension
    let set_cmd = Qail::redis_set(session_id, session_data.as_bytes().to_vec()).redis_ex(3600);
    redis.execute(&set_cmd).await?;
    println!("      SET {} (TTL: 1 hour)", session_id);
    
    // GET using Qail
    let get_cmd = Qail::redis_get(session_id);
    redis.execute(&get_cmd).await?;
    let data = redis.get_str(session_id).await?;
    println!("      GET {} → {:?}", session_id, data);
    
    // TTL check
    let ttl = redis.ttl(session_id).await?;
    println!("      TTL {} → {} seconds remaining", session_id, ttl);

    // Rate limiting
    println!("\n   📌 Rate Limiting (Qail AST):");
    let rate_key = "rate:api:user:12345";
    redis.del(rate_key).await?; // Reset for demo
    
    // INCR using Qail::redis_incr()
    let count1 = redis.incr(rate_key).await?;
    let count2 = redis.incr(rate_key).await?;
    let count3 = redis.incr(rate_key).await?;
    
    // EXPIRE using Qail::redis_expire()
    let expire_cmd = Qail::redis_expire(rate_key, 60);
    redis.execute(&expire_cmd).await?;
    
    println!("      INCR {} → {} → {} → {}", rate_key, count1, count2, count3);
    println!("      EXPIRE {} 60s (window reset)", rate_key);

    // Cleanup
    redis.del(session_id).await?;
    redis.del(rate_key).await?;

    // =========================================================================
    // Summary: Unified Qail AST
    // =========================================================================
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  🎯 UNIFIED QAIL AST");
    println!("═══════════════════════════════════════════════════════════════");
    println!("
  // PostgreSQL - Qail AST
  Qail::get(\"users\").filter(\"active\", Eq, true).limit(10)
  
  // Qdrant - Qail AST
  Qail::search(\"products\").vector(&embedding).limit(10)
  
  // Redis - Qail AST (NEW!)
  Qail::redis_set(\"session\", data).redis_ex(3600)
  Qail::redis_get(\"session\")
  Qail::redis_incr(\"counter\")
  
  Same Qail type. Same philosophy. Different backends.
");

    println!("═══════════════════════════════════════════════════════════════");
    println!("  🎉 QAIL DECIDES");
    println!("═══════════════════════════════════════════════════════════════\n");

    Ok(())
}
