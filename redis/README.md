# qail-redis

[![Crates.io](https://img.shields.io/crates/v/qail-redis.svg)](https://crates.io/crates/qail-redis)
[![Documentation](https://docs.rs/qail-redis/badge.svg)](https://docs.rs/qail-redis)

> ⚠️ **ALPHA** - Early development. API may change.

**"Redis stores time — QAIL decides."**

Native Redis driver using the unified QAIL AST. Part of the QAIL decision layer: PostgreSQL (facts) + Qdrant (meaning) + Redis (time).

## Installation

```toml
[dependencies]
qail-redis = "0.14"
```

## Quick Start

```rust
use qail_redis::{RedisDriver, RedisExt};
use qail_core::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut driver = RedisDriver::connect("127.0.0.1", 6379).await?;
    
    // Unified Qail AST - same pattern as PostgreSQL/Qdrant
    driver.execute(&Qail::redis_set("session:123", b"data".to_vec()).redis_ex(3600)).await?;
    
    let value = driver.get_str("session:123").await?;
    println!("Session: {:?}", value);
    
    Ok(())
}
```

## The QAIL Decision Layer

qail-redis is designed to work alongside `qail-pg` and `qail-qdrant` for unified data access:

```rust
use qail_core::prelude::*;
use qail_pg::PgDriver;
use qail_qdrant::QdrantDriver;
use qail_redis::{RedisDriver, RedisExt};

// Connect to all three backends
let pg = PgDriver::connect("postgres://localhost/app").await?;
let qdrant = QdrantDriver::connect("http://localhost:6334").await?;
let mut redis = RedisDriver::connect("127.0.0.1", 6379).await?;

// PostgreSQL: Facts (persistent relational data)
let users = pg.query(&Qail::get("users").filter("active", Eq, true)).await?;

// Qdrant: Meaning (semantic similarity)
let similar = qdrant.search(&Qail::search("products").vector(&embedding).limit(10)).await?;

// Redis: Time (ephemeral state, caching, sessions)
redis.execute(&Qail::redis_set("cache:user:123", user_json).redis_ex(300)).await?;
let cached = redis.get_str("cache:user:123").await?;
```

Same `Qail` type. Same philosophy. Different backends.

## Available Commands

| Command | Qail Constructor | Description |
|---------|-----------------|-------------|
| GET | `Qail::redis_get("key")` | Get value |
| SET | `Qail::redis_set("key", value)` | Set value |
| SET EX | `.redis_ex(seconds)` | With TTL |
| SET NX | `.redis_nx()` | Only if not exists |
| SET XX | `.redis_xx()` | Only if exists |
| DEL | `Qail::redis_del("key")` | Delete key |
| INCR | `Qail::redis_incr("key")` | Increment |
| DECR | `Qail::redis_decr("key")` | Decrement |
| TTL | `Qail::redis_ttl("key")` | Get TTL |
| EXPIRE | `Qail::redis_expire("key", 60)` | Set TTL |
| EXISTS | `Qail::redis_exists("key")` | Check existence |
| PING | `Qail::redis_ping()` | Health check |

## Connection Pooling

```rust
use qail_redis::{RedisPool, PoolConfig};

let pool = RedisPool::new(
    PoolConfig::new("127.0.0.1", 6379).max_connections(10)
);

let mut conn = pool.get().await?;
conn.set("key", b"value").await?;
// Connection returns to pool on drop
```

## Features

- **Unified Qail AST**: Same `Qail` type as PostgreSQL and Qdrant
- **Native RESP3**: Direct wire protocol encoding (no string parsing)
- **Connection Pooling**: `RedisPool` with semaphore concurrency
- **Fluent API**: `RedisExt` trait for `.redis_ex()`, `.redis_nx()`, etc.
- **Async/Await**: Built on Tokio

## License

MIT OR Apache-2.0
