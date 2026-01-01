# Redis Driver

**"Redis stores time — QAIL decides."**

The `qail-redis` crate provides a unified Qail AST interface for Redis operations.

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
    println!("Value: {:?}", value);
    
    Ok(())
}
```

## Unified API

Redis uses the same `Qail` type as PostgreSQL and Qdrant:

| Database   | Constructor | Purpose |
|------------|-------------|---------|
| PostgreSQL | `Qail::get("users")` | Query facts |
| Qdrant | `Qail::search("products")` | Search meaning |
| Redis | `Qail::redis_get("session")` | Retrieve time-bound data |

## Available Commands

### String Commands

```rust
// GET key
Qail::redis_get("mykey")

// SET key value
Qail::redis_set("mykey", b"value".to_vec())

// SET with TTL (seconds)
Qail::redis_set("mykey", b"value".to_vec()).redis_ex(3600)

// SET only if not exists
Qail::redis_set("mykey", b"value".to_vec()).redis_nx()

// SET only if exists
Qail::redis_set("mykey", b"value".to_vec()).redis_xx()
```

### Counter Commands

```rust
// INCR key
Qail::redis_incr("counter")

// DECR key  
Qail::redis_decr("counter")
```

### Key Commands

```rust
// DEL key
Qail::redis_del("mykey")

// EXISTS key
Qail::redis_exists("mykey")

// TTL key
Qail::redis_ttl("mykey")

// EXPIRE key seconds
Qail::redis_expire("mykey", 60)
```

### Other Commands

```rust
// PING
Qail::redis_ping()
```

## RedisExt Trait

The `RedisExt` trait provides fluent methods for Redis-specific options:

```rust
use qail_redis::RedisExt;

// Chain TTL onto SET
Qail::redis_set("key", b"value".to_vec())
    .redis_ex(3600)  // EX seconds
    .redis_nx()       // Only if not exists
```

## Connection Pooling

```rust
use qail_redis::{RedisPool, PoolConfig};

let config = PoolConfig::new("127.0.0.1", 6379)
    .max_connections(10);

let pool = RedisPool::new(config);

// Get a connection from the pool
let mut conn = pool.get().await?;
conn.set("key", b"value").await?;
```

## Native RESP3 Protocol

`qail-redis` implements native RESP3 protocol encoding/decoding:

- **Zero string parsing**: Commands are encoded directly from AST
- **Full RESP3 support**: Booleans, doubles, maps, nulls
- **Efficient**: Direct wire protocol writes

## The QAIL Vision

> "Postgres stores facts, Qdrant stores meaning, Redis stores time — QAIL decides."

With `qail-redis`, you now have a unified API across all three database paradigms:

```rust
// Facts (PostgreSQL)
Qail::get("users").filter("active", Eq, true)

// Meaning (Qdrant)
Qail::search("products").vector(&embedding).limit(10)

// Time (Redis)
Qail::redis_set("session:123", session_data).redis_ex(3600)
```

Same `Qail` type. Same philosophy. Different backends.
