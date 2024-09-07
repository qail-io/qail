# qail-pg

**PostgreSQL driver for QAIL - native wire protocol**

[![Crates.io](https://img.shields.io/crates/v/qail-pg.svg)](https://crates.io/crates/qail-pg)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A high-performance PostgreSQL driver that speaks the wire protocol directly. No SQL strings, no SQL injection - just pure AST-to-wire encoding.

## Features

- **AST-Native** - Compiles QAIL AST directly to PostgreSQL wire protocol
- **28% Faster** - Benchmarked at 1.36M rows/s COPY (vs asyncpg at 1.06M rows/s)
- **Query Pipelining** - 24x faster batch operations via `pipeline_batch()`
- **SSL/TLS** - Production-ready with `tokio-rustls`
- **SCRAM-SHA-256** - Secure password authentication
- **Connection Pooling** - Built-in `PgPool`
- **Transactions** - Full `begin`/`commit`/`rollback` support

## Installation

```toml
[dependencies]
qail-pg = "0.9"
qail-core = "0.9"
```

## Quick Start

```rust
use qail_core::ast::{QailCmd, builders::*};
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect with password
    let mut driver = PgDriver::connect_with_password(
        "localhost", 5432, "postgres", "mydb", "password"
    ).await?;

    // Build a query using QAIL AST
    let cmd = QailCmd::get("users")
        .columns([col("id"), col("name"), col("email")])
        .filter(eq("active", true))
        .order_by([("created_at", Desc)])
        .limit(10);

    // Execute and fetch rows
    let rows = driver.fetch_all(&cmd).await?;
    
    for row in rows {
        let name: String = row.get("name");
        println!("User: {}", name);
    }

    Ok(())
}
```

## High-Performance Batch Operations

```rust
// Execute 10,000 queries in a single network round-trip
let cmds: Vec<QailCmd> = (0..10_000)
    .map(|i| QailCmd::add("events")
        .columns(["user_id", "event_type"])
        .values([Value::Int(i), Value::String("login".to_string())])
    ).collect();

let count = driver.pipeline_batch(&cmds).await?;
println!("Inserted {} rows", count);
```

## COPY Protocol (Bulk Insert)

```rust
use qail_pg::protocol::CopyEncoder;

// Build COPY data
let mut encoder = CopyEncoder::new();
for i in 0..1_000_000 {
    encoder.begin_row();
    encoder.write_i64(i);
    encoder.write_str(&format!("user_{}", i));
    encoder.end_row();
}

// Execute COPY
driver.copy_bulk_bytes("users", &["id", "name"], encoder.finish()).await?;
```

## Connection Pooling

```rust
use qail_pg::PgPool;

// Create a pool with 10 connections
let pool = PgPool::new(
    "localhost", 5432, "postgres", "mydb", Some("password"), 10
).await?;

// Acquire a connection
let mut conn = pool.acquire().await?;
let rows = conn.fetch_all(&cmd).await?;
```

## SSL/TLS Support

qail-pg uses `tokio-rustls` for TLS connections:

```rust
// SSL is auto-negotiated during connection
let driver = PgDriver::connect_with_password(
    "pg.example.com", 5432, "user", "db", "pass"
).await?;
```

## Ergonomic Expression Builders

qail-pg works seamlessly with qail-core's ergonomic builders:

```rust
use qail_core::ast::builders::*;

// COUNT(*) FILTER (WHERE condition)
count_filter(vec![eq("status", "active")]).alias("active_count")

// NOW() - INTERVAL '24 hours'
now_minus("24 hours")

// CASE WHEN ... ELSE ... END
case_when(gt("score", 80), text("pass"))
    .otherwise(text("fail"))
    .alias("result")

// Type casting
cast(col("amount"), "float8")
```

## Type Support

| PostgreSQL Type | Rust Type |
|-----------------|-----------|
| `text`, `varchar` | `String` |
| `int4`, `int8` | `i32`, `i64` |
| `float8` | `f64` |
| `bool` | `bool` |
| `uuid` | `uuid::Uuid` |
| `jsonb` | `serde_json::Value` |
| `timestamp` | `chrono::DateTime<Utc>` |
| `date` | `chrono::NaiveDate` |
| `numeric` | `rust_decimal::Decimal` |

## License

MIT

## 🤝 Contributing & Support

We welcome issue reports on GitHub! Please provide detailed descriptions to help us reproduce and fix the problem. We aim to address critical issues within 1-5 business days.

> [!CAUTION]
> **Alpha Software**: QAIL is currently in **alpha**. While we strive for stability, the API is evolving to ensure it remains ergonomic and truly AST-native. **Do not use in production environments yet.**
