# qail-pg

**PostgreSQL driver for QAIL - native wire protocol**

[![Crates.io](https://img.shields.io/crates/v/qail-pg.svg)](https://crates.io/crates/qail-pg)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A high-performance PostgreSQL driver that speaks the wire protocol directly. AST-first execution with no application-level SQL string interpolation on the safe path.

## SQL String vs SQL Bytes

- **SQL string**: text query built in app code (format/concat/interpolate).
- **SQL bytes**: PostgreSQL frontend/backend protocol bytes (`Parse`, `Bind`, `Execute`, result frames) and encoded bind values.
- **What qail-pg does**: compiles QAIL AST into protocol messages and typed values.
- **What PostgreSQL still does**: server-side parse/plan/execute is still normal PostgreSQL behavior.

## Inspect Real Wire Bytes

Run the built-in demo:

```bash
cargo run -p qail-pg --example wire_bytes_demo
```

For:

```rust
Qail::get("users")
    .select_all()
    .filter("active", Eq, true)
```

you will see:

- SQL view: `SELECT * FROM users WHERE active = $1`
- bind param `$1`: `74` (`'t'` for boolean true)
- wire frames: `Parse (P) + Bind (B) + Describe (D) + Execute (E) + Sync (S)`

This shows the exact protocol-byte path used by the driver.

## Features

- **AST-Native** - Compiles QAIL AST directly to PostgreSQL wire protocol
- **28% Faster** - Benchmarked at 1.36M rows/s COPY (vs asyncpg at 1.06M rows/s)
- **Query Pipelining** - 24x faster batch operations via `pipeline_batch()`
- **SSL/TLS** - Production-ready with `tokio-rustls`
- **Password Auth Modes** - Supports SCRAM-SHA-256, MD5, and cleartext server flows
- **Connection Pooling** - Built-in `PgPool`
- **Transactions** - Full `begin`/`commit`/`rollback` support

## Installation

> [!CAUTION]
> **Beta Software**: QAIL is currently in **beta**. The API is stabilizing and is battle-tested in production. Breaking changes may still occur between minor versions.

```toml
[dependencies]
qail-pg = "0.25.0"
qail-core = "0.25.0"
```

`qail-pg` is AST-only. Raw SQL helper APIs were removed.

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

Apache-2.0

## 🤝 Contributing & Support

We welcome issue reports on GitHub! Please provide detailed descriptions to help us reproduce and fix the problem. We aim to address critical issues within 1-5 business days.
