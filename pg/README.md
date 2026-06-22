# qail-pg

**QAIL Postgres Driver** - typed AST queries executed through the native
PostgreSQL wire protocol.

[![Crates.io](https://img.shields.io/crates/v/qail-pg.svg)](https://crates.io/crates/qail-pg)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A high-performance async PostgreSQL driver that speaks the wire protocol
directly. `qail-pg` is the driver layer of QAIL and is intended for backend
code that wants AST-first query construction instead of SQL-string-centric app
code.

## Positioning

qail-pg is a **Rust PostgreSQL driver** for teams that want:

- a typed AST query surface instead of SQL-string-centric app code
- explicit tenant-scoping APIs on query objects
- direct wire-protocol execution with pipelining and COPY support
- prepared AST execution, pooling, LISTEN/NOTIFY, COPY, replication helpers,
  and production protocol failure handling

## Quick Comparison

| Need | `qail-pg` | `tokio-postgres` | `sqlx` |
|---|---|---|---|
| Primary query API | Typed Qail AST | SQL strings | SQL strings (+ checked macros) |
| App-side SQL interpolation path | No on AST path | Yes | Yes |
| Built-in tenant context model | Yes (`RlsContext`) | App-managed | App-managed |
| Auto-REST companion | Yes (`qail-gateway`) | No | No |

## SQL String vs SQL Bytes

- **SQL string**: text query built in app code (format/concat/interpolate).
- **SQL bytes**: PostgreSQL frontend/backend protocol bytes (`Parse`, `Bind`, `Execute`, result frames) and encoded bind values.
- **What qail-pg does**: compiles QAIL AST into protocol messages and typed values.
- **What PostgreSQL still does**: server-side parse/plan/execute is still normal PostgreSQL behavior.

## Legacy Syntax Notice

You may still find pre-1.0 search results showing symbolic QAIL strings like `get::users•@id@email@role[active=true][lim=10]` or old macro examples like `qail!("get::users:'id'email [ 'active == true ]")`.

Those are historical docs from older releases. `qail-pg 1.3.x` is documented
and optimized around the native AST path:

```rust
let query = Qail::get("users")
    .columns(["id", "email", "role"])
    .eq("active", true)
    .limit(10);

let rows = driver.fetch_all(&query).await?;
```

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
- **Query Pipelining** - batch operations via `pipeline_execute_count()`
- **SSL/TLS** - Production-ready with `tokio-rustls`
- **Password Auth Modes** - Supports SCRAM-SHA-256, MD5, and cleartext server flows
- **Protocol 3.2 Ready** - Requests startup protocol 3.2 by default with one-shot fallback to 3.0 on explicit protocol rejection
- **Cancel-Key Compatibility** - Supports variable-length cancel keys via bytes-native APIs (legacy i32 wrappers retained for 4-byte keys)
- **Connection Pooling** - Built-in `PgPool`
- **Transactions** - Full `begin`/`commit`/`rollback` support
- **Enterprise Auth Hooks** - optional GSS/Kerberos token-provider integration
  without moving C/FFI auth control into the Rust core

## Installation

```toml
[dependencies]
qail-pg = "1.3.4"
qail-core = "1.3.4"
```

The primary runtime path is AST-native. Use `Qail::get/add/set/del`,
expression builders, and typed bind values instead of formatting SQL strings in
application code.

## Quick Start

```rust
use qail_core::prelude::*;
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut driver = PgDriver::connect_with_password(
        "localhost", 5432, "postgres", "mydb", "password"
    ).await?;

    let tenant_id = "018f6a60-4d5f-7a9d-9f4c-7dd8c338f1d2";
    let ctx = RlsContext::tenant(tenant_id);

    let cmd = Qail::get("users")
        .columns(["id", "name", "email"])
        .eq("active", true)
        .order_by("created_at", Desc)
        .with_rls(&ctx)?
        .limit(10);

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
// pipeline_execute_count() uses AstPipelineMode::Auto by default
let cmds: Vec<Qail> = (0..10_000)
    .map(|i| Qail::add("events")
        .set_value("user_id", i)
        .set_value("event_type", "login")
    ).collect();

let count = driver.pipeline_execute_count(&cmds).await?;
println!("Inserted {} rows", count);

// Override strategy explicitly when needed
let count_cached = driver
    .pipeline_execute_count_with_mode(&cmds, qail_pg::AstPipelineMode::Cached)
    .await?;
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
