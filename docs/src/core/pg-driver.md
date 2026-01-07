# PostgreSQL Driver

The `qail-pg` crate provides a **native PostgreSQL driver** with AST-native wire protocol encoding. It communicates directly with Postgres at the wire level — no libpq, no ORM, no SQL strings.

## Features

- **AST-Native** — Direct AST to wire protocol, no SQL strings
- **Zero-Alloc** — Reusable buffers, no heap allocation per query
- **LRU Statement Cache** — Bounded cache (100 max), auto-evicts
- **SSL/TLS** — Full TLS with mutual TLS (mTLS) support
- **SCRAM-SHA-256** — Secure password authentication
- **Connection Pooling** — Efficient resource management with RLS-safe checkout
- **COPY Protocol** — Bulk insert for high throughput (1.63M rows/sec)
- **Pipeline Execution** — Multiple queries per round-trip
- **Cursors** — Stream large result sets
- **Transactions** — BEGIN/COMMIT/ROLLBACK

---

## Architecture

Understanding **which type to use** is the most important concept:

```
PgPool (manages N connections, handles checkout/return)
  └── PooledConnection (auto-returns to pool on Drop, resets RLS)
        └── PgConnection (raw TCP/TLS stream, wire protocol I/O)
              └── PgDriver (convenience wrapper over PgConnection)
```

| Type | Use When |
|------|----------|
| `PgDriver` | Quick scripts, benchmarks, single-connection use cases |
| `PgConnection` | You need raw control (TLS, mTLS, Unix sockets, manual lifecycle) |
| `PgPool` | **Production code** — multi-connection, concurrent workloads |
| `PooledConnection` | You called `pool.acquire()` — auto-returns when dropped |

**Rule of thumb:** If you're building a server, use `PgPool`. Everything else is for specialized cases.

---

## Connection Methods

Choose based on your deployment:

| Scenario | Method | Notes |
|----------|--------|-------|
| Local dev (`pg_hba.conf = trust`) | `PgDriver::connect()` | No password required |
| Password auth (most common) | `PgDriver::connect_with_password()` | Auto SCRAM-SHA-256 |
| Cloud DB (RDS, Cloud SQL, Supabase) | `PgConnection::connect_tls()` | Server-side TLS |
| Zero-trust / mTLS | `PgConnection::connect_mtls()` | Client certificate |
| Unix socket (same host) | `PgConnection::connect_unix()` | Lowest latency |
| `.env` / `DATABASE_URL` | `PgDriver::connect_env()` | Parses URL format |
| Custom config | `PgDriver::builder()` | Builder pattern for full control |

### Basic Connection

```rust
use qail_pg::PgDriver;

// Trust mode (no password)
let driver = PgDriver::connect("localhost", 5432, "user", "db").await?;

// With password (auto-detects MD5 or SCRAM-SHA-256)
let driver = PgDriver::connect_with_password(
    "localhost", 5432, "user", "db", "password"
).await?;

// From DATABASE_URL env var
let driver = PgDriver::connect_env().await?;
```

### SSL/TLS

```rust
use qail_pg::PgConnection;

// Standard TLS — verifies server certificate
let conn = PgConnection::connect_tls("localhost", 5432, "user", "db").await?;
```

### Mutual TLS (Client Certificates)

```rust
use qail_pg::{PgConnection, TlsConfig};

let config = TlsConfig {
    client_cert_pem: cert_bytes,
    client_key_pem: key_bytes,
    ca_cert_pem: Some(ca_bytes),
};

let conn = PgConnection::connect_mtls("localhost", 5432, "user", "db", config).await?;
```

### Unix Socket

```rust
let conn = PgConnection::connect_unix(
    "/var/run/postgresql",  // socket directory
    "user",
    "db"
).await?;
```

---

## AST-Native Queries

All queries are constructed through the typed AST — no raw SQL strings.

```rust
use qail_core::Qail;

let cmd = Qail::get("users").select_all().limit(10);

// Fetch all rows
let rows = driver.fetch_all(&cmd).await?;

// Fetch one row
let row = driver.fetch_one(&cmd).await?;

// Execute mutation (returns affected rows)
let affected = driver.execute(&cmd).await?;
```

---

## Statement Cache (LRU)

Prepared statements are cached automatically. The AST is hashed by structure, so identical query shapes reuse the same prepared statement.

```rust
// Cache is bounded (default: 100 statements)
// Auto-evicts least recently used when full

let (size, capacity) = driver.cache_stats();  // (42, 100)

// Manual clear if needed
driver.clear_cache();
```

| Method | Description |
|--------|-------------|
| `fetch_all()` | Uses cache (~25,000 q/s) |
| `fetch_all_uncached()` | Skips cache |
| `cache_stats()` | Returns (current, max) |
| `clear_cache()` | Frees all cached statements |

---

## Pipeline Methods

Pipelining sends multiple queries in a **single network round-trip**. This is the key to high throughput. Choose based on your needs:

```
Do you need return values from each query?
├── Yes
│   └── pipeline_ast()              — full parse/bind/execute per query, returns rows
│
└── No (fire-and-forget mutations)
    ├── Repeating the same query shape with different params?
    │   ├── pipeline_ast_cached()   — hash-based statement reuse
    │   └── pipeline_prepared_fast()— named prepared statement reuse
    │
    └── Different query shapes?
        ├── pipeline_ast_fast()     — parse+bind+execute, discard results
        ├── pipeline_simple_fast()  — simple query protocol (no params)
        └── pipeline_bytes_fast()   — pre-encoded buffers (fastest possible)
```

### Quick Reference

| Method | Returns Rows? | Statement Caching | Relative Speed |
|--------|:---:|:---:|:---:|
| `pipeline_ast()` | ✅ | Hash-based | ★★★ |
| `pipeline_ast_fast()` | ❌ | None | ★★★★ |
| `pipeline_ast_cached()` | ❌ | Hash + LRU | ★★★★★ |
| `pipeline_bytes_fast()` | ❌ | Pre-encoded | ★★★★★ |
| `pipeline_prepared_fast()` | ❌ | Named | ★★★★★ |
| `pipeline_prepared_zerocopy()` | ❌ | Named + zero-copy | ★★★★★ |
| `pipeline_prepared_ultra()` | ❌ | Named + ultra | ★★★★★+ |

### Example: Pipelined Inserts

```rust
let commands: Vec<QailCmd> = users.iter().map(|u| {
    Qail::add("users")
        .set("name", &u.name)
        .set("email", &u.email)
        .build()
}).collect();

// Fire-and-forget — fastest for bulk mutations
let affected = driver.pipeline_ast_fast(&commands).await?;

// With results — slower but returns inserted rows
let rows = driver.pipeline_ast(&commands).await?;
```

---

## Connection Pooling

```rust
use qail_pg::{PgPool, PoolConfig};

let config = PoolConfig::new("localhost", 5432, "user", "db")
    .password("secret")
    .max_connections(20)
    .min_connections(5);

let pool = PgPool::connect(config).await?;

// Acquire connection (auto-returned when dropped)
let mut conn = pool.acquire().await?;
conn.simple_query("SELECT 1").await?;

// Check idle count
let idle = pool.idle_count().await;
```

### Pool with RLS (Multi-Tenant)

```rust
use qail_core::RlsContext;

let ctx = RlsContext {
    operator_id: "tenant-123".into(),
    agent_id: Some("agent-456".into()),
    is_super_admin: false,
};

// Acquire + set RLS context in one call
// On Drop: auto-clears RLS context, then returns to pool
let mut conn = pool.acquire_with_rls(&ctx).await?;
```

> **Important:** When using `acquire_with_rls()`, the RLS context is automatically cleared when the connection is returned to the pool. This prevents cross-tenant data leakage — a connection used by Tenant A will never carry Tenant A's context when checked out by Tenant B.

### Pool Configuration

```rust
use std::time::Duration;

let config = PoolConfig::new("localhost", 5432, "user", "db")
    .idle_timeout(Duration::from_secs(600))    // 10 min
    .acquire_timeout(Duration::from_secs(30))  // 30 sec
    .connect_timeout(Duration::from_secs(10)); // 10 sec
```

| Option | Default | Description |
|--------|---------|-------------|
| `max_connections` | 10 | Maximum pool size |
| `min_connections` | 1 | Minimum idle connections |
| `idle_timeout` | 10 min | Stale connections auto-discarded |
| `acquire_timeout` | 30 sec | Max wait for connection |
| `connect_timeout` | 10 sec | Max time to establish new connection |
| `max_lifetime` | 30 min | Max age of any connection |
| `test_on_acquire` | true | Ping connection before returning |

---

## Bulk Insert (COPY Protocol)

High-performance bulk insert using PostgreSQL's COPY protocol. Benchmarked at **1.63M rows/sec** for 100M rows.

```rust
use qail_core::ast::Value;

let cmd = Qail::add("users").columns(&["name", "email"]);

let rows = vec![
    vec![Value::Text("Alice".into()), Value::Text("a@x.com".into())],
    vec![Value::Text("Bob".into()), Value::Text("b@x.com".into())],
];

let count = driver.copy_bulk(&cmd, &rows).await?;
// count = 2
```

### Performance Comparison

| Operation | Rows/sec | Notes |
|-----------|----------|-------|
| **COPY bulk insert** | **1.63M** | Native COPY protocol |
| Pipelined INSERT | 180K | Extended Query |
| Single INSERT | 22K | Per-statement |

---

## Cursor Streaming

Stream large result sets in batches:

```rust
let cmd = Qail::get("logs").select_all();

let batches = driver.stream_cmd(&cmd, 1000).await?;
for batch in batches {
    for row in batch {
        // Process row
    }
}
```

---

## Transactions

```rust
let mut conn = pool.acquire().await?;

conn.begin_transaction().await?;
// ... queries ...
conn.commit().await?;

// Or rollback on error
conn.rollback().await?;
```

---

## Row Decoding

### By Index
```rust
let name = row.get_string(0);
let age = row.get_i32(1);
```

### By Column Name (Recommended)
```rust
// Safer — column order changes don't break code
let name = row.get_string_by_name("name");
let age = row.get_i32_by_name("age");
let email = row.get_string_by_name("email");

// Check if NULL
if row.is_null_by_name("deleted_at") { ... }
```

Available `get_by_name` methods:
- `get_string_by_name`, `get_i32_by_name`, `get_i64_by_name`
- `get_f64_by_name`, `get_bool_by_name`
- `get_uuid_by_name`, `get_json_by_name`
- `is_null_by_name`, `column_index`

---

## Supported Types

| Rust Type | PostgreSQL Type |
|-----------|-----------------|
| `i16/i32/i64` | `INT2/INT4/INT8` |
| `f32/f64` | `FLOAT4/FLOAT8` |
| `bool` | `BOOLEAN` |
| `String` | `TEXT/VARCHAR` |
| `Vec<u8>` | `BYTEA` |
| `Uuid` | `UUID` |
| `Timestamp` | `TIMESTAMPTZ` |
| `Date` | `DATE` |
| `Time` | `TIME` |
| `Json` | `JSONB` |
| `Numeric` | `NUMERIC/DECIMAL` |

---

## ⚠️ Raw SQL (Discouraged)

`execute_raw` exists for legacy compatibility but **violates AST-native philosophy**.

```rust
// ❌ Avoid
driver.execute_raw("BEGIN").await?;

// ✅ Prefer AST-native
conn.begin_transaction().await?;
```
