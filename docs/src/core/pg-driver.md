# PostgreSQL Driver

The `qail-pg` crate provides a native PostgreSQL driver with AST-native wire protocol encoding.

## Features
- **AST-Native** — Direct AST to wire protocol, no SQL strings
- **Zero-Alloc** — Reusable buffers, no heap allocation per query
- **LRU Statement Cache** — Bounded cache (1000 max), auto-evicts
- **SSL/TLS** — Full TLS with mutual TLS (mTLS) support
- **SCRAM-SHA-256** — Secure password authentication
- **Connection Pooling** — Efficient resource management
- **COPY Protocol** — Bulk insert for high throughput
- **Cursors** — Stream large result sets
- **Transactions** — BEGIN/COMMIT/ROLLBACK

---

## Statement Cache (LRU)

Prepared statements are cached automatically for best performance.

```rust
// Cache is bounded (default: 1000 statements)
// Auto-evicts least recently used when full

let (size, capacity) = driver.cache_stats();  // (42, 1000)

// Manual clear if needed
driver.clear_cache();
```

| Method | Description |
|--------|-------------|
| `fetch_all()` | Uses cache (4,500+ q/s) |
| `fetch_all_uncached()` | Skips cache |
| `cache_stats()` | Returns (current, max) |
| `clear_cache()` | Frees all cached statements |

## Connection

```rust
use qail_pg::PgDriver;

// Trust mode (no password)
let driver = PgDriver::connect("localhost", 5432, "user", "db").await?;

// With password (auto-detects MD5 or SCRAM-SHA-256)
let driver = PgDriver::connect_with_password(
    "localhost", 5432, "user", "db", "password"
).await?;
```

---

## SSL/TLS

### Standard TLS
```rust
use qail_pg::PgConnection;

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

---

## AST-Native Queries

```rust
let cmd = Qail::get("users").select_all().limit(10);

// Fetch all rows
let rows = driver.fetch_all(&cmd).await?;

// Fetch one row
let row = driver.fetch_one(&cmd).await?;

// Execute mutation (returns affected rows)
let affected = driver.execute(&cmd).await?;
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

### Pool Timeout Configuration

```rust
use std::time::Duration;

let config = PoolConfig::new("localhost", 5432, "user", "db")
    .idle_timeout(Duration::from_secs(600))    // 10 min
    .acquire_timeout(Duration::from_secs(30))  // 30 sec
    .connect_timeout(Duration::from_secs(10)); // 10 sec
```

| Option | Default | Description |
|--------|---------|-------------|
| `idle_timeout` | 10 min | Stale connections auto-discarded |
| `acquire_timeout` | 30 sec | Max wait for connection |
| `connect_timeout` | 10 sec | Max time to establish new connection |

---

## Bulk Insert (COPY Protocol)

High-performance bulk insert using PostgreSQL's COPY protocol:

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
use qail_pg::PgConnection;

let mut conn = PgConnection::connect("localhost", 5432, "user", "db").await?;

conn.begin_transaction().await?;
// ... queries ...
conn.commit().await?;

// Or rollback on error
conn.rollback().await?;
```

---

## ⚠️ Raw SQL (Discouraged)

`execute_raw` exists for legacy compatibility but **violates AST-native philosophy**.

Use AST-native alternatives:
- Transactions: `conn.begin_transaction()`, `conn.commit()`, `conn.rollback()`
- DDL: Use QAIL schema syntax and migrate command

```rust
// ❌ Avoid
driver.execute_raw("BEGIN").await?;

// ✅ Prefer AST-native
let mut conn = pool.acquire().await?;
conn.begin_transaction().await?;
// ... queries ...
conn.commit().await?;
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
// Safer - column order changes don't break code
let name = row.get_string_by_name("name");
let age = row.get_i32_by_name("age");
let email = row.get_string_by_name("email");

// Check if NULL
if row.is_null_by_name("deleted_at") { ... }
```

Available get_by_name methods:
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
