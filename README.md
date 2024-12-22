# 🪝 QAIL — Native AST PostgreSQL Driver

> **The world's first AST-native PostgreSQL driver. No SQL strings. No ORM. Just bytes.**

[![Crates.io](https://img.shields.io/badge/crates.io-qail-orange)](https://crates.io/crates/qail)
[![Docs](https://img.shields.io/badge/docs-qail.io-blue)](https://qail.io/docs)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.14.21-green)](CHANGELOG.md)

---

## What is QAIL?

QAIL is a **native AST PostgreSQL driver**. Instead of passing SQL strings through your stack, you work directly with a typed AST (Abstract Syntax Tree). This AST compiles directly to PostgreSQL wire protocol bytes — no string interpolation, no SQL injection, no parsing at runtime.

```rust
// Traditional: String → Parse → Execute
sqlx::query!("SELECT id, email FROM users WHERE active = $1", true)

// QAIL: AST → Binary → Execute (no parsing!)
Qail::get("users").columns(["id", "email"]).eq("active", true)
```

---

## Quick Start

```rust
use qail_core::Qail;
use qail_pg::PgDriver;

// Connect
let mut driver = PgDriver::connect("localhost", 5432, "user", "mydb").await?;

// Build query as AST
let query = Qail::get("users")
    .columns(["id", "email", "name"])
    .eq("active", true)
    .order_by("created_at", Desc)
    .limit(10);

// Execute (AST → binary wire protocol → Postgres)
let rows = driver.fetch_all(&query).await?;

for row in rows {
    println!("{}: {}", row.get::<i32>(0), row.get::<&str>(1));
}
```

### CLI

```bash
# Install
cargo install qail

# Execute query
qail exec "get users 'id'email[active=true]" --db postgres://localhost/mydb

# Pull schema from database
qail pull postgres://localhost/mydb

# Generate typed Rust schema
qail types schema.qail > src/generated/schema.rs
```

---

## Performance

QAIL's AST-native architecture delivers **146% faster** query execution than SQLx:

| Driver | Latency | QPS | vs QAIL |
|--------|---------|-----|---------|
| SQLx | 93µs | 10,718 | 141% slower |
| SeaORM | 75µs | 13,405 | 93% slower |
| **QAIL** | **39µs** | **25,825** | baseline |

### Why QAIL is Faster

1. **Pre-Computed Wire Bytes**: Static parts pre-computed during AST build
2. **Zero-Alloc Hot Path**: Only parameters serialized at execution
3. **No SQL Parsing**: AST → binary, no string → AST → binary

---

## Architecture

```
qail.rs/
├── core/          # AST + Parser + Validator + Codegen
├── pg/            # PostgreSQL Driver (binary protocol)
├── cli/           # Command-line tool
├── lsp/           # Language server for IDEs
├── wasm/          # Browser playground
├── ffi/           # C-API for FFI
├── go/            # Go bindings
└── py/            # Python bindings
```

---

## Features

### Compile-Time Validation

```rust
// At build time, QAIL validates against schema.qail:
// ✅ Table exists?
// ✅ Column exists?
// ✅ Type compatible?

let q = Qail::get("users")  // ✅ users table exists
    .columns(["id", "email"])  // ✅ columns exist
    .eq("active", true);  // ✅ active is BOOLEAN
```

### Migrations

```bash
# Create migration
qail migrate create add_users_table

# Apply migrations
qail migrate up --db $DATABASE_URL

# Rollback
qail migrate down --db $DATABASE_URL
```

### Type-Safe Schema

```bash
# Generate typed Rust from schema.qail
qail types schema.qail > src/schema.rs
```

```rust
// Generated: src/schema.rs
pub struct Users;
impl Users {
    pub fn id() -> TypedColumn<i32> { ... }
    pub fn email() -> TypedColumn<String> { ... }
    pub fn active() -> TypedColumn<bool> { ... }
}

// Usage with compile-time type checking
Qail::get(Users)
    .columns([Users::id(), Users::email()])
    .eq(Users::active(), true)  // Type checked!
```

---

## Production Use

QAIL is used in production at [ExampleApp](https://example.com) — a ferry booking platform handling real transactions.

### Binary Size Optimization (engine-example-com)

| Dependency | Before | After | Replacement |
|------------|--------|-------|-------------|
| AWS SDK | 67 MB | 55 MB | Custom SigV4 |
| async-graphql | 55 MB | 52 MB | Removed |
| openssl | 52 MB | 46 MB | x509-parser |
| **Total** | **67 MB** | **46 MB** | **-31%** |

---

## Roadmap

### Current (v0.14.x)
- ✅ AST-native query builder
- ✅ Binary wire protocol
- ✅ Compile-time validation
- ✅ Migrations with impact analysis
- ✅ Type generation

### Future (v1.0)
- 🔜 QAIL Gateway (replace REST/GraphQL)
- 🔜 Row-level security policies
- 🔜 Client SDKs (JavaScript, Swift)
- 🔜 Real-time subscriptions

---

## Documentation

- 📖 [Full Documentation](https://qail.io/docs)
- 🎮 [Playground](https://qail.io/play)
- 📊 [Benchmarks](https://qail.io/benchmarks)

---

## License

MIT © 2025-2026 QAIL Contributors

<p align="center">
  <strong>Built with 🦀 Rust</strong><br>
  <a href="https://qail.io">qail.io</a>
</p>
