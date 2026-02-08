# 🪝 QAIL — AST-Native PostgreSQL Driver with Built-in RLS

> **Rust gives you memory safety. QAIL gives you correctness safety.**

[![Crates.io](https://img.shields.io/badge/crates.io-qail-orange)](https://crates.io/crates/qail)
[![Docs](https://img.shields.io/badge/docs-qail.rs-blue)](https://qail.rs/docs)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.15.8-green)](CHANGELOG.md)

---

## What is QAIL?

QAIL is a **native AST PostgreSQL driver** with **built-in Row-Level Security**. Instead of passing SQL strings through your stack, you work directly with a typed AST (Abstract Syntax Tree). This AST compiles directly to PostgreSQL wire protocol bytes — no string interpolation, no SQL injection, no parsing at runtime.

The only Rust PostgreSQL driver with AST-level tenant injection for multi-tenant SaaS.

```rust
// Traditional: String → Parse → Execute
sqlx::query!("SELECT id, email FROM users WHERE active = $1", true)

// QAIL: AST → Binary → Execute (no parsing, no injection surface)
Qail::get("users").columns(["id", "email"]).eq("active", true)
```

---

## Quick Start

```rust
use qail_core::{Qail, rls::RlsContext};
use qail_pg::PgDriver;

// Connect
let mut driver = PgDriver::connect("localhost", 5432, "user", "mydb").await?;

// Multi-tenant: scope every query to this operator
let ctx = RlsContext::operator(operator_id);

// Build query as AST with RLS
let query = Qail::get("users")
    .columns(["id", "email", "name"])
    .eq("active", true)
    .order_by("created_at", Desc)
    .limit(10)
    .with_rls(&ctx);  // ← tenant-scoped automatically

// Execute (AST → binary wire protocol → Postgres)
let rows = driver.fetch_all(&query).await?;
```

### CLI

```bash
# Install
cargo install qail

# Initialize a project
qail init --name myapp --mode postgres --url postgres://localhost/mydb

# Execute queries
qail exec "get users'id'email[active=true]" --url postgres://localhost/mydb
qail exec "cnt orders[status = 'paid']" --url postgres://...  # COUNT(*)
qail exec "get users" --url postgres://... --json | jq '.[].email'

# Schema management
qail pull postgres://localhost/mydb           # Introspect → schema.qail
qail diff old.qail new.qail                   # Compare schemas
qail diff _ schema.qail --live --url pg://... # Drift detection vs live DB

# Migrations
qail migrate up v1:v2 postgres://...
qail migrate reset schema.qail postgres://... # Nuclear: drop + recreate
qail migrate status postgres://...            # Rich tabular status

# Generate typed Rust schema
qail types schema.qail > src/generated/schema.rs
```

---

## Features

### 🔐 Built-in Row-Level Security

The first Rust PG driver with native multi-tenant isolation:

```rust
use qail_core::rls::RlsContext;

// Four scope constructors for real-world SaaS
let ctx = RlsContext::operator(op_id);              // Single operator
let ctx = RlsContext::agent(agent_id);              // Single agent
let ctx = RlsContext::operator_and_agent(op, ag);   // Agent within operator
let ctx = RlsContext::super_admin();                // Bypasses RLS

// Every query is automatically scoped — no manual WHERE clauses
Qail::get("bookings").with_rls(&ctx)
```

### 🔗 Compile-Time Relation Safety (v0.15.8)

`TypedQail<T>` carries table types through the builder chain. Invalid joins fail at compile time:

```rust
use schema::{users, posts};

// ✅ Compiles — tables are related via ref:users.id
Qail::typed(users::table)
    .join_related(posts::table)
    .typed_column(users::email())
    .typed_eq(users::active(), true)

// ❌ Compile Error — no RelatedTo<Products> impl
Qail::typed(users::table).join_related(products::table)
```

### 🛡️ Protected Columns

Compile-time data governance with capability witnesses:

```rust
// Protected columns require capability proof
let admin_cap = CapabilityProvider::mint_admin();  // After JWT verification

Qail::get(users::table)
    .with_cap(&admin_cap)
    .column(users::email)                    // Public — always allowed
    .column_protected(users::password_hash)  // Protected — now allowed!
```

### 🎯 Type-Safe Schema Codegen

```bash
qail types schema.qail > src/schema.rs
```

```rust
// Generated: compile-time checked columns
Qail::get(users::TABLE)
    .typed_column(users::id())
    .typed_eq(users::active(), true)   // ← must be bool
    .typed_eq(users::age(), "wrong")   // ❌ Compile Error: &str ≠ i32
```

### ⚡ AST-Native Migrations

```bash
qail migrate plan old.qail new.qail       # Preview with impact analysis
qail migrate up v1:v2 postgres://... -c ./src  # Apply with codebase check
qail migrate reset schema.qail postgres://...  # Atomic drop + recreate
qail diff _ schema.qail --live --url pg://...  # Drift detection
qail migrate status postgres://...            # Rich tabular history
```

---

## Performance

QAIL's AST-native architecture delivers **146% faster** query execution than SQLx:

| Driver | Latency | QPS | vs QAIL |
|--------|---------|-----|---------|
| SQLx | 93µs | 10,718 | 141% slower |
| SeaORM | 75µs | 13,405 | 93% slower |
| **QAIL** | **39µs** | **25,825** | baseline |

**Why:** Pre-computed wire bytes, zero-alloc hot path, no SQL parsing.

---

## Architecture

```
qail.rs/
├── core/          # AST + Parser + Validator + Typed System + RLS
├── pg/            # PostgreSQL Driver (binary protocol + pool)
├── cli/           # qail exec, pull, types, migrate
├── lsp/           # Language server for IDEs
├── daemon/        # IPC daemon (any language via socket)
├── gateway/       # QAIL Gateway (binary protocol proxy)
├── wasm/          # Browser/Node.js via WASM
├── ffi/           # C-API for FFI
├── go/            # Go bindings (via daemon IPC)
├── py/            # Python bindings (via PyO3)
├── php/           # PHP bindings
├── qdrant/        # Qdrant vector database driver
├── redis/         # Redis driver
├── mysql/         # MySQL driver (planned)
└── encoder/       # Wire protocol encoder
```

---

## Status (v0.15.8)

| Feature | Status |
|---------|--------|
| SSL/TLS, SCRAM-SHA-256 | ✅ |
| Connection Pooling | ✅ |
| Row-Level Security (RLS) | ✅ |
| Multi-Tenant Isolation | ✅ |
| TypedQail<T> Relations | ✅ |
| Protected Columns | ✅ |
| Query Plan Caching | ✅ |
| Transactions + Savepoints | ✅ |
| CTEs, Subqueries, EXISTS | ✅ |
| Window Functions (OVER) | ✅ |
| JSON/JSONB, Arrays | ✅ |
| COPY Protocol | ✅ |
| UPSERT, RETURNING | ✅ |
| LATERAL JOIN | ✅ |
| UNION/INTERSECT/EXCEPT | ✅ |
| Materialized Views | ✅ |
| EXPLAIN / EXPLAIN ANALYZE | ✅ |
| LOCK TABLE | ✅ |
| Batch Transactions | ✅ |
| Statement Timeout | ✅ |
| Unix Socket & mTLS | ✅ |
| SSH Tunneling | ✅ |

---

## Production Use

QAIL powers [Sailtix](https://sailtix.com) — a multi-operator maritime booking platform with full RLS tenant isolation across 27+ tables.

---

## Documentation

- 📖 [Full Documentation](https://qail.rs/docs)
- 📝 [Blog: Why Every Rust PG Driver Ignores RLS](https://qail.rs/blog/why-every-rust-pg-driver-ignores-rls)
- 🎮 [Playground](https://qail.rs/playground)
- 📊 [Benchmarks](https://qail.rs/benchmarks)
- 📋 [Changelog](https://qail.rs/changelog)
- 💡 [The Manifesto](https://qail.rs/philosophy)

---

## License

MIT © 2025-2026 QAIL Contributors

<p align="center">
  <strong>Built with 🦀 Rust</strong><br>
  <a href="https://qail.rs">qail.rs</a>
</p>
