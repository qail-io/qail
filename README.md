# Qail — Application-to-Database Pipeline with Compile-Time Safety

> **From your app to PostgreSQL wire bytes — one typed AST, zero strings, built-in tenant isolation.**

[![Crates.io](https://img.shields.io/badge/crates.io-qail-orange)](https://crates.io/crates/qail)
[![Docs](https://img.shields.io/badge/docs-qail.rs-blue)](https://qail.rs/docs)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.20.4-green)](CHANGELOG.md)

---

## The Problem

Every SaaS backend has the same three bugs waiting to happen:

1. **N+1 queries** — Your ORM fires 151 queries where 1 would do. You find out in production.
2. **SQL injection** — One string interpolation mistake. That's all it takes.
3. **Broken tenant isolation** — A missing `WHERE operator_id = ?` leaks another customer's data.

These aren't edge cases. They're the *default* outcome of string-based SQL.

## The Fix

Qail is a **native AST PostgreSQL driver**. Instead of passing SQL strings, you work with a typed Abstract Syntax Tree that compiles directly to PostgreSQL wire protocol bytes.

```rust
// String-based (every other driver): parse → plan → execute
sqlx::query!("SELECT id, email FROM users WHERE active = $1", true)

// Qail: AST → binary wire bytes → execute (no parsing, no injection surface)
Qail::get("users").columns(["id", "email"]).eq("active", true)
```

**N+1 is structurally impossible.** The AST guides you to `.join()` — there's no "lazy loading" to accidentally trigger. Security is compiled in, not bolted on.

---

## Benchmark: Proof, Not Marketing

We ran the same complex query — 3×LEFT JOIN across 4 tables, filtered, sorted, 50 rows — using five approaches against a real PostgreSQL database. 100 iterations, `--release` mode.

| # | Approach | Avg Latency | DB Queries | vs Qail |
|---|----------|------------|------------|---------|
| 1 | **Qail AST** | **449µs** | **1** | baseline |
| 2 | REST + `?expand=` | 635µs | 1 | 1.4× slower |
| 3 | GraphQL + DataLoader | 1.52ms | 3 | **3.4×** |
| 4 | GraphQL naive | 18.2ms | 151 | **40×** |
| 5 | REST naive | 22.5ms | 151 | **50×** |

> **The headline isn't 0.4ms vs 0.6ms.** It's that 90% of developers ship Approach 4 or 5 — the one that's **40-50× slower** — because their tools let them.
>
> Qail makes it impossible to write the slow version. The AST naturally guides you to `.join()`. That's the product.

<details>
<summary><strong>Methodology notes (click to expand)</strong></summary>

- **Cache equalization:** A 10-iteration global warmup loads all data pages into Postgres buffer cache *before* any timed approach runs. All approaches start from identical cache state.
- **Fair protocol:** All five approaches use Qail's binary driver internally, isolating the architectural difference (1 query vs N+1) from protocol overhead.
- **DataLoader:** Realistically batches N lookups into `WHERE id IN (...)` queries — the standard GraphQL optimization.
- **REST + expand:** Server-side JOIN (same query as Qail) + JSON serialization overhead. The 1.4× gap is pure JSON ser/de cost.
- **Network latency:** Local benchmark = 0ms latency. In production (app → RDS), each extra round trip adds 1-2ms. The gap between Qail (1 trip) and DataLoader (3 trips) widens significantly.
- Run it yourself: `DATABASE_URL=... cargo run --example battle_comparison --features chrono,uuid --release`

</details>

---

## Quick Start

```rust
use qail_core::prelude::*;
use qail_pg::PgDriver;

// Connect
let mut driver = PgDriver::connect("localhost", 5432, "user", "mydb").await?;

// Multi-tenant: scope every query to this operator
let ctx = RlsContext::operator(operator_id);

// Build & execute
let orders = Qail::get("orders")
    .columns(["id", "total", "status"])
    .join(JoinKind::Left, "users", "orders.user_id", "users.id")
    .column("users.email AS customer_email")
    .eq("orders.status", "paid")
    .order_by("orders.created_at", Desc)
    .limit(25)
    .with_rls(&ctx);  // ← tenant-scoped automatically

let rows = driver.fetch_all(&orders).await?;
```

### CLI

```bash
cargo install qail

qail init --name myapp --mode postgres --url postgres://localhost/mydb
qail exec "get users'id'email[active=true]" --url postgres://localhost/mydb
qail pull postgres://localhost/mydb              # Introspect → schema.qail
qail diff _ schema.qail --live --url pg://...    # Drift detection
qail migrate up v1:v2 postgres://...             # Apply migrations
qail types schema.qail > src/generated/schema.rs # Typed codegen
```

---

## Why Qail > String SQL

### 🔐 Security Is Compiled In

| Threat | String SQL | Qail |
|--------|-----------|------|
| SQL injection | Possible (one mistake) | **Impossible** (binary AST, no strings) |
| Tenant data leak | Missing WHERE clause | **RLS injected automatically** |
| Query abuse | Unbounded depth/joins | **AST validates at compile time** |
| IDOR | Must check per endpoint | **Tenant isolation built into protocol** |

```rust
// RLS: four scope constructors for real-world SaaS
let ctx = RlsContext::operator(op_id);              // Single operator
let ctx = RlsContext::agent(agent_id);              // Single agent
let ctx = RlsContext::operator_and_agent(op, ag);   // Agent within operator
let ctx = RlsContext::super_admin();                // Full access

// Every query is automatically scoped
Qail::get("bookings").with_rls(&ctx)  // ← no manual WHERE needed
```

### 🔗 Compile-Time Relation Safety

Invalid joins fail at compile time, not at 3am in production:

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

Compile-time data governance — sensitive columns require capability proof:

```rust
let admin_cap = CapabilityProvider::mint_admin();  // After JWT verification

Qail::get(users::table)
    .with_cap(&admin_cap)
    .column(users::email)                    // Public — always allowed
    .column_protected(users::password_hash)  // Protected — requires cap
```

---

## Qail Gateway

Auto-REST API server with zero backend code. Point it at a Postgres database, get a full API:

```
GET    /api/{table}?expand=users&sort=-created_at&limit=10
GET    /api/{table}/:id
POST   /api/{table}
PATCH  /api/{table}/:id
DELETE /api/{table}/:id
GET    /api/{table}/_explain    # EXPLAIN ANALYZE
GET    /api/{table}/_aggregate  # count, sum, avg, min, max
```

**A complete REST API layer for PostgreSQL:**

- ✅ Auto-REST CRUD for all tables
- ✅ FK-based JOIN expansion (`?expand=`) + nested expansion
- ✅ Full-text search (`?search=`)
- ✅ WebSocket subscriptions + live queries
- ✅ Event triggers (mutation → webhook with retry)
- ✅ JWT auth (HS256/RS256) + API key auth
- ✅ YAML policy engine + column permissions
- ✅ Query allow-listing + complexity limits
- ✅ Prometheus metrics + request tracing
- ✅ NDJSON streaming + cursor pagination
- ✅ OpenAPI spec generation

---

## Architecture

```
qail.rs/
├── core/       AST engine, parser, validator, typed system, RLS, migrations
├── pg/         PostgreSQL driver (binary wire protocol, connection pool)
├── gateway/    Auto-REST API server (Axum)
├── cli/        qail exec, pull, diff, migrate, types
├── encoder/    Wire protocol encoder + C FFI for language bindings
├── qdrant/     Qdrant vector DB driver (optional)
├── workflow/   Workflow engine
└── sdk/        Language bindings (TypeScript, Swift, Kotlin)
```

---

## Feature Status (v0.20.4)

| Category | Features |
|----------|----------|
| **Core SQL** | SELECT, INSERT, UPDATE, DELETE, UPSERT, RETURNING, COPY |
| **Joins** | INNER, LEFT, RIGHT, FULL, CROSS, LATERAL, self-joins |
| **Advanced** | CTEs, Subqueries, EXISTS, Window Functions, UNION/INTERSECT/EXCEPT |
| **Types** | JSON/JSONB, Arrays, UUID, Timestamps, Enums, Composite |
| **Security** | RLS, TypedQail, Protected Columns, Capability Witnesses |
| **Migrations** | AST-native diffing, drift detection, impact analysis |
| **Schema** | Views, Materialized Views, Functions, Triggers, Extensions, Sequences, Grants |
| **Performance** | Connection Pool, Query Cache (LRU+TTL), Prepared Statements, Binary Protocol |
| **Connection** | SSL/TLS, SCRAM-SHA-256, Unix Socket |
| **Operations** | EXPLAIN ANALYZE, Statement Timeout, LOCK TABLE, Batch Transactions |

---

## Production

Qail powers [Sailtix](https://sailtix.com) — a multi-operator maritime booking platform with full RLS tenant isolation across 27+ tables, serving real customers in production.

---

## Documentation

- 📖 [Full Documentation](https://qail.rs/docs)
- 📊 [Benchmarks](https://qail.rs/benchmarks)
- 📋 [Changelog](CHANGELOG.md)
- 💡 [The Manifesto](https://qail.rs/philosophy)

---

## License

Apache-2.0 © 2025-2026 Qail Contributors

<p align="center">
  <strong>Built with 🦀 Rust</strong><br>
  <a href="https://qail.rs">qail.rs</a>
</p>
