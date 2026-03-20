# Qail — Application-to-Database Pipeline with Compile-Time Safety

> **From your app to PostgreSQL wire bytes — one typed AST, zero application SQL strings, built-in tenant isolation.**

[![Crates.io](https://img.shields.io/badge/crates.io-qail-orange)](https://crates.io/crates/qail)
[![Docs](https://img.shields.io/badge/docs-dev.qail.io-blue)](https://dev.qail.io/docs)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.26.1-green)](CHANGELOG.md)

---

## The Problem

Every SaaS backend has the same three bugs waiting to happen:

1. **N+1 queries** — Your ORM fires 151 queries where 1 would do. You find out in production.
2. **SQL injection** — One string interpolation mistake. That's all it takes.
3. **Broken tenant isolation** — A missing `WHERE tenant_id = ?` leaks another customer's data.

These aren't edge cases. They're the *default* outcome of string-based SQL.

## The Fix

Qail is a **native AST PostgreSQL pipeline**. Instead of writing ad-hoc SQL strings in application code, you build a typed Abstract Syntax Tree that compiles to PostgreSQL wire protocol bytes.

```rust
// String-based (every other driver): parse → plan → execute
"SELECT id, email FROM users WHERE active = $1"

// Qail: AST → protocol bytes → server parse/plan/execute
// (no app-side SQL interpolation surface)
Qail::get("users").columns(["id", "email"]).eq("active", true)
```

### SQL String vs SQL Bytes (Exact Meaning)

- **SQL string**: query text assembled in app code (manual concatenation/interpolation, dynamic formatting, etc.).
- **SQL bytes**: frontend/backend protocol frames and typed bind-value bytes emitted by the driver.
- **Qail claim**: "no SQL strings" means no **application-level string interpolation path** on the AST flow.
- **PostgreSQL behavior**: PostgreSQL still performs normal parse/plan/execute on received statements (or reuses prepared plans).

**N+1 is structurally discouraged by design and enforced by build-time detection.** The AST guides you to `.join()` (no implicit lazy-loading path), and validator/lint rules catch looped query patterns before production.

### Best Fit / Not Fit

- **Best fit:** PostgreSQL-first SaaS backends that want strong tenant isolation, AST safety, and high-throughput query execution.
- **Not fit:** teams that need GraphQL-first ecosystems, cross-database federation, or SQL-string-centric workflows.

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

> **The headline isn't 0.4ms vs 0.6ms.** It's that many teams ship Approach 4 or 5 — the one that's **40-50× slower** — because their tools allow N+1 by default.
>
> Qail makes the slow version hard to write and easy to catch in CI. The AST guides you to `.join()`, and N+1 detection closes the gap.

<details>
<summary><strong>Methodology notes (click to expand)</strong></summary>

- **Cache equalization:** A 10-iteration global warmup loads all data pages into Postgres buffer cache *before* any timed approach runs. All approaches start from identical cache state.
- **Fair protocol:** All five approaches use Qail's binary driver internally, isolating the architectural difference (1 query vs N+1) from protocol overhead.
- **DataLoader:** Realistically batches N lookups into `WHERE id IN (...)` queries — the standard GraphQL optimization.
- **REST + expand:** Server-side JOIN (same query as Qail) + JSON serialization overhead. The 1.4× gap is pure JSON ser/de cost.
- **Network latency:** Local benchmark = 0ms latency. In production (app → RDS), each extra round trip adds 1-2ms. The gap between Qail (1 trip) and DataLoader (3 trips) widens significantly.
- **Benchmark context:** These are snapshot numbers from Feb 2026. Absolute latency varies by hardware/Postgres config; relative N+1 vs single-query behavior is the key signal.
- Run it yourself: `DATABASE_URL=... cargo run --example battle_comparison --features chrono,uuid --release`

</details>

---

## Quick Start

```rust
use qail_core::prelude::*;
use qail_pg::PgDriver;

// Connect
let mut driver = PgDriver::connect("localhost", 5432, "user", "mydb").await?;

// Multi-tenant: scope every query to this tenant
let ctx = RlsContext::tenant(tenant_id);

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
qail diff _ schema.qail --live --url postgres://...    # Drift detection
qail migrate up v1:v2 postgres://...             # Apply migrations
qail types schema.qail > src/generated/schema.rs # Typed codegen
```

---

## Why Qail > String SQL

### 🔐 Security Is Compiled In

| Threat | String SQL | Qail |
|--------|-----------|------|
| SQL injection | Possible (one mistake) | **Prevented on AST path** (no app-side SQL interpolation) |
| Tenant data leak | Missing WHERE clause | **RLS injected automatically** |
| Query abuse | Unbounded depth/joins | **AST validates at compile time** |
| IDOR | Must check per endpoint | **Tenant isolation built into protocol** |

```rust
// RLS: tenant-first constructors
let ctx = RlsContext::tenant(tenant_id);            // Single tenant (preferred)
let ctx = RlsContext::tenant_and_agent(tenant_id, agent_id); // Agent/reseller within tenant
let ctx = RlsContext::global();                     // Shared data (tenant_id IS NULL)
let token = SuperAdminToken::for_system_process("admin");
let ctx = RlsContext::super_admin(token);           // Full bypass (internal only)

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
├── encoder/    Wire protocol encoder + FFI/runtime internals
├── qdrant/     Qdrant vector DB driver (optional)
├── workflow/   Workflow engine
└── sdk/        Direct SDKs (TypeScript, Swift, Kotlin)
```

---

## SDK Status

| Platform | Status | Distribution |
|----------|--------|--------------|
| TypeScript | ✅ Supported | `npm install @qail/client` |
| Swift | ✅ Supported | Source package in `sdk/swift` |
| Kotlin | ✅ Supported | Gradle module in `sdk/kotlin` |
| Node.js native binding | ⏸ Deferred | Not shipped yet |

`tenant_id` is the runtime contract across gateway and RLS paths. Legacy `operator_id` runtime compatibility aliases were removed in `v0.26.0`.

---

## N+1 Detection

Compile-time static analysis catches query-in-loop patterns before they reach production. Powered by QAIL semantic Rust scanning (no `syn` dependency on the runtime analyzer path).

### Rules

| Code | Severity | Trigger | Example |
|------|----------|---------|---------|
| N1-001 | ⚠ Warning | Query inside `for`/`while`/`loop` | `for x in items { conn.fetch_all(&q) }` |
| N1-002 | ⚠ Warning | Loop variable used in query args | `for id in ids { Qail::get("t").eq("id", id) }` |
| N1-003 | ⚠ Warning | Function with query called in loop | `for x in xs { load_user(conn, x) }` |
| N1-004 | ❌ Error | Query in nested loop (depth ≥ 2) | `for g in groups { for x in g { ... } }` |

### Suppression

```rust
// Disable on the next line
// qail-lint:disable-next-line N1-001
conn.fetch_all(&cmd).await?;

// Disable inline
conn.fetch_all(&cmd).await?; // qail-lint:disable-line N1-001
```

### Build Integration (`build.rs`)

Runs automatically via `validate()` when using Qail's build-time checks:

| Env Var | Values | Default |
|---------|--------|---------|
| `QAIL_NPLUS1` | `off` \| `warn` \| `deny` | `warn` |
| `QAIL_NPLUS1_MAX_WARNINGS` | integer | `50` |
| `QAIL_SCAN_DIRS` | comma-separated source roots | `src` |

Monorepo example:

```bash
# Scan multiple Rust roots during build validation + N+1 checks
QAIL_SCAN_DIRS="src,apps/api/src,crates/billing/src" cargo build
```

### CLI

```bash
qail check schema.qail --src ./src              # Shows N+1 warnings
qail check schema.qail --src ./src --nplus1-deny # Fails on any N+1
```

### LSP

N+1 diagnostics appear automatically in your editor for `.rs` files with diagnostic codes `N1-001`..`N1-004`.

### Remediation

```rust
// ❌ N+1: one query per item
for id in &ids {
    let user = conn.fetch_one(&Qail::get("users").eq("id", id)).await?;
}

// ✅ Batch: single query
let users = conn.fetch_all(
    &Qail::get("users").in_vals("id", &ids)
).await?;
```

---

## Feature Status (March 2026)

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

### Connection Pool Maintenance

Activate background pool health maintenance (idle connection cleanup + `min_connections` backfill) by calling `spawn_pool_maintenance` after creating the pool:

```rust
let pool = qail_pg::PgPool::connect(config).await?;
qail_pg::spawn_pool_maintenance(pool.clone());
```

---

## Documentation

- 📖 [Full Documentation](https://dev.qail.io/docs)
- 📊 [Benchmarks](https://dev.qail.io/benchmarks)
- 📋 [Changelog](CHANGELOG.md)
- 💡 [The Manifesto](https://dev.qail.io/philosophy)

---

## License

Apache-2.0 © 2025-2026 Qail Contributors

<p align="center">
  <strong>Built with 🦀 Rust</strong><br>
  <a href="https://dev.qail.io">dev.qail.io</a>
</p>
