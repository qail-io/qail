# QAIL Documentation

> **The AST-Native Query Compiler with Built-in Row-Level Security**

QAIL compiles typed query ASTs directly to database wire protocols. No application-level SQL string interpolation on the AST path. Built-in multi-tenant data isolation via PostgreSQL RLS context setup.

## Product Map

| Concept | Crate | Purpose |
|---------|-------|---------|
| AST Kernel | `qail-core` | Typed AST, parser, expressions, RLS context, native access policy |
| Postgres Driver | `qail-pg` | Async PostgreSQL wire-protocol execution |
| Access Gateway | `qail-gateway` | AutoREST, WebSocket, OpenAPI, auth/RLS/policy enforcement |
| SchemaOps CLI | `qail` | Schema pull, live drift diff, phased migrations, lint, codegen |
| Flow Engine | `qail-workflow` | Declarative workflow state machines |
| Flow Ledger | `qail-workflow-postgres` | PostgreSQL workflow leases, state, idempotency, side effects, timeouts |
| Vector Bridge | `qail-qdrant` | Qdrant vector search with AST-compatible filters |

For a deeper orientation, read the [Platform Map](./platform-map.md). It
explains which crate owns each safety boundary and which surface to choose for
driver, gateway, schema, workflow, or vector workloads.

## Latest Updates (June 2026)

- QAIL is now on the `v1.3.4` stable line across the Rust workspace crates and CLI.
- Linux `io_uring` transport is now explicit opt-in; Tokio remains the default
  unless `[postgres].io_uring = true`, `?io_uring=true`, driver/pool options,
  or `QAIL_PG_IO_BACKEND=io_uring` enables it.
- The public API is the AST/DSL path: `Qail::get/add/set/del`, typed expressions, relation helpers, RLS contexts, and driver/pool execution.
- Compatibility aliases that hid fallible behavior were removed: use `with_rls(&ctx)?` and `join_on(...)?` directly.
- Legacy raw SQL builder APIs remain out of the normal runtime path; use AST-native commands and session AST helpers instead.
- PostgreSQL cancel-key APIs are bytes-native, matching protocol `3.0` and `3.2` behavior.
- Native vertical access policy is now first-class through `qail_core::access` and optional `[access]` config for operation and column permissions alongside PostgreSQL RLS.
- Workflow charge side effects now carry stable idempotency keys, origin
  metadata, and redacted display payloads for payment/chat surfaces.
- Migration docs use the expand/backfill/contract apply model instead of presenting up/down as the primary workflow.

Read [Access Policy](./features/access-policy.md) for the vertical permission
model and [Workflows](./features/workflows.md) for Flow Engine / Flow Ledger
production semantics.

## Philosophy: AST = Meaning

> **If a database doesn't let us encode semantic intent, we don't fake it.**

QAIL compiles typed query ASTs directly to database wire protocols with typed value encoding.

## SQL String vs SQL Bytes

- **SQL string**: text query assembled in application code.
- **SQL bytes**: PostgreSQL protocol message bytes (`Parse`/`Bind`/`Execute` and results) plus encoded bind values.
- **QAIL guarantee**: AST flow removes app-side SQL interpolation as an injection surface.
- **PostgreSQL behavior**: server parse/plan/execute still applies normally.

## Legacy Syntax Notice

Some search engines still surface old QAIL pages showing symbolic forms such as `get::users•@id@email@role[active=true][lim=10]` or macro snippets such as `qail!("get::users:'id'email [ 'active == true ]")`.

Those pages are from historical pre-1.0 releases and are not the current API guidance.

Current QAIL `1.3.4` application code should use the native AST/DSL path:

```rust
let query = Qail::get("users")
    .columns(["id", "email", "role"])
    .eq("active", true)
    .limit(10);

let rows = driver.fetch_all(&query).await?;
```

### Supported Databases

| Tier | Category | Supported | Driver |
|------|----------|-----------|--------|
| **1** | **SQL-AST** | **PostgreSQL** | `qail-pg` — Native wire protocol, AST-to-bytes |
| **2** | **Vector-AST** | **Qdrant** | `qail-qdrant` — gRPC + REST, vector search |

> Redis support (`qail-redis`) was removed in `v0.20.0`.

### ❌ Not Supported
* Database protocols outside PostgreSQL and Qdrant are not part of the supported surface.

## Quick Example

```rust
use qail_core::{Qail, Operator, SortOrder};

// Build a query with the AST builder
let cmd = Qail::get("users")
    .columns(["id", "email", "name"])
    .filter("active", Operator::Eq, true)
    .order_by("created_at", SortOrder::Desc)
    .limit(10);

// Execute with qail-pg driver
let mut driver = PgDriver::connect("localhost", 5432, "user", "db").await?;
let rows = driver.fetch_all(&cmd).await?;
```

## Current Status (Production Ready, Actively Hardened)

| Feature | Status |
|---------|--------|
| SSL/TLS | ✅ |
| SCRAM-SHA-256 Auth | ✅ |
| Connection Pooling | ✅ |
| AST-Native Migrations | ✅ |
| JSON/JSONB Types | ✅ |
| UUID, Timestamps, INTERVAL | ✅ |
| CTEs (WITH) | ✅ |
| DISTINCT ON | ✅ |
| CASE WHEN | ✅ |
| Ergonomic Builders | ✅ |
| qail-lsp (IDE) | ✅ |
| COPY Protocol | ✅ |
| Arrays (Value::Array) | ✅ |
| Transactions (BEGIN/COMMIT/ROLLBACK) | ✅ |
| Query Plan Caching | ✅ |
| Window Functions (OVER) | ✅ |
| Subqueries & EXISTS | ✅ |
| UPSERT (ON CONFLICT) | ✅ |
| RETURNING Clause | ✅ |
| LATERAL JOIN | ✅ |
| Unix Socket & mTLS | ✅ |
| Savepoints | ✅ |
| UNION/INTERSECT/EXCEPT | ✅ |
| TRUNCATE | ✅ |
| Batch Transactions | ✅ |
| Statement Timeout | ✅ |
| EXPLAIN / EXPLAIN ANALYZE | ✅ |
| LOCK TABLE | ✅ |
| Connection Timeout | ✅ |
| Materialized Views | ✅ |
| Row-Level Security (RLS) | ✅ |
| Multi-Tenant Isolation | ✅ |
| `TypedQail<T>` Relations | ✅ |
| Protected Columns | ✅ |
| LISTEN/NOTIFY/UNLISTEN | ✅ |

> **Note:** QAIL's AST-native design eliminates app-side SQL interpolation on the AST path. Query plan caching (`prepare()`, `pipeline_prepared_fast()`) is a PostgreSQL performance optimization, not the primary security boundary.

## Why Some SQL Features Don't Exist in QAIL

QAIL is **AST-first**, not SQL-string-first. Many traditional SQL "security features" exist to mitigate string-construction risks that AST pipelines avoid by design:

| SQL Feature | Why It Exists | QAIL Replacement |
|-------------|---------------|------------------|
| **Parameterized Queries** | Prevent string injection | Built in — `Value::Param` is a typed AST node, not a string hole |
| **Prepared Statements** (for security) | Separate SQL from data | Not primary defense — AST already separates structure from data |
| **Query Escaping** | Sanitize user input | Not primary path — values are typed (`Value::Text`, `Value::Int`) |
| **SQL Validators** | Detect malformed queries | AST validation + build-time checks handle this path |

### The AST Guarantee

```rust
// SQL String (vulnerable):
let sql = format!("SELECT * FROM users WHERE id = {}", user_input);

// QAIL AST (structure is not interpolated from user text):
Qail::get("users").filter("id", Operator::Eq, user_input)
// user_input becomes Value::Int(123) or Value::Text("...") 
// — never interpolated into a string
```

## Getting Help

- [GitHub Repository](https://github.com/qail-io/qail)
- [Issue Tracker](https://github.com/qail-io/qail/issues)


## Contributing & Support

We welcome issue reports on GitHub! Please provide detailed descriptions to help us reproduce and fix the problem. We aim to address critical issues within 1-5 business days.

> [!NOTE]
> **Stable Release**: QAIL is now on the `1.x` stable line. Breaking changes should be treated as release-line decisions and documented in `CHANGELOG.md`.
