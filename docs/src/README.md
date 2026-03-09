# QAIL Documentation

> **The AST-Native Query Compiler with Built-in Row-Level Security**

QAIL compiles typed query ASTs directly to database wire protocols. No application-level SQL string interpolation on the AST path. Built-in multi-tenant data isolation via RLS. The only Rust PostgreSQL driver with AST-level tenant injection.

## Philosophy: AST = Meaning

> **If a database doesn't let us encode semantic intent, we don't fake it.**

QAIL compiles typed query ASTs directly to database wire protocols with typed value encoding.

## SQL String vs SQL Bytes

- **SQL string**: text query assembled in application code.
- **SQL bytes**: PostgreSQL protocol message bytes (`Parse`/`Bind`/`Execute` and results) plus encoded bind values.
- **QAIL guarantee**: AST flow removes app-side SQL interpolation as an injection surface.
- **PostgreSQL behavior**: server parse/plan/execute still applies normally.

### Supported Databases

| Tier | Category | Supported | Driver |
|------|----------|-----------|--------|
| **1** | **SQL-AST** | **PostgreSQL** | `qail-pg` — Native wire protocol, AST-to-bytes |
| **2** | **Vector-AST** | **Qdrant** | `qail-qdrant` — gRPC + REST, vector search |

> Redis support (`qail-redis`) was removed in `v0.20.0`.

### ❌ Not Supported
* **Oracle, SQL Server, MySQL:** Proprietary/Closed protocols.

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
let rows = driver.query(&cmd).await?;
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

// QAIL AST (impossible to inject):
Qail::get("users").filter("id", Operator::Eq, user_input)
// user_input becomes Value::Int(123) or Value::Text("...") 
// — never interpolated into a string
```

## Getting Help

- [GitHub Repository](https://github.com/qail-io/qail)
- [Issue Tracker](https://github.com/qail-io/qail/issues)


## 🤝 Contributing & Support

We welcome issue reports on GitHub! Please provide detailed descriptions to help us reproduce and fix the problem. We aim to address critical issues within 1-5 business days.

> [!CAUTION]
> **Beta Software**: QAIL is currently in **beta**. The API is stabilizing and is battle-tested in production. Breaking changes may still occur between minor versions.
