# QAIL Documentation

> **The AST-Native Query Compiler with Built-in Row-Level Security**

QAIL compiles typed query ASTs directly to database wire protocols. No SQL strings. No injection surface. Built-in multi-tenant data isolation via RLS. The only Rust PostgreSQL driver with AST-level tenant injection.

## Philosophy: AST = Meaning

> **If a database doesn't let us encode semantic intent, we don't fake it.**

QAIL compiles typed query ASTs directly to database wire protocols. No SQL strings. No injection surface.

### Supported Databases

| Tier | Category | Supported | Driver |
|------|----------|-----------|--------|
| **1** | **SQL-AST** | **PostgreSQL** | `qail-pg` — Native wire protocol, AST-to-bytes |
| **2** | **Vector-AST** | **Qdrant** | `qail-qdrant` — gRPC + REST, vector search |
| **3** | **KV-Command** | **Redis** | `qail-redis` — Native RESP3 protocol |

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

## Current Status (~80% Production Ready)

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
| TypedQail<T> Relations | ✅ |
| Protected Columns | ✅ |
| LISTEN/NOTIFY/UNLISTEN | ✅ |

> **Note:** QAIL's AST-native design eliminates SQL injection by construction — no strings, no injection surface. Query plan caching (`prepare()`, `pipeline_prepared_fast()`) is purely a PostgreSQL performance optimization, not a security measure.

## Why Some SQL Features Don't Exist in QAIL

QAIL speaks **AST**, not SQL strings. Many traditional SQL "security features" are solutions to string-based problems that don't exist in an AST-native world:

| SQL Feature | Why It Exists | QAIL Replacement |
|-------------|---------------|------------------|
| **Parameterized Queries** | Prevent string injection | Not needed — `Value::Param` is a typed AST node, not a string hole |
| **Prepared Statements** (for security) | Separate SQL from data | Not needed — AST has no SQL text to inject into |
| **Query Escaping** | Sanitize user input | Not needed — values are typed (`Value::Text`, `Value::Int`), never interpolated |
| **SQL Validators** | Detect malformed queries | Not needed — invalid AST won't compile |

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
