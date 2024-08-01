# QAIL Documentation

> **The AST-Native Query Compiler**

QAIL compiles typed query ASTs directly to database wire protocols. No SQL strings. No injection surface. Just pure, type-safe queries.

## Why QAIL?

| Aspect | SQL Strings | QAIL AST |
|--------|-------------|----------|
| **Type Safety** | Runtime errors | Compile-time |
| **Injection Risk** | Possible | Impossible |
| **Portability** | Dialect-specific | Universal |

## Quick Example

```rust
use qail_core::ast::{QailCmd, Operator, SortOrder};

// Build a query with the AST builder
let cmd = QailCmd::get("users")
    .columns(["id", "email", "name"])
    .filter("active", Operator::Eq, true)
    .order_by("created_at", SortOrder::Desc)
    .limit(10);

// Execute with qail-pg driver
let mut driver = PgDriver::connect("localhost", 5432, "user", "db").await?;
let rows = driver.query(&cmd).await?;
```

## Current Status (~70% Production Ready)

| Feature | Status |
|---------|--------|
| SSL/TLS | ✅ |
| SCRAM-SHA-256 Auth | ✅ |
| Connection Pooling | ✅ |
| AST-Native Migrations | ✅ |
| JSON/JSONB Types | ✅ |
| UUID, Timestamps | ✅ |
| CTEs (WITH) | ✅ |
| DISTINCT ON | ✅ |
| CASE WHEN | ✅ |
| Ergonomic Builders | ✅ |
| qail-lsp (IDE) | ✅ |
| COPY Protocol | ✅ |
| Arrays (Value::Array) | ✅ |
| Transactions (BEGIN/COMMIT) | ✅ |
| Savepoints | 🚧 |
| Prepared Statements | 🚧 |

## Getting Help

- [GitHub Repository](https://github.com/qail-rs/qail)
- [Issue Tracker](https://github.com/qail-rs/qail/issues)

