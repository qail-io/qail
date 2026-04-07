# qail-core

**The AST-native query builder** — No application SQL string building, no ORM magic, just type-safe expressions.

[![Crates.io](https://img.shields.io/crates/v/qail-core.svg)](https://crates.io/crates/qail-core)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Why AST-Native?

| Approach | How it works | Safety Level |
|----------|--------------|--------------|
| **String-based** | SQL as strings | Requires parameterization |
| **ORM** | Macros generate SQL | Compile-time safe |
| **AST-Native** (QAIL) | Typed AST → Protocol bytes | **Structurally safe** |

QAIL builds queries as an Abstract Syntax Tree that compiles directly to protocol bytes.

- **SQL string** = text query assembled in application code.
- **SQL bytes** = protocol frames + typed values emitted from AST.
- **Scope** = QAIL removes app-side SQL interpolation on the AST path.

## Legacy Syntax Notice

If search engines send you to old QAIL pages showing symbolic forms like `get::users•@id@email@role[active=true][lim=10]` or old macro snippets like `qail!("get::users:'id'email [ 'active == true ]")`, those are historical pre-1.0 docs from immutable old releases.

For `qail-core 0.27.x`, the canonical path is the native AST builder API, for example:

```rust
Qail::get("users")
    .columns(["id", "email", "role"])
    .eq("active", true)
    .limit(10)
```

## Installation

> [!CAUTION]
> **Release Candidate**: QAIL is now in the **release-candidate** phase. The API is near-stable and battle-tested in production. Breaking changes are expected to be rare and limited to critical correctness/security fixes before 1.0.


```toml
[dependencies]
qail-core = "0.27.8"
```

## Quick Start

```rust
use qail_core::{Qail, Operator};
use qail_core::ast::builders::*;

// Build a query as typed AST
let cmd = Qail::get("users")
    .columns([col("id"), col("name"), col("email")])
    .filter(eq("active", true))
    .order_by([("created_at", Desc)])
    .limit(10);

// Use with qail-pg driver
let rows = driver.fetch_all(&cmd).await?;
```

## Ergonomic Expression Builders

```rust
use qail_core::ast::builders::*;

// Aggregates with FILTER
count_filter(vec![eq("status", "active")]).alias("active_users")

// Time expressions  
now_minus("24 hours")  // NOW() - INTERVAL '24 hours'

// CASE WHEN
case_when(gt("score", 80), text("pass"))
    .otherwise(text("fail"))
    .alias("result")

// Type casting
cast(col("amount"), "float8")
```

## Features

- **Type-safe expressions** — Compile-time checked query building
- **Ergonomic builders** — `count()`, `sum()`, `case_when()`, `now_minus()`
- **Full SQL support** — CTEs, JOINs, DISTINCT ON, aggregates with FILTER
- **JSON operators** — `->`, `->>`, `@>`, `?`
- **Schema parser** — Parse DDL into structured AST

## Ecosystem

| Crate | Purpose |
|-------|---------|
| **qail-core** | AST builder, parser, expression helpers |
| [qail-pg](https://crates.io/crates/qail-pg) | PostgreSQL driver (AST → wire protocol) |
| [qail](https://crates.io/crates/qail) | CLI tool for migrations and schema ops |

## License

Apache-2.0

## 🤝 Contributing & Support

We welcome issue reports on GitHub! Please provide detailed descriptions to help us reproduce and fix the problem. We aim to address critical issues within 1-5 business days.
