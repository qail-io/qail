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

## Installation

> [!CAUTION]
> **Beta Software**: QAIL is currently in **beta**. The API is stabilizing and is battle-tested in production. Breaking changes may still occur between minor versions.


```toml
[dependencies]
qail-core = "0.26.2"
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
