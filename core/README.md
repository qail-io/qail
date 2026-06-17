# qail-core

**QAIL AST Kernel** - typed query AST, expression builders, schema validation,
RLS context, and native access policy.

[![Crates.io](https://img.shields.io/crates/v/qail-core.svg)](https://crates.io/crates/qail-core)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## What It Owns

`qail-core` is the semantic center of QAIL. It does not open sockets and it is
not the CLI. It defines the query and schema model shared by the Postgres
driver, Access Gateway, SchemaOps CLI, Qdrant Vector Bridge, and Flow Engine.

| Concept name | In this crate |
|---|---|
| AST Kernel | `Qail`, `Expr`, builders, parser, formatter |
| Policy Matrix | `qail_core::access` table/operation/column checks |
| RLS Context | `RlsContext`, tenant/user/super-admin witnesses |
| Schema Model | migration schema parser, diff model, typed codegen |
| Source Scanner | optional analyzer/build scanner for query references |

## Why AST-Native?

| Approach | How it works | Safety Level |
|----------|--------------|--------------|
| **String-based** | SQL as strings | Requires parameterization |
| **ORM** | Macros generate SQL | Compile-time safe |
| **AST-Native** (QAIL) | Typed AST → Protocol bytes | **Structurally safe** |

QAIL builds queries as an Abstract Syntax Tree. Drivers and tools can then
validate, rewrite, format, encode, or execute that structure without asking
application code to concatenate SQL strings.

- **SQL string** = text query assembled in application code.
- **SQL bytes** = protocol frames + typed values emitted from AST.
- **Scope** = QAIL removes app-side SQL interpolation on the AST path.

## Legacy Syntax Notice

If search engines send you to old QAIL pages showing symbolic forms like `get::users•@id@email@role[active=true][lim=10]` or old macro snippets like `qail!("get::users:'id'email [ 'active == true ]")`, those are historical pre-1.0 docs from immutable old releases.

For `qail-core 1.3.x`, the canonical path is the native AST builder API, for example:

```rust
Qail::get("users")
    .columns(["id", "email", "role"])
    .eq("active", true)
    .limit(10)
```

## Installation

```toml
[dependencies]
qail-core = "1.3.3"
```

## Quick Start

```rust
use qail_core::prelude::*;

// Build a query as typed AST
let cmd = Qail::get("users")
    .columns(["id", "name", "email"])
    .eq("active", true)
    .order_by("created_at", Desc)
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

- **Typed AST builders** - `Qail::get/add/set/del`, joins, filters, CTEs,
  aggregates, window expressions, MERGE, and RETURNING.
- **Policy Matrix** - native table, operation, and column policy checks before
  an AST reaches a driver or gateway.
- **RLS context model** - tenant-first execution witnesses and super-admin
  bypass tokens for internal use.
- **Schema parser and diff model** - PostgreSQL-oriented `schema.qail`
  parsing, validation, and migration planning structures.
- **Build/source scanner** - optional analyzer support for N+1 and stale schema
  reference diagnostics.

## Ecosystem

| Crate | Purpose |
|-------|---------|
| **qail-core** | AST Kernel, parser, expression helpers, RLS/access policy |
| [qail-pg](https://crates.io/crates/qail-pg) | PostgreSQL driver (AST → wire protocol) |
| [qail](https://crates.io/crates/qail) | CLI tool for migrations and schema ops |
| [qail-gateway](https://crates.io/crates/qail-gateway) | Access Gateway for AutoREST/WebSocket/OpenAPI |

## License

Apache-2.0

## 🤝 Contributing & Support

We welcome issue reports on GitHub! Please provide detailed descriptions to help us reproduce and fix the problem. We aim to address critical issues within 1-5 business days.
