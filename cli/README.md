# qail

**QAIL SchemaOps CLI** - schema pull, drift diff, phased migrations, lint,
typed codegen, and AST query tooling for the QAIL ecosystem.

[![Crates.io](https://img.shields.io/crates/v/qail.svg)](https://crates.io/crates/qail)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Installation

```bash
cargo install qail
```

## Where This Fits

QAIL has several crates. The `qail` crate is the command-line tool, not the
PostgreSQL driver. Start here when you need migration and schema operations:

| Surface | Crate | Use it for |
|---|---|---|
| SchemaOps CLI | `qail` | pull, check, diff, migrate, codegen, lint |
| AST Kernel | `qail-core` | typed AST, expressions, RLS context, access policy |
| Postgres Driver | `qail-pg` | async PostgreSQL wire-protocol execution |
| Access Gateway | `qail-gateway` | AutoREST, WebSocket, OpenAPI, policy enforcement |
| Flow Engine | `qail-workflow` | declarative state-machine workflows |
| Flow Ledger | `qail-workflow-postgres` | Postgres-backed workflow state/idempotency |

## Primary Workflows

### Schema Operations

```bash
# Extract schema from database
qail pull --url postgres://user:pass@host/db > schema.qail

# Compare desired schema against live database drift
qail diff _ schema.qail --live --url postgres://user:pass@host/db

# Compare two schema files
qail diff old.qail new.qail

# Validate schema and optional source references
qail check schema.qail --src ./src

# Format QAIL files
qail fmt schema.qail
```

### Migration Operations

```bash
# Create a new migration
qail migrate create add_users_table --author "dev"

# Preview migration SQL
qail migrate plan old.qail:new.qail

# Apply phased migrations from deltas/
qail migrate apply --phase expand
qail migrate apply --phase backfill --backfill-chunk-size 10000
qail migrate apply --phase contract --codebase ./src

# Explicit rollback when needed
qail migrate rollback --to base --url postgres://...
```

### Query REPL

```bash
# Interactive query transpiler
qail repl

> get users fields id, name where active = true
SELECT id, name FROM users WHERE active = true
```

## SQL String vs SQL Bytes

- `qail repl` shows SQL text for inspection/debugging.
- Runtime execution with `qail-pg` is AST-first and protocol-byte based.
- In other words: SQL text is a tooling view here, not the required authoring model for app code.

## Legacy Syntax Notice

Old QAIL releases experimented with symbolic text syntax such as `get::users•@id@email@role[active=true][lim=10]` and macro snippets such as `qail!("get::users:'id'email [ 'active == true ]")`.

Those examples are **legacy** and may still appear on old `docs.rs` pages in
search results. They are not the current `1.3.x` recommendation.

Current QAIL application code should use the native AST builder API, while the `qail` CLI remains a tooling surface for schema work, REPL inspection, and migration operations.

## Schema Format

QAIL uses a concise, version-controlled schema format:

```sql
-- schema.qail
table users (
    id uuid primary key default gen_random_uuid(),
    email text not null unique,
    created_at timestamptz default now()
);

table orders (
    id uuid primary key,
    user_id uuid references users(id),
    total numeric(10,2)
);
```

## Features

- **Drift Guard** - compare a live PostgreSQL database with `schema.qail`.
- **Phased migrations** - apply expand, backfill, and contract phases.
- **Strict receipts** - track applied migrations in `_qail_migrations`.
- **Source scanner** - catch stale schema references before contract cleanup.
- **Schema modules** - split, merge, and doctor modular schema directories.
- **Typed codegen** - generate Rust schema helpers from `schema.qail`.
- **AST query tooling** - inspect and execute QAIL statements without making
  SQL string construction the application runtime model.

## Ecosystem

| Crate | Purpose |
|-------|---------|
| **qail** | CLI tool for schema and migration operations |
| [qail-core](https://crates.io/crates/qail-core) | AST builder, parser, expression helpers, RLS/access policy |
| [qail-pg](https://crates.io/crates/qail-pg) | PostgreSQL driver (AST → wire protocol) |
| [qail-gateway](https://crates.io/crates/qail-gateway) | Access Gateway for AutoREST/WebSocket/OpenAPI |
| [qail-workflow](https://crates.io/crates/qail-workflow) | Flow Engine for business state machines |
| [qail-workflow-postgres](https://crates.io/crates/qail-workflow-postgres) | Flow Ledger storage backend |

## License

Apache-2.0

## 🤝 Contributing & Support

We welcome issue reports on GitHub! Please provide detailed descriptions to help us reproduce and fix the problem. We aim to address critical issues within 1-5 business days.
