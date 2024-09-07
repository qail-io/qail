# qail

**Schema-first database toolkit** — Pull, diff, migrate, validate.

[![Crates.io](https://img.shields.io/crates/v/qail.svg)](https://crates.io/crates/qail)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## Installation

```bash
cargo install qail
```

## Commands

### Schema Operations

```bash
# Extract schema from database
qail pull postgres://user:pass@host/db -o schema.qail

# Compare two schemas
qail diff old.qail new.qail

# Check best practices
qail lint schema.qail

# Format QAIL files
qail fmt schema.qail
```

### Migration Operations

```bash
# Create a new migration
qail migrate create add_users_table --author "dev"

# Preview migration SQL
qail migrate plan old.qail:new.qail

# Apply migrations
qail migrate up old.qail:new.qail postgres://...

# Rollback migrations
qail migrate down postgres://...
```

### Query REPL

```bash
# Interactive query transpiler
qail repl

> get users fields id, name where active = true
SELECT id, name FROM users WHERE active = true
```

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

- **Schema extraction** — Pull live schemas from PostgreSQL
- **Smart diffing** — Detect additions, removals, modifications
- **Impact analysis** — Warn about breaking changes
- **Foreign key validation** — Ensure referential integrity
- **Data-safe migrations** — Preview SQL before applying
- **Type-safe queries** — Transpile QAIL to SQL

## Ecosystem

| Crate | Purpose |
|-------|---------|
| **qail** | CLI tool for schema and migration operations |
| [qail-core](https://crates.io/crates/qail-core) | AST builder, parser, expression helpers |
| [qail-pg](https://crates.io/crates/qail-pg) | PostgreSQL driver (AST → wire protocol) |

## License

MIT

## 🤝 Contributing & Support

We welcome issue reports on GitHub! Please provide detailed descriptions to help us reproduce and fix the problem. We aim to address critical issues within 1-5 business days.

> [!CAUTION]
> **Alpha Software**: QAIL is currently in **alpha**. While we strive for stability, the API is evolving to ensure it remains ergonomic and truly AST-native. **Do not use in production environments yet.**
