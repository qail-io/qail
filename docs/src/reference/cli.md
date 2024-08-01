# CLI Commands

The `qail` command-line tool.

## Installation

```bash
cargo install qail
```

## Commands

### `qail parse`

Parse QAIL text syntax to SQL:

```bash
qail parse "get users fields * where active = true"
# SELECT * FROM users WHERE active = true
```

### `qail pull`

Extract schema from database:

```bash
qail pull postgres://user:pass@localhost/db > schema.qail
```

### `qail diff`

Compare two schemas and show migration commands:

```bash
qail diff old.qail new.qail
```

### `qail check`

Validate a schema file or preview migration safety:

```bash
# Validate schema
qail check schema.qail
# ✓ Schema is valid
#   Tables: 80
#   Columns: 1110
#   Indexes: 287
#   ✓ 82 primary key(s)

# Check migration safety
qail check old.qail:new.qail
# ✓ Both schemas are valid
# Migration preview: 4 operation(s)
#   ✓ 3 safe operation(s)
#   ⚠️  1 reversible operation(s)
```

### `qail migrate up`

Apply migrations:

```bash
qail migrate up old.qail:new.qail postgres://...
```

### `qail migrate down`

Rollback migrations:

```bash
qail migrate down old.qail:new.qail postgres://...
```

### `qail migrate plan`

Preview migration SQL without executing (dry-run):

```bash
qail migrate plan old.qail:new.qail
# 📋 Migration Plan (dry-run)
# ┌─ UP (2 operations) ─────────────────────────────────┐
# │ 1. ALTER TABLE users ADD COLUMN verified BOOLEAN
# │ 2. CREATE INDEX idx_users_email ON users (email)
# └─────────────────────────────────────────────────────┘
# ┌─ DOWN (2 operations) ───────────────────────────────┐
# │ 1. ALTER TABLE users DROP COLUMN verified
# │ 2. DROP INDEX IF EXISTS idx_users_email
# └─────────────────────────────────────────────────────┘

# Save to file
qail migrate plan old.qail:new.qail --output migration.sql
```

### `qail migrate analyze`

Analyze codebase for breaking changes before migrating:

```bash
qail migrate analyze old.qail:new.qail --codebase ./src
# 🔍 Migration Impact Analyzer
# Scanning codebase...
#   Found 395 query references
#
# ⚠️  BREAKING CHANGES DETECTED
# ┌─ DROP TABLE promotions (6 references) ─────────────┐
# │ ❌ src/repository/promotion.rs:89 → INSERT INTO...
# │ ❌ src/repository/promotion.rs:264 → SELECT...
# └────────────────────────────────────────────────────┘
```

### `qail watch`

Watch schema file for changes and auto-generate migrations:

```bash
qail watch schema.qail
# 👀 QAIL Schema Watch Mode
#    Watching: schema.qail
#    Press Ctrl+C to stop
# [14:32:15] ✓ Detected 2 change(s):
#        ALTER TABLE users ADD COLUMN avatar_url TEXT

# With database connection
qail watch schema.qail --url postgres://... --auto-apply
```

### `qail fmt`

Format QAIL text:

```bash
qail fmt "get users fields *" --indent
```

## Options

| Flag | Description |
|------|-------------|
| `-d, --dialect` | Target SQL dialect (pg, mysql) |
| `-f, --format` | Output format (sql, ast, json) |
| `-v, --verbose` | Verbose output |
| `--version` | Show version |
| `--help` | Show help |
