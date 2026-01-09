# CLI Commands

The `qail` command-line tool.

## Installation

```bash
cargo install qail
```

## Commands

### `qail init`

Initialize a new QAIL project with interactive setup:

```bash
qail init
# 🪝 QAIL Project Initialization
# Project name: my_app
# Select database mode:
#   1. PostgreSQL only
#   2. Qdrant only
#   3. Hybrid (PostgreSQL + Qdrant)
# ...
```

Generates `qail.toml` and necessary migration files.

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

### `qail migrate apply`

Apply file-based migrations from `migrations/` directory:

```bash
qail migrate apply
# → Found 1 migrations to apply
# ✓ Connected to qail_test
#   → 001_qail_queue.up.qail... ✓
# ✓ All migrations applied successfully!
```

Reads `qail.toml` for database connection if not provided via `--url`.

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

### `qail lint`

Check schema for best practices and potential issues:

```bash
qail lint schema.qail
# 🔍 Schema Linter
# ⚠ 144 warning(s)
# ℹ 266 info(s)
#
# ⚠ users.customer_id Possible FK column without references()
#   → Consider adding '.references("table", "id")' for referential integrity
#
# ⚠ orders Missing updated_at column
#   → Add 'updated_at TIMESTAMPTZ not_null' for audit trail

# Strict mode (errors only, for CI)
qail lint schema.qail --strict
```

**Lint Checks:**

| Check | Level | Description |
|-------|-------|-------------|
| Missing primary key | 🔴 ERROR | Every table needs a PK |
| Missing created_at/updated_at | ⚠️ WARNING | Audit trail columns |
| `_id` column without `references()` | ⚠️ WARNING | FK integrity |
| Uppercase table names | ⚠️ WARNING | Use snake_case |
| SERIAL vs UUID | ℹ️ INFO | Consider UUID for distributed |
| Nullable without default | ℹ️ INFO | Consider default value |

### `qail sync generate`

Generate trigger migrations from `[[sync]]` rules in `qail.toml`:

```bash
qail sync generate
# → Generating sync triggers...
# ✓ Created migrations/002_qail_sync_triggers.up.qail
```

Used in Hybrid mode to automatically create PostgreSQL triggers that push changes to the `_qail_queue` table.

### `qail worker`

Start the background worker to sync data from PostgreSQL to Qdrant:

```bash
qail worker --interval 1000 --batch 100
# 👷 QAIL Hybrid Worker v0.14.12
# 🔌 Qdrant: Connected (localhost:6334)
# 🐘 Postgres: Connected (5 connections)
# 
# [2026-01-02 10:00:00] 🔄 Syncing... (pending: 0)
```

**Options:**
- `--interval <ms>`: Polling interval (default: 1000ms)
- `--batch <size>`: Batch size for sync (default: 100)

### `qail migrate status`

View migration history for a database:

```bash
qail migrate status postgres://...
# 📋 Migration Status
#   Database: mydb
#   Migration table: _qail_migrations
#   ✓ Migration history table is ready
```

### `qail exec`

Execute type-safe QAIL statements against a database:

```bash
# Inline QAIL execution
qail exec "get users fields id, email where active = true" --url postgres://...
qail exec "add::users" --url postgres://... --tx

# From file
qail exec -f seed.qail --url postgres://...

# Dry-run (preview generated SQL)
qail exec "get::users" --dry-run
# 📋 Parsed 1 QAIL statement(s)
# 🔍 DRY-RUN MODE - Generated SQL:
# Statement 1:
#   SELECT * FROM users
# No changes made.
```

**Options:**
- `-f, --file <FILE>`: Path to `.qail` file with statements (one per line)
- `-u, --url <URL>`: Database connection URL
- `--tx`: Wrap all statements in a transaction
- `--dry-run`: Preview generated SQL without executing

**Features:**
- Type-safe execution via QAIL AST (`driver.execute(ast)`)
- Batch execution (multiple statements per file)
- Transaction support with automatic rollback on error
- Comments supported (`#` and `--`)

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
