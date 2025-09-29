# CLI Commands

The `qail` command-line tool — v0.15.8.

## Installation

```bash
cargo install qail
```

## Commands

### `qail init`

Initialize a new QAIL project. Auto-detects running PostgreSQL instances on the host, Docker, and Podman.

```bash
# Interactive mode — scans for running databases
qail init
# 🪝 QAIL Project Initialization
# Scanning for PostgreSQL instances...
#   ✓ Found 2 instance(s):
#     1.  🖥  host localhost:5432 (host)
#     2.  🐳 docker localhost:5433 (docker) — my-pg
#     3.  Enter URL manually
# Select [1-3]:

# Non-interactive mode (CI/scripting)
qail init --name myapp --mode postgres --url postgres://localhost/mydb
qail init --name myapp --mode hybrid --url postgres://localhost/mydb --deployment docker
```

**Options:**
- `--name <NAME>`: Project name
- `--mode <MODE>`: Database mode (`postgres`, `qdrant`, `hybrid`)
- `--url <URL>`: Database URL (skips interactive prompt)
- `--deployment <TYPE>`: Deployment type (`host`, `docker`, `podman`)

Generates `qail.toml` and necessary migration files.

---

### `qail exec`

Execute QAIL statements against a database:

```bash
# Inline queries
qail exec "get users'id'email[active = true]" --url postgres://...
qail exec "add users fields name, email values 'Alice', 'a@test.com'" --url postgres://... --tx
qail exec "set users[id = 1] fields name = 'Bob'" --url postgres://...

# Count rows
qail exec "cnt orders[status = 'paid']" --url postgres://...
# → SELECT COUNT(*) FROM orders WHERE status = 'paid'

# JSON output (pipe-friendly)
qail exec "get users" --url postgres://... --json
qail exec "get users" --url postgres://... --json | jq '.[].email'

# From file
qail exec -f seed.qail --url postgres://...

# Dry-run (preview generated SQL)
qail exec "get users'*'" --dry-run
# 📋 Parsed 1 QAIL statement(s)
# 🔍 DRY-RUN MODE — Generated SQL:
#   SELECT * FROM users

# With SSH tunnel
qail exec "get users" --url postgres://remote/db --ssh user@bastion
```

**Syntax:**

```
add <table> fields <col1>, <col2> values <val1>, <val2>
set <table>[id = $1] fields name = 'new', updated_at = now
del <table>[id = $1]
get <table>'id'name[active = true]
cnt <table>[active = true]
```

**Value Types:**

| Type | Examples |
|------|----------|
| Strings | `'hello'`, `"world"` |
| Numbers | `42`, `3.14`, `-1` |
| Booleans | `true`, `false` |
| Null | `null` |
| Parameters | `$1`, `$2`, `:name` |
| Intervals | `24h`, `7d`, `30m` |
| JSON | `["a", "b"]`, `{"key": "val"}` |
| Timestamp | `now` |

**Options:**
- `-f, --file <FILE>`: Path to `.qail` file with statements
- `-u, --url <URL>`: Database connection URL
- `--json`: Output SELECT results as JSON array
- `--tx`: Wrap all statements in a transaction
- `--dry-run`: Preview generated SQL without executing
- `--ssh <USER@HOST>`: SSH tunnel via bastion host

---

### `qail pull`

Extract schema from a live database:

```bash
qail pull postgres://user:pass@localhost/db > schema.qail
```

---

### `qail diff`

Compare two schemas or detect drift against a live database:

```bash
# Compare two schema files
qail diff old.qail new.qail
qail diff old.qail new.qail --format json

# Live drift detection (introspects running database)
qail diff _ schema.qail --live --url postgres://localhost/mydb
# Drift detection: [live DB] → schema.qail
#   → Introspecting live database...
#     80 tables, 287 indexes introspected
#   ✅ No drift detected — live DB matches schema file.
```

**Options:**
- `--format <FMT>`: Output format (`sql`, `json`, `pretty`)
- `--live`: Use live database introspection as "old" schema
- `--url <URL>`: Database URL (required with `--live`)

---

### `qail check`

Validate a schema file or preview migration safety:

```bash
# Validate schema
qail check schema.qail
# ✓ Schema is valid
#   Tables: 80 | Columns: 1110 | Indexes: 287
#   ✓ 82 primary key(s)

# Check migration safety
qail check old.qail:new.qail
# ✓ Both schemas are valid
# Migration preview: 4 operation(s)
#   ✓ 3 safe operation(s)
#   ⚠️  1 reversible operation(s)
```

---

## Migrate Commands

### `qail migrate status`

View migration history with rich tabular output:

```bash
qail migrate status postgres://...
# 📋 Migration Status — mydb
# ┌──────────┬────────────────────┬─────────────────────┬──────────────┐
# │ Version  │ Name               │ Applied At          │ Checksum     │
# ├──────────┼────────────────────┼─────────────────────┼──────────────┤
# │ 001      │ qail_queue         │ 2026-02-01 10:00:00 │ a3b8d1...    │
# │ 002      │ add_users          │ 2026-02-05 14:32:00 │ f81d4f...    │
# └──────────┴────────────────────┴─────────────────────┴──────────────┘
```

### `qail migrate up`

Apply migrations:

```bash
qail migrate up v1.qail:v2.qail postgres://...

# With codebase check (warns about breaking references)
qail migrate up v1.qail:v2.qail postgres://... -c ./src
```

### `qail migrate down`

Rollback migrations:

```bash
qail migrate down v1.qail:v2.qail postgres://...
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

# Save to file
qail migrate plan old.qail:new.qail --output migration.sql
```

### `qail migrate analyze`

Analyze codebase for breaking changes before migrating:

```bash
qail migrate analyze old.qail:new.qail --codebase ./src
# 🔍 Migration Impact Analyzer
# Scanning codebase... Found 395 query references
#
# ⚠️  BREAKING CHANGES DETECTED
# ┌─ DROP TABLE promotions (6 references) ─────────────┐
# │ ❌ src/repository/promotion.rs:89 → INSERT INTO...
# │ ❌ src/repository/promotion.rs:264 → SELECT...
# └────────────────────────────────────────────────────┘
```

### `qail migrate apply`

Apply file-based migrations from `migrations/` directory:

```bash
qail migrate apply
# → Found 1 migrations to apply
# ✓ Connected to mydb
#   → 001_qail_queue.up.qail... ✓
# ✓ All migrations applied successfully!
```

### `qail migrate reset`

Nuclear option — drop all objects, clear history, re-apply target schema:

```bash
qail migrate reset schema.qail postgres://...
# ⚠️  This will DROP all tables, clear migration history, and recreate from schema.
# Phase 1: DROP all tables...
# Phase 2: CLEAR migration history...
# Phase 3: CREATE from schema...
# ✓ Reset complete
```

### `qail migrate shadow`

Test migrations against a shadow database:

```bash
qail migrate shadow v1.qail:v2.qail postgres://shadow-db/...
```

---

## Other Commands

### `qail explain`

Parse and explain a QAIL query:

```bash
qail "get users'id'email[active = true]"
# SELECT id, email FROM users WHERE active = true
```

### `qail repl`

Interactive QAIL REPL — type queries, see SQL in real-time:

```bash
qail repl
# 🪝 QAIL REPL v0.15.8
# Type QAIL queries, see SQL output.
# qail> get users[active = true]
# → SELECT * FROM users WHERE active = true
```

### `qail types`

Generate typed Rust schema from `.qail` file:

```bash
qail types schema.qail > src/generated/schema.rs
```

### `qail watch`

Watch schema file for changes and auto-generate migrations:

```bash
qail watch schema.qail --url postgres://... --auto-apply
```

### `qail lint`

Check schema for best practices:

```bash
qail lint schema.qail
# 🔍 Schema Linter
# ⚠ 144 warning(s)
# ℹ 266 info(s)
```

| Check | Level | Description |
|-------|-------|-------------|
| Missing primary key | 🔴 ERROR | Every table needs a PK |
| Missing created_at/updated_at | ⚠️ WARNING | Audit trail columns |
| `_id` column without `references()` | ⚠️ WARNING | FK integrity |
| Uppercase table names | ⚠️ WARNING | Use snake_case |

### `qail sync generate`

Generate trigger migrations from `[[sync]]` rules in `qail.toml` (Hybrid mode):

```bash
qail sync generate
# ✓ Created migrations/002_qail_sync_triggers.up.qail
```

### `qail worker`

Start the background worker to sync PostgreSQL → Qdrant (Hybrid mode):

```bash
qail worker --interval 1000 --batch 100
```

---

## Global Options

| Flag | Description |
|------|-------------|
| `-d, --dialect` | Target SQL dialect (`pg`, `mysql`) |
| `-f, --format` | Output format (`sql`, `ast`, `json`) |
| `-v, --verbose` | Verbose output |
| `--version` | Show version |
| `--help` | Show help |
