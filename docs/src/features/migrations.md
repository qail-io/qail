# Migrations

QAIL supports two migration workflows:
1. **Schema-Diff (State-Based):** Compare standard schema files (good for evolving production DBs)
2. **File-Based Phased Migrations:** Apply `.qail` files from `deltas/` through expand, backfill, and contract phases

---

## 1. Schema-Diff Workflow (State-Based)

QAIL uses an **intent-aware** `.qail` schema format that solves the ambiguity problem of state-based migrations.

## The Problem with JSON/State-Based Migrations

```json
// v1: {"users": {"username": "text"}}
// v2: {"users": {"name": "text"}}
```

Did we **rename** `username → name` or **delete + add**? JSON can't express intent.

## The Solution: `.qail` Schema Format

```qail
# schema.qail - Human readable, intent-aware
table users {
  id serial primary_key
  name text not_null
  email text unique
}

# Migration hints express INTENT
rename users.username -> users.name
```

### Single File vs Modular Directory

Qail supports both:

- **Single file**: `schema.qail`
- **Modular**: `schema/*.qail` (recursive), optional `schema/_order.qail`

Modular schema is useful when one file becomes very large. If `_order.qail` exists, listed modules load first; in strict mode, every module must be listed.

Repository examples:

- `examples/schema/single/schema.qail`
- `examples/schema/modular/schema/`

## Workflow

### 1. Pull Current Schema

```bash
qail pull postgres://user:pass@localhost/db > v1.qail
```

### 2. Create New Version

Edit `v2.qail` with your changes and any migration hints:

```qail
table users {
  id serial primary_key
  name text not_null          # was 'username'
  email text unique
  created_at timestamp not_null
}

rename users.username -> users.name
```

### 3. Preview Migration

```bash
qail diff v1.qail v2.qail
# Output:
# ALTER TABLE users RENAME COLUMN username TO name;
# ALTER TABLE users ADD COLUMN created_at TIMESTAMP NOT NULL;
```

### 4. Apply Migration

```bash
qail migrate apply --phase expand
qail migrate apply --phase backfill --backfill-chunk-size 10000
qail migrate apply --phase contract --codebase ./src
```

### 5. Rollback (if needed)

```bash
qail migrate rollback --to 20260527090000_add_user_name.expand.up.qail
# or apply explicit down files
qail migrate apply --direction down
```

---

## 2. File-Based Workflow (Expand / Backfill / Contract)

For hybrid projects or simple setups, use phased `.qail` files in the `deltas/` directory.

### Structure

```text
deltas/
  └── 20260527090000_add_user_name/
      ├── expand.qail
      ├── backfill.qail
      └── contract.qail
```

### Applying Migrations

```bash
# Apply one safety phase at a time
qail migrate apply --phase expand
qail migrate apply --phase backfill --backfill-chunk-size 10000
qail migrate apply --phase contract --codebase ./src
```

### Generating from Sync Rules

Hybrid projects can auto-generate migrations for sync triggers:

```bash
qail sync generate
# Creates phased delta files for qail sync triggers
```
## Migration Hints

| Hint | Description |
|------|-------------|
| `rename table.old -> table.new` | Rename column (not drop+add) |
| `transform expr -> table.col` | Data transformation hint |
| `drop confirm table.col` | Explicit drop confirmation |

---

## 3. Drift Detection

Compare a live database against a `.qail` schema file to find unexpected drift:

```bash
qail diff _ schema.qail --live --url postgres://localhost/mydb
# Drift detection: [live DB] → schema.qail
#   → Introspecting live database...
#     80 tables, 287 indexes introspected
#
#   ✅ No drift detected — live DB matches schema file.
```

If drift exists, it shows categorized changes with risk levels:

```bash
# 🔴 HIGH   — missing column (was dropped outside migrations)
# 🟡 MEDIUM — index mismatch
# 🟢 LOW    — default value difference
```

---

## 4. Migration Reset

Nuclear option for development — drops everything and recreates from schema:

```bash
qail migrate reset schema.qail postgres://...
# Phase 1: DROP all tables (FK-ordered)
# Phase 2: CLEAR migration history
# Phase 3: CREATE from schema
# ✓ Reset complete
```

> ⚠️ **Warning:** This is destructive. Use only in development or staging.

---

## 5. Migration Status

Rich tabular view of migration history:

```bash
qail migrate status postgres://...
# 📋 Migration Status — mydb
# ┌──────────┬────────────────────┬─────────────────────┬──────────────┐
# │ Version  │ Name               │ Applied At          │ Checksum     │
# ├──────────┼────────────────────┼─────────────────────┼──────────────┤
# │ 001      │ qail_queue         │ 2026-02-01 10:00:00 │ a3b8d1...    │
# └──────────┴────────────────────┴─────────────────────┴──────────────┘
```
