# Migrations

QAIL supports two migration workflows:
1. **Schema-Diff (State-Based):** Compare standard schema files (good for evolving production DBs)
2. **File-Based (Sequential):** Apply `.qail` files from `migrations/` directory (good for hybrid setups)

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
qail migrate up v1.qail:v2.qail postgres://...
```

### 5. Rollback (if needed)

```bash
qail migrate down v1.qail:v2.qail postgres://...
```

---

## 2. File-Based Workflow (Sequential)

For hybrid projects or simple setups, you can use sequential `.qail` files in the `migrations/` directory.

### Structure

```text
migrations/
  ├── 001_initial_schema.up.qail
  ├── 001_initial_schema.down.qail
  ├── 002_add_users.up.qail
  └── 002_add_users.down.qail
```

### Applying Migrations

```bash
# Applies all pending .up.qail files
qail migrate apply
```

### Generating from Sync Rules

Hybrid projects can auto-generate migrations for sync triggers:

```bash
qail sync generate
# Creates: migrations/00X_qail_sync_triggers.up.qail
```
## Migration Hints

| Hint | Description |
|------|-------------|
| `rename table.old -> table.new` | Rename column (not drop+add) |
| `transform expr -> table.col` | Data transformation hint |
| `drop confirm table.col` | Explicit drop confirmation |
