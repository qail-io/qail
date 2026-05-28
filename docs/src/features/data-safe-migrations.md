# Data-Safe Migrations

QAIL provides enterprise-grade migration safety features that protect your data during schema changes.

## Overview

| Feature | Description |
|---------|-------------|
| **Impact Analysis** | Shows exactly what data will be affected |
| **Pre-Migration Backup** | Option to backup before destructive changes |
| **Record-Level Backup** | JSONB-based data backup in database |
| **Shadow Database** | Blue-green migrations for zero-downtime |

## Phase 1: Impact Analysis & Backup Prompt

When running migrations with destructive operations, QAIL analyzes the impact:

```
$ qail migrate apply --phase contract --codebase ./src --url postgres://...

🚨 Migration Impact Analysis
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  DROP COLUMN users.email → 1,234 values at risk
  DROP TABLE  sessions    → 5,678 rows affected
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Total: 6,912 records at risk

Choose an option:
  [1] Proceed (I have my own backup)
  [2] Backup to files (_qail_snapshots/)
  [3] Backup to database (with rollback support)
  [4] Cancel migration
```

### Options Explained

- **[1] Proceed** - Continue without QAIL backup (you manage your own)
- **[2] File Backup** - Export affected data to `_qail_snapshots/` directory
- **[3] Database Backup** - Store data in `_qail_data_snapshots` table (enables true rollback)
- **[4] Cancel** - Abort the migration

## Phase 2: Record-Level Database Backup

When you choose option `[3]`, QAIL creates a snapshot table:

```sql
-- Automatically created
CREATE TABLE _qail_data_snapshots (
    id SERIAL PRIMARY KEY,
    migration_version VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255),
    row_id TEXT NOT NULL,
    value_json JSONB NOT NULL,
    snapshot_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### What Gets Backed Up

| Operation | Backup Content |
|-----------|---------------|
| DROP COLUMN | Column values with row IDs |
| DROP TABLE | Full table as JSONB objects |
| ALTER TYPE | Original values before cast |

### True Data Rollback

After migration, you can restore data:

```bash
# Schema rollback (uses applied migration history)
qail migrate rollback --to 20260527090000_add_user_name.expand.up.qail --url postgres://...

# Data rollback (restores values)
# Coming in future release: qail rollback --data
```

## Phase 3: Shadow Database (Blue-Green)

For zero-downtime migrations, use shadow database mode:

```bash
# Step 1: Create shadow, apply migrations, sync data
qail migrate shadow old.qail:new.qail postgres://...

🔄 Shadow Migration Mode
━━━━━━━━━━━━━━━━━━━━━━━━━━
  [1/4] Creating shadow database: mydb_shadow ✓
  [2/4] Applying migration to shadow... ✓
  [3/4] Syncing data from primary to shadow...
    ✓ users (1,234 rows)
    ✓ orders (5,678 rows)
    ✓ Synced 2 tables, 6,912 rows
  [4/4] Shadow ready for validation

  Shadow URL: postgres://...mydb_shadow

  Available Commands:
    qail migrate promote → Switch traffic to shadow
    qail migrate abort   → Drop shadow, keep primary
```

### Shadow Workflow

1. **Create Shadow** - New database with new schema
2. **Apply Migrations** - Run DDL on shadow only
3. **Sync Data** - Copy data from primary
4. **Validate** - Test your application against shadow
5. **Promote or Abort** - Make the decision

### Promote (Go Live)

```bash
$ qail migrate promote postgres://...

🚀 Promoting Shadow to Primary
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  [1/3] Renaming mydb → mydb_old_20241226 ✓
  [2/3] Renaming mydb_shadow → mydb ✓
  [3/3] Keeping old database as backup

✓ Shadow promoted successfully!
  Old database preserved as: mydb_old_20241226
  To clean up: DROP DATABASE mydb_old_20241226
```

### Abort (Rollback)

```bash
$ qail migrate abort postgres://...

🛑 Aborting Shadow Migration
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Dropping shadow database: mydb_shadow

✓ Shadow database dropped. Primary unchanged.
```

## Comparison with Other Tools

| Feature | QAIL | Prisma | SeaORM | Liquibase |
|---------|------|--------|--------|-----------|
| Schema Migrations | ✅ | ✅ | ✅ | ✅ |
| Impact Analysis | ✅ | ❌ | ❌ | ❌ |
| Pre-Migration Backup | ✅ | ❌ | ❌ | ❌ |
| Record-Level Backup | ✅ | ❌ | ❌ | ❌ |
| Shadow Database | ✅ | ❌ | ❌ | ❌ |
| True Data Rollback | ✅ | ❌ | ❌ | ❌ |

## Best Practices

1. **Always use database backup** for production migrations
2. **Test in shadow** before promoting
3. **Keep old database** for 24-48 hours after promotion
4. **Use transactions** (QAIL does this automatically)

## Configuration

No configuration required! Features are enabled by default when destructive operations are detected.
