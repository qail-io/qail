# Migration Impact Analyzer

Prevents runtime errors by scanning your codebase before running migrations.

## Why Use It?

Dropping a table or column is easy—but if your code still references it, you'll get runtime errors. The analyzer:

1. **Scans your codebase** for QAIL AST/text query usage and SQL markers
2. **Detects breaking changes** like dropped tables/columns
3. **Shows exact file:line locations** with code snippets
4. **Prevents downtime** by catching issues before production

## Usage

```bash
qail migrate analyze old.qail:new.qail --codebase ./src
```

## Real-World Example

Testing against a production codebase:

```
🔍 Migration Impact Analyzer

  Schema: 001_initial_schema.up.qail → breaking_change.qail
  Codebase: ~/api.fortunebali.com/src

Scanning codebase...
🔍 Analyzing files...
   ├── 🦀 main.rs (AST: 60 refs)
   └── 1 files analyzed

  Found 60 query references

⚠️  BREAKING CHANGES DETECTED

Affected files: 1

┌─ DROP TABLE admin_otps (11 references) ─────────────────────────┐
│ ❌ main.rs:397 → Qail::del("admin_otps")
│ ❌ main.rs:402 → Qail::add("admin_otps")
│ ❌ main.rs:403 → .columns(["email", "code_hash", "expires_at"])
│ ... and 8 more
└──────────────────────────────────────────────────────────────────┘

┌─ DROP TABLE inquiries (11 references) ─────────────────────────┐
│ ❌ main.rs:238 → Qail::add("inquiries")
│ ❌ main.rs:239 → .columns(["name", "email", ...])
│ ... and 9 more
└──────────────────────────────────────────────────────────────────┘

┌─ DROP COLUMN portfolio.status (2 references) ─────────────────┐
│ ❌ main.rs:179 → uses status in .columns(["id" +8])
│ ⚠️  RAW SQL main.rs:225 → "SELECT id, title, status FROM..."
└──────────────────────────────────────────────────────────────────┘

What would you like to do?
  1. Run anyway (DANGEROUS - will cause 5 runtime errors)
  2. Dry-run first (show SQL, don't execute)
  3. Let me fix the code first (exit)
```

## Semantic Scanning Modes

| Mode | Badge | Detection |
|------|-------|-----------|
| **Rust Semantic** | 🦀 | QAIL semantic scanner for `Qail::get()`, `Qail::add()`, call-flow and loop-aware query diagnostics |
| **Text Literal** | 📘📍🐍 | String-literal extraction with comment masking for SQL/QAIL marker detection in non-Rust files |

The analyzer auto-detects file types and uses the most appropriate scanning method.

`syn` is no longer required on the runtime analyzer path for this semantic detection flow.

## Rollback Safety Analysis

> [!WARNING]
> **Data-Destructive Changes Cannot Be Rolled Back!**

Some migrations are irreversible. The analyzer identifies:

| Change | Rollback Safe? | Why |
|--------|---------------|-----|
| `ADD COLUMN` | ✅ Yes | Can `DROP COLUMN` |
| `DROP COLUMN` | ❌ **No** | **Data lost permanently** |
| `DROP TABLE` | ❌ **No** | **Data lost permanently** |
| `RENAME` | ✅ Yes | Can rename back |
| `ADD INDEX` | ✅ Yes | Can drop index |
| `TRUNCATE` | ❌ **No** | **Data lost permanently** |

## Breaking Change Types

| Change Type | Severity | Description |
|-------------|----------|-------------|
| `DROP TABLE` | 🔴 Critical | Table referenced in code → runtime errors |
| `DROP COLUMN` | 🔴 Critical | Column queries will fail |
| `RENAME TABLE` | 🟡 Warning | Code needs updating |
| `RENAME COLUMN` | 🟡 Warning | Code needs updating |
| `TYPE CHANGE` | 🟡 Warning | May cause type mismatch |

## CI/CD Integration

For GitHub Actions, use `--ci` flag for annotations:

```yaml
- name: Check migration safety
  run: qail migrate analyze $OLD:$NEW --codebase ./src --ci
```

This outputs GitHub Actions annotations that appear inline in PR diffs:

```
::error file=src/main.rs,line=225,title=Breaking Change::Column 'portfolio.status' is being dropped but referenced here
```

## Best Practices

1. **Always run before `migrate up`**
   ```bash
   qail migrate analyze old.qail:new.qail --codebase ./src
   qail migrate up old.qail:new.qail $DATABASE_URL
   ```

2. **Use with `migrate plan` for full preview**
   ```bash
   qail migrate plan old.qail:new.qail     # See SQL
   qail migrate analyze old.qail:new.qail  # Check codebase
   qail migrate up old.qail:new.qail $URL  # Apply
   ```

3. **Handle irreversible changes carefully**
   - Backup data before `DROP TABLE` or `DROP COLUMN`
   - Consider soft-delete (add `deleted_at` column) instead of hard delete
