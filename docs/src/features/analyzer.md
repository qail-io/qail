# Migration Impact Analyzer

The Migration Impact Analyzer scans your codebase before running migrations to detect breaking changes that could cause runtime errors.

## Why Use It?

Dropping a table or column is easy—but if your code still references it, you'll get runtime errors. The analyzer:

1. **Scans your codebase** for QAIL and raw SQL queries
2. **Detects breaking changes** like dropped tables/columns
3. **Shows exact file locations** where code needs updating
4. **Prevents downtime** by catching issues before they hit production

## Usage

```bash
qail migrate analyze old.qail:new.qail --codebase ./src
```

## Example Output

```
🔍 Migration Impact Analyzer

  Schema: old.qail → new.qail
  Codebase: ./src

Scanning codebase...
  Found 395 query references

⚠️  BREAKING CHANGES DETECTED

Affected files: 1

┌─ DROP TABLE promotions (6 references) ─────────────────────────┐
│ ❌ src/repository/promotion.rs:89 → INSERT INTO promotions (
│ ❌ src/repository/promotion.rs:264 → SELECT COUNT(*) FROM promotions
│ ❌ src/repository/promotion.rs:288 → UPDATE promotions SET
│ ❌ src/repository/promotion.rs:345 → DELETE FROM promotions WHERE
└────────────────────────────────────────────────────────────────┘

What would you like to do?
  1. Run anyway (DANGEROUS - will cause 1 runtime errors)
  2. Dry-run first (show SQL, don't execute)
  3. Let me fix the code first (exit)
```

## Supported Query Patterns

The analyzer detects:

### QAIL Queries
- `get::users` → SELECT from users
- `set::users` → UPDATE users
- `add::users` → INSERT INTO users
- `del::users` → DELETE FROM users

### Raw SQL
- `SELECT ... FROM table`
- `INSERT INTO table`
- `UPDATE table SET`
- `DELETE FROM table`

## Supported File Types

- `.rs` (Rust)
- `.ts` (TypeScript)
- `.js` (JavaScript)  
- `.py` (Python)

## Breaking Change Types

| Change Type | Severity | Description |
|-------------|----------|-------------|
| `DROP TABLE` | 🔴 Critical | Table referenced in code will cause runtime errors |
| `DROP COLUMN` | 🔴 Critical | Column queries will fail |
| `RENAME` | 🟡 Warning | Code needs updating to use new name |
| `TYPE CHANGE` | 🟡 Warning | May cause type mismatch errors |

## Best Practices

1. **Always run before `migrate up`**
   ```bash
   qail migrate analyze old.qail:new.qail --codebase ./src
   qail migrate up old.qail:new.qail $DATABASE_URL
   ```

2. **Add to CI/CD pipeline**
   ```yaml
   - name: Check migration safety
     run: qail migrate analyze $OLD:$NEW --codebase ./src
   ```

3. **Use with `migrate plan` for full preview**
   ```bash
   qail migrate plan old.qail:new.qail     # See SQL
   qail migrate analyze old.qail:new.qail  # Check codebase
   qail migrate up old.qail:new.qail $URL  # Apply
   ```
