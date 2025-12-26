# Named Migrations

Named migrations provide version-controlled migration files with metadata for better tracking.

## Creating a Named Migration

```bash
qail migrate create add_user_avatars --depends 002_add_users --author orion
```

Output:
```
📝 Creating Named Migration

  ✓ Created: migrations/20251226071129_add_user_avatars.qail

  Migration: 20251226071129_add_user_avatars
  Depends:   002_add_users
  Author:    orion
```

## Migration File Format

```sql
-- migration: 20251226071129_add_user_avatars
-- depends: 002_add_users
-- author: orion
-- created: 2025-12-26T07:11:29+08:00

+table avatars {
  id UUID primary_key
  user_id UUID not_null references(users.id)
  url TEXT not_null
}
```

## Metadata Fields

| Field | Description |
|-------|-------------|
| `migration` | Unique name (timestamp_description) |
| `depends` | Comma-separated list of dependencies |
| `author` | Author of the migration |
| `created` | ISO 8601 timestamp |

## CLI Options

```bash
qail migrate create <name>
  -d, --depends <migration>  # Dependencies (comma-separated)
  -a, --author <name>        # Author attribution
```

## Dependency Resolution

QAIL validates dependencies before applying migrations:
- Checks all dependencies exist
- Detects circular dependencies
- Applies in topological order
