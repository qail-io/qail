# Migration Multi-File Samples

Working test commands for `qail migrate` validated against a local PostgreSQL database.

## Schema Files

### `v1.qail` — Initial State

```qail
# 4 tables, 2 indexes, FK relationships

table _test_operators {
  id uuid primary_key
  name text not_null
  email text not_null unique
  created_at timestamptz not_null default NOW()
}

table _test_vessels {
  id uuid primary_key
  operator_id uuid not_null references _test_operators(id)
  vessel_name text not_null
  vessel_type text not_null
  capacity i32 not_null default 0
  is_active bool not_null default true
  old_field text nullable
  created_at timestamptz not_null default NOW()
}

table _test_routes {
  id uuid primary_key
  operator_id uuid not_null references _test_operators(id)
  origin text not_null
  destination text not_null
  distance_km decimal nullable
  created_at timestamptz not_null default NOW()
}

table _test_to_drop {
  id serial primary_key
  data text nullable
}

index idx_test_vessels_operator on _test_vessels (operator_id)
index idx_test_routes_operator on _test_routes (operator_id)
```

### `v2.qail` — Target State

```qail
# Changes from v1:
#   1. _test_vessels: rename old_field -> legacy_notes
#   2. _test_vessels: add description (TEXT, nullable)
#   3. _test_vessels: change capacity from i32 to i64
#   4. _test_routes: drop distance_km column
#   5. _test_to_drop: dropped entirely
#   6. _test_schedules: new table (with FK to vessels and routes)
#   7. New indexes on _test_schedules
#   8. Drop idx_test_routes_operator index

table _test_operators {
  id uuid primary_key
  name text not_null
  email text not_null unique
  created_at timestamptz not_null default NOW()
}

table _test_vessels {
  id uuid primary_key
  operator_id uuid not_null references _test_operators(id)
  vessel_name text not_null
  vessel_type text not_null
  capacity i64 not_null default 0
  is_active bool not_null default true
  legacy_notes text nullable
  description text nullable
  created_at timestamptz not_null default NOW()
}

table _test_routes {
  id uuid primary_key
  operator_id uuid not_null references _test_operators(id)
  origin text not_null
  destination text not_null
  created_at timestamptz not_null default NOW()
}

table _test_schedules {
  id uuid primary_key
  vessel_id uuid not_null references _test_vessels(id)
  route_id uuid not_null references _test_routes(id)
  departure_at timestamptz not_null
  arrival_at timestamptz not_null
  price_cents i64 not_null default 0
  status text not_null default 'draft'
  created_at timestamptz not_null default NOW()
}

index idx_test_vessels_operator on _test_vessels (operator_id)
index idx_test_schedules_vessel on _test_schedules (vessel_id)
index idx_test_schedules_route on _test_schedules (route_id)

rename _test_vessels.old_field -> _test_vessels.legacy_notes
drop _test_routes.distance_km confirm
drop _test_to_drop confirm
```

### `seed.qail` — Test Data

```qail
# Operators
add _test_operators fields id, name, email values 'a0000000-0000-0000-0000-000000000001', 'ExampleApp Demo', 'demo@example.com'
add _test_operators fields id, name, email values 'a0000000-0000-0000-0000-000000000002', 'Qail Test', 'test@qail.io'

# Vessels
add _test_vessels fields id, operator_id, vessel_name, vessel_type, capacity, old_field values 'b0000000-0000-0000-0000-000000000001', 'a0000000-0000-0000-0000-000000000001', 'KM Tuna Express', 'ferry', 200, 'some old note'
add _test_vessels fields id, operator_id, vessel_name, vessel_type, capacity values 'b0000000-0000-0000-0000-000000000002', 'a0000000-0000-0000-0000-000000000002', 'MV Bali Star', 'speedboat', 50

# Routes (with distance_km that will be dropped)
add _test_routes fields id, operator_id, origin, destination, distance_km values 'c0000000-0000-0000-0000-000000000001', 'a0000000-0000-0000-0000-000000000001', 'Padang Bai', 'Lembar', 45
add _test_routes fields id, operator_id, origin, destination, distance_km values 'c0000000-0000-0000-0000-000000000002', 'a0000000-0000-0000-0000-000000000002', 'Merak', 'Bakauheni', 28

# To-drop table (columns: id serial, data text)
add _test_to_drop fields data values 'this row will be dropped'
add _test_to_drop fields data values 'second row also dropped'
```

---

## Test Commands

> Replace `DB_URL` with your connection string, e.g. `postgresql://user@localhost:5432/example-db`

### 1. Create empty.qail

```bash
touch empty.qail
```

### 2. Bootstrap v1 (CREATE 4 tables + 2 indexes)

```bash
qail migrate up empty.qail:v1.qail "$DB_URL"
# → 6 migrations applied (FK-ordered: operators → to_drop → vessels → routes → indexes)
```

### 3. Seed data

```bash
qail exec -f seed.qail --url "$DB_URL" --tx
# → 8 INSERTs in a single transaction
```

### 4. Plan v1 → v2 (dry-run)

```bash
qail migrate plan v1.qail:v2.qail
# → 9 operations: RENAME, DROP COLUMN, DROP TABLE, CREATE TABLE, ADD COLUMN,
#   ALTER TYPE, CREATE INDEX ×2, DROP INDEX
```

### 5. Migrate UP v1 → v2 (with real data)

```bash
qail migrate up v1.qail:v2.qail "$DB_URL"
# Impact analysis: DROP COLUMN distance_km → 2 values, DROP TABLE → 2 rows
# Choose [1] to proceed
# → 9 migrations applied atomically
```

### 6. Verify new table (INSERT with FKs)

```bash
qail exec -f verify_insert.qail --url "$DB_URL"
```

Where `verify_insert.qail`:
```qail
add _test_schedules fields id, vessel_id, route_id, departure_at, arrival_at, price_cents, status values "e0000000-0000-0000-0000-000000000001", "b0000000-0000-0000-0000-000000000001", "c0000000-0000-0000-0000-000000000001", "2026-03-01T08:00:00+08:00", "2026-03-01T12:00:00+08:00", 150000, "confirmed"
```

### 7. Verify dropped table

```bash
qail exec 'get _test_to_drop' --url "$DB_URL"
# → Error: relation "_test_to_drop" does not exist ← confirms DROP TABLE worked
```

### 8. Check migration status

```bash
qail migrate status "$DB_URL"
# → Migration history table is ready (2 records)
```

### 9. Rollback v2 → v1

```bash
qail migrate down v2.qail:v1.qail "$DB_URL"
# ⚠️ Warns about unsafe type narrowing: capacity BIGINT → INT
# Choose [y] to proceed
# → 10 rollbacks applied atomically
```

### 10. Full teardown v1 → empty (FK-ordered drops)

```bash
qail migrate down v1.qail:empty.qail "$DB_URL"
# → Drops children first (vessels, routes) then parent (operators)
# → 6 rollbacks applied atomically
```

### 11. Cleanup migration history

```bash
qail exec 'del _qail_migrations' --url "$DB_URL"
```

---

## DDL Operations Covered

| Operation | Syntax | Status |
|-----------|--------|--------|
| CREATE TABLE | `table name { ... }` | ✅ |
| DROP TABLE | `drop table_name confirm` | ✅ |
| ADD COLUMN | auto-detected from diff | ✅ |
| DROP COLUMN | `drop table.column confirm` | ✅ |
| RENAME COLUMN | `rename table.old -> table.new` | ✅ |
| ALTER TYPE | auto-detected from diff | ✅ |
| CREATE INDEX | `index name on table (cols)` | ✅ |
| DROP INDEX | auto-detected from diff | ✅ |
| FK constraints | `references table(col)` | ✅ |
| Atomic transactions | automatic BEGIN/COMMIT/ROLLBACK | ✅ |
| Impact analysis | automatic row count for destructive ops | ✅ |
| Type narrowing warnings | automatic on rollback | ✅ |
| FK-ordered CREATE | parents before children | ✅ |
| FK-ordered DROP | children before parents | ✅ |
