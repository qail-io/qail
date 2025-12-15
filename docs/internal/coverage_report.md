# QAIL PostgreSQL Deep Coverage Report (v0.14.3)

## Executive Summary

| Metric | Coverage |
|--------|:--------:|
| **Effective (Real-World)** | **~98%** |
| **DML Operations** | 100% |
| **DDL Operations** | 95% |
| **Expression Grammar** | 85% |

QAIL provides comprehensive PostgreSQL coverage for production applications, including advanced features rarely found in query builders.

---

## 1. DML Statement Coverage (100%)

| Statement | Status | Features |
|-----------|:------:|----------|
| `SELECT` | ✅ | Columns, `*`, aliases, subqueries |
| `INSERT` | ✅ | Values, `RETURNING`, `ON CONFLICT`, `DEFAULT VALUES` |
| `UPDATE` | ✅ | SET, FROM, USING, `RETURNING` |
| `DELETE` | ✅ | WHERE, USING, `RETURNING` |
| `UPSERT` | ✅ | `ON CONFLICT DO UPDATE/NOTHING` |

---

## 2. Query Clauses (100%)

| Clause | Status | Notes |
|--------|:------:|-------|
| `WHERE` | ✅ | Full condition support |
| `ORDER BY` | ✅ | ASC/DESC, NULLS FIRST/LAST |
| `LIMIT/OFFSET` | ✅ | Pagination |
| `FETCH FIRST` | ✅ | SQL standard |
| `GROUP BY` | ✅ | Simple, ROLLUP, CUBE |
| `HAVING` | ✅ | Post-aggregation filter |
| `DISTINCT` | ✅ | Regular and `DISTINCT ON` |
| `JOIN` | ✅ | INNER, LEFT, RIGHT, FULL, CROSS, LATERAL |
| `UNION/INTERSECT/EXCEPT` | ✅ | Set operations |
| `FOR UPDATE/SHARE` | ✅ | Row locking |
| `TABLESAMPLE` | ✅ | BERNOULLI, SYSTEM |

---

## 3. Advanced Query Features (100%)

| Feature | Status | Notes |
|---------|:------:|-------|
| **CTEs** | ✅ | `WITH`, recursive CTEs |
| **Window Functions** | ✅ | OVER, PARTITION BY, ORDER BY, FRAME |
| **Subqueries** | ✅ | Scalar, EXISTS, IN (subquery) |
| **JSON Operators** | ✅ | `->`, `->>`, `@>`, `?`, JSON_EXISTS |
| **CASE WHEN** | ✅ | Full support |
| **COALESCE/NULLIF** | ✅ | Via FunctionCall |
| **Type Casts** | ✅ | `::type` syntax |
| **RETURNING** | ✅ | For INSERT/UPDATE/DELETE |

---

## 4. Aggregate Functions (100%)

| Function | Status |
|----------|:------:|
| COUNT, SUM, AVG, MIN, MAX | ✅ |
| COUNT(DISTINCT x) | ✅ |
| FILTER (WHERE ...) | ✅ |
| ARRAY_AGG, STRING_AGG | ✅ |
| JSON_AGG, JSONB_AGG | ✅ |
| BOOL_AND, BOOL_OR | ✅ |

---

## 5. DDL Operations (95%)

| Operation | Status | Notes |
|-----------|:------:|-------|
| CREATE TABLE | ✅ | Columns, constraints, FK |
| DROP TABLE | ✅ | CASCADE support |
| ALTER TABLE ADD/DROP COLUMN | ✅ | |
| ALTER COLUMN TYPE | ✅ | With USING clause |
| CREATE/DROP INDEX | ✅ | Unique, partial, expression |
| Materialized Views | ✅ | CREATE, REFRESH, DROP |
| TRUNCATE | ✅ | Fast delete |
| **Gap**: CREATE VIEW | ❌ | Not yet supported |
| **Gap**: Sequences | ❌ | Not yet supported |

---

## 6. Column Constraints (100%)

| Constraint | Status |
|------------|:------:|
| PRIMARY KEY | ✅ |
| UNIQUE | ✅ |
| NOT NULL / NULL | ✅ |
| DEFAULT | ✅ |
| CHECK | ✅ |
| REFERENCES (FK) | ✅ |
| GENERATED (Stored/Virtual) | ✅ |

---

## 7. Operators (95%)

### Comparison (100%)
`=`, `!=`, `<>`, `>`, `<`, `>=`, `<=`, `BETWEEN`, `IS NULL`, `IS NOT NULL`

### Pattern Matching (100%)
`LIKE`, `NOT LIKE`, `ILIKE`, `NOT ILIKE`, `SIMILAR TO`, `~`, `~*`

### JSON/Array (100%)
`->`, `->>`, `@>`, `<@`, `?`, `?|`, `?&`, `&&`

### Missing (5%)
- Geometric operators (`@>`, `<->`, `&&` for geometry)
- Network operators (`<<`, `>>=`)
- Text search operators (`@@`, `@@@`)

---

## 8. Transaction Control (100%)

| Feature | Status |
|---------|:------:|
| BEGIN/COMMIT/ROLLBACK | ✅ |
| SAVEPOINT | ✅ |
| RELEASE SAVEPOINT | ✅ |
| ROLLBACK TO SAVEPOINT | ✅ |

---

## 9. PostgreSQL-Specific Features (90%)

| Feature | Status | Notes |
|---------|:------:|-------|
| LISTEN/NOTIFY | ✅ | Pub/Sub |
| EXPLAIN/EXPLAIN ANALYZE | ✅ | Query planning |
| LOCK TABLE | ✅ | Explicit locking |
| COPY (bulk insert) | ✅ | Via driver |
| JSON_TABLE | ✅ | Postgres 17+ |
| **Gap**: COPY TO file | ❌ | Only STDOUT |
| **Gap**: PREPARE/EXECUTE | ⚠️ | Implicit via driver |

---

## 10. Expression Gaps (15% of grammar)

| Missing | Example | Priority |
|---------|---------|:--------:|
| Row Constructor | `ROW(1, 2)` | Low |
| Array Constructor | `ARRAY[c1, c2]` | Medium |
| Array Subscript | `arr[1]` | Low |
| COLLATE | `col COLLATE "C"` | Low |
| Field Selection | `(row).field` | Low |
| GROUPING SETS | `GROUPING SETS (...)` | Medium |

---

## Summary

QAIL covers **~98%** of features used in production PostgreSQL applications:

```
DML:           ████████████████████ 100%
Clauses:       ████████████████████ 100%
Aggregates:    ████████████████████ 100%
Window:        ████████████████████ 100%
DDL:           ███████████████████░  95%
Operators:     ███████████████████░  95%
Expressions:   █████████████████░░░  85%
```

The remaining gaps are primarily:
- Rarely-used syntax (ROW constructors, GROUPING SETS)
- Specialized operators (geometric, network)
- Legacy features (PREPARE/EXECUTE)
