# REST API Reference

The gateway auto-discovers all tables and exposes them as REST endpoints under `/api/`.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/{table}` | List with filters, sort, pagination |
| `GET` | `/api/{table}/:id` | Get by primary key |
| `GET` | `/api/{table}/_explain` | EXPLAIN ANALYZE |
| `GET` | `/api/{table}/_aggregate` | Aggregations |
| `GET` | `/api/{table}/:id/{child}` | Nested resources (FK-based) |
| `POST` | `/api/{table}` | Create (single or batch) |
| `PATCH` | `/api/{table}/:id` | Partial update |
| `DELETE` | `/api/{table}/:id` | Delete by primary key |

---

## Filtering

All filter operators are supported as query parameters:

```
GET /api/orders?status=paid                              # Exact match
GET /api/orders?total=gt.100                             # Greater than
GET /api/orders?total=gte.50&total=lte.200               # Range
GET /api/orders?status=in.(paid,shipped)                 # IN list
GET /api/orders?name=like.*ferry*                        # Pattern match
GET /api/orders?name=ilike.*FERRY*                       # Case-insensitive
GET /api/orders?notes=is_null                            # NULL check
GET /api/orders?status=ne.cancelled                      # Not equal
GET /api/orders?tags=contains.premium                    # Array contains
```

### Operator Reference

| Operator | SQL | Example |
|----------|-----|---------|
| `eq` (default) | `=` | `?status=paid` |
| `ne` | `!=` | `?status=ne.cancelled` |
| `gt` / `gte` | `>` / `>=` | `?total=gte.100` |
| `lt` / `lte` | `<` / `<=` | `?age=lt.30` |
| `in` | `IN (...)` | `?status=in.(active,pending)` |
| `like` | `LIKE` | `?email=like.*@gmail*` |
| `ilike` | `ILIKE` | `?name=ilike.*john*` |
| `is_null` | `IS NULL` | `?deleted_at=is_null` |
| `contains` | `@>` | `?tags=contains.premium` |

---

## Full-Text Search

```
GET /api/products?search=ferry+bali                      # Search all text columns
GET /api/products?search=ferry&search_columns=name,desc  # Search specific columns
```

Uses PostgreSQL's `to_tsvector` / `websearch_to_tsquery` — supports natural language queries.

---

## Sorting

```
GET /api/orders?sort=created_at                          # Ascending (default)
GET /api/orders?sort=-total                              # Descending (prefix -)
GET /api/orders?sort=-status,created_at                  # Multi-column
```

---

## Pagination

```
GET /api/orders?limit=25&offset=50                       # Offset-based
GET /api/orders?limit=25&cursor=eyJpZCI6...              # Cursor-based
```

---

## Column Selection & Distinct

```
GET /api/orders?select=id,status,total                   # Return specific columns
GET /api/orders?distinct=status                           # Distinct values
```

---

## Relationships & Expansion

### FK-Based JOIN Expansion

Automatically resolves foreign key relationships with `?expand=`:

```
GET /api/orders?expand=users                              # Inline user object
GET /api/orders?expand=users,operators                    # Multiple relations
GET /api/orders?expand=nested:users                       # Nested FK expansion
```

Response includes the full related object inline — **no N+1 queries**. The gateway performs a server-side JOIN.

### Nested Resource Routes

Access child resources through parent:

```
GET /api/operators/:id/orders                             # All orders for operator
GET /api/users/:id/bookings?status=confirmed              # Filtered child resources
```

---

## Mutations

### Create

```bash
# Single insert
curl -X POST /api/orders -d '{"user_id": "...", "total": 100}'

# Batch insert
curl -X POST /api/orders -d '[{"total": 100}, {"total": 200}]'
```

### Update

```bash
curl -X PATCH /api/orders/:id -d '{"status": "shipped"}'
```

### Upsert

```bash
curl -X POST /api/orders -H "X-Upsert: true" \
  -H "X-On-Conflict: order_number" \
  -d '{"order_number": "ORD-001", "total": 150}'
```

### Delete

```bash
curl -X DELETE /api/orders/:id
```

### Returning Clause

All mutations support `?returning=id,status` to get back specific columns after the operation.

---

## Aggregations

```
GET /api/orders/_aggregate?fn=count                       # COUNT(*)
GET /api/orders/_aggregate?fn=sum&column=total            # SUM(total)
GET /api/orders/_aggregate?fn=avg&column=total            # AVG
GET /api/orders/_aggregate?fn=min&column=created_at       # MIN
GET /api/orders/_aggregate?fn=max&column=total            # MAX
GET /api/orders/_aggregate?fn=count&status=paid           # Filtered aggregation
```

Supported functions: `count`, `sum`, `avg`, `min`, `max`.
