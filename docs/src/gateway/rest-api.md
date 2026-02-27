# REST API Reference

The gateway auto-discovers all tables and exposes them as REST endpoints under `/api/`.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/{table}` | List with filters, sort, pagination |
| `GET` | `/api/{table}/:id` | Get by primary key |
| `GET` | `/api/{table}/_explain` | EXPLAIN ANALYZE |
| `GET` | `/api/{table}/aggregate` | Aggregations |
| `GET` | `/api/{table}/_aggregate` | Aggregations (compat alias) |
| `GET` | `/api/{table}/:id/{child}` | Nested resources (FK-based) |
| `POST` | `/api/rpc/{function}` | Function RPC with JSON args |
| `POST` | `/api/{table}` | Create (single or batch) |
| `PATCH` | `/api/{table}/:id` | Partial update |
| `DELETE` | `/api/{table}/:id` | Delete by primary key |

---

## Filtering

All filter operators are supported as query parameters.
Both key-style (`column.op=value`) and value-style (`column=op.value`) are accepted:

```
GET /api/orders?status=paid                              # Exact match
GET /api/orders?status.eq=paid                           # Exact match (key-style)
GET /api/orders?total=gt.100                             # Greater than
GET /api/orders?total.gte=50&total.lte=200               # Range (key-style)
GET /api/orders?total=gte.50&total=lte.200               # Range
GET /api/orders?status=in.(paid,shipped)                 # IN list
GET /api/orders?status.in=paid,shipped                   # IN list (key-style)
GET /api/orders?name=like.*ferry*                        # Pattern match
GET /api/orders?name=ilike.*FERRY*                       # Case-insensitive
GET /api/orders?notes=is_null                            # NULL check
GET /api/orders?notes.is_null=true                       # NULL check (key-style)
GET /api/orders?status=ne.cancelled                      # Not equal
GET /api/orders?status.ne=cancelled                      # Not equal (key-style)
GET /api/orders?tags=contains.premium                    # Array contains
```

### Operator Reference

| Operator | SQL | Example |
|----------|-----|---------|
| `eq` (default) | `=` | `?status=paid` |
| `ne` | `!=` | `?status=ne.cancelled` or `?status.ne=cancelled` |
| `gt` / `gte` | `>` / `>=` | `?total=gte.100` |
| `lt` / `lte` | `<` / `<=` | `?age=lt.30` |
| `in` | `IN (...)` | `?status=in.(active,pending)` or `?status.in=active,pending` |
| `like` | `LIKE` | `?email=like.*@gmail*` |
| `ilike` | `ILIKE` | `?name=ilike.*john*` |
| `is_null` | `IS NULL` | `?deleted_at=is_null` or `?deleted_at.is_null=true` |
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
GET /api/orders?sort=total:desc,created_at:asc           # Multi-column (explicit)
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

`expand=` is for forward (many-to-one / one-to-one) relations. For reverse one-to-many expansion, use `nested:` to avoid parent-row duplication.

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
GET /api/orders/aggregate?fn=count                        # COUNT(*)
GET /api/orders/aggregate?fn=sum&column=total             # SUM(total)
GET /api/orders/aggregate?fn=avg&column=total             # AVG
GET /api/orders/aggregate?fn=min&column=created_at        # MIN
GET /api/orders/aggregate?fn=max&column=total             # MAX
GET /api/orders/aggregate?fn=count&status=paid            # Filtered aggregation
GET /api/orders/_aggregate?fn=count                        # Alias (compat)
```

Supported functions: `count`, `sum`, `avg`, `min`, `max`.

---

## RPC Functions

Call PostgreSQL functions directly:

```bash
# Named arguments (object body)
curl -X POST /api/rpc/search_orders \
  -d '{"tenant_id":"acme","limit":25}'

# Positional arguments (array body)
curl -X POST /api/rpc/rebuild_index \
  -d '["orders", true]'
```

Accepted body formats:
- JSON object: named args (`arg => value`)
- JSON array: positional args
- scalar/null: single positional arg
- empty body: no args
