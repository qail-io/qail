# Qail Gateway

Qail Gateway is an **auto-REST API server** that turns your PostgreSQL database into a full-featured API — with zero backend code. Point it at a database, get instant CRUD, real-time subscriptions, and enterprise security.

**90% Hasura feature parity** — without GraphQL complexity. Binary AST protocol instead of string-based queries.

## Quick Start

```bash
# Set your database URL and run
DATABASE_URL=postgres://user:pass@localhost:5432/mydb cargo run --release
```

The gateway auto-discovers all tables and exposes them as REST endpoints:

```
GET    /api/{table}              # List (with filters, sort, pagination)
GET    /api/{table}/:id          # Get by ID
POST   /api/{table}              # Create
PATCH  /api/{table}/:id          # Update
DELETE /api/{table}/:id          # Delete
GET    /api/{table}/_explain     # EXPLAIN ANALYZE
GET    /api/{table}/_aggregate   # Aggregations
GET    /api/{table}/:id/{child}  # Nested resources (FK-based)
```

## Query API

### Filtering

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

### Full-Text Search

```
GET /api/products?search=ferry+bali                      # Search all text columns
GET /api/products?search=ferry&search_columns=name,desc  # Search specific columns
```

Uses PostgreSQL's `to_tsvector` / `websearch_to_tsquery` — supports natural language queries.

### Sorting

```
GET /api/orders?sort=created_at                          # Ascending (default)
GET /api/orders?sort=-total                              # Descending (prefix -)
GET /api/orders?sort=-status,created_at                  # Multi-column
```

### Pagination

```
GET /api/orders?limit=25&offset=50                       # Offset-based
GET /api/orders?limit=25&cursor=eyJpZCI6...              # Cursor-based
```

### Distinct

```
GET /api/orders?distinct=status                          # Distinct values
```

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

## Aggregations

```
GET /api/orders/_aggregate?fn=count                       # COUNT(*)
GET /api/orders/_aggregate?fn=sum&column=total            # SUM(total)
GET /api/orders/_aggregate?fn=avg&column=total            # AVG
GET /api/orders/_aggregate?fn=min&column=created_at       # MIN
GET /api/orders/_aggregate?fn=max&column=total            # MAX
GET /api/orders/_aggregate?fn=count&status=paid           # Filtered aggregation
```

## Authentication & Security

### JWT Authentication

The gateway validates JWT tokens and extracts tenant context for RLS:

```yaml
# gateway.yaml
auth:
  jwt:
    secret: "your-hs256-secret"        # HS256
    # OR
    jwks_url: "https://..."            # RS256 via JWKS
  claims:
    operator_id: "x-hasura-operator-id"
    user_id: "x-hasura-user-id"
    role: "x-hasura-role"
```

### Header-Based Dev Auth

For development, pass claims directly as headers:

```bash
curl -H "x-operator-id: uuid" -H "x-user-id: uuid" /api/orders
```

### Webhook Auth

Delegate authentication to an external service:

```yaml
auth:
  webhook:
    url: "https://auth.example.com/verify"
    headers_forward: ["Authorization", "Cookie"]
```

### Row-Level Security (RLS)

Every query is automatically scoped to the authenticated tenant via PostgreSQL's native RLS. The gateway sets session variables (`app.operator_id`, `app.user_id`) before each query — **no manual WHERE clauses needed**.

### YAML Policy Engine

Fine-grained access control per table, per role:

```yaml
policies:
  orders:
    roles:
      agent:
        select: true
        insert: true
        update: true
        delete: false
        columns: ["id", "status", "total", "created_at"]  # Column-level
        filter:                                             # Row-level
          operator_id: "x-operator-id"
      viewer:
        select: true
        columns: ["id", "status"]
```

### Query Allow-Listing

Lock down which queries can run in production:

```yaml
security:
  allow_list:
    enabled: true
    queries:
      - "GET /api/orders"
      - "GET /api/orders/:id"
  complexity_limit: 10        # Max query depth
  rate_limit: 100             # Requests per minute
```

## Real-Time

### WebSocket Subscriptions

Subscribe to table changes via WebSocket (PostgreSQL LISTEN/NOTIFY):

```javascript
const ws = new WebSocket('ws://localhost:8080/ws');
ws.send(JSON.stringify({
  type: 'subscribe',
  table: 'orders',
  filter: { status: 'paid' }
}));

ws.onmessage = (event) => {
  const { type, data } = JSON.parse(event.data);
  // type: "INSERT" | "UPDATE" | "DELETE"
  console.log('Change:', type, data);
};
```

### Live Queries

Auto-refresh query results when underlying data changes:

```javascript
ws.send(JSON.stringify({
  type: 'live_query',
  query: '/api/orders?status=paid&sort=-created_at&limit=10',
  interval: 2000  // Poll interval in ms
}));
```

### Event Triggers

Fire webhooks on database mutations:

```yaml
events:
  - name: order_created
    table: orders
    operations: [INSERT]
    webhook: "https://api.example.com/hooks/order-created"
    retry:
      count: 3
      interval: 5000
  - name: order_updated
    table: orders
    operations: [UPDATE]
    webhook: "https://api.example.com/hooks/order-updated"
```

## Performance

### Response Streaming

For large datasets, stream results as NDJSON:

```
GET /api/large_table?stream=true
```

Each row is sent as a newline-delimited JSON object — no buffering the entire result set.

### EXPLAIN Endpoint

Inspect query plans without touching production:

```
GET /api/orders/_explain?status=paid&expand=users
```

Returns the PostgreSQL `EXPLAIN ANALYZE` output for the generated query.

### Prepared Statement Caching

The gateway caches prepared statements per query shape, eliminating repeated parse overhead.

### Query Cache

LRU cache with TTL and table-level invalidation. Identical queries hit cache instead of the database.

## Observability

### Prometheus Metrics

```
GET /metrics
```

Exposes request counts, latencies, error rates, and connection pool stats.

### Request Tracing

Every response includes:
- `x-request-id` — unique request identifier
- `x-response-time` — duration in milliseconds

### Health Check

```
GET /health
```

### Schema Introspection

```
GET /api/_schema         # Full schema with tables, columns, types, FKs
GET /api/_openapi        # Auto-generated OpenAPI 3.0 spec
```

## Benchmark: Why Gateway > GraphQL

The gateway's `?expand=` does server-side JOINs — the same approach as Qail AST but over HTTP:

| Approach | Avg Latency | DB Queries | vs Qail |
|----------|------------|------------|---------|
| **Qail AST** (binary) | **449µs** | 1 | baseline |
| **Gateway** (`?expand=`) | 635µs | 1 | 1.4× |
| GraphQL + DataLoader | 1.52ms | 3 | 3.4× |
| GraphQL naive (N+1) | 18.2ms | 151 | **40×** |

The 1.4× gap is pure JSON serialization overhead. On the wire, the gateway executes the exact same single-query JOIN as the Qail driver.

> **Unlike GraphQL**, the gateway makes N+1 structurally impossible. `?expand=` always resolves to a server-side JOIN — there's no resolver pattern to misconfigure.
