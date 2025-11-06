# Performance & Observability

The gateway is built for production throughput with prepared statement caching, query caching, rate limiting, and full observability.

---

## Query Cache

LRU cache with configurable TTL and automatic table-level invalidation.

```toml
[gateway.cache]
enabled = true
max_entries = 1000
ttl_secs = 60
```

### Behavior

- **Cache key:** Normalized query string + auth context hash
- **Invalidation:** Automatic on any mutation (`INSERT`, `UPDATE`, `DELETE`) to the same table
- **Eviction:** LRU when capacity is reached
- **TTL:** Configurable per-entry time-to-live
- **Correctness:** Concurrent reads during a mutation never serve stale data — invalidation is atomic

### Cache Headers

Responses include cache status headers:

```
X-Cache: HIT                    # Served from cache
X-Cache: MISS                   # Fresh query executed
```

---

## Prepared Statement Caching

The gateway caches prepared statements per query shape, eliminating repeated parse overhead. This is separate from the query cache — it caches the PostgreSQL server-side prepared statement, not the result.

---

## Rate Limiting

Built-in token-bucket rate limiter keyed by client IP:

```toml
[gateway]
rate_limit_rate = 100.0         # Tokens refilled per second
rate_limit_burst = 200          # Maximum burst capacity
```

When exceeded, the gateway returns:

```
HTTP/1.1 429 Too Many Requests
Retry-After: 1
```

```json
{
  "error": {
    "code": "RATE_LIMITED",
    "message": "Too many requests",
    "status": 429
  }
}
```

---

## Request Timeouts

All queries have a configurable timeout (default: 30 seconds, set via `statement_timeout_ms` in `qail.toml`). Long-running queries are cancelled and return:

```json
{
  "error": {
    "code": "TIMEOUT",
    "message": "Request timed out",
    "status": 408
  }
}
```

---

## Prometheus Metrics

```
GET /metrics
```

Exposes request counts, latencies (p50/p95/p99), error rates, cache hit ratios, and connection pool stats in Prometheus format.

---

## Request Tracing

Every response includes tracing headers:

```
X-Request-Id: req-uuid-...       # Unique request identifier
X-Response-Time: 12ms            # Duration
```

---

## Health Check

```
GET /health
```

Returns `200 OK` when the gateway and database connection are healthy.

---

## Schema Introspection

```
GET /api/_schema         # Full schema: tables, columns, types, FKs
GET /api/_openapi        # Auto-generated OpenAPI 3.0 spec
```

The OpenAPI spec is generated from the live database schema — always up to date.

---

## EXPLAIN Cost Guard

The gateway runs `EXPLAIN` on read queries **before execution** to reject expensive queries early.

Configure thresholds in `qail.toml`:

```toml
[gateway]
explain_max_cost = 100000.0    # Reject if estimated cost exceeds this
explain_max_rows = 1000000     # Reject if estimated rows exceed this
```

Per-role overrides allow analytics roles to run heavier queries:

```toml
[gateway.role_overrides.reporting]
explain_max_cost = 500000.0
```

Rejected queries return a structured `QUERY_TOO_EXPENSIVE` error:

```json
{
  "code": "QUERY_TOO_EXPENSIVE",
  "message": "Query rejected: estimated cost 150000 exceeds limit 100000...",
  "details": "{\"estimated_cost\":150000,\"cost_limit\":100000,\"estimated_rows\":2000000,\"row_limit\":1000000,\"suggestions\":[\"Add WHERE clauses\",\"Reduce ?expand depth\"]}"
}
```

The `details` field is machine-readable JSON — client SDKs can parse it to display actionable suggestions.

### EXPLAIN Endpoint

Inspect query plans manually:

```
GET /api/orders/_explain?status=paid&expand=users
```

Returns the PostgreSQL `EXPLAIN ANALYZE` output for the generated query.

---

## Error Responses

All errors follow a consistent JSON structure:

```json
{
  "error": {
    "code": "QUERY_ERROR",
    "message": "column \"foo\" does not exist",
    "status": 400,
    "request_id": "req-uuid-..."
  }
}
```

### Error Codes

| Code | HTTP | Description |
|------|------|-------------|
| `RATE_LIMITED` | 429 | Rate limit exceeded |
| `CONCURRENCY_LIMIT` | 429 | Tenant concurrency limit reached |
| `QUERY_TOO_EXPENSIVE` | 422 | EXPLAIN cost/row estimate exceeded threshold |
| `TIMEOUT` | 408 | Query exceeded statement timeout |
| `PARSE_ERROR` | 400 | Malformed query parameters |
| `QUERY_ERROR` | 400 | Database query failed |
| `UNAUTHORIZED` | 401 | Authentication failed |
| `FORBIDDEN` | 403 | Policy denied access |
| `NOT_FOUND` | 404 | Resource not found |
| `INTERNAL` | 500 | Unexpected server error |

---

## Benchmark: Gateway vs GraphQL

The gateway's `?expand=` does server-side JOINs — same approach as the Qail AST driver but over HTTP:

| Approach | Avg Latency | DB Queries | vs Qail |
|----------|------------|------------|---------|
| **Qail AST** (binary) | **449µs** | 1 | baseline |
| **Gateway** (`?expand=`) | 635µs | 1 | 1.4× |
| GraphQL + DataLoader | 1.52ms | 3 | 3.4× |
| GraphQL naive (N+1) | 18.2ms | 151 | **40×** |

The 1.4× gap is pure JSON serialization overhead. On the wire, the gateway executes the exact same single-query JOIN as the Qail driver.

> **Unlike GraphQL**, the gateway makes N+1 structurally impossible. `?expand=` always resolves to a server-side JOIN — there's no resolver pattern to misconfigure.
