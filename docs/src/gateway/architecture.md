# Architecture & Invariants

This page describes the gateway's internal architecture — the request lifecycle, security boundaries, and the invariants that make multi-tenant isolation structural rather than behavioral.

**Read time: ~10 minutes.**

---

## Request Lifecycle

Every HTTP request flows through a fixed, ordered pipeline:

```
 Client Request
       │
 ┌─────▼─────────────────────────────────────────────────────┐
 │  1. CORS + Security Headers                               │
 │     X-Content-Type-Options: nosniff                       │
 │     X-Frame-Options: DENY                                 │
 │     Body size limit: 2 MiB                                │
 ├───────────────────────────────────────────────────────────┤
 │  2. Rate Limiter                                          │
 │     Token bucket per IP (configurable burst + rate)       │
 │     → 429 Too Many Requests on exhaustion                 │
 ├───────────────────────────────────────────────────────────┤
 │  3. Authentication                                        │
 │     JWT validation (HS256) → extract operator_id,         │
 │     user_id, role from claims                             │
 │     Fallback: header-based (dev only, no JWT_SECRET)      │
 ├───────────────────────────────────────────────────────────┤
 │  4. Policy Engine                                         │
 │     YAML-defined per-table, per-role access control       │
 │     Column filtering at AST level                         │
 │     → 403 Forbidden on policy violation                   │
 ├───────────────────────────────────────────────────────────┤
 │  5. Tenant Concurrency Gate                               │
 │     Per-tenant semaphore (configurable permits)           │
 │     Prevents one tenant from consuming all connections    │
 │     → 429 on tenant saturation                            │
 ├───────────────────────────────────────────────────────────┤
 │  6. Connection Acquisition                                │
 │     pool.acquire_with_rls(RlsContext) or                  │
 │     pool.acquire_with_rls_timeout(RlsContext, timeout)    │
 │     Sets PostgreSQL GUCs:                                 │
 │       set_config('app.current_operator_id', '...', false) │
 │       set_config('app.is_super_admin', '...', false)      │
 ├───────────────────────────────────────────────────────────┤
 │  7. EXPLAIN Pre-Check (reads only)                        │
 │     EXPLAIN on generated SQL before execution             │
 │     Rejects if cost > explain_max_cost                    │
 │              or rows > explain_max_rows                   │
 │     → QUERY_TOO_EXPENSIVE with structured JSON detail     │
 ├───────────────────────────────────────────────────────────┤
 │  8. Query Execution                                       │
 │     AST → SQL transpilation → prepared statement cache    │
 │     PostgreSQL RLS policies filter rows invisibly         │
 │     Result row cap: max_result_rows (configurable)        │
 ├───────────────────────────────────────────────────────────┤
 │  9. Connection Release                                    │
 │     DISCARD ALL → clears server-side state                │
 │     Client caches cleared (prepared stmts, column info)   │
 │     Connection returned to pool in clean state            │
 └───────────────────────────────────────────────────────────┘
       │
 JSON Response + X-Request-Id + X-Response-Time
```

---

## Connection Safety Model

The connection pool enforces a strict lifecycle:

### Acquisition

Three public methods — `acquire_raw()` is **crate-internal only**:

| Method | RLS | Timeout | Use Case |
|--------|-----|---------|----------|
| `acquire_with_rls(ctx)` | ✅ Set | Default | Normal tenant queries |
| `acquire_with_rls_timeout(ctx, ms)` | ✅ Set | Custom | Gateway with `statement_timeout_ms` |
| `acquire_system()` | ✅ Empty | Default | Schema introspection, migrations |
| `acquire_raw()` ⚠️ | ❌ None | Default | **Internal only** — requires `// SAFETY:` comment |

### Release

Every connection release executes:

```sql
DISCARD ALL;   -- Clears prepared statements, temp tables, GUCs, session state
```

Followed by client-side cleanup:

```rust
conn.prepared_statements.clear();
conn.stmt_cache.clear();
conn.column_info_cache.clear();

debug_assert!(conn.prepared_statements.is_empty(),
    "INVARIANT VIOLATED: prepared statements survived DISCARD ALL");
```

This guarantees **zero state leakage** between tenants sharing the same physical connection.

### Why `acquire_raw()` Is Restricted

`acquire_raw()` returns a connection with **no RLS context**. If used for tenant queries, it would bypass row-level security entirely.

Every internal call site must include a `// SAFETY:` comment explaining why raw acquisition is justified (typically: "RLS context is set immediately on the next line"). This convention is enforced via CI:

```bash
# Must return empty — every acquire_raw() must have a SAFETY comment
grep -rn "acquire_raw" pg/src/ | grep -v "// SAFETY:"
```

---

## Core Invariants

These are the properties the system guarantees. If any are violated, it's a bug.

### 1. Fail-Closed RLS

Every tenant query runs on a connection where RLS GUCs are set **before** any SQL executes. If GUC setup fails, the connection is not used — the error propagates to the caller.

There is no code path where a tenant query runs on a connection with another tenant's context.

### 2. Cost-Bounded Execution

Read queries are gated by EXPLAIN analysis before execution. If the estimated cost or row count exceeds configured limits, the query is rejected **before** touching the database.

Limits are configurable per role via `[gateway.role_overrides.<role>]` in `qail.toml`, allowing analytics roles to run heavier queries without weakening default safety.

### 3. Tenant Isolation on Connection Reuse

When a connection is released and re-acquired by a different tenant:

1. `DISCARD ALL` destroys all server-side state
2. Client-side caches are cleared
3. New RLS context is set for the new tenant

The integration test `test_pool_connection_recycling_isolation` verifies this with a pool of size 1, forcing the same physical connection to serve two different tenants sequentially.

### 4. Bounded Resource Consumption

| Resource | Bound | Mechanism |
|----------|-------|-----------|
| Connections | `max_connections` | Pool size cap |
| Per-tenant connections | `tenant_max_concurrent` | Semaphore per operator |
| Result rows | `max_result_rows` | Row cap per query |
| Query duration | `statement_timeout_ms` | PostgreSQL `SET LOCAL` |
| Query cost | `explain_max_cost` | EXPLAIN pre-check |
| Request body | 2 MiB | Axum body limit layer |
| Cache entries | `max_entries` | moka TinyLFU eviction |
| Cache entry TTL | `ttl_secs` | moka time-to-live |

### 5. Graceful Shutdown

On `SIGTERM` or `Ctrl+C`:

1. Stop accepting new connections
2. Wait for in-flight requests to complete
3. Drain the connection pool
4. Exit cleanly

No request is silently dropped. No connection is leaked.

---

## Component Map

```
GatewayState (shared Arc across all handlers)
├── pool: PgPool                    — Connection pool with RLS-aware acquisition
├── config: GatewayConfig           — All configuration (qail.toml + env overrides)
├── policy_engine: PolicyEngine     — YAML-defined per-table, per-role policies
├── schema: SchemaRegistry          — Auto-discovered table/column/FK metadata
├── cache: QueryCache               — moka-backed LRU with table-level invalidation
├── rate_limiter: RateLimiter       — Token bucket rate limiting
├── explain_cache: ExplainCache     — Cached EXPLAIN results per query shape
├── explain_config: ExplainConfig   — Cost/row thresholds for EXPLAIN pre-check
├── tenant_semaphore: TenantSemaphore — Per-tenant concurrency limiter
├── event_engine: EventTriggerEngine  — Webhook triggers on mutations
└── user_operator_map: HashMap      — JWT user_id → operator_id resolution cache
```

---

## Where to Hook Observability

| Signal | Endpoint / Header | Format |
|--------|-------------------|--------|
| Metrics | `GET /metrics` | Prometheus |
| Request ID | `X-Request-Id` response header | UUID |
| Latency | `X-Response-Time` response header | Duration string |
| Health | `GET /health` | `200 OK` or error |
| Schema | `GET /api/_schema` | JSON |
| OpenAPI | `GET /api/_openapi` | OpenAPI 3.0 JSON |
| Cache stats | Via `/metrics` | hit/miss/entries/weighted_size |
