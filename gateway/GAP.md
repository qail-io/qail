## Gap Analysis — Qail Gateway vs Hasura

**Current: ~90% feature parity** (excluding GraphQL — Qail AST is the replacement)

| Category | Hasura | Qail | % |
|---|---|---|---|
| Query API | 10 features | 10 | 100% |
| Relationships | 4 | 4 | 100% |
| Mutations | 4 | 4 | 100% |
| Auth & Security | 6 | 5 | 83% |
| Real-time | 3 | 3 | 100% |
| Performance | 5 | 4 | 80% |
| Observability | 4 | 3 | 75% |
| DevEx | 5 | 4 | 80% |
| Protocol | 3 | 3 | 100% |
| **Total** | **46** | **40** | **~90%** |

> Qail advantage: binary AST protocol, native Rust perf, compile-time RLS — Hasura doesn't have these.

---

### ✅ What We Have
- Auto-REST CRUD (all tables)
- FK-based JOIN expansion (`?expand=`) + **nested FK expansion** (`?expand=nested:users`)
- Nested resource routes (`/parent/:id/child`)
- Connection pooling + Postgres-native RLS
- YAML policy engine (filter injection) + column-level permissions
- JWT auth (HS256/RS256) + header-based dev auth + **webhook auth**
- WebSocket subscriptions (LISTEN/NOTIFY) + **live queries** (auto-refresh)
- **Event triggers** (mutation → webhook dispatch with retry)
- LRU query cache with TTL + table invalidation + **prepared stmt caching**
- Binary AST protocol (bincode)
- Batch queries + upsert + returning clause
- Prometheus metrics + **request tracing** (x-request-id, x-response-time)
- Health check
- Schema introspection + OpenAPI spec generation
- Query allow-listing + complexity/depth limits
- **EXPLAIN ANALYZE endpoint** (`GET /api/{table}/_explain`)
- **NDJSON response streaming** (`?stream=true`)

### ❌ Key Gaps

#### Query API ~~(biggest gap)~~ ✅ Phase 1 complete
- [x] Filter operators: `gt`, `lt`, `gte`, `lte`, `in`, `like`, `ilike`, `is_null`, `is_not_null`, `ne`, `not_in`, `not_like`, `contains`
- [x] Multi-column sorting
- [x] Aggregation endpoint (`count`, `sum`, `avg`, `min`, `max`)
- [x] Distinct queries
- [x] Cursor-based pagination
- [x] Full-text search

#### Mutations ✅ Phase 1 complete
- [x] Batch insert (array body)
- [x] Upsert (`ON CONFLICT`)
- [x] Returning clause

#### Security ✅ Phase 2 complete
- [x] Column-level permissions
- [x] Rate limiting
- [x] Query allow-listing
- [ ] Webhook authentication mode (header-based proxy; async call TODO)

#### Real-time ✅ Phase 3 complete
- [x] Event triggers (DB → webhook on mutation)
- [x] Live queries (auto-refresh over WS)
- [x] Nested object/array FK responses

#### DevEx ✅ Phase 2 complete
- [x] Schema introspection endpoint (`GET /api/_schema`)
- [x] OpenAPI spec generation (`GET /api/_openapi`)
- [x] Query complexity/depth limits
- [ ] Console UI (web dashboard)

#### Performance ✅ Phase 4 complete
- [x] Prepared statement caching (`pg/src/driver/query.rs`)
- [x] Response streaming (NDJSON)
- [x] Query EXPLAIN endpoint
- [x] Request tracing middleware

#### Observability ✅ Phase 4 complete
- [x] Request tracing (x-request-id, duration_ms per request)
- [x] Prometheus metrics
- [ ] Full OpenTelemetry collector integration

#### Not Planned (too large / different philosophy)
- GraphQL API layer
- Multi-database support
- Remote schema stitching

---

### Phased Roadmap

| Phase | Scope | Effort | Target % |
|---|---|---|---|
| 1 | Query API essentials (filters, agg, sort, pagination) | ~1 week | 50% |
| 2 | Security + DevEx (permissions, rate limit, OpenAPI) | ~1 week | 65% |
| 3 | Real-time + Events (triggers, live queries, nested responses) | ~1.5 weeks | 80% |
| 4 | Performance + Observability (prepared stmts, OTEL, explain) | ~1 week | 90% |
| 5 | Console UI + Qail | ~3+ weeks | 100% |

> **Phases 1-2 (~2 weeks) = 35% → 65%. That's the 80/20 play.**
