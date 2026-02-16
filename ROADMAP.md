# QAIL Roadmap 2026: The Provably Correct Backend

> *"If it compiles, the query is correct."*

---

## 1. First-Class Relations ✅

### Phase 1: Runtime Registry ✅ (v0.15.0)
- [x] `ref:` syntax in `schema.qail`
- [x] `RelationRegistry` for runtime lookup
- [x] `Qail::join_on("table")` — string-based API

### Phase 2: Fully Typed Codegen ✅ (v0.16.0)
- [x] `build.rs` generates `schema_gen.rs` with typed structs
- [x] Table structs implement `Table` trait
- [x] `TypedColumn<T>` with Rust type mapping (SQL→Rust: uuid→Uuid, text→String, etc.)
- [x] `Qail::get()` accepts typed table markers via `AsRef<str>`
- [x] `typed_column()` accepts `TypedColumn<T>` in `builders/typed.rs`

### Phase 3: Logic-Safe Relations ✅ (v0.16.0)
- [x] `RelatedTo<T>` trait in `typed.rs`
- [x] Codegen generates bidirectional `RelatedTo` impls from `ref:` annotations
- [x] `TypedQail<T>` wrapper + `join_related()` with trait bound
- [x] Compile error if joining unrelated tables

### Phase 4: Compile-Time Data Governance ✅ (v0.16.0)
- [x] `protected` keyword in `schema.qail`
- [x] `TypedColumn<T, P>` with `Policy` generic
- [x] `Public` and `Protected` marker traits
- [x] `CapQuery::column_protected()` with `PolicyAllowedBy<C>` check
- [x] Compile-time failure verified via type system

---

## 2. SaaS Multi-Tenant Isolation (RLS) ✅

### Phase 1: Driver-Level Context ✅ (v0.14.21)
- [x] `RlsContext` struct in `core/src/rls.rs` — operator_id, agent_id, is_super_admin
- [x] `PgDriver.set_rls_context(ctx)` — calls `set_config()`
- [x] `PgDriver.clear_rls_context()` — resets to safe defaults

### Phase 2: Pool-Level RLS Acquisition ✅ (v0.15.6)
- [x] `PgPool.acquire_with_rls(ctx)` — acquire + set context in one call
- [x] `PooledConnection` auto-clears RLS on Drop — no stale tenant leaks

### Phase 3: Policy Definition API ✅ (v0.15.6)
- [x] `RlsPolicy` builder in `core/src/migrate/policy.rs`
- [x] `AlterOp::ForceRowLevelSecurity` + builders
- [x] SQL transpiler in `core/src/transpiler/policy.rs`
- [x] `rls_setup_sql()` — ENABLE + FORCE + CREATE POLICY in one call

### Phase 4: AST-Level Query Injection ✅ (v0.15.6)
- [x] `TenantRegistry` + `TENANT_TABLES` global in `core/src/rls/tenant.rs`
- [x] Auto-detect via `from_build_schema()` — tables with `operator_id` auto-register
- [x] `Qail::with_rls(ctx)` — GET/SET/DEL → filter, ADD → payload
- [x] Super admins + unregistered tables bypass injection

---

## 3. Schema DDL — Full PostgreSQL Coverage ✅

### Phase 1: Core Objects ✅ (v0.15.9)
- [x] CREATE/DROP EXTENSION — `extension "uuid-ossp" schema public version "1.1"`
- [x] CREATE TYPE AS ENUM / ALTER TYPE ADD VALUE / DROP TYPE
- [x] CREATE/ALTER/DROP SEQUENCE — `sequence order_seq { start 1000 increment 1 cache 10 cycle }`
- [x] CREATE/DROP TABLE with all column properties (PK, FK, NOT NULL, defaults, CHECK)
- [x] Multi-column foreign keys — `foreign_key (a, b) references t(x, y)`
- [x] Expression indexes — `index idx on users ((lower(email)))`

### Phase 2: Programmable Objects ✅ (v0.15.9)
- [x] CREATE/DROP VIEW + MATERIALIZED VIEW — `view name $$ SELECT ... $$`
- [x] CREATE/DROP FUNCTION (PL/pgSQL with `$$` body)
- [x] CREATE/DROP TRIGGER — `trigger trg on users before update execute func`
- [x] GRANT/REVOKE — `grant select, insert on users to app_role`
- [x] COMMENT ON TABLE/COLUMN — `comment on users.email "Primary contact"`
- [x] `enable_rls` / `force_rls` table directives

### Phase 3: RLS Policy Definition ✅ (v0.18.5)
- [x] Policy syntax in `.qail` — `policy name on table for cmd using $$ ... $$ with_check $$ ... $$`
- [x] Permissive + Restrictive policies
- [x] Per-command scope: ALL, SELECT, INSERT, UPDATE, DELETE
- [x] Role targeting: `to app_user`

---

## 4. Database Introspection — Fully AST-Native ✅

### Phase 1: Core Table Introspection ✅ (v0.14.8)
- [x] `qail pull --url postgres://...` — introspects live database → `schema.qail`
- [x] Tables, columns, types, defaults, constraints, indexes
- [x] Identity columns (GENERATED ALWAYS AS IDENTITY)
- [x] Foreign keys with actions (CASCADE, SET NULL, RESTRICT, etc.)
- [x] CHECK constraints (comparison, between, compound)

### Phase 2: Programmable Objects ✅ (v0.15.9)
- [x] Views + materialized views
- [x] Functions — `Qail::get("information_schema.routines")` + `Qail::get("information_schema.parameters")`
- [x] Triggers — `Qail::get("information_schema.triggers")`
- [x] Grants/permissions
- [x] Sequences, enums, extensions, comments

### Phase 3: Zero `fetch_raw` ✅ (v0.18.5)
- [x] Functions introspection via `information_schema.routines` + `information_schema.parameters` + `pg_catalog.pg_proc`
- [x] RLS Policies via `Qail::get("pg_policies")` with `qual`/`with_check` extraction
- [x] `policy_parser` module in `qail-core` — reusable SQL→Expr parser
- [x] **Result: zero raw SQL in `introspection.rs`** — every query is AST-native

---

## 5. Migration Engine ✅

### Phase 1: Schema Diffing ✅ (v0.13.0)
- [x] `diff_schemas()` — old vs new schema comparison
- [x] `AlterOp` enum: AddColumn, DropColumn, AlterType, RenameColumn, etc.
- [x] FK-ordered table creation (parent before child, reverse for drops)
- [x] Intent-aware hints: `rename`, `transform`, `drop confirm`

### Phase 2: CLI Commands ✅ (v0.14.12+)
- [x] `qail migrate apply` — apply pending `.qail` migrations
- [x] `qail migrate create <name>` — timestamped up/down file pairs
- [x] `qail migrate status` — rich tabular output with version, applied_at, checksum
- [x] `qail migrate reset <schema> <url>` — drop-all + re-apply
- [x] `qail migrate up` / `qail migrate down` — forward/rollback
- [x] `qail diff --live --url <db>` — schema drift detection

### Phase 3: Enterprise Features ✅ (v0.14.7+)
- [x] Shadow migrations — COPY streaming, safe promote
- [x] Subdirectory migration discovery — `deltas/YYYYMMDDHHMMSS_name/up.qail`
- [x] `.sql` file rejection with warning — enforces `.qail` barrier
- [x] `MigrationClass` enum: Reversible, DataLosing, Irreversible
- [x] Impact analyzer — `qail migrate analyze` scans codebase for affected queries

---

## 6. Multi-Driver Unified AST ✅

### PostgreSQL (qail-pg) ✅
- [x] Zero-copy wire protocol encoder — AST directly to PostgreSQL binary protocol
- [x] Prepared statement caching (LRU, 1000 max) — ~5,000 q/s
- [x] 10-connection pool: 1.3M queries/second
- [x] SCRAM-SHA-256 authentication
- [x] TLS/SSL support (rustls)
- [x] COPY streaming for bulk import/export
- [x] `QailRow` trait for struct mapping — `fetch_typed::<T>()`

### Qdrant (qail-qdrant) ✅
- [x] Zero-copy gRPC driver — 13% faster than official client
- [x] HTTP/2 batch pipelining — 4x speedup
- [x] Connection pooling with semaphore concurrency

### Redis (qail-redis) — Removed
- Removed in v0.20.0 — replaced by in-process Moka + LRU caching

---

## 7. CLI Toolchain ✅

- [x] `qail init` — project scaffold (supports `--url`, `--deployment` for CI)
- [x] `qail pull` — live DB → `schema.qail` (fully AST-native)
- [x] `qail exec` — type-safe QAIL execution (`--json` for piping, `--tx` for transactions)
- [x] `qail diff` — schema comparison + live drift detection
- [x] `qail types` — `schema.qail` → Rust typed schema module
- [x] `qail check` — validate `.qail` syntax
- [x] `qail worker` — hybrid outbox daemon (PG → Qdrant sync)
- [ ] LSP server (`qail-lsp`) — planned, not yet shipped

---

## 8. Schema-as-Proof System ✅

> *The schema becomes a type parameter. The compiler becomes the theorem prover.*

### Phase 1: Column Existence Proof ✅
- [x] `schema.qail` → codegen → `Table` trait with typed columns
- [x] `TypedQail<T>` carries table type as phantom parameter
- [x] Column references via `TypedColumn<T>` — type-safe at compile time
- [x] No proc macros, no database connection at compile time

### Phase 2: Type-Safe Filters ✅
- [x] `ColumnValue<C>` trait — `typed_eq(age, "hello")` = compile error when `age: TypedColumn<i64>`
- [x] `typed_filter()` ensures operand type matches column type

### Phase 3: Join Validity Graph ✅
- [x] FK relationship graph encoded via `RelatedTo<T>` trait
- [x] N-way joins proven valid via `join_related()` requiring `T: RelatedTo<U>`
- [x] Invalid joins = compile error

### Phase 4: RLS Proof Witness ✅
- [x] `RequiresRls` marker trait on tables with `operator_id`
- [x] `DirectBuild` marker trait on tables without — `.build()` available directly
- [x] Queries without `.with_rls()` on `RequiresRls` tables = compile error (no `.build()` method)
- [x] `RlsQuery<T>` wrapper — sealed proof witness, only produced by `TypedQail<T>::with_rls(ctx)`
- [x] **Data leakage is a type error, not a security incident**

---

## 9. Native Versioning (Data Virtualization) ✅

> *Row-level Copy-on-Write branching — overlay-based reads and writes without cloning tables.*

### Phase 1: Branch Infrastructure ✅ (v0.18.6)
- [x] `BranchContext` in `core/src/branch.rs` — branch identity struct
- [x] `branch_sql.rs` — DDL for `_qail_branches` + `_qail_branch_rows`, session vars, CRUD SQL
- [x] `PgPool::acquire_with_branch(ctx)` — session variable injection

### Phase 2: Gateway Integration ✅ (v0.18.6)
- [x] `X-Branch-ID` header extraction → `BranchContext`
- [x] Branch CRUD API: `POST/GET/DELETE /api/_branch`, `POST /api/_branch/:name/merge`
- [x] Auth guards on all branch endpoints

### Phase 3: Copy-on-Write ✅ (v0.18.6)
- [x] CoW Read: `apply_branch_overlay` merges overlay with main table (list + get-by-id)
- [x] CoW Write: `redirect_to_overlay` intercepts INSERT/UPDATE/DELETE on active branch
- [x] Transactional merge: overlay → main tables with BEGIN/COMMIT/ROLLBACK

### Phase 4: CLI ✅ (v0.18.6)
- [x] `qail branch create <name>` — insert into `_qail_branches`
- [x] `qail branch list` — list all branches with status
- [x] `qail branch delete <name>` — soft-delete
- [x] `qail branch merge <name>` — apply overlay + mark merged

---

## 10. Infrastructure-Aware Compiler — Deferred

> *Verify external resources at compile time.*

- [ ] `schema.qail` extensions: `bucket`, `queue`, `topic`
- [ ] `build.rs` validates resources exist in Terraform/AWS/GCP
- [ ] Compile error if referencing non-existent infra

> **Note:** Low priority — focusing on gateway features and crates.io publish first.

---

## 11. Market Readiness ⏳

> *From engine to product — the last mile to developer adoption.*

### Phase 1: Auth Cookbook & Deployment ✅

- [x] Auth integration guide — `docs/auth-cookbook.md`
  - JWT parsing (extract `tenant_id`, `user_id`)
  - API key → tenant lookup
  - Env var reference, RLS policy examples
- [x] Health endpoints — `/health` (public), `/health/internal` (pool stats + tenant guard)
- [x] Official `Dockerfile` (multi-stage, non-root, HEALTHCHECK)
- [x] `docker-compose.yml` quickstart (Postgres 17 + Qail)
- [x] Structured logging (tracing-subscriber with env filter)

### Phase 2: TypeScript SDK ✅

- [x] Builder API — Prisma/Drizzle-style query builder (`from`/`into`/`update`/`delete`)
- [x] HTTP transport (native fetch, zero dependencies)
- [x] Text fallback — raw DSL via `query()` + `batch()`
- [x] WebSocket realtime subscriptions via `subscribe()`
- [x] npm package (`sdk/typescript/`) + README

### Phase 3: Prometheus Metrics ✅

- [x] Wire up existing `QueryTimer` to Prometheus counters
- [x] Histogram: query latency by table + action
- [x] Gauge: pool utilization (active / idle / waiting)
- [x] Counter: cache hits vs misses (prepared stmt + parse cache + result cache)
- [x] `/metrics` endpoint (Prometheus scrape format, admin_token protected)

### Phase 4: Relation Embedding — Nested Selects ✅

- [x] FK graph discovery — `SchemaRegistry::relation_for()` + `children_of()`
- [x] Flat expansion via LEFT JOIN — `?expand=orders` or `?expand=orders,products`
- [x] Nested JSON expansion — `?expand=nested:orders` (batched WHERE IN, not N+1)
  - Forward FK → nested object, Reverse FK → nested array
- [x] Nested list routes — `GET /api/{parent}/{id}/{child}`
- [x] LATERAL JOIN + `json_agg()` in core AST and PG encoder
- [x] Depth limit enforcement — configurable `max_expand_depth` (default: 4)

### Phase 5: OpenAPI & Documentation ✅

- [x] OpenAPI spec auto-generation from schema (`GET /api/_openapi`)
- [x] Schema introspection endpoint (`GET /api/_schema`)
- [x] "First query in 5 minutes" quickstart — `docs/quickstart.md`
- [x] Swagger UI integration (`GET /docs` — CDN-hosted, dark theme, auto-JWT)

### Phase 7: Auto-REST API Generation ✅ (v0.20.3)

- [x] Schema-driven CRUD routes — `GET/POST/PUT/PATCH/DELETE /api/{table}`
- [x] Aggregation endpoints — `GET /api/{table}/aggregate`
- [x] Nested routes from FK graph — `GET /api/{parent}/{id}/{child}`
- [x] EXPLAIN pre-check endpoints — `GET /api/{table}/explain`
- [x] DevEx routes — `/api/_tables`, `/api/_openapi`, `/api/_health`
- [x] RLS policy parsing from schema files
- [x] UUID handling refinement in REST API context

### Phase 6: Realtime Documentation ✅

- [x] LISTEN / NOTIFY already built (AST-native: `Qail::listen`, `Qail::notify`, `Qail::unlisten`)
- [x] WebSocket handler in `gateway/src/ws.rs`
- [x] Client SDK WebSocket integration — `qail.subscribe()` method
- [x] Realtime patterns guide — `docs/realtime-patterns.md`
  - Live dashboard feeds, tenant-scoped events, collaborative presence
- [x] Example: real-time order feed with trigger + SDK

---

## 12. Gateway Performance ✅

> *Wire-protocol pipeline — eliminating the N+1 roundtrip problem.*

### Pipelined RLS Execution ✅ (v0.20.1)
- [x] `fetch_all_with_rls()` — RLS setup + query in single `write_all` syscall
- [x] `rls_sql_with_timeout()` — public API for RLS SQL generation
- [x] 2 roundtrips per request (down from 3+): pipeline + COMMIT
- [x] Removed redundant `RESET statement_timeout` (SET LOCAL is transaction-scoped)

### Benchmark Results (Feb 13, 2026)

**Sustained 60s × c20 (3-gateway comparison):**

| Gateway    | Plain SELECT | CTE d10  | P50    |
|------------|-------------|----------|--------|
| **Qail**   | **16,925**  | **14,818** | **0.5ms** |
| PostgREST  | 6,031       | 4,458    | 2.6ms  |
| Hasura v2  | 2,674       | —        | 7.6ms  |

**Burst 60s × c100 (CTE depth=10):**

| Gateway    | req/s      | Avg Latency | P99       |
|------------|-----------|-------------|-----------|
| **Qail**   | **11,823** | **8.5ms**  | **34ms**  |
| PostgREST  | 643        | 155ms       | 999ms     |

**Key properties:**
- Throughput invariant to CTE depth (0.5ms P50 at depth 3 and depth 10)
- +1,739% faster than PostgREST under burst (c100 + deep CTE)
- Zero errors across 900K+ requests sustained

---

## Current Status (Feb 16, 2026) — v0.20.4

| Section | Status | Version |
|---|---|---|
| 1. First-Class Relations | ✅ Complete (4/4 phases) | v0.16.0 |
| 2. SaaS RLS Isolation | ✅ Complete (4/4 phases) | v0.15.6 |
| 3. Schema DDL Coverage | ✅ Complete (3/3 phases) | v0.18.5 |
| 4. Database Introspection | ✅ Complete (3/3 phases, zero raw SQL) | v0.18.5 |
| 5. Migration Engine | ✅ Complete (3/3 phases) | v0.15.9 |
| 6. Multi-Driver AST | ✅ Complete (PG + Qdrant) | v0.20.4 |
| 7. CLI Toolchain | ✅ Complete (LSP planned) | v0.15.7 |
| 8. Schema-as-Proof | ✅ Complete (4/4 phases) | v0.16.0 |
| 9. Data Virtualization | ✅ Complete (4/4 phases) | v0.18.6 |
| 10. Infra-Aware Compiler | 💤 Deferred | — |
| 11. Market Readiness | ✅ Complete (7/7 phases) | v0.20.3 |
| 12. Gateway Performance | ✅ Complete (pipelined RLS) | v0.20.4 |