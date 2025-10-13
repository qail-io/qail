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

### Redis (qail-redis) ✅
- [x] Unified `Qail::redis_get()` / `Qail::redis_set()` API
- [x] Native RESP3 protocol encoder
- [x] Connection pooling

---

## 7. CLI Toolchain ✅

- [x] `qail init` — project scaffold (supports `--url`, `--deployment` for CI)
- [x] `qail pull` — live DB → `schema.qail` (fully AST-native)
- [x] `qail exec` — type-safe QAIL execution (`--json` for piping, `--tx` for transactions)
- [x] `qail diff` — schema comparison + live drift detection
- [x] `qail types` — `schema.qail` → Rust typed schema module
- [x] `qail check` — validate `.qail` syntax
- [x] `qail worker` — hybrid outbox daemon (PG → Qdrant sync)
- [x] LSP server (`qail-lsp`) for editor integration

---

## 8. Schema-as-Proof System ⏳

> *The schema becomes a type parameter. The compiler becomes the theorem prover.*

### Phase 1: Column Existence Proof
- [ ] `schema.qail` → codegen → `Schema` trait with associated types per table
- [ ] `Qail<S: Schema>` carries schema as phantom type parameter
- [ ] Column selection proven at compile time — `"foo"` on `users` = compile error if column doesn't exist
- [ ] No proc macros, no external provers, no database connection at compile time

### Phase 2: Type-Safe Filters
- [ ] Filter comparisons proven against column types — `age > "hello"` = compile error
- [ ] `TypedFilter<S>` ensures operand type matches column type
- [ ] Aggregation functions checked against source types

### Phase 3: Join Validity Graph
- [ ] FK relationship graph encoded at type level
- [ ] N-way joins proven valid via `RelatedTo<T>` chain
- [ ] Ambiguous join paths = compile error

### Phase 4: RLS Proof Witness
- [ ] RLS-protected tables get marker trait
- [ ] Queries without `.with_rls()` on protected tables = compile error
- [ ] `RlsProof<T>` witness type provided by middleware
- [ ] **Data leakage becomes a type error, not a security incident**

---

## 9. Native Versioning (Data Virtualization) ⏳

> *"GitHub for Databases" — branching at the application layer.*

- [ ] Gateway middleware with `X-Branch-ID` header
- [ ] Row-level branching (`WHERE _branch_id = ?`)
- [ ] Copy-on-Write strategy for writes
- [ ] CLI: `qail branch create <name>`, `qail checkout <name>`

---

## 10. Infrastructure-Aware Compiler ⏳

> *Verify external resources at compile time.*

- [ ] `schema.qail` extensions: `bucket`, `queue`, `topic`
- [ ] `build.rs` validates resources exist in Terraform/AWS/GCP
- [ ] Compile error if referencing non-existent infra

---

## Current Status (Feb 10, 2026) — v0.18.5

| Section | Status | Version |
|---|---|---|
| 1. First-Class Relations | ✅ Complete (4/4 phases) | v0.16.0 |
| 2. SaaS RLS Isolation | ✅ Complete (4/4 phases) | v0.15.6 |
| 3. Schema DDL Coverage | ✅ Complete (3/3 phases) | v0.18.5 |
| 4. Database Introspection | ✅ Complete (3/3 phases, zero raw SQL) | v0.18.5 |
| 5. Migration Engine | ✅ Complete (3/3 phases) | v0.15.9 |
| 6. Multi-Driver AST | ✅ Complete (PG + Qdrant + Redis) | v0.14.13 |
| 7. CLI Toolchain | ✅ Complete | v0.15.7 |
| 8. Schema-as-Proof | ⏳ Planned | — |
| 9. Data Virtualization | ⏳ Planned | — |
| 10. Infra-Aware Compiler | ⏳ Planned | — |