# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.24.5] - 2026-03-09

### Changed

- **Versioning:** Bumped Rust crates to `0.24.5` (`qail-core`, `qail-pg`, `qail-gateway`, `qail`, `qail-qdrant`, `qail-workflow`, `qail-encoder`, `qail-lsp`).
- **Docs domain:** Updated crate and CLI doc links from `qail.rs`/`qail.io` to `dev.qail.io` for developer-facing docs.
- **README snippets:** Updated installation version examples to `0.24.5` across workspace READMEs.

## [0.24.4] - 2026-03-09

### Changed

- **Versioning:** Bumped Rust crates to `0.24.4` (`qail-core`, `qail-pg`, `qail-gateway`, `qail`, `qail-qdrant`, `qail-workflow`, `qail-encoder`, `qail-lsp`).
- **Docs:** Standardized "SQL string vs SQL bytes" wording across workspace READMEs and docs to clarify:
  - "No SQL strings" means no app-side SQL interpolation on the AST path.
  - PostgreSQL still performs normal server-side parse/plan/execute.
- **Web docs:** Updated qail-web copy to use simpler language for AST-path safety and protocol semantics.

## [0.24.2] - 2026-03-07

### Added

- **Core (AST):** `skip_locked` flag on `Qail` — `FOR UPDATE SKIP LOCKED` clause in SELECT transpiler for safe concurrent claim patterns.
- **Core (RLS):** `RlsContext::user(user_id)` constructor — user-scoped context that sets `app.current_user_id` for row-level isolation by authenticated end-user.
- **Core (RLS):** `has_user()` and `user_id()` query methods on `RlsContext`.
- **Core (tests):** 3 new tests — user context creation, display semantics, and no-user-in-other-constructors invariant.
- **PG (RLS):** `context_to_sql` and `context_to_sql_with_timeouts` now emit `set_config('app.current_user_id', ...)` for user-scoped DB policies.
- **PG (tests):** 3 new tests — user context SQL emission, empty user_id handling, and SQL injection sanitization for user_id.

### Changed

- **Core (RLS):** All existing `RlsContext` constructors (`tenant`, `operator`, `agent`, `super_admin`, `global`, `empty`, `operator_and_agent`) now initialize `user_id` to empty string for backwards compatibility.

## [0.24.1] - 2026-03-06

### Added

- **Core (build validation):** function-scoped SuperAdmin audit in syn scanner with one-shot `// qail:allow(super_admin)` suppression bound to the next Qail call.
- **Core (tests):** expanded syn-scanner and RLS audit regression coverage for drift gate, allow-comment semantics, and explicit tenant-scope checks.
- **CI:** new dependency review workflow for pull requests.

### Changed

- **Core (RLS audit):** explicit tenant scope matcher tightened to `tenant_id` only for super-admin safety diagnostics.
- **Core (diagnostics):** duplicate schema/RLS build diagnostics are now deduplicated before emission.
- **Docs:** refreshed README and auth/RLS documentation to align with current architecture and hardening behavior.
- **Gateway/PG:** hardening and configuration refinements across cache defaults, connection/session lifecycle, and RLS plumbing.

### Removed

- **CI:** removed deferred `publish-wasm` workflow while wasm publishing remains out of scope.

## [0.21.0] - 2026-01-24

### Breaking Changes ⚠️

- **Core:** Removed `SuperAdminToken::issue()` — all call sites must migrate to named constructors:
  - `SuperAdminToken::for_system_process(reason)` — cron jobs, startup, reference-data endpoints
  - `SuperAdminToken::for_webhook(source)` — inbound callbacks (WhatsApp, Xendit, Midtrans)
  - `SuperAdminToken::for_auth(operation)` — login, register, token refresh
  - **Compile-time enforcement:** any remaining `issue()` call is now a hard compiler error (`E0599`)

### Added

- **Core:** `Qail::is_raw_sql()` — detects raw SQL commands for gateway pass-through
- **Core:** Named `SuperAdminToken` constructors with mandatory reason/source parameters for audit trails
- **PG:** Raw SQL pass-through in AST encoder — `Qail::raw_sql()` queries bypass AST-to-SQL translation and execute verbatim while preserving RLS context
- **PG:** `PgPool::acquire_for_tenant(tenant_id)` — convenience method for tenant-scoped connections
- **PG:** 5 new tests for raw SQL encoding (simple SELECT, WITH/CTE, multi-line, mixed case, whitespace)
- **PG:** DDL session action encoders — `CALL`, `DO`, `SET`, `SHOW`, `RESET` (`pg/src/protocol/ast_encoder/ddl.rs`)

### Fixed

- **PG:** Raw SQL queries routed through gateway no longer produce `syntax error at or near "SELECT"` — encoder now writes raw SQL verbatim instead of attempting AST re-encoding
- **Core:** `rls_proof_demo` and `spark_safety_demo` examples updated to use named constructors
- **Core:** All RLS integration tests updated to use named constructors

## [0.20.6] - 2026-01-05

### Changed

- **Docs:** Comprehensive documentation coverage sweep — all 6 crates now grade A+ (0 missing items)
  - `qail-core`: 28 items fixed (typed builders, validator, schema registry, transpiler DDL, transformer, migrate schema, AST builders, parser)
  - `qail-pg`: 14 items fixed (temporal types, driver, pool, connection, encoder, batch, DML encoders, expressions)
  - `qail-gateway`: 8 items fixed (cache, policy, metrics, schema)
  - `qail-qdrant`: 32 items fixed (PointId, PayloadValue, Point, ScoredPoint, VectorData, FieldType, Distance, DimensionMismatch)
  - `qail-workflow`: 24 items fixed (PaymentKind, Currency, ChargeStatus, ChannelKind, StateChange, WorkflowError)
  - `qail-cli`: 3 items fixed (parse_pg_url, parse_url_parts, snapshot_download)
- **Docs:** Added `# Arguments` sections to 41 complex functions across all crates
- **Docs:** Expanded 12 short/placeholder doc comments with full descriptions

## [0.20.5] - 2025-12-15

### Changed

- **Deps:** `rand` 0.9.2 → 0.10.0 (`Rng` trait renamed to `RngExt`)
- **Deps:** `thiserror` 2.0.17 → 2.0.18
- **Deps:** `proptest` 1.9.0 → 1.10.0
- **Deps:** `metrics-exporter-prometheus` 0.16.2 → 0.18.1
- **Docs:** Fixed overclaims in README (version badge, architecture tree, connection features, Hasura comparison)
- **Docs:** Updated ROADMAP to v0.20.4 — LSP marked WIP, Redis marked removed (Moka + LRU), Infra Compiler deferred
- **Project:** Added Dependabot for weekly Cargo dependency updates
- **Project:** Closed `bincode` 3.0.0 PR — tombstone release (project abandoned)
- **Gateway:** Migrated binary endpoint from `bincode` to `postcard` — actively maintained, smaller payloads

## [0.20.4] - 2025-12-15

### Added

- **Gateway:** `qdrant` feature flag — `qail-qdrant` is now an optional dependency (default = on)
  - Build without Qdrant: `cargo build -p qail-gateway --no-default-features`
  - Vector operations return descriptive error when feature is disabled
- **Project:** `SECURITY.md` — vulnerability disclosure policy with GitHub Security Advisories

### Fixed

- **Gateway:** 14 integration tests (`bench_pg_vs_qdrant`, `hybrid_rag`, `e2e_qdrant`, `e2e_weird`) marked `#[ignore]` — no longer block `cargo test --workspace` without live infra
- **Git:** Scrubbed ~150 MB of build artifacts from history (`.so`, `.dylib`, `.a`, `.o`, `zig-cache/`, `docs/book/`, PHP build cache)
- **Git:** Removed `schema.qail` (production DB schema) from all historical commits

## [0.20.3] - 2025-12-15

### Added

- **Gateway:** Split monolithic `handler.rs` (1,049 lines) into modular `handler/` directory:
  - `handler/mod.rs` — shared types (`QueryResponse`, `BatchRequest`) and re-exports
  - `handler/query.rs` — query execution (text, binary, fast, batch)
  - `handler/admin.rs` — health checks and Swagger UI
  - `handler/convert.rs` — PgRow→JSON conversion with OID-directed type mapping
  - `handler/qdrant.rs` — Qdrant vector operations
- **Gateway:** 22 new unit tests for `handler/convert.rs` covering all OID branches, fallback guessing, and `pg_array_to_json` edge cases (gateway tests: 81 → 103)
- **Project:** Added `CONTRIBUTING.md` — build instructions, project structure, code style, PR process

### Fixed

- **Gateway:** Replaced 4 `eprintln!()` calls with `tracing` macros in `concurrency.rs` and `tenant_guard.rs`
- **Gateway:** Implemented `From<GatewayError> for ApiError` to bridge gateway and API error types
- **Gateway:** Fixed 6 rustdoc errors (private intra-doc links, unclosed HTML tag, bare URLs)
- **Docs:** Corrected license badge and footer in README.md from MIT to Apache-2.0

## [0.20.2] - 2025-12-09

### Added

- **CLI:** Fix primary key introspection for composite keys
- **SDK:** Generated build artifacts for Swift, TypeScript, and Kotlin SDKs
- **Gateway:** Fast query execution endpoint (`/qail/fast`) with shape-based caching

## [0.20.1] - 2025-11-11

### Added

- **PG:** Transaction-local RLS — tenant context now scoped to transaction lifetime
- **PG:** Prepared statement caching for repeated query patterns

## [0.20.0] - 2025-11-06

### Added

- **PG:** PostgreSQL `EXPLAIN` pre-check support — rejects expensive queries before execution
- **Core:** `SuperAdminToken` — cryptographic proof for RLS bypass authorization
- **PG:** Wire protocol security tests — SQL injection and binary smuggling hardening
- **Gateway:** Comprehensive cache correctness and TTL expiry tests

### Changed

- **Gateway:** License updated to BSL-1.1

### Removed

- **Core:** Redis module removed (replaced by PostgreSQL-native caching)

## [0.19.1] - 2025-10-24

### Changed

- **Core:** Replaced stringly-typed column schema with `ColumnType` AST — type-safe schema representation
- **Docs:** README update with improved examples

## [0.19.0] - 2025-10-24

### Added

- **Gateway:** Data virtualization branching with CLI commands
- **Gateway:** Column-level access control policies

### Fixed

- **Gateway:** P0 fixes — rate limiter correctness, REST response caching, compression middleware
- **Gateway:** Clippy cleanup across workspace

## [0.18.6] - 2025-10-14

### Added

- **Core:** Codegen emits `RequiresRls` / `DirectBuild` markers for compile-time tenant safety
- **Core:** `rls_proof_demo` and `spark_safety_demo` — comprehensive compile-time safety showcases
- **Core:** RLS Proof Witness — compile-time tenant enforcement patterns

## [0.18.5] - 2025-10-14


### Added

- **Core:** `policy_parser` module (`qail_core::migrate::policy_parser`) — reusable SQL→AST parser for RLS policy expressions
  - `parse_policy_expr()` — converts raw `pg_policies.qual`/`with_check` SQL into typed `Expr` AST
  - `strip_outer_parens()`, `find_top_level_op()` — paren-aware SQL utilities for downstream use
  - Handles `current_setting()::type` tenant checks, `OR`/`AND` combinators, session bool checks
  - Falls back to `Expr::Raw(String)` for non-standard patterns
  - 4 unit tests covering tenant check, OR combinator, raw fallback, paren stripping
- **CLI:** Fully AST-native introspection — **zero `fetch_raw` calls** in `introspection.rs`
  - Functions: `Qail::get("information_schema.routines")` + `Qail::get("information_schema.parameters")` + `Qail::get("pg_catalog.pg_proc")`
  - RLS Policies: `Qail::get("pg_policies")` with `qual`/`with_check` column extraction
  - Triggers: `Qail::get("information_schema.triggers")` (unchanged)

### Fixed

- **PG:** `battle_qail_row` example now requires `chrono` and `uuid` features — no longer breaks `cargo test` without feature flags
- **CLI:** Removed ~166 lines of duplicate policy parsing code from `introspection.rs` (moved to core)

## [0.15.9] - 2025-09-29

### Added

- **Core:** CREATE/DROP EXTENSION support — `extension "uuid-ossp" schema public version "1.1"`
- **Core:** COMMENT ON TABLE/COLUMN — `comment on users.email "Primary contact"`
- **Core:** CREATE/ALTER/DROP SEQUENCE — `sequence order_seq { start 1000 increment 1 cache 10 cycle }`
- **Core:** CREATE TYPE ... AS ENUM / ALTER TYPE ADD VALUE / DROP TYPE — `enum status { active, inactive }`
- **Core:** Expression indexes — `index idx on users ((lower(email)))`
- **Core:** Multi-column foreign keys — `foreign_key (a, b) references t(x, y)`
- **Core:** CREATE/DROP VIEW + MATERIALIZED VIEW — `view name $$ SELECT ... $$`
- **Core:** CREATE/DROP FUNCTION (PL/pgSQL with `$$` body)
- **Core:** CREATE/DROP TRIGGER — `trigger trg on users before update execute func`
- **Core:** GRANT/REVOKE — `grant select, insert on users to app_role`
- **Core:** `ViewDef`, `SchemaFunctionDef`, `SchemaTriggerDef`, `Grant`, `GrantAction`, `Privilege` schema model types
- **Core:** `to_qail_string` serializer for views, functions, triggers, and grants
- **CLI:** Subdirectory migration discovery — supports `deltas/YYYYMMDDHHMMSS_name/up.qail` layout
- **CLI:** `.sql` file rejection with warning — enforces type-safe `.qail` barrier
- **CLI:** Configurable `deltas/` directory — reads `migrations_dir` from `qail.toml`, defaults to `deltas/`, falls back to `migrations/`
- **CLI:** `resolve_deltas_dir()` — centralized directory resolution for all migration commands

### Fixed

- **CLI:** Missing `multi_column_fks` field in shadow introspection
- **CLI:** Missing `expressions` field in shadow index introspection

## [0.15.7] - 2025-09-23

### Added

- **Core:** `TypedQail<T>` — table-typed query wrapper for compile-time relationship safety
  - `Qail::typed(table)` creates a typed builder carrying the source table type
  - `join_related(target)` with `RelatedTo<T>` trait bound — compiler rejects invalid joins
  - `typed_column()`, `typed_columns()` for batch typed column selection
  - `typed_eq()`, `typed_filter()` for type-safe filtering
  - Delegation: `with_cap()`, `with_rls()`, `limit()`, `offset()`, `order_by()`
- **Roadmap:** Section 1 (First-Class Relations) — all 4 phases marked complete
- **PG:** `PgDriver::query_ast()` — like `execute()` but returns `QueryResult` with column names + row data (for SELECT/GET)
- **PG:** `QueryResult` struct — decoded column headers from `RowDescription` + text rows from `DataRow`
- **CLI:** `qail exec` now displays SELECT results as formatted tables with column headers, separators, NULL (`∅`), and row count
- **Core:** `table[filter]` shorthand — `get users[active = true]` desugars to `get users where active = true`
  - Handles nested brackets, quoted strings, and existing WHERE clauses
- **CLI:** `qail migrate status` now displays rich tabular output with version, name, applied_at, and checksum
- **CLI:** `qail migrate reset <schema> <url>` — one-command drop-all + clear history + re-apply target schema
- **CLI:** `qail exec --json` — pipe-friendly JSON output for SELECT queries, suppresses all decorative output
- **CLI:** `qail diff --live --url <db>` — schema drift detection, introspects live DB and compares against `.qail` file
- **Core:** `cnt` / `count` action — `cnt users[active = true]` → `SELECT COUNT(*) FROM users WHERE active = true`
- **CLI:** `qail init` now supports `--url` and `--deployment` flags for non-interactive/CI mode
- **CLI:** Comprehensive `--help` text added to all `exec` and `migrate` subcommands with examples

### Fixed

- **Core:** `RENAME COLUMN` panic — `Action::Mod` was unsupported in AST encoder, added `encode_rename_column`
- **Core:** Duplicate `AlterDrop` — drop hint + auto-detected column drop both emitted the same ALTER DROP
- **Core:** FK ordering on `DROP TABLE` — `diff_schemas` now sorts dropped tables in reverse FK order (children before parents)

## [0.15.6] - 2025-09-22

### Added

- **Core:** `RlsContext` in `qail_core::rls` — shared multi-tenant context for all drivers (pg, qdrant, redis)
  - `RlsContext::operator()`, `::agent()`, `::operator_and_agent()`, `::super_admin()` constructors
  - `has_operator()`, `has_agent()`, `bypasses_rls()` query methods
  - `Display` impl for logging/debugging
- **PG:** Driver-level RLS support for multi-tenant SaaS:
  - `PgDriver::set_rls_context(ctx)` — sets PostgreSQL session variables via `set_config()`
  - `PgDriver::clear_rls_context()` — resets to safe defaults
  - `PgDriver::rls_context()` — getter for current context
  - `PgPool::acquire_with_rls(ctx)` — acquire connection with pre-configured tenant isolation
- **PG:** `rls` module with PostgreSQL-specific SQL generation (`context_to_sql`, `reset_sql`)
- **PG:** Pool-level RLS auto-clear on Drop — `PooledConnection` now resets tenant context via `reset_sql()` before returning dirty connections to pool (prevents cross-tenant data leakage)
- **Core:** AST-native RLS Policy Definition API (`qail_core::migrate::policy`):
  - `RlsPolicy` builder with `for_all()`, `for_select()`, `restrictive()`, `to_role()`, `using(Expr)`, `with_check(Expr)`
  - `tenant_check()`, `session_bool_check()`, `or()`, `and()` — typed AST combinators (no raw SQL)
  - `PolicyTarget`, `PolicyPermissiveness` enums
- **Core:** `AlterOp::ForceRowLevelSecurity(bool)` + `AlterTable::force_rls()` / `no_force_rls()` builders
- **Core:** Policy transpiler (`qail_core::transpiler::policy`):
  - `create_policy_sql()` — `RlsPolicy` → `CREATE POLICY ... USING (...) WITH CHECK (...)`
  - `drop_policy_sql()` — `DROP POLICY IF EXISTS ... ON ...`
  - `alter_table_sql()` — full `AlterOp` → SQL transpiler (ENABLE/DISABLE/FORCE RLS, ADD/DROP COLUMN, etc.)
  - `rls_setup_sql()` — convenience: ENABLE + FORCE + CREATE POLICY in one call
  - `check_expr_to_sql()` — `CheckExpr` AST → SQL
- **Core:** AST-level Tenant Injection API (`qail_core::rls::tenant`):
  - `TenantRegistry` — tracks which tables require tenant-scope injection
  - `register_tenant_table()`, `register_tenant_tables()`, `lookup_tenant_column()`, `load_tenant_tables()`
  - Auto-detection via `from_build_schema()`: tables with `operator_id` column are auto-registered
- **Core:** `Qail::with_rls(ctx)` — one-call AST-level tenant isolation:
  - `GET/SET/DEL` → injects `WHERE operator_id = $value` filter
  - `ADD/Upsert` → auto-sets `operator_id` in INSERT payload
  - Super admins and unregistered tables bypass injection

## [0.14.21] - 2024-12-21

### Fixed

- **PG:** `parse_database_url()` now URL-decodes user and password:
  - Before: `%2B` and `%3D` were sent literally to PostgreSQL
  - After: Properly decoded to `+` and `=`
  - Fixes password auth failures when DATABASE_URL contains special characters


## [0.14.20] - 2024-12-20

### Breaking Changes ⚠️

- **v2 Syntax Only:** Removed v1 horizontal syntax (`get::table:'col[cond]`). Parser now only accepts v2 keyword syntax:
  - ✅ `get users fields id, email where active = true`
  - ❌ `get::users:'id'email[active=true]` — **Now a parse error!**

### New Features

- **Compile-Time Type Safety (`qail types`):** Full Diesel-like type checking for QAIL queries
  - `qail types schema.qail -o schema.rs` — generate typed Rust schema
  - `TypedColumn<T>` for each column with SQL→Rust type mapping
  - `typed_eq()`, `typed_ne()`, `typed_gt()`, `typed_lt()` builder methods
  - `ColumnValue<T>` trait enforces compile-time type compatibility
  - Reserved keywords escaped automatically (`type` → `r#type`)

### Fixed

- **Example gating:** `battle_qail_row` example now requires `--features chrono,uuid`

### Updated

- All doc comments, tests, examples updated to v2 syntax
- Scanner regex patterns kept for legacy codebase detection (`qail migrate analyze`)


## [0.14.18] - 2024-12-10

### New Features

- **`qail exec` Command:** Type-safe QAIL AST execution for seeding and admin tasks
  - `qail exec "get users fields *" --url postgres://...` — execute QAIL query
  - `qail exec -f seed.qail --url postgres://...` — execute from file
  - `--tx` flag wraps all statements in a transaction with auto-rollback on error
  - `--dry-run` previews generated SQL without executing
  - Batch execution: one QAIL statement per line in `.qail` files
  - Comments supported (`#` and `--`)

### Documentation

- Updated CLI doc comments to v2 QAIL syntax examples
- Added `qail exec` to CLI reference documentation

## [0.14.17] - 2024-10-03

### New Features

- **`QailRow` Trait:** Native struct mapping without proc macros
  - Implement `columns()` and `from_row()` for automatic struct mapping
  - No external dependencies - pure trait-based approach
- **`fetch_typed::<T>()` Method:** Automatic struct conversion
  - `let users: Vec<User> = driver.fetch_typed::<User>(&query).await?`
  - Supports any type implementing `QailRow`
- **`fetch_one_typed::<T>()` Method:** Single-row typed fetch
  - Returns `Option<T>` for zero-or-one row queries

### Bug Fixes

- **Fixed JSON Array Index Encoding:** Integer keys now output `->0` instead of `->'0'`
  - Before: `metadata->'vessel_bookings'->'0'->>'field'` ❌
  - After: `metadata->'vessel_bookings'->0->>'field'` ✅
- **Fixed JSON Operator Precedence:** Added parentheses around `JsonAccess` expressions
  - Prevents `A || B || C->'d'` from being parsed as `((A||B)||C)->'d'`


## [0.14.16] - 2024-10-03

### Critical Bug Fixes

- **Fixed ORDER BY Multi-Cage Encoding:** Critical bug where encoder only processed the FIRST `.order_by()` call and ignored subsequent ones due to `break;` statement. This caused `DISTINCT ON` queries with multiple ORDER BY columns to return random rows instead of the most recent.
- **Fixed `encode_expr` Fallback:** Unhandled Expr variants (JsonAccess, FunctionCall, etc.) now delegate to full encoder instead of outputting `*`.
- **Fixed Partition Cage Encoding:** `.group_by_expr()` now properly encodes explicit GROUP BY expressions.

### New Features

- **Ergonomic Row Extraction:** Added unwrap-free helper methods to `PgRow`:
  - `text(idx)` - Returns String, defaults to empty
  - `text_or(idx, default)` - Returns String with custom default
  - `int(idx)` - Returns i64, defaults to 0
  - `float(idx)` - Returns f64, defaults to 0.0
  - `boolean(idx)` - Returns bool, defaults to false
  - `datetime(idx)` - Returns `Option<DateTime<Utc>>` (feature: `chrono`)
  - `uuid_typed(idx)` - Returns `Option<Uuid>` (feature: `uuid`)

## [0.14.15] - 2024-10-02

### Critical Bug Fixes

- **Fixed SCRAM-SHA-256 Authentication:** Critical bug where `connect_with_password()` function signature had `database` and `password` parameters in unexpected order. Callers were passing `(host, port, user, password, database)` but signature was `(host, port, user, database, password)`, causing password auth to fail with wrong credentials.
- **Fixed INSERT Column Ordering:** `set_value()` pattern was generating INSERT without column names, causing PostgreSQL to expect values in table column order. Now extracts column names from `condition.left` when `cmd.columns` is empty.
- **Fixed INSERT RETURNING:** Added RETURNING clause encoding for `returning_all()` to work properly.

### New Features

- **`PgDriver::connect_env()`:** Native DATABASE_URL parsing - no more manual URL extraction
- **`PgDriver::connect_url(url)`:** Connect using any PostgreSQL URL string
- Full URL parsing with proper auth, host:port, and database extraction

## [0.14.14] - 2024-09-28

### Security Hardening (Battle-Tested)

- **Fixed Protocol Desync:** Transaction errors now properly invalidate prepared statement cache
- **Fixed OOM Attack Vector:** Added `MAX_MESSAGE_SIZE` (1GB) check in all recv methods
- **Fixed Parameter Overflow:** Added client-side check for > 32,767 params (`EncodeError::TooManyParameters`)
- **Added `PgError::Encode` variant:** Consistent error propagation for encoding failures
- **Strict UTF-8 Validation:** `PgRow::get_string()` now returns `None` for invalid UTF-8 instead of replacement

### New Features

- **Query Cancellation:** Added `CancelToken` and `PooledConnection::cancel_token()` for query cancellation
- **Worker Skip Locked:** Upgraded `qail worker` to use atomic `FOR UPDATE SKIP LOCKED` pattern

### Fixed

- All encoder methods (`encode_bind`, `encode_extended_query`, etc.) now return `Result`
- Refactored `EncodeError` to shared `pg/src/protocol/error.rs`

## [0.14.13] - 2024-09-28

### New Crate: qail-redis — Unified Qail AST

**"Postgres stores facts, Qdrant stores meaning, Redis stores time — QAIL decides."**

- **Unified Qail API:** Redis commands use the same `Qail` AST
  - `Qail::redis_get("key")`, `Qail::redis_set("key", value)`
  - `Qail::redis_incr("key")`, `Qail::redis_del("key")`
  - `Qail::redis_ttl("key")`, `Qail::redis_expire("key", 60)`
- **RedisExt Trait:** Fluent methods for Redis-specific options
  - `.redis_ex(seconds)` - SET with TTL
  - `.redis_nx()` / `.redis_xx()` - SET conditions
- **Redis Actions in Core:** Added to `Action` enum for consistency
  - `Action::RedisGet`, `Action::RedisSet`, `Action::RedisDel`
  - `Action::RedisIncr`, `Action::RedisDecr`, `Action::RedisTtl`
- **Native RESP3 Protocol:** Direct wire encoding (no string parsing)
- **Connection Pooling:** `RedisPool` with semaphore concurrency
- **Full Test Suite:** 16 unit tests passing

## [0.14.12] - 2024-09-27

### Hybrid Architecture (PostgreSQL ↔ Qdrant)
- **`qail worker` daemon:** Polls `_qail_queue` outbox table and syncs to Qdrant
  - Connection retry with exponential backoff (500ms → 30s, 10 attempts)
  - Circuit breaker: 5 consecutive errors trigger auto-reconnect
  - Per-item error handling: never crashes, marks failed items with `retry_count`
- **`qail migrate apply` command:** Applies `.qail` files from migrations/ folder
  - Reads from `qail.toml` postgres.url automatically
  - Parses Schema syntax (`table name (...)`) and generates DDL
  - Supports function/trigger translation from QAIL to PL/pgSQL
- **`qail sync generate` command:** Generates trigger migrations from `[[sync]]` rules
- **`qail init` hybrid mode:** Creates `_qail_queue` table migration

### Qdrant Proto Fixes (4 critical encoding bugs)
- **Distance enum:** Fixed values (Cosine=1, Euclid=2, Dot=3 per Qdrant proto)
- **CreateCollection:** Fixed `vectors_config` field from 2 to 10 (0x52)
- **PointStruct:** Fixed `vectors` field from 3 to 4 (0x22)
- **Vector encoding:** Simplified to use deprecated packed floats (works correctly)

### Fixed
- Clippy warnings: `derivable_impls`, `sort_by_key`, `collapsible_if`, deref
- Init generates Schema-compatible `.qail` syntax (parentheses + commas)

## [0.14.11] - 2024-09-24

### Qdrant Performance (4x Speedup)
- **HTTP/2 Batch Pipelining:** `search_batch()` multiplexes requests over single connection (4.00x faster than sequential)
- **Connection Pooling:** `QdrantPool` with semaphore concurrency (1.46x faster)
- **Zero-Allocation Buffer:** Removed `BytesMut::clone()` in favor of `split()` for true zero-copy
- **Documentation:** Added `PERFORMANCE.md` Qdrant section and new benchmark web page

## [0.14.10] - 2024-09-23

### New Crate: qail-qdrant
- **Zero-Copy gRPC Driver:** High-performance Qdrant client
  - `proto_encoder.rs`: Direct protobuf wire encoding with memcpy for vectors
  - `proto_decoder.rs`: Zero-copy response parsing (SearchResponse, ScoredPoint)
  - `grpc_transport.rs`: Raw HTTP/2 gRPC using h2 crate
  - `GrpcDriver`: Combines encoder + transport for 13% faster than official client
- **REST Driver:** `QdrantDriver` with HTTP client (reqwest)
  - Search, upsert, delete, collection management
  - `Point`, `PointId`, `Payload`, `ScoredPoint` types
- **Benchmark:** QAIL 1.13x faster than official qdrant-client (199µs vs 225µs)
  - Encoding overhead: only 133ns (0.1% of latency)

### Core AST Extensions
- `Action::Search`, `Action::Upsert`, `Action::Scroll` for vector operations
- `Value::Vector(Vec<f32>)` for embeddings

## [0.14.9] - 2024-09-22

### Security
- **PG:** Reject literal NULL bytes (0x00) in `execute_raw()` - prevents connection state pollution
- **PG:** `encode_value()` returns `Result<(), EncodeError>` for proper error handling
- **PG:** New `EncodeError` type in `ast_encoder::error` module

### Refactored
- DML encoders (`encode_select`, `encode_insert`, `encode_update`, `encode_delete`, `encode_export`) now return `Result`
- Clippy-clean: all `unit_arg` warnings fixed in match blocks

## [0.14.8] - 2024-09-19

### Production Hardening
- **PG:** `close()` async method sends Terminate packet ('X') for graceful shutdown
- **PG:** `Drop` impl sends Terminate via `try_write()` for TCP/Unix sockets
- **CLI:** Identity column support (GENERATED ALWAYS AS IDENTITY) in introspection
- **Core:** SERIAL→INTEGER conversion for ALTER TABLE commands

### Verified
- Pool overhead: **9.5μs/checkout** (excellent - microseconds, not milliseconds)
- 3D/4D arrays: Work correctly (not a bug)
- All chaos tests passed: Type Torture, Pool Starvation, Zombie Client

## [0.14.7] - 2024-09-19

### Enterprise Shadow Migrations
- **COPY Streaming:** Zero-dependency data sync via COPY TO/FROM protocol
- **State Persistence:** `_qail_shadow_state` table stores diff commands for recovery
- **Safe Promote (Option B):** Apply migration to primary, don't swap databases
- **Column Intersection:** Sync handles ADD/DROP COLUMN scenarios correctly
- **Data Drift Warning:** Promote warns about changes since shadow sync

### Stress Tested
- Promote without shadow → proper error message
- Double abort → idempotent
- ADD COLUMN migration → fixed column intersection bug
- Full promote workflow → verified migration applied to primary

## [0.14.6] - 2024-09-18

### Fixed
- **CLI:** Shadow migration bug - now applies base schema (CREATE TABLEs) before diff commands
- **Core:** Added `schema_to_commands()` function for AST-native schema conversion
- **Docs:** Updated Migration Impact Analyzer documentation with real test output

### Performance
- **PG Pool:** 10-connection pool benchmark: **1.3M queries/second** (78M queries in 60s)
- **Benchmark:** Single connection: 336K q/s → Pool: 1.3M q/s (~4x throughput)

### Added
- **CLI:** Shadow migration now shows `[1.5/4]` step for base schema application
- **Docs:** Added Rollback Safety Analysis table to analyzer docs
- **Docs:** Added CI/CD integration section with GitHub Actions `--ci` flag

## [0.14.4] - 2024-09-14

### Performance (Zero-Alloc Encoding + LRU Cache)
- **PG:** `fetch_all()` now uses prepared statement caching by default (~5,000 q/s)
- **PG:** Added reusable `sql_buf` and `params_buf` to `PgConnection` - zero heap allocations
- **PG:** Bounded LRU cache for statements (default: 100 max, auto-evicts oldest)
- **PG:** New `clear_cache()` and `cache_stats()` methods for cache management
- **PG:** `fetch_all_uncached()` available for one-off queries

### Benchmark Results (50K iterations, CTE with JOIN)

🚀 **~5,000 queries/second** with 201μs latency — the fastest Rust PostgreSQL driver

## [0.14.3] - 2024-09-12

### Added
- **CLI:** `qail migrate create` now generates timestamped `.up.qail` and `.down.qail` file pairs
  - Format: `YYYYMMDDHHMMSS_name.up.qail` / `YYYYMMDDHHMMSS_name.down.qail`
  - Inline metadata: `@name`, `@created`, `@author`, `@depends`
  - Example: `qail migrate create add_users --author "orion"`

## [0.14.2] - 2024-09-09

### Added

**Wire Protocol Encoders (AST-Native):**
- `DISTINCT ON (col1, col2, ...)` queries
- `COUNT(*) FILTER (WHERE ...)` aggregate syntax
- Window `FRAME` clause (`ROWS/RANGE BETWEEN ... AND ...`)
- `GROUP BY` with `ROLLUP`, `CUBE`, and `GROUPING SETS`
- `CREATE VIEW` and `DROP VIEW` DDL
- Comprehensive tests: `complex_test.rs`, `expr_test.rs`

**Expression System (100% Grammar Coverage):**
- `Expr::ArrayConstructor` - `ARRAY[col1, col2, ...]`
- `Expr::RowConstructor` - `ROW(a, b, c)`
- `Expr::Subscript` - Array/string subscripting `arr[1]`
- `Expr::Collate` - Collation expressions `col COLLATE "C"`
- `Expr::FieldAccess` - Composite field selection `(row).field`
- `GroupByMode::GroupingSets(Vec<Vec<String>>)` - `GROUPING SETS ((a, b), (c))`
- `Action::CreateView` and `Action::DropView` for view management

**CLI Improvements:**
- `qail diff --pretty` displays `MigrationClass` (reversible/data-losing/irreversible)

### Changed
- `Expr::Window.params` from `Vec<Value>` to `Vec<Expr>` for native AST philosophy
- Expression, DML, and DDL coverage now 100% for standard PostgreSQL

## [0.14.1] - 2024-09-09

### Fixed
- **PG:** Critical bug in `encode_update()` where column names were encoded as `$1` placeholders instead of actual column names when using `.columns().values()` pattern.

### Added
- **PG:** Comprehensive battle test suite (`battle_test.rs`) with 19 query operations covering INSERT, SELECT, UPDATE, DELETE, JOINs, pagination, and DISTINCT.
- **PG:** Modularized `values.rs` into `values/` directory with `expressions.rs` for better extensibility.

## [0.14.0] - 2024-09-08

### Added
- **CLI:** `MigrationClass` enum for classifying migrations: `Reversible`, `DataLosing`, `Irreversible`.
- **CLI:** Type safety warnings for unsafe rollbacks (TEXT → INT requires USING clause).
- **CLI:** `is_safe_cast()` and `is_narrowing_type()` helpers in `migrations/types.rs`.
- **Core:** FK ordering regression tests for parent-before-child table creation.

### Changed
- **CLI:** Modularized `migrations.rs` (1044 lines) into 9 focused modules:
  - `types.rs`: MigrationClass enum and type safety helpers
  - `up.rs`: migrate_up with codebase impact analysis
  - `down.rs`: migrate_down with unsafe type warnings
  - `analyze.rs`: CI-integrated codebase scanner
  - `plan.rs`, `watch.rs`, `status.rs`, `create.rs`: Other operations

## [0.13.2] - 2024-09-08

### Added
- **Schema:** Added `version` field to `Schema` struct for version directive support (`-- qail: version=N`).

### Fixed
- **CLI:** `migrate down` now uses natural `current:target` argument order instead of confusing swap logic.
- **CLI:** `migrate` parser now correctly handles `--` SQL-style comments and version directives.
- **DDL:** Foreign key `REFERENCES` constraint now correctly emitted in CREATE TABLE statements.
- **DDL:** Tables now created in FK dependency order (parent before child).
- **CLI:** Unsafe type-change rollbacks now warn before proceeding (TEXT → INT requires USING clause).
- **Code:** Collapsed nested if statements using Rust 2024 let-chains (clippy fixes).

## [0.13.1] - 2024-09-08

### Fixed
- **Docs:** Updated incorrect version numbers in READMEs (was referencing 0.9).
- **Docs:** Fixed alignment issues in website examples.

### Known Issues
- Type-change rollback (e.g., TEXT → INT) requires manual `USING` clause in PostgreSQL. Rollback will fail if cast is not automatic.

## [0.13.0] - 2024-09-08

### Breaking Changes ⚠️
- **Core:** Renamed `QailCmd` struct to `Qail` for a cleaner, "calmer" API.
  - *Note:* v0.12.0 still supported `QailCmd`. This release enforces the rename.
  - Rust: `QailCmd::get("table")` -> `Qail::get("table")`
  - Python: `from qail import QailCmd` -> `from qail import Qail`
- **Bindings:** Renamed C/FFI exported functions to remove `_cmd` suffix.
  - `qail_cmd_encode` -> `qail_encode`
  - `qail_cmd_free` -> `qail_free`

### Added
- **Core:** Added `impl Default` for `Qail` struct.
- **Go:** Updated Go bindings to support new `Qail` struct name and FFI symbols.
