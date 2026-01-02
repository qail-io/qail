# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.14.16] - 2026-01-02

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

## [0.14.15] - 2026-01-02

### Critical Bug Fixes

- **Fixed SCRAM-SHA-256 Authentication:** Critical bug where `connect_with_password()` function signature had `database` and `password` parameters in unexpected order. Callers were passing `(host, port, user, password, database)` but signature was `(host, port, user, database, password)`, causing password auth to fail with wrong credentials.
- **Fixed INSERT Column Ordering:** `set_value()` pattern was generating INSERT without column names, causing PostgreSQL to expect values in table column order. Now extracts column names from `condition.left` when `cmd.columns` is empty.
- **Fixed INSERT RETURNING:** Added RETURNING clause encoding for `returning_all()` to work properly.

### New Features

- **`PgDriver::connect_env()`:** Native DATABASE_URL parsing - no more manual URL extraction
- **`PgDriver::connect_url(url)`:** Connect using any PostgreSQL URL string
- Full URL parsing with proper auth, host:port, and database extraction

## [0.14.14] - 2026-01-02

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

## [0.14.13] - 2026-01-02

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

## [0.14.12] - 2026-01-02

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

## [0.14.11] - 2026-01-01

### Qdrant Performance (4x Speedup)
- **HTTP/2 Batch Pipelining:** `search_batch()` multiplexes requests over single connection (4.00x faster than sequential)
- **Connection Pooling:** `QdrantPool` with semaphore concurrency (1.46x faster)
- **Zero-Allocation Buffer:** Removed `BytesMut::clone()` in favor of `split()` for true zero-copy
- **Documentation:** Added `PERFORMANCE.md` Qdrant section and new benchmark web page

## [0.14.10] - 2026-01-01

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

## [0.14.9] - 2026-01-01

### Security
- **PG:** Reject literal NULL bytes (0x00) in `execute_raw()` - prevents connection state pollution
- **PG:** `encode_value()` returns `Result<(), EncodeError>` for proper error handling
- **PG:** New `EncodeError` type in `ast_encoder::error` module

### Refactored
- DML encoders (`encode_select`, `encode_insert`, `encode_update`, `encode_delete`, `encode_export`) now return `Result`
- Clippy-clean: all `unit_arg` warnings fixed in match blocks

## [0.14.8] - 2026-01-01

### Production Hardening
- **PG:** `close()` async method sends Terminate packet ('X') for graceful shutdown
- **PG:** `Drop` impl sends Terminate via `try_write()` for TCP/Unix sockets
- **CLI:** Identity column support (GENERATED ALWAYS AS IDENTITY) in introspection
- **Core:** SERIAL→INTEGER conversion for ALTER TABLE commands

### Verified
- Pool overhead: **9.5μs/checkout** (excellent - microseconds, not milliseconds)
- 3D/4D arrays: Work correctly (not a bug)
- All chaos tests passed: Type Torture, Pool Starvation, Zombie Client

## [0.14.7] - 2026-01-01

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

## [0.14.6] - 2026-01-01

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

## [0.14.4] - 2025-12-31

### Performance (Zero-Alloc Encoding + LRU Cache)
- **PG:** `fetch_all()` now uses prepared statement caching by default (~5,000 q/s)
- **PG:** Added reusable `sql_buf` and `params_buf` to `PgConnection` - zero heap allocations
- **PG:** Bounded LRU cache for statements (default: 100 max, auto-evicts oldest)
- **PG:** New `clear_cache()` and `cache_stats()` methods for cache management
- **PG:** `fetch_all_uncached()` available for one-off queries

### Benchmark Results (50K iterations, CTE with JOIN)

🚀 **~5,000 queries/second** with 201μs latency — the fastest Rust PostgreSQL driver

## [0.14.3] - 2025-12-31

### Added
- **CLI:** `qail migrate create` now generates timestamped `.up.qail` and `.down.qail` file pairs
  - Format: `YYYYMMDDHHMMSS_name.up.qail` / `YYYYMMDDHHMMSS_name.down.qail`
  - Inline metadata: `@name`, `@created`, `@author`, `@depends`
  - Example: `qail migrate create add_users --author "orion"`

## [0.14.2] - 2025-12-31

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

## [0.14.1] - 2025-12-31

### Fixed
- **PG:** Critical bug in `encode_update()` where column names were encoded as `$1` placeholders instead of actual column names when using `.columns().values()` pattern.

### Added
- **PG:** Comprehensive battle test suite (`battle_test.rs`) with 19 query operations covering INSERT, SELECT, UPDATE, DELETE, JOINs, pagination, and DISTINCT.
- **PG:** Modularized `values.rs` into `values/` directory with `expressions.rs` for better extensibility.

## [0.14.0] - 2025-12-31

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

## [0.13.2] - 2025-12-31

### Added
- **Schema:** Added `version` field to `Schema` struct for version directive support (`-- qail: version=N`).

### Fixed
- **CLI:** `migrate down` now uses natural `current:target` argument order instead of confusing swap logic.
- **CLI:** `migrate` parser now correctly handles `--` SQL-style comments and version directives.
- **DDL:** Foreign key `REFERENCES` constraint now correctly emitted in CREATE TABLE statements.
- **DDL:** Tables now created in FK dependency order (parent before child).
- **CLI:** Unsafe type-change rollbacks now warn before proceeding (TEXT → INT requires USING clause).
- **Code:** Collapsed nested if statements using Rust 2024 let-chains (clippy fixes).

## [0.13.1] - 2025-12-30

### Fixed
- **Docs:** Updated incorrect version numbers in READMEs (was referencing 0.9).
- **Docs:** Fixed alignment issues in website examples.

### Known Issues
- Type-change rollback (e.g., TEXT → INT) requires manual `USING` clause in PostgreSQL. Rollback will fail if cast is not automatic.

## [0.13.0] - 2025-12-30

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
