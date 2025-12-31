# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.14.4] - 2025-12-31

### Performance (Zero-Alloc Encoding)
- **PG:** Added reusable `sql_buf` and `params_buf` to `PgConnection` - eliminates heap allocations on repeated queries
- **PG:** New `encode_cmd_reuse()` function for zero-allocation AST-to-wire encoding
- **PG:** `fetch_all()` now uses prepared statement caching by default
- **PG:** LIMIT/OFFSET now use parameterized values ($N) instead of embedded text - dramatically improves cache hit rate

### Enterprise Memory Management (LRU Cache)
- **PG:** Bounded LRU cache for prepared statements (default: 1000 statements)
- **PG:** Auto-evicts least recently used statements when cache is full
- **PG:** New `clear_cache()` method to manually free statement cache
- **PG:** New `cache_stats()` method returns (current_size, max_capacity)

### Benchmark Results (50K iterations, CTE with JOIN)
| Driver | Queries/Sec | Latency |
|--------|-------------|---------|
| **qail-pg (cached)** | **4,534 q/s** | 221μs |
| SeaORM | 4,032 q/s | 248μs |
| SQLx | 3,573 q/s | 280μs |

- **27% faster than SQLx** (+961 q/s)
- **12% faster than SeaORM** (+502 q/s)

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
