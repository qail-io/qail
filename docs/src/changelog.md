# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v1.3.5)

- **PostgreSQL io_uring opt-in security**: Linux `io_uring` plain-TCP transport no longer auto-enables on kernel support; enable it explicitly with `[postgres].io_uring = true`, `?io_uring=true`, driver/pool options, or `QAIL_PG_IO_BACKEND=io_uring`.
- **Tokio remains the safe default**: building with the `qail-pg/io_uring` feature only makes the backend available; TLS, mTLS, and GSSENC paths continue to use Tokio transport.
- **Workflow payment hardening**: charge side effects now default to stable workflow idempotency keys, support order-origin metadata, and store redacted payment display payloads for chat/notification use.
- **Release line**: Rust workspace crates and install snippets are bumped to `1.3.5`.

## v1.3.0 Highlights

- Detailed changelog: [QAIL.rs v1.3.0: Native Vertical Policy and the Audit Pass Behind It](https://dev.qail.io/blog/qail-rs-v1-3-0-deep-audit-hardening)
- **Native vertical access policy**: `qail_core::access` adds deny-by-default table policies, role/scope requirements, operation permissions, and read/write/returning column rules before AST execution.
- **Gateway policy integration**: `[access]` in `qail.toml` loads TOML/JSON policies so gateway REST, QAIL text/binary/batch, transaction, RPC, nested, expanded, and live-query paths can enforce vertical table and column boundaries alongside PostgreSQL RLS.
- **PostgreSQL statement cache safety**: hot prepared statements are promoted, evicted, reparsed, and retained only in states that match the real backend statement lifecycle.
- **Migration verification**: composite foreign-key options now survive parse/diff/apply and strict post-apply checks verify table constraints against the live database.
- **Gateway hardening**: precise numerics, oversized integers, Qdrant JSON integer drift, transaction subqueries, branch replay, and tenant guard exemptions are handled on explicit fail-closed paths.
- **Workflow and encoder fixes**: workflow guards, charge amounts, branch cursors, null bind params, zero-parameter binds, and Qdrant vector byte order were tightened.
- **SDK path safety**: TypeScript, Kotlin, and Swift SDK builders encode table and ID path segments before constructing REST routes.
- **Real database validation**: PostgreSQL lab coverage passed for strict migrations, MERGE, access-checked execution, seeded RLS, and gateway native access policy behavior.

## v1.2.1 Highlights

- **Schema parser compatibility**: pulled PostgreSQL schemas now accept table-level `enable_rls` and `force_rls` directives.
- **PostgreSQL type parsing**: multi-word types such as `DOUBLE PRECISION` and `TIMESTAMP WITH TIME ZONE` parse correctly from pulled schemas.
- **Comment parsing**: schema comments containing quoted examples no longer break parsing.

## v1.2.0 Highlights

- **PostgreSQL protocol safety**: COPY, LISTEN/NOTIFY, replication, pooled fetch, driver fetch, query, and pipeline paths now fail closed and desynchronize bad connections on malformed backend state.
- **NUL and UTF-8 hardening**: savepoints, SQL rendering, AST SQL buffers, gateway explain SQL, COPY text rows, backend wire strings, and PostgreSQL URL decoding now reject invalid input instead of silently mutating it.
- **Real database validation**: PostgreSQL 18 lab coverage passed for MERGE, set operations, recursive CTEs, cursor cleanup, COPY callback recovery, LISTEN/NOTIFY payloads, savepoint rejection, and NUL query rejection.

## v1.1.1 Highlights

- **Live migration introspection**: schema pulls and shadow verification now account for generated columns, identity defaults, expression indexes, enum extensions, and composite foreign-key drift.
- **Migration replay safety**: post-apply checks compare constraints, defaults, generated expressions, indexes, and extension dependencies against the real database state.
- **Branch overlay coverage**: live PostgreSQL audit paths verify merge, set-operation, recursive-CTE behavior, and bad overlay replay failure modes.

## v1.1.0 Highlights

- **Workflow engine hardening**: fixed nested loop context preservation, wait-event resume validation, timeout fallbacks, and transition checkpointing.
- **Gateway security hardening**: tightened tenant guards, RLS policy injection, write-side column policies, idempotency, transaction paths, branch overlays, and REST mutation/event semantics.
- **Qdrant tenant safety**: tenant-scoped vector upserts now namespace point IDs while preserving the caller-facing original ID.
- **Branch overlay correctness**: branch reads and merges now use deterministic chronological ordering with post-policy filtering and projection.
- **Durable events**: webhook delivery now has an outbox-backed path and stricter old/new payload handling.
- **Runtime surface cleanup**: the supported stable runtime is centered on PostgreSQL and Qdrant; legacy SQLite/DynamoDB/MongoDB transpiler symbols remain compatibility-only for 1.x consumers, and obsolete PostgreSQL examples were removed.

## v1.0.0 Highlights

- Promoted QAIL to 1.0.0 Stable, declaring the API complete and production-grade.
- **gRPC Connection State Machine**: Implemented concurrent reconnection protection using a connection generation counter in the Qdrant engine.
- **Webhook Scaling**: Scaled webhook concurrency limit to 512 paired with safe timeouts.
- **Connection Pool Locking**: Replaced async-wait locks with standard library `unwrap` synchronization under heavy concurrent loads.
- **Workspace Crates**: All workspace crates, internal path dependencies, and VSCode LSP extension bumped to `1.0.0`.
- **API Cleanup Carried Into 1.0**: `try_with_rls()` and `try_join_on()` compatibility aliases are gone; call `with_rls(&ctx)?` and `join_on(...)?` on the fallible path.
- **Raw SQL Runtime Surface**: `Qail::raw_sql(...)`, `Qail::is_raw_sql()`, `Qail::raw_where(...)`, and `Qail::nextval(...)` are not part of the stable public runtime path.
- **Cancel-Key API**: legacy `i32` cancel-key wrappers are gone; use bytes-native cancel-key APIs.
- **Error Conversion**: broad `From<QailBuildError> for String` compatibility conversion was removed so callers keep structured build errors.
