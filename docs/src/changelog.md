# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v1.2.0)

- **PostgreSQL protocol safety**: COPY, LISTEN/NOTIFY, replication, pooled fetch, driver fetch, query, and pipeline paths now fail closed and desynchronize bad connections on malformed backend state.
- **NUL and UTF-8 hardening**: savepoints, SQL rendering, AST SQL buffers, gateway explain SQL, COPY text rows, backend wire strings, and PostgreSQL URL decoding now reject invalid input instead of silently mutating it.
- **Real database validation**: PostgreSQL 18 lab coverage passed for MERGE, set operations, recursive CTEs, cursor cleanup, COPY callback recovery, LISTEN/NOTIFY payloads, savepoint rejection, and NUL query rejection.

## Current Highlights (v1.1.1)

- **Workflow engine hardening**: fixed nested loop context preservation, wait-event resume validation, timeout fallbacks, and transition checkpointing.
- **Gateway security hardening**: tightened tenant guards, RLS policy injection, write-side column policies, idempotency, transaction paths, branch overlays, and REST mutation/event semantics.
- **Qdrant tenant safety**: tenant-scoped vector upserts now namespace point IDs while preserving the caller-facing original ID.
- **Branch overlay correctness**: branch reads and merges now use deterministic chronological ordering with post-policy filtering and projection.
- **Durable events**: webhook delivery now has an outbox-backed path and stricter old/new payload handling.
- **Runtime surface cleanup**: the supported stable runtime is centered on PostgreSQL and Qdrant; legacy SQLite/DynamoDB/MongoDB transpiler symbols remain compatibility-only for 1.x consumers, and obsolete PostgreSQL examples were removed.

## Current Highlights (v1.0.0)

- Promoted QAIL to 1.0.0 Stable, declaring the API complete and production-grade.
- **gRPC Connection State Machine**: Implemented concurrent reconnection protection using a connection generation counter in the Qdrant engine.
- **Webhook Scaling**: Scaled webhook concurrency limit to 512 paired with safe timeouts.
- **Connection Pool Locking**: Replaced async-wait locks with standard library `unwrap` synchronization under heavy concurrent loads.
- **Workspace Crates**: All workspace crates, internal path dependencies, and VSCode LSP extension bumped to `1.0.0`.
- **API Cleanup Carried Into 1.0**: `try_with_rls()` and `try_join_on()` compatibility aliases are gone; call `with_rls(&ctx)?` and `join_on(...)?` on the fallible path.
- **Raw SQL Runtime Surface**: `Qail::raw_sql(...)`, `Qail::is_raw_sql()`, `Qail::raw_where(...)`, and `Qail::nextval(...)` are not part of the stable public runtime path.
- **Cancel-Key API**: legacy `i32` cancel-key wrappers are gone; use bytes-native cancel-key APIs.
- **Error Conversion**: broad `From<QailBuildError> for String` compatibility conversion was removed so callers keep structured build errors.
