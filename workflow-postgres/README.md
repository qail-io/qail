# qail-workflow-postgres

PostgreSQL storage backend for `qail-workflow`.

This crate keeps the workflow engine generic while giving Postgres users a
storage-backed executor for workflow state, leases, operation idempotency,
side-effect replay, and timeout due-row discovery.

All database operations are built as QAIL AST commands and executed through
`qail-pg`. The crate does not call raw SQL execution APIs.

Side-effect rows move through `started`, `failed`, and `completed` states.
Failed side effects are retryable; completed side effects are immutable and
replayed from their stored result. Apps should still pass the stable workflow
side-effect id through to external providers as an idempotency key whenever a
duplicate notification, charge, or mutation would be unsafe.
