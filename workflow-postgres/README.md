# qail-workflow-postgres

PostgreSQL storage backend for `qail-workflow`.

This crate keeps the workflow engine generic while giving Postgres users a
storage-backed executor for workflow state, leases, operation idempotency,
side-effect replay, and timeout due-row discovery.

All database operations are built as QAIL AST commands and executed through
`qail-pg`. The crate does not call raw SQL execution APIs.
