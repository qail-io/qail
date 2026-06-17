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

Deployment note: current workflow side-effect operation ids use the v2 format
that includes the workflow state generation. Deploy this over an empty/drained
workflow side-effect ledger, or explicitly accept that already-running v1
side-effect rows cannot protect in-flight workflows from replay under the new
identity format.

Started workflow operations and side effects are treated as in progress until
their in-progress TTL expires. This lets a later worker recover from process
crashes that happen after `begin_*` but before `fail_*` or `complete_*`.
Choose a TTL longer than the expected max runtime for one workflow operation,
or pair it with workflow leases and provider idempotency keys for long-running
effects.

Workflow leases receive an opaque, versioned owner token from `qail-workflow`
rather than the raw logical worker name. This acts as a fencing token: a stale
worker release cannot delete a later lease acquisition that reused the same
logical owner name. Custom executors should persist and compare the full owner
string they receive.
