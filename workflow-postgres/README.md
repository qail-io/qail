# qail-workflow-postgres

**QAIL Flow Ledger** - PostgreSQL storage backend for `qail-workflow`.

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

## Tables

The default table names are:

- `qail_workflow_states`
- `qail_workflow_leases`
- `qail_workflow_operations`
- `qail_workflow_side_effects`

`PgWorkflowTables` lets applications override the names when a dedicated schema
or per-app prefix is required.

## Store Construction

Small tools and tests can create a store from a single driver:

```rust
let store = PgWorkflowStore::connect_url(database_url).await?;
```

Production services should usually share the app's `qail_pg::PgPool` instead:

```rust
let pool = qail_pg::PgPool::connect(qail_pg::PoolConfig::from_url(database_url)?).await?;
let store = PgWorkflowStore::from_pool(pool);
```

The pooled path still uses QAIL AST operations only. It acquires raw pooled
connections for the Flow Ledger because these tables are internal runtime
state, not tenant-scoped application data.

## Rollout Checklist

- Create the Flow Ledger tables before enabling workflow workers.
- Deploy over an empty/drained v1 side-effect ledger, or explicitly accept
  replay risk for already-running workflows.
- Configure worker lease TTL and in-progress TTL longer than the expected
  operation runtime.
- Pass side-effect operation ids through to external providers as idempotency
  keys.
- Keep scheduler invocations stable and unique per timeout batch.
