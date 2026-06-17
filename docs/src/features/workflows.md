# Workflows

QAIL Flow Engine is a storage-agnostic workflow state machine for business
flows that pause, resume, branch, notify, charge, and run QAIL AST queries.

QAIL Flow Ledger is the PostgreSQL-backed executor wrapper that stores the
runtime guarantees needed for production: workflow state, leases, operation
idempotency, side-effect replay, and timeout due-row discovery.

## When To Use It

Use workflows for app-level processes where a single HTTP request is too small
to model the real business state:

- WhatsApp booking recovery after an operator declines
- vendor notification and accept/decline loops
- payment charge creation followed by confirmation events
- timeout fallback paths when a user, vendor, or provider never responds
- multi-step inventory or booking reconciliation

Do not use workflows as a replacement for database transactions. Use normal
transactions for one atomic database change. Use workflows when the process
spans external systems and time.

## Definition Model

A workflow definition is a named state machine. Each transition owns a list of
steps:

```rust
use std::time::Duration;
use qail_core::Qail;
use qail_workflow::{ChannelKind, WorkflowDefinition, WorkflowStep};

let definition = WorkflowDefinition::new("booking_recovery")
    .version("2026-06")
    .initial_state("operator_declined")
    .transition("operator_declined", "waiting_for_vendor", vec![
        WorkflowStep::query(
            &Qail::get("vendor_slots")
                .columns(["id", "phone", "price"])
                .eq("status", "available")
                .limit(10),
            Some("alternatives"),
        ),
        WorkflowStep::for_each("alternatives", vec![
            WorkflowStep::notify(
                ChannelKind::WhatsApp,
                "booking_opportunity",
                "item.phone",
            ),
        ]),
        WorkflowStep::wait_or(
            "vendor.accepted",
            Duration::from_secs(900),
            vec![WorkflowStep::transition("timed_out")],
        ),
    ]);
```

`WorkflowStep::query` persists QAIL wire text produced by
`qail_core::wire::encode_cmd_text`; it is not a raw SQL payload.

## Runtime Operations

The public runtime entry points are:

| Operation | Use |
|-----------|-----|
| `run_workflow` | Execute from the current state until completion or wait |
| `resume_workflow_with_event` | Resume a paused workflow with a named external event |
| `timeout_workflow` | Execute the `on_timeout` fallback for the current wait |
| `timeout_due_workflows` | Drain due workflow ids from the executor and timeout each one |

Use `WorkflowRunOptions` in production:

```rust
use std::time::Duration;
use qail_workflow::WorkflowRunOptions;

let options = WorkflowRunOptions::default()
    .with_lease("worker-a", Duration::from_secs(30))
    .with_idempotency_key("webhook:vendor.accepted:event-123");
```

A lease prevents concurrent workers from executing the same workflow id at the
same time. An idempotency key prevents the same external event, scheduler tick,
or retry from running the workflow operation twice.

## Executor Contract

`qail-workflow` is generic. Your application implements `WorkflowExecutor` for
business-side effects:

- execute a QAIL query payload
- send a notification
- create a payment charge
- save/load workflow state
- optionally acquire/release leases
- optionally record operation idempotency
- optionally record side-effect results
- optionally find due timeout rows

The default trait methods are permissive so tests and in-memory experiments are
easy. Production executors should implement the runtime hooks. Otherwise, the
engine can pause/resume, but it cannot prove distributed lease ownership,
idempotent resume, or durable side-effect replay.

## Flow Ledger For PostgreSQL

`qail-workflow-postgres` wraps an app executor:

```rust
use qail_workflow_postgres::{PgWorkflowExecutor, PgWorkflowStore};

let store = PgWorkflowStore::connect_url(database_url).await?;
store.install_schema().await?;

let executor = PgWorkflowExecutor::new(app_executor, store);
```

For long-running services, reuse the service pool:

```rust
let pool = qail_pg::PgPool::connect(qail_pg::PoolConfig::from_url(database_url)?).await?;
let store = PgWorkflowStore::from_pool(pool);
```

The default tables are:

- `qail_workflow_states`
- `qail_workflow_leases`
- `qail_workflow_operations`
- `qail_workflow_side_effects`

All store operations are built as QAIL AST commands and executed through
`qail-pg`.

## Exactly-Once Semantics

The engine provides replay control, not magic exactly-once delivery.

| Risk | QAIL layer | Still owned by app/provider |
|------|------------|-----------------------------|
| Two workers resume same workflow id | Workflow lease | Lease TTL sizing and worker discipline |
| Duplicate webhook event | Operation idempotency key | Stable event id selection |
| Process crashes after query/charge/notify | Side-effect ledger | Provider idempotency key for external effects |
| Scheduler fires timeout twice | Timeout due-row claim + idempotency key | Stable scheduler batch ids |
| Completed side effect is reached again | Stored side-effect result replay | Result schema compatibility |

For payment and notification providers, pass the workflow side-effect operation
id as the provider idempotency key whenever duplicates would be unsafe.

## Side-Effect Ledger

The engine wraps query, notify, and charge steps with stable side-effect ids.
The ledger returns one of two decisions:

- `Execute`: run the side effect now.
- `AlreadyCompleted`: skip execution and replay the stored result when needed.

Query and charge steps store their result because later workflow steps may need
that value in context. Notification steps normally store no result; once they
are completed, a replay skips delivery.

Failed side effects can be retried. Started side effects remain in progress
until the in-progress TTL expires; this lets another worker recover from a
crash after `begin_*` but before `fail_*` or `complete_*`.

## Timeout Model

`WorkflowStep::wait_or` stores the expected event and a deadline in the cursor.
The engine does not spawn a background scheduler. Production apps should run a
worker that calls `timeout_due_workflows` with a stable idempotency key for the
drain attempt.

The Postgres Flow Ledger discovers due rows with row locking and a claim TTL so
multiple timeout workers can run without picking the same workflow row at the
same time.

## Definition Versioning

Workflow contexts carry definition name and version. Resume and timeout paths
validate cursor state against the supplied definition so an old paused cursor
does not blindly continue through a different branch layout.

For production upgrades:

1. Version workflow definitions.
2. Keep old definitions available until paused runs drain.
3. Avoid changing step order or branch paths for in-flight versions.
4. Use explicit migration logic when an in-flight context shape changes.

## v2 Side-Effect Id Rollout

Current side-effect operation ids include workflow state generation. This fixes
replay identity for workflows that revisit the same state/step path after a
state transition.

Deploy this over an empty or drained side-effect ledger. If a deployment has
already-running v1 side-effect rows, either drain them before upgrading or
explicitly accept replay risk for those in-flight workflows.

## Production Checklist

- Wrap the app executor with `PgWorkflowExecutor` for Postgres deployments.
- Install Flow Ledger tables before enabling workers.
- Use `WorkflowRunOptions::with_lease` for run/resume/timeout execution.
- Use stable idempotency keys for webhook events and scheduler batches.
- Pass workflow side-effect ids to payment/notification providers.
- Set lease TTL and in-progress TTL longer than normal operation runtime.
- Keep old workflow definitions available while paused runs exist.
- Test crash/retry cases around notify, charge, query, resume, and timeout.
