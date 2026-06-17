# qail-workflow

**QAIL Flow Engine** - declarative state-machine workflows for QAIL-driven
systems.

[![Crates.io](https://img.shields.io/crates/v/qail-workflow.svg)](https://crates.io/crates/qail-workflow)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Installation

```toml
[dependencies]
qail-workflow = "1.3.3"
qail-core = "1.3.3"
```

## Features

- Step-based workflow execution
- State-machine transitions
- Timers, branching, and notifications
- Query step execution through QAIL wire payloads
- Operation idempotency hooks for run, resume, and timeout calls
- Side-effect checkpoints for notifications, charges, and query steps
- Version/cursor validation to avoid stale resume and branch drift
- Timeout fallback execution through caller-provided schedulers

## Storage

`qail-workflow` is storage-agnostic. Implement `WorkflowExecutor` in your app,
or use `qail-workflow-postgres` when you want the QAIL Flow Ledger: PostgreSQL
tables for workflow state, leases, idempotency, side-effect replay, and timeout
due-row discovery.

The engine makes side effects safer, but provider calls still need app-level
idempotency. Pass the workflow side-effect id to payment, notification, and
external mutation providers whenever duplicate delivery would be unsafe.

## License

Apache-2.0
