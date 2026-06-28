# qail-workflow

**QAIL Flow Engine** - declarative state-machine workflows for QAIL-driven
systems.

[![Crates.io](https://img.shields.io/crates/v/qail-workflow.svg)](https://crates.io/crates/qail-workflow)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Installation

```toml
[dependencies]
qail-workflow = "1.3.5"
qail-core = "1.3.5"
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

Charge steps now attach that workflow side-effect id to `ChargeRequest` as the
provider idempotency key when the app has not already set one. Payment display
data stored in workflow context is redacted for chat/notification use: QRIS and
virtual-account fields can be shown, card payments should expose only the
provider redirect/payment link. Use `order_origin` (`whatsapp`, `mcp`, `web`,
`ios_app`, `android_app`, or `api`) to keep WhatsApp, MCP/ChatGPT, web, and
native-app orders separate in provider metadata and downstream order records.

## License

Apache-2.0
