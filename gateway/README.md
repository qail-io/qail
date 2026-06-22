# qail-gateway

**QAIL Access Gateway** - schema-driven AutoREST, WebSocket, OpenAPI, RLS, and
native access-policy enforcement with direct PostgreSQL execution via
`qail-pg`.

[![Crates.io](https://img.shields.io/crates/v/qail-gateway.svg)](https://crates.io/crates/qail-gateway)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## What It Provides

- Auto-REST CRUD routes from loaded schema
- QAIL text and binary query endpoints
- WebSocket query + live-query support
- JWT/API-key auth hooks + native table/operation/column policy engine
- RLS-aware DB acquisition and guard rails
- Query allow-list + complexity limits
- EXPLAIN and observability endpoints
- OpenAPI and typed contract endpoints for SDK/client generation

## Query Input Modes

- `POST /qail` accepts **text DSL** input (string request body).
- `POST /qail/binary` accepts **binary AST** payloads (no text-SQL interpolation path).
- Both paths execute through `qail-pg`; use binary mode when you want strict AST ingress semantics. PostgreSQL still parses SQL text on execution.

## Installation

```toml
[dependencies]
qail-gateway = "1.3.4"
qail-core = "1.3.4"
qail-pg = "1.3.4"
```

## Quick Start

```rust
use qail_gateway::Gateway;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let gateway = Gateway::builder()
        .secure_defaults() // optional: hardened profile
        .database("postgres://postgres:postgres@localhost:5432/app")
        .schema("schema.qail")
        .policy("policies.yaml")
        .bind("0.0.0.0:8080")
        .build_and_init()
        .await?;

    gateway.serve().await?;
    Ok(())
}
```

## Main Routes

- `GET /api/{table}`
- `GET /api/{table}/:id`
- `POST /api/{table}`
- `PATCH /api/{table}/:id`
- `DELETE /api/{table}/:id`
- `POST /qail`
- `POST /qail/binary`
- `GET /metrics`
- `GET /health`

## Notes

`production_strict=true` enables fail-closed startup checks for required security configuration.

Set `QAIL_SECURITY_PROFILE=secure` to apply the hardened profile automatically in the example server loader.

## Related Crates

| Concept name | Crate | Role |
|---|---|---|
| AST Kernel | `qail-core` | shared typed AST, RLS context, access policy |
| Postgres Driver | `qail-pg` | wire-protocol execution |
| SchemaOps CLI | `qail` | schema pull, drift diff, migrations, lint |
| Flow Engine | `qail-workflow` | app-owned business workflows |
