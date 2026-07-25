# Platform Map

QAIL is a small platform made of separate crates. The important split is not
the crate names; it is which layer owns which safety boundary.

## Layers

| Layer | Crate | Owns |
|-------|-------|------|
| AST Kernel | `qail-core` | Query structure, values, expressions, RLS context, schema AST, migration planning, access-policy checks |
| Postgres Driver | `qail-pg` | PostgreSQL protocol execution, typed value encoding, RLS session setup, pooling, prepared statement cache |
| Access Gateway | `qail-gateway` | HTTP/WebSocket surface, JWT/dev auth, REST/RPC mapping, tenant guard, allow-listing, gateway policy checks |
| SchemaOps CLI | `qail` | `schema.qail` workflows, pull, check, live diff, phased migration apply, typed codegen |
| Flow Engine | `qail-workflow` | Workflow state machine, waits, resumes, timeouts, branch cursors, side-effect checkpoint hooks |
| Flow Ledger | `qail-workflow-postgres` | Postgres-backed workflow state, leases, idempotency ledger, side-effect replay, timeout due-row discovery |
| Vector Bridge | `qail-qdrant` | Qdrant vector search and tenant-aware metadata filters |

## Choose The Smallest Surface

Use `qail-core` + `qail-pg` when your app already owns routing, auth, and
business logic. This is the normal Rust driver mode:

```rust
use qail_core::prelude::*;
use qail_pg::PgDriver;

let ctx = RlsContext::tenant(tenant_id).with_user(user_id);
let cmd = Qail::get("orders")
    .columns(["id", "status", "total"])
    .eq("status", "paid")
    .with_rls(&ctx)?;

let rows = driver.fetch_all(&cmd).await?;
```

Use `qail-gateway` when the database schema should become a controlled API
surface. The gateway adds HTTP/WebSocket routing, JWT authentication,
operation/column access policy, request limits, EXPLAIN guardrails, and
tenant-boundary checks around the same AST path.

Use `qail` when changing schema. The CLI is the state/schema tool: it can pull a
live PostgreSQL schema, validate `schema.qail`, compare drift, and apply
explicit expand/backfill/contract migrations.

Use `qail-workflow` when business flows need durable pause/resume semantics.
Use `qail-workflow-postgres` when those flows need production storage for
leases, idempotency, side-effect replay, and timeout due-row discovery.

Use `qail-qdrant` only for vector workloads. It is not a relational database
adapter; it shares the tenant-aware AST model where that maps cleanly to
Qdrant filters.

## Safety Ownership

QAIL does not claim that one crate magically owns every safety property. The
boundaries are explicit:

| Safety property | Owner |
|-----------------|-------|
| No application SQL interpolation on the AST path | `qail-core` builders + `qail-pg` execution |
| Row isolation | PostgreSQL RLS policies, with QAIL setting transaction-local context |
| Operation and column permissions | `qail_core::access`, optionally loaded by `qail-gateway` |
| HTTP auth and request shaping | `qail-gateway` |
| Schema drift detection | `qail` CLI and `qail-core` migration planner |
| Durable workflow replay protection | `qail-workflow` hooks + executor implementation |
| Postgres-backed workflow leases/idempotency | `qail-workflow-postgres` |
| External provider exactly-once behavior | The application/provider via idempotency keys |

## Supported Backends

PostgreSQL is the primary SQL backend. The AST, migration model, RLS context,
gateway, and workflow ledger are designed around PostgreSQL semantics.

Qdrant is supported for vector search through `qail-qdrant`.

Other database protocols are not part of the supported runtime surface. QAIL
does not fake support for engines whose semantics do not map cleanly to its
AST model.

## Naming

The crate names are intentionally literal. The docs use product names to make
the architecture easier to discuss:

- AST Kernel: `qail-core`
- Postgres Driver: `qail-pg`
- Access Gateway: `qail-gateway`
- SchemaOps CLI: `qail`
- Flow Engine: `qail-workflow`
- Flow Ledger: `qail-workflow-postgres`
- Vector Bridge: `qail-qdrant`

When debugging production behavior, start from the product name and then inspect
the owning crate.
