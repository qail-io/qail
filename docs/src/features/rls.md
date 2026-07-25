# Row-Level Security (RLS)

QAIL carries tenant/user scope through the AST and driver pipeline, then
`qail-pg` sets transaction-local PostgreSQL GUCs before executing tenant
queries. PostgreSQL RLS policies remain the database enforcement boundary.

This gives application code a tenant-first API while keeping row isolation in
the database:

```rust
use qail_core::prelude::*;

let ctx = RlsContext::tenant(tenant_id).with_user(user_id);

let query = Qail::get("bookings")
    .columns(["id", "status", "total"])
    .eq("status", "confirmed")
    .with_rls(&ctx)?;

let rows = driver.fetch_all(&query).await?;
```

## The Problem

Multi-tenant apps fail when row ownership is treated as a convention:

```rust
// Easy to remember in one handler
let sql = "SELECT * FROM bookings WHERE tenant_id = $1";

// Easy to forget in another handler
let sql = "SELECT * FROM invoices";
```

QAIL does not replace PostgreSQL RLS. It makes the tenant context explicit in
the query/connection lifecycle so app code does not hand-roll scope setup on
every call.

## Context Constructors

| Constructor | Scope | Use case |
|-------------|-------|----------|
| `RlsContext::tenant(id)` | One tenant | Normal SaaS tenant scope |
| `RlsContext::tenant(id).with_user(user_id)` | Tenant plus end user | Tenant dashboards with user-owned rows |
| `RlsContext::tenant_and_agent(tenant, agent)` | Tenant plus secondary agent/reseller | Legacy reseller/operator policies inside a tenant |
| `RlsContext::agent(id)` | Agent only | Legacy driver-level scope; prefer tenant-based contexts for gateway apps |
| `RlsContext::user(id)` | User only | Auth flows or user-scoped policies before tenant is known |
| `RlsContext::global()` | Shared/platform rows | `tenant_id IS NULL` style reference data |
| `RlsContext::empty()` | No tenant scope | Startup introspection, migrations, health checks |
| `RlsContext::super_admin(token)` | Full RLS bypass | Internal-only cross-tenant operations |

`SuperAdminToken` cannot be fabricated with public fields. It must be created
through a named constructor such as `for_system_process`, `for_webhook`, or
`for_auth`, which makes bypass intent visible at the call site.

## PostgreSQL Session Context

`qail-pg` opens a transaction and sets transaction-local context before the
query runs:

```sql
BEGIN;
SET LOCAL statement_timeout = ...;
SET LOCAL app.is_global = 'false';
SELECT
  set_config('app.current_user_id',   '<user>',   true),
  set_config('app.current_tenant_id', '<tenant>', true),
  set_config('app.current_agent_id',  '<agent>',  true),
  set_config('app.is_super_admin',    'false',    true);
```

On release, the connection commits the transaction. Transaction-local GUCs and
`SET LOCAL` values reset on `COMMIT`, while prepared statement caches can remain
hot for reuse.

## PostgreSQL Policy Example

```sql
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders FORCE ROW LEVEL SECURITY;

CREATE POLICY orders_tenant_isolation ON orders
  FOR ALL
  USING (
    tenant_id = current_setting('app.current_tenant_id', true)::uuid
    OR current_setting('app.is_super_admin', true) = 'true'
  )
  WITH CHECK (
    tenant_id = current_setting('app.current_tenant_id', true)::uuid
    OR current_setting('app.is_super_admin', true) = 'true'
  );
```

Use a database role that is not a superuser and does not have `BYPASSRLS`.
Superusers bypass RLS regardless of client-side discipline.

## Horizontal And Vertical Access

RLS is horizontal: it decides which rows are visible or writable.

Native access policy is vertical: it decides which tables, operations, roles,
scopes, and columns are allowed before the query reaches PostgreSQL.

Use both:

```toml
[access]
enabled = true
path = "access-policy.toml"
```

See [Access Policy](./access-policy.md) for operation and column semantics.

## Gateway Behavior

`qail-gateway` extracts tenant/user/role/scope from JWT claims. `tenant_id` is
the primary runtime contract. A legacy `agent_id` claim is only used as a
secondary scope when `tenant_id` is present; it does not create tenant scope by
itself.

Header-based dev auth can provide the same claims only when `QAIL_DEV_MODE=true`
and the gateway is bound safely for development.

## Guarantees And Non-Guarantees

| Property | Boundary |
|----------|----------|
| Tenant/user context is set before tenant queries | QAIL driver/gateway |
| Row filtering and write checks | PostgreSQL RLS policies |
| Operation and column permissions | Native access policy |
| Cross-tenant internal jobs | Explicit `super_admin` contexts |
| Provider/app authorization outside PostgreSQL | Application code |

If the database policy is wrong, QAIL cannot infer the correct row rule. If the
application uses a raw connection outside the RLS-aware path, it owns the risk.

## Operational Checklist

- Use `RlsContext::tenant(...)` as the default runtime scope.
- Attach `with_user(...)` when database policies need user ownership.
- Keep `agent_id` as a secondary legacy scope, not the primary tenant identity.
- Use transaction-local GUCs through `qail-pg` pool/driver APIs.
- Enable and force PostgreSQL RLS on tenant-owned tables.
- Run app roles as `NOBYPASSRLS` non-superusers.
- Use native access policy for vertical permissions.
- Keep `super_admin` token creation limited to named internal paths.
