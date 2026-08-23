# Row-Level Security (RLS)

QAIL carries tenant/user scope through the AST and driver pipeline, then
`qail-pg` sets transaction-local PostgreSQL GUCs before executing tenant
queries. PostgreSQL RLS policies remain the database enforcement boundary.

This gives application code a tenant-first API while keeping row isolation in
the database:

```rust
use qail_core::prelude::*;

let ctx = RlsContext::tenant(tenant_id).with_user(user_id);

// Application startup: declare the complete AST-scoping registry once.
qail_core::rls::init_scope_registries_from_tables(
    &[("bookings", "tenant_id")],
    &[],
)?;

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

## Declare The Isolation Mode At Startup

`with_rls()` refuses to run until the process explicitly chooses its isolation
mode. This prevents a missing registry initialization from turning every call
into an unscoped query.

For a process that owns a parsed `schema.qail`, initialize both scope
registries from that schema:

```rust
let source = std::fs::read_to_string("schema.qail")?;
let schema = qail_core::migrate::parse_qail(&source)?;
let counts = qail_core::rls::init_scope_registries(&schema)?;
```

Embedded applications without a schema file can declare tables directly:

```rust
qail_core::rls::init_scope_registries_from_tables(
    &[("orders", "tenant_id")],
    &[("listings", "seller_id")],
)?;
```

Two explicit no-injection modes are available:

- `declare_policy_only_isolation(reason)` for applications that deliberately
  rely on PostgreSQL policies alone.
- `declare_no_scoped_tables(reason)` for a normal API whose schema genuinely
  contains no tenant- or owner-scoped tables.

The first declaration seals the process mode. Conflicting declarations fail,
and unavailable registry state is an error rather than an empty registry.

## Tenant And Owner Table Declarations

Tenant scope is inferred only from a literal `tenant_id` column. User/owner
scope is always explicit:

```qail
table orders {
  id UUID primary_key
  tenant_id UUID not_null
  enable_rls
}

table listings {
  id UUID primary_key
  seller_id UUID not_null
  owner seller_id
  enable_rls
}
```

For `orders`, `with_rls()` injects `tenant_id = ctx.tenant_id`. For `listings`,
it injects `seller_id = ctx.user_id`. A table may carry both declarations; both
predicates then apply. Missing required context returns `RlsScopeMissing`
instead of executing an unscoped query. Owner columns are never inferred from
names such as `user_id` or `seller_id`.

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

`RlsContext::empty()`, `agent(...)`, or `user(...)` cannot scope a registered
tenant table. Likewise, a context without a user cannot scope an owner table.
Those mismatches are build errors, not no-ops.

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

Absent UUID-shaped values are written as the nil UUID rather than an empty
string, so policies that cast `current_setting(...)` to `uuid` remain valid for
tenant-only, user-only, and global contexts.

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
- Declare `owner <column>` for user-owned tables; never rely on a suggestive
  column name alone.
- Initialize the scope registries—or explicitly declare a no-injection
  mode—before any call to `with_rls()`.
- Keep `agent_id` as a secondary legacy scope, not the primary tenant identity.
- Use transaction-local GUCs through `qail-pg` pool/driver APIs.
- Enable and force PostgreSQL RLS on tenant-owned tables.
- Run app roles as `NOBYPASSRLS` non-superusers.
- Use native access policy for vertical permissions.
- Keep `super_admin` token creation limited to named internal paths.
