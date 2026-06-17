# Access Policy

QAIL has two different access-control layers:

- Row-Level Security (RLS) handles horizontal isolation: which rows a tenant,
  user, or platform context can see.
- Native access policy handles vertical isolation: which tables, operations,
  columns, roles, and scopes a subject can use before the AST reaches the
  database.

Use both for production SaaS APIs. RLS protects row ownership in PostgreSQL;
the access policy protects the API and AST surface from reading or writing
fields the subject should not be able to touch.

## Where It Runs

Native access policy lives in `qail_core::access`. It checks a `Qail` command
directly, so it works before PostgreSQL execution:

```rust
use qail_core::access::{
    AccessContext, AccessOperation, AccessPolicy, ColumnRule, TableAccessPolicy,
};
use qail_core::Qail;

let policy = AccessPolicy::new().with_table(
    "orders",
    TableAccessPolicy::new()
        .allow_operations([AccessOperation::Read, AccessOperation::Update])
        .read_columns(ColumnRule::only(["id", "status", "total"]))
        .write_columns(ColumnRule::only(["status"]))
        .require_any_role(["operator"])
        .require_scopes(["orders:read"]),
);

let ctx = AccessContext::subject("user-1")
    .with_tenant("tenant-1")
    .with_role("operator")
    .with_scope("orders:read");

let cmd = Qail::get("orders").columns(["id", "status"]);
policy.check_command(&ctx, &cmd)?;
```

`qail-gateway` loads this policy when `[access]` is configured and applies it
to REST, QAIL text/binary, batch, transaction, RPC, and live-query paths before
execution.

## Configuration

Enable gateway integration through `qail.toml`:

```toml
[access]
enabled = true
path = "access-policy.toml"
```

Policy files may be TOML or JSON. YAML is not supported for the native access
policy.

Example `access-policy.toml`:

```toml
default_decision = "deny"

[tables.orders]
operations = ["read", "update"]
denied_operations = ["delete"]
read_columns = { only = ["id", "status", "total", "created_at"] }
write_columns = { only = ["status"] }
returning_columns = { only = ["id", "status"] }
require_any_role = ["operator", "administrator"]
require_scopes = ["orders:read"]

[tables.order_audit]
operations = ["read"]
read_columns = { except = ["internal_note"] }
require_any_role = ["administrator"]
require_scopes = ["audit:read"]
```

Use `default_decision = "deny"` for production. `AccessPolicy::new()` is
deny-by-default. `AccessPolicy::allow_by_default()` exists for trusted internal
tools, not public API exposure.

## Operation Semantics

QAIL maps commands to the operations that must be allowed:

| AST action | Required operation |
|------------|--------------------|
| `GET`, `COUNT`, `EXPORT`, `WITH`, `SEARCH`, `SCROLL` | `read` |
| `ADD` | `create` |
| `SET`, `PUT`, `OVER` | `update` |
| `UPSERT` | `create` and maybe `update`, depending on conflict action |
| `DEL` | `delete` |
| `MERGE` | The operations used by each MERGE clause |

For `MERGE`, QAIL checks the target action clauses. A `WHEN MATCHED UPDATE`
requires `update`; `WHEN NOT MATCHED INSERT` requires `create`; `DELETE`
requires `delete`. The source table or source query also needs `read` policy.

## Column Semantics

Column rules can be:

| Rule | Meaning |
|------|---------|
| `any` | Any column is allowed |
| `deny_all` | No column is allowed |
| `{ only = [...] }` | Only listed columns are allowed |
| `{ except = [...] }` | Any column except listed columns is allowed |

Restrictive read rules reject wildcard projections. If a user can only read
`id` and `status`, then `Qail::get("orders")` is rejected because it means an
implicit wildcard. Use explicit columns:

```rust
Qail::get("orders").columns(["id", "status"])
```

Read policy also covers filter columns, `RETURNING`, `DISTINCT ON`, grouping
sets, window partition/order expressions, relevant payload right-hand column
references, and recursively checked subqueries/CTEs.

Write policy covers explicit insert, update, upsert, and MERGE target columns.
When a restrictive write rule is active, positional or ambiguous payloads fail
closed because QAIL cannot prove which target column is being written.

## Expressions And Fail-Closed Behavior

The checker allows expressions it can map back to a governed column. Examples:

- `column`
- `table.column`
- JSON access that starts from a concrete column
- aggregate expressions with a concrete column argument
- aliases that preserve a concrete source column name

It rejects shapes that cannot be enforced precisely under a restrictive policy:

- wildcard projections
- unsupported projection expressions
- raw function payload values under restrictive read policy
- restrictive policy on auxiliary tables in `UPDATE FROM` or `DELETE USING`
- restrictive policy on a MERGE table source unless the source is represented
  as an explicit source query with projected columns

This behavior is intentional. If QAIL cannot prove that the AST respects the
policy, the command is denied instead of guessed.

## Relationship To Gateway Policy

`qail-gateway` still has a gateway policy engine for legacy per-table route
rules and filter injection. The native access policy is the current vertical
permission model because it lives in `qail-core` and checks the AST itself.

For new deployments:

1. Use PostgreSQL RLS for row ownership.
2. Use native `[access]` policy for operation and column permissions.
3. Use gateway allow-listing, rate limits, EXPLAIN guardrails, and RPC controls
   as defense-in-depth around exposed routes.

## Production Checklist

- Keep `default_decision = "deny"`.
- Avoid wildcard projections for restricted roles.
- Split public and sensitive columns into explicit read/write rules.
- Require roles and scopes for privileged tables.
- Give source tables a read policy when using MERGE, subqueries, joins, or
  mutation source clauses.
- Use PostgreSQL RLS with a non-superuser role that has `NOBYPASSRLS`.
- Treat `super_admin` contexts as internal-only and log their call sites.
