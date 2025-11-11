# Authentication & Security

The gateway provides multiple layers of security: JWT authentication, PostgreSQL Row-Level Security integration, a YAML policy engine, and query allow-listing.

> **Note:** Webhook-based authentication has been removed. JWT (`JWT_SECRET`) is the only supported authentication mechanism. If your reverse proxy needs to authenticate, forward user identity within the JWT — not via custom headers.

---

## JWT Authentication

The gateway validates JWT tokens and extracts tenant context for RLS.

Set `JWT_SECRET` as an environment variable or in `qail.toml`:

```toml
# qail.toml
[gateway]
jwt_secret = "your-hs256-secret"
```

Or set via environment variable (takes precedence over TOML):

```bash
export JWT_SECRET="your-hs256-secret"
```

The extracted claims (`operator_id`, `user_id`, `role`) are set as PostgreSQL session variables before every query, enabling native RLS enforcement:

```sql
set_config('app.current_operator_id', '<from JWT>', false);
set_config('app.current_user_id', '<from JWT>', false);
set_config('app.role', '<from JWT>', false);
```

---

## Header-Based Dev Auth

For development, pass claims directly as headers:

```bash
curl -H "x-operator-id: uuid" -H "x-user-id: uuid" /api/orders
```

> **Warning:** Header-based auth is only active when `QAIL_DEV_MODE=true` is set **and** `JWT_SECRET` is **not set**. The gateway will **refuse to start** in dev mode when the bind address is not `localhost` — preventing accidental exposure of header-based auth on public interfaces.

---
---

## Row-Level Security (RLS)

Every query is automatically scoped to the authenticated tenant via PostgreSQL's native RLS. The gateway sets session variables before each query:

```sql
-- Automatically executed before every query:
set_config('app.current_operator_id', '<from JWT>', false);
set_config('app.current_user_id', '<from JWT>', false);
set_config('app.role', '<from JWT>', false);
```

Your PostgreSQL RLS policies reference these variables:

```sql
CREATE POLICY tenant_isolation ON orders
  FOR ALL
  USING (operator_id = current_setting('app.operator_id')::uuid);

ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders FORCE ROW LEVEL SECURITY;
```

**No manual WHERE clauses needed.** The gateway + RLS combination provides database-level multi-tenancy.

> **Important:** Your application database role must be a non-superuser with `NOBYPASSRLS`. Superusers bypass RLS even with `FORCE ROW LEVEL SECURITY`.

---

## YAML Policy Engine

Fine-grained access control per table, per role:

```yaml
policies:
  orders:
    roles:
      agent:
        select: true
        insert: true
        update: true
        delete: false
        columns: ["id", "status", "total", "created_at"]  # Column-level
        filter:                                             # Row-level
          operator_id: "x-operator-id"
      viewer:
        select: true
        columns: ["id", "status"]
```

### Column Permissions

Control which columns each role can read or write:

- **Whitelist:** Only the listed columns are returned/allowed.
- **Blacklist:** All columns except the listed ones are returned/allowed.

Policies are applied at the AST level before the query reaches PostgreSQL — denied columns never leave the database.

### Operation Permissions

Control CRUD operations per role per table:

| Permission | Operations |
|-----------|-----------|
| `select` | `GET` list and single |
| `insert` | `POST` create |
| `update` | `PATCH` update |
| `delete` | `DELETE` delete |

---

## Query Allow-Listing

Lock down which queries can run in production:

```toml
# qail.toml
[gateway.security]
allow_list_enabled = true
allow_list = [
  "GET /api/orders",
  "GET /api/orders/:id",
]
complexity_limit = 10
rate_limit = 100
```

When enabled, any query pattern not in the allow-list is rejected with `403 Forbidden`. This provides defense-in-depth: even if auth is bypassed, only pre-approved query shapes can execute.

---

## Security Summary

| Threat | Traditional REST | QAIL Gateway |
|--------|-----------------|-------------|
| SQL injection | Possible (one mistake) | **Impossible** (binary AST) |
| Tenant data leak | Missing WHERE clause | **RLS auto-injected** |
| N+1 catastrophe | Default behavior | **Structurally impossible** |
| Over-fetching | Manual column control | **Policy-enforced** |
| Query abuse | Rate limiting only | **Allow-list + rate limit** |

---

## Internal Endpoint Protection (M4)

The `/metrics` and `/health/internal` endpoints expose operational details. Protect them in production:

```toml
# qail.toml
[gateway]
admin_token = "your-secret-admin-token"
```

When set, both endpoints require `Authorization: Bearer <admin_token>`. Without the token, they return `401 Unauthorized`.

Alternatively, restrict access via network policy (firewall rules, reverse proxy).
