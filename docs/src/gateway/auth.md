# Authentication & Security

The gateway provides multiple layers of security: JWT authentication,
PostgreSQL Row-Level Security integration, native operation/column access
policy, gateway route policy compatibility, and query allow-listing.

> **Note:** Webhook-based authentication has been removed. JWT (`JWT_SECRET`) is the only supported authentication mechanism. If your reverse proxy needs to authenticate, forward user identity within the JWT — not via custom headers.

---

## JWT Authentication

The gateway validates JWT tokens and extracts tenant context for RLS.

Set `JWT_SECRET` as an environment variable:

```bash
export JWT_SECRET="your-hs256-secret"
```

The extracted claims (`tenant_id`, `user_id`, `role`) are set as PostgreSQL session variables before every query, enabling native RLS enforcement:

```sql
set_config('app.current_tenant_id', '<from JWT>', true);
set_config('app.current_user_id', '<from JWT>', true);
set_config('app.current_agent_id', '<from JWT agent_id claim>', true);
set_config('app.is_super_admin', 'false', true);
```

---

## Header-Based Dev Auth

For development, pass claims directly as headers:

```bash
curl \
  -H "x-user-id: user-123" \
  -H "x-user-role: operator" \
  -H "x-tenant-id: tenant-abc" \
  /api/orders
```

> **Warning:** Header-based auth is only active when `QAIL_DEV_MODE=true` is set.
> This works independently of `JWT_SECRET` — you can have both JWT and dev-mode headers active simultaneously.
> If a `Bearer` token is provided but fails validation, the request is **denied** (not degraded to dev-mode or anonymous).
> Startup is fail-closed in dev mode: gateway boot is rejected unless bind address is localhost and `JWT_SECRET` is set.

---

## Row-Level Security (RLS)

Every query is scoped to the authenticated tenant through PostgreSQL's native
RLS. The gateway sets transaction-local session variables before each query:

```sql
-- Automatically executed before every query:
set_config('app.current_tenant_id', '<from JWT>', true);
set_config('app.current_user_id', '<from JWT>', true);
set_config('app.current_agent_id', '<from JWT agent_id claim>', true);
set_config('app.is_super_admin', 'false', true);
```

Your PostgreSQL RLS policies reference these variables:

```sql
CREATE POLICY tenant_isolation ON orders
  FOR ALL
  USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders FORCE ROW LEVEL SECURITY;
```

The gateway + RLS combination provides database-level multi-tenancy when the
database policies are enabled and the app role cannot bypass RLS.

> **Important:** Your application database role must be a non-superuser with `NOBYPASSRLS`. Superusers bypass RLS even with `FORCE ROW LEVEL SECURITY`.

---

## Native Access Policy

Fine-grained operation and column control is handled by the native access
policy in `qail_core::access`. Enable it in `qail.toml`:

```toml
[access]
enabled = true
path = "access-policy.toml"
```

Example `access-policy.toml`:

```toml
default_decision = "deny"

[tables.orders]
operations = ["read", "update"]
read_columns = { only = ["id", "status", "total", "created_at"] }
write_columns = { only = ["status"] }
returning_columns = { only = ["id", "status"] }
require_any_role = ["operator", "administrator"]
require_scopes = ["orders:read"]

[tables.users]
operations = ["read"]
read_columns = { only = ["id", "email", "display_name"] }
require_any_role = ["administrator"]
require_scopes = ["users:read"]
```

Policy files may be TOML or JSON. They are checked against the QAIL AST before
execution. See [Access Policy](../features/access-policy.md) for full
semantics, including MERGE/source-query handling and fail-closed expression
rules.

> Migration note: `operator_id` JWT claims are preserved in extra claims but are
> not mapped into `tenant_id`. Use a `tenant_id` claim for tenant scope.

## Gateway Policy Compatibility

The gateway still supports the older YAML route policy engine through
`[gateway].policy` / `policy_path` for compatibility with existing deployments:

```yaml
policies:
  - name: orders_operator_read
    table: orders
    role: operator
    operations: [read]
    filter: "tenant_id = $tenant_id"
    allowed_columns: ["id", "status", "total", "created_at"]
```

For new deployments, prefer native `[access]` policy for vertical permissions
because it lives in `qail-core` and checks the AST command directly.

### Column Permissions

Control which columns each role can read or write:

- **Whitelist:** Only the listed columns are returned/allowed.
- **Blacklist:** All columns except the listed ones are returned/allowed.

Policies are applied at the AST level before the query reaches PostgreSQL — denied columns never leave the database.

### Operation Permissions

Control CRUD operations per role per table:

| Permission | Operations |
|-----------|-----------|
| `read` | `GET` list and single |
| `create` | `POST` create |
| `update` | `PATCH` update |
| `delete` | `DELETE` delete |

---

## Query Allow-Listing

Lock down which queries can run in production:

```toml
# qail.toml
[gateway]
allow_list_path = "allow_list.txt"
```

When enabled, any query pattern not in the allow-list is rejected with `403 Forbidden`. This provides defense-in-depth: even if auth is bypassed, only pre-approved query shapes can execute.

---

## RPC Contract Hardening

Harden `/api/rpc/{function}` with strict function naming and signature checks:

```toml
[gateway]
rpc_require_schema_qualified = true
rpc_allowlist_path = "rpc_allowlist.txt"
rpc_signature_check = true
```

`rpc_allowlist_path` format:

```text
# One function per line (case-insensitive)
api.search_orders
public.health_check
```

What each control does:

| Setting | Effect |
|--------|--------|
| `rpc_require_schema_qualified` | Rejects unqualified calls like `search_orders`; requires `schema.function` |
| `rpc_allowlist_path` | Blocks RPC calls not explicitly listed |
| `rpc_signature_check` | For named-arg JSON bodies, rejects unknown argument keys not present in PostgreSQL function signatures |

When `rpc_signature_check=true`, the gateway also uses a parser-only PostgreSQL probe (`PREPARE ...; DEALLOCATE`) to align overload resolution with PostgreSQL itself before execution.

RPC DevEx endpoint:

- `GET /api/_rpc/contracts` returns callable function signatures (`identity_args`, defaults, variadic, return type) for typed client generation.

RPC result format control:

- Optional header `x-qail-result-format: binary` enables binary column format on RPC responses.
- Default is `x-qail-result-format: text`.

---

## Database Auth/TLS Hardening

Gateway database transport/auth policy can be configured through `database_url` query parameters:

```toml
[gateway]
database_url = "postgresql://app:secret@db.internal:5432/app\
?sslmode=require\
&sslrootcert=/etc/qail/ca.pem\
&channel_binding=require\
&auth_mode=scram_only"
```

Supported parameters:

| Parameter | Values | Effect |
|-----------|--------|--------|
| `sslmode` | `disable`, `prefer`, `require` (`verify-ca`/`verify-full` map to `require`) | TLS policy |
| `sslrootcert` | file path | Custom CA bundle for server cert validation |
| `sslcert` + `sslkey` | file paths | Enable mTLS client cert auth |
| `channel_binding` | `disable`, `prefer`, `require` | SCRAM channel-binding policy |
| `auth_mode` | `scram_only`, `gssapi_only`, `compat` | Auth policy preset |
| `auth_scram` / `auth_md5` / `auth_cleartext` | boolean | Fine-grained mechanism toggles |
| `auth_kerberos` / `auth_gssapi` / `auth_sspi` | boolean | Enterprise auth mechanism toggles |
| `gss_provider` | `linux_krb5`, `callback`, `custom` | Selects built-in Linux krb5 provider vs external callback wiring |
| `gss_service` | string (default `postgres`) | Kerberos service used for host-based target (`service@host`) |
| `gss_target` | string | Optional full host-based target override |
| `gss_connect_retries` | integer (default `2`) | Retries transient GSS/Kerberos connect/auth failures |
| `gss_retry_base_ms` | integer ms (default `150`) | Base delay for exponential GSS retry backoff |
| `gss_circuit_threshold` | integer (default `8`) | Failures in window before local GSS circuit opens |
| `gss_circuit_window_ms` | integer ms (default `30000`) | Rolling window for circuit failure counting |
| `gss_circuit_cooldown_ms` | integer ms (default `15000`) | Cooldown while open circuit blocks new connect attempts |

If `sslcert` or `sslkey` is provided, both must be set.

If `gss_provider=linux_krb5` is set, build the gateway with feature `enterprise-gssapi` on Linux.

Startup runs Kerberos preflight checks and emits clear diagnostics for common misconfiguration
(missing explicit credential cache/keytab paths, invalid `KRB5_CONFIG`, etc).
The gateway does not perform enterprise SSO login or ticket acquisition. It
uses the configured provider to consume Kerberos/GSS tokens from your existing
OS credential cache, keytab, sidecar, or identity infrastructure, then applies
the configured database auth policy fail-closed.

Example:

```toml
[gateway]
database_url = "postgresql://app@db.internal:5432/app\
?sslmode=require\
&auth_mode=gssapi_only\
&gss_provider=linux_krb5\
&gss_service=postgres\
&gss_connect_retries=3\
&gss_retry_base_ms=200\
&gss_circuit_threshold=8\
&gss_circuit_window_ms=30000\
&gss_circuit_cooldown_ms=15000"
```

---

## Security Summary

| Threat | Traditional REST | QAIL Gateway |
|--------|-----------------|-------------|
| SQL injection | Possible (one mistake) | **Prevented on AST path** |
| Tenant data leak | Missing WHERE clause | **RLS context set before execution** |
| N+1 catastrophe | Default behavior | **JOIN/expand plus scanner guardrails** |
| Over-fetching | Manual column control | **Native access policy column rules** |
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
