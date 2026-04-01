# Authentication & Security

The gateway provides multiple layers of security: JWT authentication, PostgreSQL Row-Level Security integration, a YAML policy engine, and query allow-listing.

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
set_config('app.current_tenant_id', '<from JWT>', false);
set_config('app.current_operator_id', '<from JWT>', false); -- legacy compat alias
set_config('app.current_user_id', '<from JWT>', false);
set_config('app.role', '<from JWT>', false);
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

Every query is automatically scoped to the authenticated tenant via PostgreSQL's native RLS. The gateway sets session variables before each query:

```sql
-- Automatically executed before every query:
set_config('app.current_operator_id', '<from JWT>', false);
set_config('app.current_tenant_id', '<from JWT>', false);
set_config('app.current_user_id', '<from JWT>', false);
set_config('app.role', '<from JWT>', false);
```

Your PostgreSQL RLS policies reference these variables:

```sql
CREATE POLICY tenant_isolation ON orders
  FOR ALL
  USING (tenant_id = current_setting('app.current_tenant_id')::uuid);

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
  - name: orders_agent_read
    table: orders
    role: agent
    operations: [read]
    filter: "tenant_id = $tenant_id"
    allowed_columns: ["id", "status", "total", "created_at"]
  - name: orders_viewer_read
    table: orders
    role: viewer
    operations: [read]
    allowed_columns: ["id", "status"]
```

> Compatibility: gateway JWT parsing still accepts legacy `operator_id` claims and maps them into `tenant_id` when `tenant_id` is absent.

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
