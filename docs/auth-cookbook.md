# Auth Cookbook — Qail Gateway

How authentication flows from HTTP request to Row-Level Security (RLS) isolation.

---

## Architecture

```
Client Request
    │  Authorization: Bearer <jwt>
    ▼
┌─────────────────────────────────────┐
│  extract_auth_from_headers()        │
│  → JwtClaims { sub, role,           │
│    tenant_id, operator_id, extra }  │
│  → AuthContext { user_id, role,     │
│    tenant_id, claims }              │
└──────────────┬──────────────────────┘
               │
               ▼  (if tenant_id missing)
┌─────────────────────────────────────┐
│  enrich_with_operator_map()         │
│  user_id → operator_id lookup       │
│  (cache loaded at startup)          │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  to_rls_context()                   │
│  → RlsContext { operator_id,        │
│    agent_id, is_super_admin }       │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  fetch_all_with_rls(cmd, rls_sql)   │
│  Pipelined: BEGIN + set_config()    │
│  + query in single roundtrip        │
└─────────────────────────────────────┘
```

---

## 1. JWT Authentication (Production)

Set one env var:

```bash
JWT_SECRET=your-256-bit-secret
```

### JWT Payload Format

**Standard JWT:**
```json
{
  "sub": "user-uuid-123",
  "role": "admin",
  "tenant_id": "operator-uuid-456",
  "exp": 1739000000
}
```

**Engine-style JWT** (also supported):
```json
{
  "user_id": "user-uuid-123",
  "role": "SuperAdmin",
  "operator_id": "operator-uuid-456",
  "exp": 1739000000
}
```

Both `sub`/`user_id` and `tenant_id`/`operator_id` are accepted via `serde(alias)`.

### Supported Algorithms

| Env Var | Algorithm | Key Type |
|---------|-----------|----------|
| `JWT_SECRET` | HS256 (default) | Shared secret |
| `JWT_PUBLIC_KEY` | RS256/RS384/RS512 | RSA public key (PEM) |

### Request Format

```bash
curl -X POST http://localhost:8080/qail \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9..." \
  -d 'get orders fields id, total, status limit 10'
```

---

## 2. Header-Based Auth (Development Only)

For local development without JWT infrastructure:

```bash
QAIL_DEV_MODE=true
```

```bash
curl -X POST http://localhost:8080/qail \
  -H "X-User-ID: user-123" \
  -H "X-User-Role: admin" \
  -H "X-Tenant-ID: operator-456" \
  -d 'get orders fields id, total limit 10'
```

> **⚠️ Security:** Dev mode is blocked from starting if bind address is not `localhost` or if `JWT_SECRET` is unset. This prevents accidental production exposure.

---

## 3. RLS Context Mapping

The gateway maps `AuthContext` → `RlsContext` for Postgres-native RLS:

| AuthContext field | RLS session variable | PostgreSQL usage |
|---|---|---|
| `tenant_id` | `app.operator_id` | `current_setting('app.operator_id')` |
| `claims["agent_id"]` | `app.agent_id` | `current_setting('app.agent_id')` |
| `role == "super_admin" \| "Administrator"` | Super admin bypass | Skips RLS entirely |

### How It's Applied

```sql
-- Pipelined in a single roundtrip (no extra latency)
BEGIN;
SET LOCAL statement_timeout = '5000';
SELECT set_config('app.operator_id', 'op-123', true);
SELECT set_config('app.agent_id', '', true);

-- Your query runs here (same roundtrip)
SELECT id, name FROM users WHERE active = true;
```

All `SET LOCAL` variables reset automatically when `COMMIT` runs on connection release.

---

## 4. Tenant Resolution (Missing tenant_id)

Some JWTs only contain `user_id` without `tenant_id`. The gateway resolves this at startup:

```
Startup: SELECT id, operator_id FROM users → user_operator_map cache
Runtime: auth.enrich_with_operator_map(&cache) fills in tenant_id
```

**Rules:**
- Does NOT overwrite existing `tenant_id` from JWT
- Skips anonymous users
- Cache is `Arc<RwLock<HashMap<String, String>>>` — thread-safe reads

---

## 5. RLS Policy Examples

### Basic tenant isolation

```sql
CREATE POLICY tenant_isolation ON orders
  FOR ALL
  TO app_user
  USING (operator_id = current_setting('app.operator_id')::uuid);
```

### Agent-scoped access

```sql
CREATE POLICY agent_orders ON orders
  FOR SELECT
  TO app_user
  USING (
    operator_id = current_setting('app.operator_id')::uuid
    AND (
      current_setting('app.agent_id') = ''
      OR agent_id = current_setting('app.agent_id')::uuid
    )
  );
```

### In QAIL schema syntax

```
policy tenant_isolation on orders
  for all
  to app_user
  using $$ operator_id = current_setting('app.operator_id')::uuid $$
```

---

## 6. Environment Variables Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | — | PostgreSQL connection string |
| `BIND_ADDRESS` | No | `0.0.0.0:8080` | Server bind address |
| `JWT_SECRET` | Prod: Yes | — | HMAC secret for JWT validation |
| `JWT_PUBLIC_KEY` | Alt | — | RSA public key (PEM) for RS256 |
| `QAIL_DEV_MODE` | No | `false` | Enable header-based auth (dev only) |
| `ADMIN_TOKEN` | No | — | Bearer token for `/health/internal` and `/metrics` |
| `RATE_LIMIT_RATE` | No | `100` | Requests per second |
| `POOL_MIN_CONNECTIONS` | No | `5` | Minimum PG pool connections |
| `STATEMENT_TIMEOUT_MS` | No | `5000` | Query timeout in milliseconds |

---

## 7. Security Guarantees

- **No RLS bypass via JWT claims** — `is_super_admin` in JWT extra claims is ignored; only `role == "super_admin" | "Administrator"` grants bypass
- **Integer tenant_id rejected** — `tenant_id: 42` causes full JWT parse failure, not silent coercion
- **Empty sub detected** — `sub: ""` correctly fails `is_authenticated()`
- **FinanceAdmin scoped** — Finance roles do NOT bypass RLS; use database-level policies
- **Connection safety** — `PooledConnection::Drop` destroys unreleased connections to prevent cross-tenant leaks
- **SET LOCAL scoping** — All RLS variables are transaction-scoped; `COMMIT` resets everything

All guarantees are verified by 11 red-team tests in `gateway/src/auth.rs`.
