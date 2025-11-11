# Security Audit Report — QAIL as Hasura Replacement

**Date:** 2025-02-12  
**Scope:** Gateway, auth, policy engine, REST API, FFI encoder, multi-tenancy

---

## Executive Summary

QAIL has strong foundations: AST-native SQL (no injection), RLS integration, policy engine with SQL escaping, rate limiting, and body size limits. Several issues should be addressed before production deployment as a Hasura replacement.

**Critical:** 1  
**High:** 2  
**Medium:** 4  
**Low:** 3  

---

## Critical Findings

### C1. Webhook Auth Bypass — Complete Authentication Bypass

**File:** `gateway/src/auth.rs:270-295`

**Issue:** `try_webhook_auth()` never calls the webhook URL. It only reads `x-webhook-user-id`, `x-webhook-role`, and `x-webhook-tenant-id` from the incoming request headers.

```rust
fn try_webhook_auth(headers: &HeaderMap, _webhook_url: &str) -> Option<AuthContext> {
    // _webhook_url is UNUSED
    let user_id = headers.get("x-webhook-user-id").and_then(|v| v.to_str().ok())?;
    let role = headers.get("x-webhook-role")...
    let tenant_id = headers.get("x-webhook-tenant-id")...
```

**Impact:** With `WEBHOOK_AUTH_URL` set, any client can send spoofed headers and authenticate as any user/tenant.

**Remediation:**
- **Option A:** Implement real webhook verification: HTTP POST to the webhook with request headers, parse response body (or response headers) for auth context.
- **Option B:** Disable webhook auth when the gateway is not behind a trusted proxy that performs the webhook call and sets these headers. Add a config flag `webhook_auth_trust_proxy_only`.
- **Option C:** Remove webhook auth until it is fully implemented.

---

## High Findings

### H1. Policy `$tenant_id` Placeholder Not Supported

**File:** `gateway/src/policy.rs:167-185`

**Issue:** `expand_filter()` replaces `$user_id`, `$role`, and keys from `auth.claims`, but **not** `$tenant_id`. Example policies use `filter: "tenant_id = $tenant_id"`, which stays literal and does not scope by tenant.

```rust
fn expand_filter(&self, template: &str, auth: &AuthContext) -> String {
    result = result.replace("$user_id", ...);
    result = result.replace("$role", ...);
    for (key, value) in &auth.claims { ... }  // tenant_id not in claims
    // $tenant_id is never replaced!
}
```

**Impact:** Tenant isolation policies that rely on `$tenant_id` do not work. RLS at the DB layer may still apply if `auth.to_rls_context()` sets operator_id, but gateway-level policy filters will be incorrect.

**Remediation:** Add `$tenant_id` expansion in `expand_filter()`:
```rust
if let Some(ref tid) = auth.tenant_id {
    result = result.replace("$tenant_id", &format!("'{}'", tid.replace('\'', "''")));
}
```

---

### H2. Sensitive Data in Logs

**File:** `gateway/src/handler.rs:107`

**Issue:** Full query text is logged at `info` level:
```rust
tracing::info!("Executing text query: {} (user: {})", query_text, auth.user_id);
```

**Impact:** PII, secrets, or PHI in queries will appear in logs and log aggregation.

**Remediation:**
- Log at `debug` or `trace` only.
- Optionally hash/summarize queries for audit (e.g., table + action).
- Ensure log retention and access controls meet compliance.

---

## Medium Findings

### M1. CORS Defaults to Allow All Origins

**File:** `gateway/src/router.rs:112-114`

**Issue:** When `cors_allowed_origins` is empty, `allow_origin(Any)` is used.

**Impact:** Any origin can make credentialed requests if the frontend sends cookies/credentials. Increases risk of CSRF or confused deputy if credentials are used.

**Remediation:** For production, require explicit origins. Consider changing default to deny when `cors_allowed_origins` is empty, or document that production must set this.

---

### M2. Dev Mode Auth Can Be Enabled in Production

**File:** `gateway/src/auth.rs:234-239`

**Issue:** `QAIL_DEV_MODE=true` enables header-based auth (`x-user-id`, `x-role`, `x-tenant-id`) without JWT. If this env var is set in production, anyone can spoof identity.

**Remediation:**
- Add a startup check that fails if `QAIL_DEV_MODE` is set and `JWT_SECRET` is unset.
- Or bind dev mode to `RUST_LOG` / a separate `NODE_ENV`-style variable.

---

### M3. FFI Encoder — Missing Null Checks on Some Pointers

**File:** `encoder/src/lib.rs`

**Issue:** Some FFI functions use `CStr::from_ptr` after a null check, but others do not. Examples:
- `qail_encode_parse`: `sql` is checked, but if `name` is null it is handled; when `name` is non-null, `CStr::from_ptr(name)` is used correctly.
- `qail_encode_bind_execute_batch`: `params.add(i)` may yield invalid pointers if `params` is non-null but points to an undersized array.

**Impact:** Undefined behavior if C callers pass invalid pointers (e.g., truncated arrays).

**Remediation:** Add explicit null checks before all `from_ptr`/deref. Validate array bounds for `params` and `params_count` before indexing.

---

### M4. `/api/_schema` and `/metrics` Lack Auth

**Files:** `gateway/src/rest.rs`, `gateway/src/metrics.rs`

**Issue:** `/api/_schema` (schema introspection) and `/metrics` (Prometheus) are unauthenticated. Schema exposure can reveal table/column structure; metrics can expose internal behavior.

**Remediation:**
- Protect with API key, mTLS, or network isolation (e.g., Prometheus scraping from trusted network only).
- Or add config: `schema_endpoint_public: false`, `metrics_endpoint_public: false` with auth when disabled.

---

## Low Findings

### L1. Document Webhook Auth Proxy Requirement

**File:** `docs/src/gateway/auth.md`

**Issue:** Docs say “The gateway forwards the specified headers to your auth endpoint,” but the implementation does not call the webhook. If the design is “trust proxy headers,” this must be clearly documented.

**Remediation:** Update docs to state that webhook auth **requires** a trusted reverse proxy that performs the webhook call and sets `x-webhook-*` headers. Warn that direct exposure with `WEBHOOK_AUTH_URL` allows header spoofing.

---

### L2. `qail_response_get_string` Returns Dangling Pointer Documentation

**File:** `encoder/src/lib.rs:679-681`

**Issue:** The returned pointer is valid only until the response is freed. Callers can easily use it after `qail_response_free()`.

**Remediation:** Document clearly and consider returning owned data (e.g., copy into caller buffer) or a different lifecycle model.

---

---

## What’s Solid

| Area | Status |
|------|--------|
| SQL injection | AST-native; values are parameterized or escaped |
| Policy filter injection | `expand_filter` escapes `$user_id`, `$role`, claims |
| RLS and tenant isolation | `acquire_raw` restricted; RLS context set before queries |
| Rate limiting | Token bucket, IP-based, max bucket cap |
| Body size | 2 MiB limit |
| Security headers | X-Content-Type-Options, X-Frame-Options, Referrer-Policy |
| Query cost checks | EXPLAIN pre-check with cost/row limits |
| Connection reuse | DISCARD ALL + RLS reset between tenants |
| Super admin abuse | JWT `is_super_admin` claim ignored; only role used |

---

## Hasura Comparison Checklist

| Feature | QAIL | Notes |
|--------|------|-------|
| JWT auth | Yes | HS256/RS256 |
| Permissions per role/table | Yes | YAML policy engine |
| RLS integration | Yes | PostgreSQL session vars |
| SQL injection resistance | Yes | AST-based |
| Rate limiting | Yes | Per-IP |
| CORS | Configurable | Default allows all — tighten for prod |
| Schema introspection | Yes | `/api/_schema` — consider protecting |
| Webhook auth | Broken | Fix or disable |
| Tenant isolation | Yes | RLS + policy filters |

---

---

## Extended Audit (Phase 2)

### E1. Cache Key Missing `tenant_id` — Cross-Tenant Cache Poisoning (High)

**Files:** `gateway/src/handler.rs:219`, `gateway/src/rest.rs:568`

**Issue:** Cache keys use `user_id` but not `tenant_id`:
- `/qail` handler: `format!("{}:{}", auth.user_id, shape_cache_key(cmd))`
- REST handler: `format!("rest:{}:{}:{}", table_name, auth.user_id, request.uri())`

When the same user has access to multiple tenants (e.g. one person managing several orgs), their `user_id` is identical across tenants. Tenant A runs `GET orders` → cached. Tenant B (same user, different tenant) runs `GET orders` → cache HIT → receives Tenant A's data.

**Remediation:** Include `tenant_id` (or `operator_id`) in all cache keys:
```rust
let tenant = auth.tenant_id.as_deref().unwrap_or("anon");
let cache_key = format!("{}:{}:{}", tenant, auth.user_id, shape_cache_key(cmd));
```

---

### E2. Batch Endpoint — No Query Count Limit (Medium)

**File:** `gateway/src/handler.rs:356-370`

**Issue:** `execute_batch` accepts `Vec<String>` with no upper bound. An attacker can send thousands of queries in one request, exhausting DB connections or CPU.

**Remediation:** Add `max_batch_queries` config (default 50–100), reject with 413 if exceeded.

---

### E3. Bincode Deserialization — Allocation Bombs (Medium)

**File:** `gateway/src/handler.rs:165`

**Issue:** `bincode::deserialize::<Qail>(&body)` uses default bincode options. A crafted payload can trigger excessive allocation or deeply nested deserialization (OOM / CPU exhaustion).

**Remediation:** Use `bincode::options()` with byte limit:
```rust
bincode::options().with_limit(64 * 1024) // 64 KiB max
    .with_fixint_encoding()
    .deserialize(&body)
```

---

### E4. Event Trigger Webhook — SSRF (Medium)

**File:** `gateway/src/event.rs:220-227`

**Issue:** Webhook URLs come from YAML config. If an attacker can influence the events config (e.g. via file write or misconfig), they could use `http://169.254.169.254/` (AWS metadata), `http://localhost:22`, or `file:///etc/passwd`. The reqwest client will POST to whatever URL is configured.

**Remediation:**
- Block private IP ranges, `localhost`, link-local, `file://`, etc.
- Validate URL scheme (https only in production).
- Consider allowlist of domains.

---

### E5. WebSocket LiveQuery — `table` Not Validated (Low)

**File:** `gateway/src/ws.rs:392-398`

**Issue:** `LiveQuery { table }` uses `table` directly in the NOTIFY channel name: `format!("{}_qail_table_{}", tenant_id, table)`. No validation that `table` exists in schema or matches expected pattern. Malformed names could cause odd behavior or errors.

**Remediation:** Validate `table` against `state.schema.table_names()` before use.

---

### E6. Branch Endpoints — No Role Check (Low)

**Files:** `gateway/src/rest.rs:2096-2159`, `2195-2243`, `2247-2381`

**Issue:** Branch create/delete/merge require `auth.is_authenticated()` but not a specific role. Any authenticated user can create or merge branches, which can affect data virtualization.

**Remediation:** Require `admin` or `super_admin` (or a configurable role) for branch operations.

---

### E7. Config File Path Traversal (Low)

**File:** `gateway/src/schema.rs:103`, `gateway/src/policy.rs:71`, `gateway/src/event.rs:121`

**Issue:** `load_from_file(path)` uses `fs::read_to_string(path)` with paths from config. If config is attacker-controlled, `../../../etc/passwd` could be used.

**Remediation:** Resolve path with `std::path::Path::canonicalize()` and enforce it stays under a configured root. Config is usually ops-controlled, so this is defense-in-depth.

---

### E8. EXPLAIN Pre-Check Fail-Open (Low)

**File:** `gateway/src/rest.rs:636-654`

**Issue:** When EXPLAIN parse fails or the query fails, the code logs and **allows** the query:
```rust
Ok(None) => { /* Parse failure — allow */ ... }
Err(e) => { /* EXPLAIN failed — allow */ ... }
```

**Impact:** Under error conditions, cost/row checks are bypassed; expensive queries can run.

**Remediation:** In `ExplainMode::Enforce`, reject when EXPLAIN fails. In `Precheck`, document the fail-open behavior as a tradeoff.

---

## Recommended Actions (Priority Order)

1. **Fix or disable webhook auth** (C1). ✅ Removed.
2. **Add `tenant_id` to cache keys** (E1). ✅ Fixed.
3. **Add `$tenant_id` to policy `expand_filter`** (H1). ✅ Fixed.
4. **Downgrade or sanitize query logging** (H2). ✅ Fixed.
5. **Add batch query limit** (E2). ✅ Fixed.
6. **Add bincode deserialization limits** (E3). ✅ Fixed.
7. **Harden CORS and dev mode** for production (M1, M2). ✅ Fixed.
8. **Protect `/api/_schema` and `/metrics`** or restrict by network (M4). ⏳ Deferred (requires new config).
9. **Validate WebSocket `table` and restrict branch operations** (E5, E6). ✅ Fixed.
10. **Event webhook SSRF** (E4). ✅ Fixed.
11. **EXPLAIN fail-open** (E8). ✅ Fixed.
12. **Config path traversal** (E7). ✅ Fixed.
13. **FFI null checks** (M3). ⏳ Deferred (encoder crate).
14. **Document webhook auth** (L1). ⏳ Pending (docs task).
15. **Document FFI pointer lifetime** (L2). ⏳ Pending (docs task).

---

## Remediation Status (2026-02-13)

| Finding | Severity | Status | Evidence |
|---------|----------|--------|----------|
| C1 | Critical | ✅ Fixed | `try_webhook_auth` removed from `auth.rs` |
| H1 | High | ✅ Fixed | `$tenant_id` expanded in `policy.rs:176` |
| H2 | High | ✅ Fixed | `handler.rs:136` uses `tracing::debug!` |
| M1 | Medium | ✅ Fixed | `cors_strict` config flag + warning in `router.rs:114` |
| M2 | Medium | ✅ Fixed | `check_dev_mode_safety()` in `server.rs:291` |
| M3 | Medium | ⏳ Deferred | FFI encoder crate — out of gateway scope |
| M4 | Medium | ⏳ Deferred | Requires new auth middleware for internal endpoints |
| L1 | Low | ⏳ Pending | Docs update needed |
| L2 | Low | ⏳ Pending | FFI doc comment needed |
| L3 | Low | ❌ Removed | cargo audit not used |
| E1 | High | ✅ Fixed | Cache key includes tenant at `handler.rs:298` |
| E2 | Medium | ✅ Fixed | `max_batch_queries` at `handler.rs:452` |
| E3 | Medium | ✅ Fixed | `bincode::options().with_limit(64*1024)` at `handler.rs:195` |
| E4 | Medium | ✅ Fixed | `validate_webhook_url()` at `event.rs:283` |
| E5 | Low | ✅ Fixed | Schema validation at `ws.rs:342` |
| E6 | Low | ✅ Fixed | Admin role check at `rest.rs:2124` |
| E7 | Low | ✅ Fixed | `validate_config_path()` at `config.rs:240`, wired in `server.rs` |
| E8 | Low | ✅ Fixed | Reject in `Enforce` mode at `rest.rs:641` |

