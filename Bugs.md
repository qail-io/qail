# Code Review Fixes (v0.20.0)

All 4 fixes from the senior dev code review have been applied and verified.

## ✅ Fixed

### 1. FFI Panic Safety (Critical)
- **File:** `encoder/src/lib.rs`
- **Issue:** `extern "C"` functions could panic across FFI boundary → UB
- **Fix:** Added `ffi_catch!` macro wrapping 7 functions in `catch_unwind`

### 2. gRPC Mutex Contention (Performance)
- **File:** `qdrant/src/transport.rs`
- **Issue:** `get_sender()` held mutex during async `ready()` → serialized all concurrent requests
- **Fix:** Clone sender under short lock, await `ready()` without lock

### 3. Unsafe Float Cast → bytemuck
- **Files:** `qdrant/Cargo.toml`, `qdrant/src/encoder.rs`
- **Issue:** Raw `unsafe { from_raw_parts }` for `&[f32]` → `&[u8]` casts
- **Fix:** Replaced 3 instances with `bytemuck::cast_slice()` (safe, zero-cost)

### 4. Regex Unwrap Nits
- **File:** `core/src/analyzer/scanner.rs`
- **Issue:** `Regex::new(...).unwrap()` gives unclear panic messages
- **Fix:** Changed to `.expect("valid ... regex")` for 10 patterns

## ⏭️ Deferred

### Env Var Safety in Tests
- **File:** `core/src/config.rs`
- Tests already correctly use `unsafe { set_env() }`. Adding `serial_test` would be ideal but not blocking.

---

## 🔜 Next Problems (from security/audit pass)

> Last audited: 2026-02-13

### ✅ 1. AST Encoder — Panic on Unsupported Action (Medium) — FIXED
- **File:** `pg/src/protocol/ast_encoder/mod.rs`
- **Was:** Unsupported actions hit `panic!()` → crash
- **Status:** No `panic!()` calls remain. All paths return `Result::Err(EncodeError::UnsupportedAction)`.

### ✅ 2. Tenant Guard — Configurable Tenant Column (Low) — FIXED
- **File:** `gateway/src/tenant_guard.rs`, `gateway/src/config.rs`
- **Was:** Hardcoded to `obj.get("operator_id")`.
- **Status:** `verify_tenant_boundary()` now accepts `tenant_column: &str`. Config field `tenant_column` added (default: `"operator_id"`). Passed from all 5 call sites (handler, rest, ws). New `custom_tenant_column` test added.

### ✅ 3. Parser — No Input Length Limit (Low) — FIXED
- **File:** `core/src/parser/mod.rs`
- **Status:** `MAX_INPUT_LENGTH = 64 * 1024` (64 KiB) is enforced before parsing. Input exceeding limit is rejected.

### ✅ 4. Query Allow-List — Wired (Low) — FIXED
- **File:** `gateway/src/middleware.rs`, `gateway/src/server.rs`, `gateway/src/handler.rs`, `gateway/src/config.rs`
- **Was:** `QueryAllowList` struct existed but was dead code.
- **Status:** Added `allow_list_path` config, `allow_list` field on `GatewayState`, loaded at init(). `execute_query()` checks allow-list before parsing, returns 403 `QUERY_NOT_ALLOWED` if rejected.

### ✅ 5. Qdrant — TLS Verification (Low) — FIXED
- **File:** `qdrant/src/transport.rs`
- **Status:** Uses `rustls` + `webpki_roots::TLS_SERVER_ROOTS` for cert verification. `connect_tls()` establishes verified TLS. Auto-detection via URL scheme.

### ✅ 6. Health Endpoint — Metrics Split (Low) — FIXED
- **File:** `gateway/src/handler.rs`, `gateway/src/router.rs`
- **Was:** `/health` returned pool stats and tenant_guard violation counts publicly.
- **Status:** `/health` now returns only `{status, version}`. Full metrics moved to `/health/internal` (pool stats + tenant guard snapshot). Operators should restrict `/health/internal` via auth or network policy.

### 🟢 7. Analyzer Regex — ReDoS Risk (Low) — PARTIALLY FIXED
- **File:** `core/src/analyzer/scanner.rs:77-86`
- **Status:** `.unwrap()` → `.expect("valid ... regex")` applied (all 10 patterns). ReDoS risk from `.+?` patterns **not yet fuzzed or simplified**.

### ✅ 8. CLI — Credential Redaction (Low) — FIXED
- **File:** `cli/src/util.rs`, `cli/src/worker.rs`
- **Was:** Passwords in connection URLs printed to console.
- **Status:** `redact_url()` function added — replaces password with `***`. Applied to `worker.rs` console output. 4 unit tests covering all URL shapes.