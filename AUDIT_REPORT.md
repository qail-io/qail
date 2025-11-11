# Codebase Security Audit

**Date:** 2026-02-13  
**Scope:** Full codebase scan for security-sensitive patterns

---

## 2. Panics in Request Paths

| File | Line | Issue | Severity |
|------|------|-------|----------|
| `gateway/src/router.rs` | 117 | CORS panic when `cors_strict=true` and empty origins | **By design** — fail-fast at startup |
| `pg/src/protocol/ast_encoder/values/expressions.rs` | 738 | ~~`panic!` on unsupported subquery action~~ | ✅ **Fixed** — AST with e.g. `Value::Subquery(Add(...))` crashes gateway |
| `qdrant/src/decoder.rs` | 774 | `panic!("Expected Float")` in decoder | **Test-only** — malformed Qdrant response |

✅ Fixed — returns `Err(EncodeError::UnsupportedAction)` for non-Get subqueries.

---

## 3. Unsafe / FFI

| File | Issue | Status |
|------|-------|--------|
| `encoder/src/lib.rs` | 22 FFI functions; `CStr::from_ptr`, `from_raw_parts`, `*params.add(i)` | Null checks present; ✅ `params` array bounds documented |
| `core/src/config.rs` | `unsafe { set_env }` in tests | Isolated to test code |
| `pg/examples/libpq_*.rs` | Raw libpq FFI | Examples only |

~~**FFI params bounds:** `qail_encode_bind_execute_batch` iterates `0..params_count` and does `unsafe { *params.add(i) }`. If the caller provides a smaller array, this is UB. Document that `params` must have at least `params_count` elements, or add a runtime check.~~ ✅ Safety doc added.

---

## 4. Deserialization

| File | Pattern | Status |
|------|---------|--------|
| `gateway/src/handler.rs` | `bincode::options().with_limit(64*1024)` | ✅ E3 fixed |
| `gateway/src/rest.rs` | `serde_json::from_slice(&body)` | Body limited to 1 MiB by `axum::body::to_bytes`; **serde_json** can still OOM on deeply nested JSON (e.g. 10⁶ levels) |
| `gateway/src/ws.rs` | `serde_json::from_str::<WsClientMessage>` | No explicit limit on WebSocket message size |
| `cli/src/shadow.rs` | `serde_json::from_str(&diff_json)` | Diff from migration; typically trusted |
| `core/src/schema.rs` | `serde_json::from_str(json)` | Schema content; add size cap if from untrusted source |

**Recommendation:** For REST/WS JSON from untrusted clients, consider `serde_json::from_slice` with a pre-sized buffer or a crate that supports depth/size limits.

---

## 5. File Reads

| File | Pattern | Risk |
|------|---------|------|
| `gateway/src/schema.rs` | `fs::read_to_string(path)` | Path from config; E7 canonicalize in place |
| `gateway/src/policy.rs` | `fs::read_to_string(path)` | Same |
| `gateway/src/event.rs` | `fs::read_to_string(path)` | Same |
| `core/src/analyzer/scanner.rs` | `fs::read_to_string(path)` | Path from analysis target; consider size limit |
| `cli/*` | Various `read_to_string` | Config/migration files; generally trusted |

**Note:** Large config files (e.g. 100+ MB) could cause memory pressure. A global `MAX_CONFIG_SIZE` would be defense-in-depth.

---

## 6. Process::Command

| File | Command | User Input? |
|------|---------|-------------|
| `cli/src/exec.rs` | `ssh -N -L ...` | `remote_host`, `remote_port`, `ssh_host` from URL/config |
| `cli/src/init.rs` | `lsof`, runtime binary | Config / discovery |
| `cli/src/time.rs` | `date` | Format strings |
| `core/src/build.rs` | `qail`, `cargo` | Build-time |
| `qdrant/examples/upsert_test.rs` | `curl` | Example only |

**Recommendation:** Validate `ssh_host`, `remote_host` in `exec.rs` (e.g. no `;`, `|`, `$()`); ensure they are not shell-injected.

---

## 7. Credentials & Secrets

| Area | Status |
|------|--------|
| `redact_url()` | ✅ Applied in CLI output (L1) |
| JWT secret | From env; not logged |
| `admin_token` | Config; bearer only |
| PG password | Parsed from URL; not logged in handler |
| Tests with `PASSWORD` | `tls_integration.rs` uses constant; acceptable for tests |

---

## 8. Unwrap / Expect in Production

High-density `.unwrap()` / `.expect()` in: `gateway`, `pg`, `qdrant`, `core`. Most are in tests or non-failure paths. Critical request handlers generally use `?` and propagate errors.

**Notable:** `gateway/src/rest.rs:433` — `serde_json::from_str(&s).unwrap_or(Value::String(s))` — fallback to string on parse failure; ensure `s` is not attacker-controlled in a security-sensitive way.

---

## 9. Summary

| Category | Findings |
|----------|----------|
| **Critical** | 0 |
| **High** | 0 |
| **Medium** | ~~1~~ 0 — `expressions.rs:738` ✅ fixed |
| **Low** | 1 — serde_json depth/OOM risk on REST (optional hardening) |
| **Info** | SSH arg validation, config file size cap |

---

## 10. Recommended Fixes

1. ~~**Replace panic in expressions.rs:738**~~ ✅ Done — returns `Err(EncodeError::UnsupportedAction)`
2. **Optional:** Add `MAX_CONFIG_SIZE` for schema/policy/event file reads.
4. **Optional:** Validate `ssh_host` / `remote_host` in CLI SSH tunnel to block shell metacharacters.
