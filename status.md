# Status (v0.20.0)

**Last updated:** 2026-02-13

## ✅ Done

### Bugs (Bugs.md)
- AST encoder — panic → `Result::Err(EncodeError::UnsupportedAction)`
- Tenant guard — configurable `tenant_column` (default `operator_id`)
- Parser — `MAX_INPUT_LENGTH` 64 KiB enforced
- Query allow-list — wired; `execute_query()` checks before parse
- Qdrant TLS — cert verification via rustls + webpki_roots
- Health endpoint — split to `/health` (public) vs `/health/internal` (metrics)
- CLI — `redact_url()` for credential redaction in error output

### PostgreSQL / AST (gap.md)
- Wire batch: Get, Add, Set, Del, **Cnt**, **Export**; DDL/utility delegates to `encode_cmd_sql_to`
- Expr::Mod, Expr::Def, Expr::Raw — no longer fall back to `*`
- Exists / NotExists — proper subquery semantics
- `encode_cmd_sql` synced with `encode_cmd_sql_to` (+16 DDL actions)
- DISTINCT ON, RETURNING, COPY, GROUPING SETS, Window, LATERAL, INTERVAL — supported
- JsonExists / JsonQuery / JsonValue — transpiler coverage
- RECURSIVE CTEs — full support

### Security Hardening
- Analyzer ReDoS — `.+?` → `[^\n]+?` + 4096-char line guard in `scanner.rs`
- JSON/JSONB operators — `?|`, `?&`, `#>`, `#>>` in operators + wire encoder
- Value::Function — injection guard (`;`, `--`, `/*`) + length cap in `expressions.rs`
- INSERT ON CONFLICT — already supported (`DoNothing` / `DoUpdate`) in wire encoder
- RECURSIVE CTEs — parser, transpiler, wire encoder all support `WITH RECURSIVE`

### Roadmap (roadmap.md)
- §10 **Infrastructure-aware compiler** — schema `bucket`/`queue`/`topic`, build-time validation

### Security Audit (SECURITY_AUDIT.md)
- **C1** Webhook auth — removed (`try_webhook_auth` deleted)
- **H1** Policy `$tenant_id` — expansion added in `expand_filter()`
- **H2** Sensitive data in logs — query text moved to `debug`/`trace`
- **M1** CORS — `cors_strict` flag to deny startup without explicit origins
- **M2** Dev mode auth — `check_dev_mode_safety()` blocks non-localhost dev mode
- **M3** FFI encoder — null checks + bounds-safe `.get()` verified
- **M4** `/api/_schema` and `/metrics` — `admin_token` config for bearer auth
- **L1** Auth docs — webhook removal noted, admin_token documented
- **L2** FFI `qail_response_get_string` — pointer lifetime doc added
- **E1** Cache key — tenant_id included in cache keys
- **E2** Batch limit — `max_batch_queries` enforced
- **E3** Bincode — `with_limit(64 * 1024)` allocation guard
- **E4** Event webhook SSRF — private IP/localhost blocked
- **E5** WebSocket LiveQuery — `table` validated against schema
- **E6** Branch endpoints — role check (admin/super_admin)
- **E7** Config path traversal — canonicalize + root validation
- **E8** EXPLAIN pre-check — fail-closed when EXPLAIN fails

### Audit (AUDIT_REPORT.md)
- **expressions.rs:738** — `Value::Subquery` with non-Get action now returns `Err(UnsupportedAction)` instead of panic

## 🔜 Next (PG_DOC_COVERAGE §8)

**SQL:** CALL, DO, SET/SHOW/RESET  
**Wire:** CopyFail send path, Close (prepared statement cleanup)
