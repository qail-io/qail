# QAIL vs PostgreSQL Documentation — Coverage Audit

**Reference:** [PostgreSQL 18 Protocol](https://www.postgresql.org/docs/current/protocol-message-formats.html) | [SQL Commands](https://www.postgresql.org/docs/current/sql-commands.html)

---

## 1. Binary Protocol — Frontend Messages (Client → Server)

| Doc Message | QAIL Status | Notes |
|-------------|-------------|-------|
| **StartupMessage** | ✅ | `connection.rs` — user, database; protocol version 196610 (3.2) |
| **Query (Q)** | ✅ | `PgEncoder::encode_query_string` — Simple Query |
| **Parse (P)** | ✅ | `PgEncoder::encode_parse` — statement name, SQL, param OIDs |
| **Bind (B)** | ✅ | `PgEncoder::encode_bind` / `encode_bind_to` |
| **Execute (E)** | ✅ | `PgEncoder::encode_execute` — portal, max_rows |
| **Sync (S)** | ✅ | `PgEncoder::encode_sync` |
| **Terminate (X)** | ✅ | `PgEncoder::encode_terminate` |
| **PasswordMessage (p)** | ✅ | Used for cleartext and SASL SCRAM |
| **SASLInitialResponse (p)** | ✅ | `auth.rs` — SCRAM-SHA-256 |
| **SASLResponse (p)** | ✅ | `auth.rs` — SCRAM client-final |
| **CopyData (d)** | ✅ | `copy_encoder.rs` — row data |
| **CopyDone (c)** | ✅ | `copy.rs:145` — `[b'c', 0,0,0,4]` |
| **CopyFail (f)** | ❌ | Not implemented — no CopyFail send path |
| **SSLRequest** | ✅ | `connection.rs` — TLS negotiation |
| **Flush (H)** | ❌ | Not used — Sync suffices for typical flow |
| **Describe (D)** | ✅ | `encoder.rs` — Describe Statement/Portal (for schema) |
| **Close (C)** | ❌ | Not used — prepared statement LRU evicts, no explicit Close |
| **CancelRequest** | ✅ | `pool.rs` — cancel_token, process_id + secret_key |
| **FunctionCall (F)** | ❌ | Not used — RPC via SQL instead |
| **GSSENCRequest** | ❌ | GSSAPI encryption — not implemented |
| **GSSResponse** | ❌ | — |

---

## 2. Binary Protocol — Backend Messages (Server → Client)

| Doc Message | QAIL Status | Notes |
|-------------|-------------|-------|
| **AuthenticationOk (R)** | ✅ | `wire.rs` |
| **AuthenticationMD5Password** | ✅ | `auth.rs` — MD5 support |
| **AuthenticationSASL** | ✅ | SCRAM-SHA-256 |
| **AuthenticationSASLContinue** | ✅ | |
| **AuthenticationSASLFinal** | ✅ | |
| **BackendKeyData (K)** | ✅ | `wire/backend.rs` + `connection/startup.rs` — process_id + variable-length cancel key bytes (`4..=256`) |
| **ParameterStatus (S)** | ✅ | `wire.rs::decode_parameter_status` |
| **ReadyForQuery (Z)** | ✅ | Transaction status I/T/E |
| **RowDescription (T)** | ✅ | Field descriptions |
| **DataRow (D)** | ✅ | |
| **CommandComplete (C)** | ✅ | INSERT oid rows, etc. |
| **ErrorResponse (E)** | ✅ | `decode_error_response` |
| **ParseComplete (1)** | ✅ | |
| **BindComplete (2)** | ✅ | |
| **NoData (n)** | ✅ | |
| **CopyInResponse (G)** | ✅ | `wire.rs` — copy-in ready |
| **CopyOutResponse (H)** | ✅ | Copy-out (Export) |
| **CopyData (d)** | ✅ | |
| **CopyDone (c)** | ✅ | |
| **NotificationResponse (A)** | ✅ | LISTEN/NOTIFY |
| **EmptyQueryResponse (I)** | ✅ | |
| **NoticeResponse (N)** | ✅ | |
| **ParameterDescription (t)** | ✅ | |
| **PortalSuspended (s)** | ❓ | Cursor row limit — may be implicitly handled |
| **CloseComplete (3)** | ❌ | No Close sent, so not expected |
| **NegotiateProtocolVersion (v)** | ✅ | `wire/backend.rs` decode + `connection/startup.rs` negotiation handling |
| **AuthenticationCleartextPassword** | ✅ | Legacy auth path |
| **AuthenticationKerberosV5, GSS, SSPI** | ❌ | Not implemented |

---

## 3. SQL Commands — QAIL Action Coverage

PostgreSQL has 150+ SQL commands. QAIL maps a subset via `Action` + transpiler.

### DML (fully covered)

| PG Command | QAIL Action | Transpiler / Wire |
|------------|-------------|-------------------|
| SELECT | Get | ✅ `build_select` |
| INSERT | Add | ✅ `build_insert` |
| UPDATE | Set | ✅ `build_update` |
| DELETE | Del | ✅ `build_delete` |
| INSERT ... ON CONFLICT (upsert) | Put | ✅ `build_upsert` |
| SELECT ... INTO | — | ❌ No Action |
| VALUES | — | ❌ No dedicated Action (can use Raw) |
| MERGE | — | ❌ PostgreSQL 15+ MERGE not in AST |

### DDL (covered via Action)

| PG Command | QAIL Action | Status |
|------------|-------------|--------|
| CREATE TABLE | Make | ✅ |
| DROP TABLE | Drop | ✅ |
| ALTER TABLE | Alter, AlterDrop, AlterType, Mod, AlterSetNotNull, etc. | ✅ |
| CREATE INDEX | Index | ✅ |
| DROP INDEX | DropIndex | ✅ |
| CREATE VIEW | CreateView | ✅ |
| DROP VIEW | DropView | ✅ |
| CREATE MATERIALIZED VIEW | CreateMaterializedView | ✅ |
| REFRESH MATERIALIZED VIEW | RefreshMaterializedView | ✅ |
| DROP MATERIALIZED VIEW | DropMaterializedView | ✅ |
| CREATE SEQUENCE | CreateSequence | ✅ |
| DROP SEQUENCE | DropSequence | ✅ |
| CREATE TYPE (enum) | CreateEnum | ✅ |
| DROP TYPE | DropEnum | ✅ |
| ALTER TYPE ADD VALUE | AlterEnumAddValue | ✅ |
| CREATE EXTENSION | CreateExtension | ✅ |
| DROP EXTENSION | DropExtension | ✅ |
| CREATE FUNCTION | CreateFunction | ✅ |
| DROP FUNCTION | DropFunction | ✅ |
| CREATE TRIGGER | CreateTrigger | ✅ |
| DROP TRIGGER | DropTrigger | ✅ |
| COMMENT ON | CommentOn | ✅ |
| GRANT / REVOKE | — | In migrations, not Action |

### Transactions & Cursors

| PG Command | QAIL Action | Status |
|------------|-------------|--------|
| BEGIN / START TRANSACTION | TxnStart | ✅ Stub |
| COMMIT | TxnCommit | ✅ |
| ROLLBACK | TxnRollback | ✅ |
| SAVEPOINT | Savepoint | ✅ |
| RELEASE SAVEPOINT | ReleaseSavepoint | ✅ |
| ROLLBACK TO SAVEPOINT | RollbackToSavepoint | ✅ |
| DECLARE (cursor) | Scroll | Qdrant; PG cursor not mapped |
| FETCH | — | ❌ |
| MOVE | — | ❌ |
| CLOSE | — | ❌ |
| PREPARE (SQL) | — | Implicit via driver |
| DEALLOCATE | — | Implicit |

### Utility & Session

| PG Command | QAIL Action | Status |
|------------|-------------|--------|
| COPY | Export | ✅ `COPY (SELECT ...) TO STDOUT` |
| TRUNCATE | Truncate | ✅ |
| EXPLAIN | Explain | ✅ |
| EXPLAIN ANALYZE | ExplainAnalyze | ✅ |
| LOCK TABLE | Lock | ✅ |
| LISTEN | Listen | ✅ |
| NOTIFY | Notify | ✅ |
| UNLISTEN | Unlisten | ✅ |
| ANALYZE | — | ❌ |
| VACUUM | — | ❌ |
| CHECKPOINT | — | ❌ |
| REINDEX | — | ❌ |
| DISCARD | — | ❌ (driver uses for RLS reset) |
| SET / SHOW / RESET | — | ❌ |
| DO | — | ❌ |
| CALL | — | ❌ |

### Not in QAIL (low priority)

| PG Command | Note |
|------------|------|
| ABORT | Same as ROLLBACK |
| CREATE/DROP DATABASE | Admin |
| CREATE/DROP SCHEMA | Migrations |
| CREATE/DROP TABLESPACE | Admin |
| CREATE/DROP PUBLICATION/SUBSCRIPTION | Replication |
| CREATE/DROP EVENT TRIGGER | Admin |
| CREATE/DROP COLLATION, CONVERSION, etc. | Niche |
| CREATE POLICY / ALTER POLICY | RLS — migrations |
| IMPORT FOREIGN SCHEMA | FDW |
| SECURITY LABEL | SELinux |
| PREPARE TRANSACTION / COMMIT PREPARED | 2PC |

---

## 4. Copy Protocol

| Doc | QAIL | Status |
|-----|------|--------|
| Copy text format | `copy_encoder.rs` | ✅ NULL=\N, \t\n\r\\\\ escape |
| Copy binary format | — | ❌ Text only |
| CopyIn (FROM) | ✅ | `COPY table FROM STDIN` flow |
| CopyOut (TO) | ✅ | Export → `COPY (SELECT...) TO STDOUT` |
| CopyBothResponse | — | Streaming replication only |
| CopyFail | — | No send path |

---

## 5. Authentication

| Method | QAIL | Status |
|--------|------|--------|
| Trust | ✅ | No password |
| Cleartext | ✅ | PasswordMessage |
| MD5 | ✅ | `auth.rs` |
| SCRAM-SHA-256 | ✅ | SASL flow |
| GSSAPI / Kerberos | ❌ | — |
| SSPI | ❌ | — |
| Client cert (TLS) | ✅ | `connect_tls` with certs |

---

## 6. Format Codes

| Doc | QAIL | Notes |
|-----|------|-------|
| Text (0) | ✅ | Default for params and results |
| Binary (1) | ⚠️ | Bind supports format codes; result format not requested as binary |
| Parameter format codes per-param | ✅ | `encode_bind` |
| Result column format codes | ⚠️ | Currently all text |

---

## 7. Summary

| Area | Coverage | Gaps |
|------|----------|------|
| **Frontend messages** | ~85% | CopyFail, Close, Flush, FunctionCall, GSSAPI |
| **Backend messages** | ~95% | PortalSuspended, CloseComplete, Kerberos/GSS |
| **SQL commands** | ~35% of full list | ANALYZE, VACUUM, FETCH, MOVE, CLOSE, MERGE, DO, CALL, SET/SHOW/RESET, admin commands |
| **Copy protocol** | Text format | Binary format, CopyFail |
| **Auth** | Trust, Cleartext, MD5, SCRAM, TLS | GSSAPI, SSPI |
| **DML coverage** | 100% for common path | SELECT INTO, MERGE, VALUES standalone |
| **DDL coverage** | High for schema objects | Admin (DB, schema, tablespace) |

QAIL targets the common CRUD + schema + COPY + transactions path. Gaps are mostly admin, replication, cursor operations, and legacy auth.

---

## 8. Implementation Plan — Worth Adding

Prioritized gaps to implement:

### SQL Commands
| Item | PG Version | Notes |
|------|------------|-------|
| **CALL** | 11+ | Procedure calls — `CALL proc(args)` |
| **DO** | 9.0+ | Anonymous PL/pgSQL blocks |
| **SET / SHOW / RESET** | — | Session variables (e.g. `SET search_path`, `SHOW timezone`) |

### Wire Protocol
| Item | Notes |
|------|-------|
| **CopyFail** | Send path for aborting COPY on error |
| **Close** | Explicit prepared statement/portal cleanup (C + name) |

### Already Covered (no work needed)
- GREATEST/LEAST — `Expr::FunctionCall`
- ANALYZE, VACUUM, CHECKPOINT, REINDEX — admin, not worth adding
- MERGE, FETCH, MOVE, CLOSE (cursor) — low demand
- GSSAPI/SSPI auth — legacy
- Binary COPY format — text format sufficient
