# QAIL Roadmap: AST-Native Database Access

## 🎯 Vision Statement

**QAIL is the universal AST for database operations.**

> "SQL is a text protocol designed for humans to type.  
> QAIL is a binary protocol designed for machines to optimize."

---

## The Evolution

```
Era 1: SQL Strings      → "Trust me, this string is safe"
Era 2: ORMs             → "Safe, but locked to one language"
Era 3: Query Builders   → "Safe, but still generates strings"
Era 4: SQLx             → "Compile-time checked SQL - the breakthrough"
Era 5: QAIL             → "Pure AST that compiles directly to wire protocol"
```

> **Acknowledgment:** SQLx pioneered compile-time SQL validation in Rust and remains the gold standard for SQL-based database access. QAIL builds on this foundation by eliminating strings entirely - a natural evolution, not a replacement.

---

## Architecture: The Layers

```
┌──────────────────────────────────────────────────────────────┐
│ Layer 1: Intent (App Code)                                    │
│   - User constructs QailCmd AST                               │
│   - Pure data, no I/O                                         │
├──────────────────────────────────────────────────────────────┤
│ Layer 2: Brain (Pure Logic)                                   │
│   - PgEncoder compiles AST → BytesMut                         │
│   - NO async, NO tokio, NO networking                         │
│   - Can compile to WASM                                       │
├──────────────────────────────────────────────────────────────┤
│ Layer 3: Muscle (Async Runtime)                               │
│   - Tokio TcpStream sends bytes                               │
│   - ONLY layer with runtime dependency                        │
│   - Swappable: tokio → async-std → glommio                    │
├──────────────────────────────────────────────────────────────┤
│ Layer 4: Reality (Database)                                   │
│   - PostgreSQL, MySQL, etc.                                   │
│   - Each speaks its own wire protocol                         │
└──────────────────────────────────────────────────────────────┘
```

---

## ✅ Completed

### Core AST (qail-core)
- [x] `QailCmd` universal AST representation
- [x] DML: `get`, `add`, `set`, `del` commands
- [x] DDL: `make` (CREATE TABLE), `index` (CREATE INDEX)
- [x] Joins: left/right/inner with ON conditions
- [x] CTEs: WITH clause support
- [x] Expressions: CASE WHEN, aggregates, window functions
- [x] Parser: Text → AST (for CLI, LSP, WASM)

### PostgreSQL Driver (qail-pg)
- [x] Wire protocol types (FrontendMessage, BackendMessage)
- [x] `PgEncoder::encode_simple_query()` - AST → BytesMut
- [x] Basic connection handling with tokio
- [x] Layer 2/3 separation (protocol/ vs driver/)

### Developer Tools
- [x] CLI: `qail` command with REPL
- [x] LSP: VS Code extension
- [x] WASM: Browser playground

### SQL Transpiler (Legacy Path)
- [x] PostgreSQL, MySQL, SQLite, SQL Server
- [x] Oracle, BigQuery, Snowflake, Redshift
- [x] MongoDB, DynamoDB, Redis, Cassandra
- [x] Elasticsearch, Neo4j, Qdrant

---

## 🚀 v0.9.0 - Wire Protocol Release

**Theme:** "AST to Bytes, No Strings Attached"

### High Priority
- [ ] Extended Query Protocol (Parse/Bind/Execute)
- [ ] Parameter binding in wire protocol
- [ ] Row decoding (bytes → typed values)
- [ ] Connection pooling skeleton

### Medium Priority
- [x] Builder API for ergonomic AST construction ✅ Done!
- [ ] Transaction support (BEGIN/COMMIT/ROLLBACK)
- [ ] Error mapping (PG error codes → Rust errors)

---

## 📦 v0.9.1 - AST-Native Migrations

**Theme:** "No SQL Files. Pure AST Diff."

### The Paradigm Shift

| Aspect | Traditional | QAIL |
|--------|-------------|------|
| **Storage** | `up.sql` / `down.sql` strings | Schema AST (JSON) |
| **Creation** | Hand-written SQL | Auto-generated diff |
| **Rollback** | Manual `down.sql` | Reverse the AST diff |
| **Dialect** | One file per database | Universal AST → any wire protocol |

### No SQL Files

```
# Traditional (string-based)
migrations/
├── 001_create_users.up.sql    ← Hand-written SQL
├── 001_create_users.down.sql  ← Hand-written rollback
└── 002_add_email.up.sql       ← Dialect-specific

# QAIL (AST-based)
schema/
├── v1.json   ← Schema snapshot (auto-generated)
├── v2.json   ← Schema snapshot (auto-generated)
└── current   ← Symlink to latest

# Migration is COMPUTED, not written!
```

### Architecture

```
┌──────────────┐     ┌──────────────┐
│ schema_v1    │ ──► │ schema_v2    │
│ (JSON)       │     │ (JSON)       │
└──────────────┘     └──────────────┘
        │                   │
        └───────┬───────────┘
                ▼
┌───────────────────────────────┐
│ DiffVisitor                   │
│ schema_v1 ⊕ schema_v2         │
│ → Vec<QailCmd>                │  ← Pure AST (AddColumn, DropColumn, etc.)
└───────────────────────────────┘
                │
                ▼
┌───────────────────────────────┐
│ AstEncoder::encode(cmd)       │  ← Layer 2: Pure bytes
└───────────────────────────────┘
                │
                ▼
┌───────────────────────────────┐
│ PostgreSQL / MySQL / SQLite   │  ← Same AST, different wire protocols
└───────────────────────────────┘
```

### CLI Workflow

```bash
# 1. Pull current schema from any database
qail pull postgres://prod/db > schema/v1.json
qail pull mysql://staging/db > schema/v1.json    # Same format!

# 2. Make changes (edit schema or pull from staging)
qail pull postgres://staging/db > schema/v2.json

# 3. See the diff (returns Vec<QailCmd>)
qail diff schema/v1.json schema/v2.json

# 4. Apply to ANY database (same AST, different wire protocol)
qail migrate up postgres://prod/db
qail migrate up mysql://prod/db
qail migrate up sqlite://local.db

# 5. Rollback (auto-computed reverse diff)
qail migrate down postgres://prod/db
```

### Implementation Plan

| Component | Location | Description |
|-----------|----------|-------------|
| **SchemaTable, SchemaColumn** | `qail-core/src/schema.rs` | AST types for schema |
| **DiffVisitor** | `qail-core/src/diff.rs` | Compare schemas → `Vec<QailCmd>` |
| **qail pull** | `qail-cli` | Extract schema from database |
| **qail diff** | `qail-cli` | Compare two schemas |
| **qail migrate** | `qail-cli` | Apply migrations |

### Why This Matters

> **The same schema .qail file works for PostgreSQL, MySQL, SQLite, MongoDB, DynamoDB.**
> 
> No more dialect-specific migration files. No more hand-writing SQL.
> Pure AST that encodes to any wire protocol.

### The .qail Schema Format ✅ NEW

We solved the "JSON can't express intent" problem by creating a **native `.qail` schema format**:

```qail
# schema.qail - Human readable, intent-aware
table users {
  id serial primary_key
  username text not_null
  email text unique
}

# Migration hints express INTENT
rename users.name -> users.username    # NOT drop + add
transform users.age * 12 -> users.age_months  # Data migration hint
```

**Why .qail beats JSON:**

| Aspect | JSON | SQL | QAIL Schema |
|--------|------|-----|-------------|
| Human-readable | 😐 | 🙂 | ✅ |
| Intent-aware | ❌ | ❌ | ✅ `rename`, `transform` |
| Diff-friendly | ❌ | 😐 | ✅ Line-by-line git diffs |
| Comments | ❌ | ✅ | ✅ `# comment` |

### Honest Limitations

AST-native migrations cover **~95% of real-world migrations** now:

| ✅ Covered (via .qail) | ❌ Still Requires Custom Logic |
|------------------------|-------------------------------|
| CREATE TABLE | Complex data transformations |
| DROP TABLE | Multi-step business logic |
| ADD COLUMN | External API calls |
| DROP COLUMN | Conditional backfills |
| CREATE INDEX | |
| **RENAME COLUMN** ✅ | |
| **Data transform hints** ✅ | |

#### Where QAIL Still Can't Help

**1. Complex Data Transformations**
```sql
UPDATE users SET age_months = CASE WHEN age_unit = 'years' THEN age * 12 ELSE age END;
```
Multi-conditional logic requires human expertise.

**2. Large Production Databases**
Auto diffs can lock tables, cause downtime. Humans often want fine-grained control.

**3. Multi-Team Workflows**
Schema diffs conflict. Migration scripts allow negotiation.

**Escape Hatch:** Generate structural SQL, add custom logic:
```bash
qail diff v1.qail v2.qail > migrations/001_structure.sql
# Add custom data migration manually
```

*Philosophy: Automate 95%, escape cleanly for the rest.*

---

## 🔮 v1.0.0 - Production Ready

**Theme:** "Replace sqlx in production"

### Core Features
- [ ] Full Extended Query Protocol
- [ ] Prepared statement caching
- [x] SSL/TLS support ✅
- [x] SCRAM-SHA-256 authentication ✅

### Performance
- [ ] Zero-copy row decoding
- [ ] Pipeline mode (batch queries)
- [ ] Benchmark suite vs sqlx/tokio-postgres

### Ecosystem
- [ ] `qail-mysql` - MySQL wire protocol
- [ ] `qail-sqlite` - SQLite (embedded, no network)
- [x] Migration tooling (.qail format) ✅

---

## 🌍 v2.0.0 - Universal Platform

**Theme:** "One AST, Every Database, Every Language"

### Multi-Database
- [ ] MySQL driver (qail-mysql)
- [ ] SQLite driver (qail-sqlite)
- [ ] Unified connection abstraction

### Multi-Language
- [ ] Python bindings (PyO3)
- [ ] JavaScript bindings (napi-rs)
- [ ] Go bindings (cgo)

### Advanced Features
- [ ] Query plan analysis
- [ ] Automatic query optimization
- [ ] Distributed transaction coordination

---

## 📊 Progress Summary

| Component | Status | Notes |
|-----------|--------|-------|
| AST (`QailCmd`) | ✅ Complete | Universal representation |
| Parser | ✅ Complete | Text → AST for tools |
| SQL Transpiler | ✅ Complete | AST → SQL text |
| PG Wire Encoder | ✅ Complete | AST → BytesMut (DDL, DML) |
| PG Driver | ✅ Complete | SSL + SCRAM auth |
| .qail Schema Format | ✅ Complete | Intent-aware migrations |
| MySQL Wire Encoder | 📋 Planned | - |
| Builder API | 📋 Planned | Ergonomic AST construction |

---

## 💡 Why AST-Native?

| Aspect | SQL Strings | QAIL AST |
|--------|-------------|----------|
| **Type Safety** | Runtime errors | Compile-time |
| **Injection Risk** | Possible | Impossible |
| **Parsing** | At runtime | At compile |
| **Portability** | Text encoding issues | Binary, exact |
| **Optimization** | Hard | AST transformations |

---

## 🏗️ Removed / Deprecated

| Component | Status | Reason |
|-----------|--------|--------|
| `qail-sqlx` | ❌ Deleted | Replaced by native drivers |
| `qail-driver` | ❌ Deleted | Merged into qail-pg |
| `qail-macros` | ❌ **Deleted** | String-based queries are anti-pattern; use `QailCmd` builder API |

> **Philosophy:** QAIL eliminates SQL strings entirely. The builder API (`QailCmd::get()`, `QailCmd::add()`) is the **only** way to construct queries. This is what makes QAIL truly AST-native - no parsing at runtime, no strings anywhere.

---

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| — | Dec 2024 | **Idea born** — "What if we had a universal query language?" |
| — | Jul 2025 | **Draft created** — Started building the transpiler internally |
| 0.6.0 | Dec 2025 | **Public release** — Universal query transpiler (still string-based, relied on SQLx) |
| 0.8.0 | Dec 2025 | Improved parser, but realized: "We're just another ORM, 80% strings" |
| 0.9.0 | Dec 2025 | **The pivot** — "Be like clib/pgx, or be nothing." Nuked strings entirely. AST-native. |
| 0.9.2 | Dec 2025 | **.qail schema format** — Intent-aware migrations. SCRAM auth. SSL/TLS. |
| 1.0.0 | TBD | Stable PostgreSQL driver — production ready |
| 2.0.0 | TBD | Return to agnostic philosophy — support all SQL/NoSQL via pure AST |

### The Journey

> **Dec 2024:** The idea — a universal query transpiler to end "Polyglot Hell."
>
> **Jul 2025:** First draft. Used it internally. Still relied on SQLx. Still 80% string manipulation like every other ORM.
>
> **Dec 2025 (0.6.0-0.8.0):** Public release. Community feedback. But something felt wrong — we were building the same thing everyone else had built.
>
> **Dec 2025 (0.9.0):** The realization: *"SQLx pioneered compile-time SQL. To matter, we must go further — eliminate strings entirely."* We nuked the string layer. Pure AST to wire protocol. No parsing at runtime. No SQL generation.
>
> **v1.0 Vision:** Stable PostgreSQL driver that proves the architecture.
>
> **v2.0 Vision:** Return to the original dream — universal database access. But this time, speaking AST, not strings. Every database (SQL and NoSQL) through one typed interface.
