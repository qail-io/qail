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
Era 4: QAIL             → "Type-safe AST that compiles to wire protocol"
```

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

**Theme:** "Schema as Code, Migrations as Data"

### The Vision

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
│ → Vec<QailCmd>                │  ← Pure AST
└───────────────────────────────┘
                │
                ▼
┌───────────────────────────────┐
│ PgEncoder::encode(cmd)        │  ← Layer 2: Pure bytes
└───────────────────────────────┘
                │
                ▼
┌───────────────────────────────┐
│ driver.execute(bytes)         │  ← Layer 3: Apply
└───────────────────────────────┘
```

### Implementation Plan

| Component | Location | Description |
|-----------|----------|-------------|
| **Schema Diff** | `qail-core/src/diff.rs` | Compare two schemas → `Vec<QailCmd>` |
| **CLI: diff** | `qail-cli` | `qail diff old.json new.json` |
| **CLI: migrate** | `qail-cli` | `qail migrate --up <db_url>` |
| **Migration Files** | `.qail/migrations/` | Store AST as JSON, not SQL |

### Why AST-Native Migrations?

| Traditional | QAIL |
|-------------|------|
| Hand-write SQL | Generate from schema diff |
| One dialect per file | Encode to any database |
| String manipulation | Type-safe AST transforms |
| Runtime errors | Compile-time validation |

### CLI Workflow

```bash
# Pull current schema
qail pull postgres://localhost/mydb > schema.json

# Make changes (add column, create table, etc.)
vim schema.json

# Generate migration
qail diff schema_old.json schema.json -o migrations/001_add_users.qail

# Preview SQL (any dialect)
qail show migrations/001_add_users.qail --dialect postgres
qail show migrations/001_add_users.qail --dialect mysql

# Apply migration
qail migrate up postgres://localhost/mydb
```

---

## 🔮 v1.0.0 - Production Ready

**Theme:** "Replace sqlx in production"

### Core Features
- [ ] Full Extended Query Protocol
- [ ] Prepared statement caching
- [ ] SSL/TLS support
- [ ] SCRAM-SHA-256 authentication

### Performance
- [ ] Zero-copy row decoding
- [ ] Pipeline mode (batch queries)
- [ ] Benchmark suite vs sqlx/tokio-postgres

### Ecosystem
- [ ] `qail-mysql` - MySQL wire protocol
- [ ] `qail-sqlite` - SQLite (embedded, no network)
- [ ] Migration tooling (from v0.9.1)

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
| SQL Transpiler | ✅ Complete | AST → SQL text (legacy) |
| PG Wire Encoder | 🔄 In Progress | AST → BytesMut |
| PG Driver | 🔄 Skeleton | Async I/O |
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
| `qail-macros` | ⏸️ Paused | AST-native doesn't need string macros |

---

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| 0.8.0 | Dec 2024 | Parser + SQL transpiler |
| 0.8.12 | Dec 2024 | nom v8 migration |
| 0.8.13 | Dec 2024 | **AST-native pivot, PgEncoder** |
| 0.9.0 | TBD | Extended Query Protocol |
| 1.0.0 | TBD | Production-ready PG driver |
