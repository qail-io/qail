# QAIL Roadmap: The Data Control Plane

## 🎯 Vision Statement

**QAIL is not an ORM. QAIL is the universal language for data access.**

> "Before JSON, data formats were fragmented. JSON became the lingua franca.  
> Before QAIL, queries are fragmented. QAIL becomes the lingua franca for data access."

---

## The Evolution

```
Level 1: Raw SQL         → "I hope this string works"
Level 2: ORMs (SeaORM)   → "Safe, but locked to one language"  
Level 3: QAIL            → "Universal, type-safe, works everywhere"
```

---

## Architecture: The Stack

| Layer | Component | Status |
|-------|-----------|--------|
| **Syntax** | QAIL Language (`get users where...`) | ✅ Complete |
| **Parser** | `nom` combinators → `QailCmd` AST | ✅ Complete |
| **Validator** | Schema-aware compile-time checks | ✅ `qail!` macro |
| **Transpiler** | AST → SQL/NoSQL dialects | ✅ 18 dialects |
| **Bindings** | Rust, Python, JS, FFI, WASM | 🔄 In Progress |

---

## ✅ Completed (v0.8.x)

### Core Parser & AST
- [x] `QailCmd` universal AST representation
- [x] DML: `get`, `add`, `set`, `del` commands
- [x] DDL: `make` (CREATE TABLE), `index` (CREATE INDEX)
- [x] Joins: left/right/inner/outer with aliases
- [x] CTEs: `with` clause support
- [x] Expressions: CASE WHEN, COALESCE, subqueries
- [x] Operators: LIKE, ILIKE, IN, BETWEEN, IS NULL
- [x] Ordering: ORDER BY, DISTINCT ON
- [x] Pagination: LIMIT, OFFSET

### SQL Transpilers (11 dialects)
- [x] PostgreSQL (primary)
- [x] MySQL / MariaDB
- [x] SQLite
- [x] SQL Server
- [x] Oracle
- [x] BigQuery
- [x] Snowflake
- [x] Redshift
- [x] DuckDB
- [x] InfluxDB

### NoSQL Transpilers (7 dialects)
- [x] MongoDB (aggregation pipeline)
- [x] DynamoDB
- [x] Redis
- [x] Cassandra (CQL)
- [x] Elasticsearch (Query DSL)
- [x] Neo4j (Cypher)
- [x] Qdrant (vector search)

### Rust Integration (qail-sqlx)
- [x] `QailExecutor` trait for `PgPool`
- [x] Named parameters (`:id`, `:status`)
- [x] `qail_params!` macro
- [x] `qail_fetch_all`, `qail_fetch_one`, `qail_execute`

### Compile-Time Safety (qail-macros) ⭐ NEW
- [x] `qail!` proc-macro with schema validation
- [x] `qail_one!`, `qail_optional!`, `qail_execute!` variants
- [x] "Did you mean?" suggestions (Levenshtein)
- [x] Schema introspection: `qail pull <db_url>`
- [x] Offline validation via `schema.json`

### Developer Tools
- [x] CLI: `qail` command with REPL
- [x] `qail pull` - schema introspection
- [x] `qail fmt` - query formatter (v2 syntax)
- [x] LSP: VS Code extension (qail-lsp)
- [x] WASM: Browser playground (qail-wasm)

---

## 🚀 v0.9.0 - Static Compiler Release

**Theme:** "Compile-time safety for everyone"

### High Priority
- [ ] `qail check` CLI linter for Python/JS
- [ ] Re-export `qail!` macros from `qail-sqlx`
- [ ] Schema diff: `qail diff` (compare schema.json versions)
- [ ] Improved error spans in macro errors

### Medium Priority
- [ ] `qail-py` Python bindings (via PyO3)
- [ ] `qail-node` Node.js bindings (via napi-rs)
- [ ] GitHub Action: `qail-check-action`

---

## 🔮 v1.0.0 - Production Ready

**Theme:** "Enterprise-grade data control plane"

### Core Features
- [ ] Query plan analysis
- [ ] Automatic query optimization hints
- [ ] Transaction support in DSL
- [ ] Migration generation from schema diffs

### Ecosystem
- [ ] `qail-studio` - Visual query builder
- [ ] `qail-proxy` - Database proxy with validation
- [ ] OpenTelemetry tracing integration

### Documentation
- [ ] Complete language specification
- [ ] Interactive tutorial site
- [ ] Video course

---

## 📊 Progress Summary

| Category | Items | Done | Progress |
|----------|-------|------|----------|
| SQL Dialects | 11 | 11 | 100% |
| NoSQL Dialects | 7 | 7 | 100% |
| Core Parser | 15 | 15 | 100% |
| qail-sqlx | 8 | 8 | 100% |
| qail-macros | 6 | 6 | 100% |
| Dev Tools | 5 | 5 | 100% |
| **v0.9 Features** | 7 | 0 | 0% |

---

## 💡 Why QAIL is Different

| Aspect | Traditional ORMs | QAIL |
|--------|-----------------|------|
| **Scope** | One language (Rust/Python/etc) | Universal |
| **Validation** | Runtime or model-based | **Schema file (offline)** |
| **Databases** | Usually SQL-only | SQL + NoSQL |
| **Distribution** | Library | **Platform** |
| **Lock-in** | Framework-specific | **Standard syntax** |

---

## 🏆 The Competitive Landscape

| Tool | Type | QAIL Advantage |
|------|------|----------------|
| SQLx | Rust ORM | QAIL has universal syntax + NoSQL |
| SeaORM | Rust ORM | QAIL works in Python/JS too |
| Prisma | Node ORM | QAIL has Rust performance + more dialects |
| LINQ | C# DSL | QAIL is not locked to Microsoft |

**QAIL is the "TypeScript for Databases"** - adding compile-time safety to a historically runtime-only domain.

---

## Version History

| Version | Date | Highlights |
|---------|------|------------|
| 0.8.0 | Dec 2024 | Initial release, parser + transpiler |
| 0.8.5 | Dec 2024 | Named parameters, qail-sqlx |
| 0.8.10 | Dec 2024 | **qail! macro, compile-time validation** |
| 0.9.0 | TBD | qail check CLI, Python/JS bindings |
| 1.0.0 | TBD | Production release |
