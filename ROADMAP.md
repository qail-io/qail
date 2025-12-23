# QAIL Roadmap

## Current Version: 0.8.10

### ✅ Completed Features

- **Core Parser**: GET, SET, ADD, DEL, MAKE, INDEX, DROP syntax
- **Named Parameters**: `:id`, `:status` syntax with `qail_params!` macro
- **Native SQLx Integration**: `QailExecutor` trait for `PgPool`
- **Multi-dialect Support**: PostgreSQL, MySQL, SQLite, Oracle, MSSQL
- **NoSQL Transpilers**: MongoDB, DynamoDB, Cassandra, Redis, Elastic, Neo4j, Qdrant
- **DDL Support**: CREATE TABLE, CREATE INDEX with constraints
- **CTE Support**: WITH clauses for complex queries
- **DISTINCT ON**: Postgres-specific column deduplication
- **LIKE/ILIKE**: Pattern matching operators
- **CASE WHEN**: Conditional expressions
- **ORDER BY/LIMIT/OFFSET**: Pagination support

---

## 🚀 v0.9.0 - Compile-Time Validation (HIGH PRIORITY)

### `qail!` Proc-Macro

**Goal**: Compile-time column and type validation like `sqlx::query!`

```rust
// Future API
let user = qail!(pool, User, "get users where id = :id", id: user_id).await?;
```

**Features**:
- [ ] Parse QAIL syntax at compile time
- [ ] Validate column names against schema
- [ ] Type-check named parameters
- [ ] Generate optimized SQL at compile time
- [ ] Schema file support (offline validation, like `.sqlx/`)
- [ ] Optional live DB validation fallback

**Why**: Makes QAIL the best of both worlds - SQLx's type safety + QAIL's ergonomics

---

## 📋 Future Features

### SQL Feature Parity
- [ ] `COUNT(*) FILTER (WHERE ...)` - Postgres aggregate filtering
- [ ] `INTERVAL` expressions - Time arithmetic
- [ ] `::type` casts - Type casting syntax
- [ ] JSON operators (`->`, `->>`) - JSON path access
- [ ] Window functions - `ROW_NUMBER()`, `RANK()`

### Developer Experience
- [ ] VS Code syntax highlighting
- [ ] LSP autocomplete for table/column names
- [ ] `qail fmt` - Code formatter
- [ ] Error messages with suggestions ("Did you mean...")

### Performance
- [ ] Query caching - Cache parsed AST
- [ ] Prepared statement reuse
- [ ] Benchmark suite

---

## 🎯 Design Principles

1. **CRUD-first**: Optimize for common CRUD operations
2. **Cross-database**: Write once, run on Postgres/MySQL/SQLite
3. **Injection-safe**: Named params, no string concatenation
4. **Ergonomic**: No counting `$1, $2, $3...`
5. **Report queries**: Keep complex analytics in raw SQL
