# 🪝 QAIL — The Universal Query Language

> **Safe but Free.** Write queries once. Run them everywhere. Zero lock-in.

[![Crates.io](https://img.shields.io/badge/crates.io-qail-orange)](https://crates.io/crates/qail)
[![npm](https://img.shields.io/badge/npm-qail--wasm-red)](https://www.npmjs.com/package/qail-wasm)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

---

## Why QAIL?

For years, developers have been trapped between two choices:

1. **Raw SQL** — Maximum freedom, but dangerous strings scattered across your codebase.
2. **ORMs / Query Builders** — Maximum safety, but a "prison" of boilerplate and language lock-in.

**QAIL is the third way.**

We moved validation from the **networking layer** to the **grammar level**. By treating queries as a compiled language instead of raw strings, QAIL provides compile-time safety with the freedom of raw SQL.

- ✅ **No language lock-in** — Same syntax in Rust, Node.js, Go, Python, PHP
- ✅ **No heavy dependencies** — Pure logic, zero networking code
- ✅ **No "big bang" migration** — Adopt incrementally, one query at a time
- ✅ **Works with your existing driver** — SQLx, pg, PDO, psycopg2, etc.

```sql
-- SQL (The Assembly)
SELECT id, email FROM users WHERE active = true LIMIT 10;
```

```bash
# QAIL (The Source Code)
get::users:'id'email [ 'active == true, 0..10 ]
```

One line. Zero ceremony. **Runs everywhere.**

---

## 🚀 Installation

### Rust (Native)

```bash
cargo install qail
```

```toml
[dependencies]
qail-core = "0.6.1"
```

### Node.js / Browser (WASM)

```bash
npm i qail-wasm
```

---

## 💡 Usage

### Rust

```rust
use qail_core::prelude::*;

// Parse and transpile
let sql = parse("get::users:'id'email [ 'active == true ]")?.to_sql();
// Returns: "SELECT id, email FROM users WHERE active = true"

// Use with your existing driver (sqlx, diesel, etc.)
let users = sqlx::query_as::<_, User>(&sql)
    .fetch_all(&pool)
    .await?;
```

### JavaScript / TypeScript

```javascript
import { parseAndTranspile } from 'qail-wasm';

const sql = parseAndTranspile("get::users:'id'email [ 'active == true ]");
// Returns: "SELECT id, email FROM users WHERE active = true"

// Use with your existing driver (pg, mysql2, etc.)
const result = await client.query(sql);
```

---

## 📖 Quick Reference

| Symbol | Name       | Function                | Example                    |
|--------|------------|-------------------------|----------------------------|
| `::`   | The Gate   | Action (get/set/del/add)| `get::`                    |
| `:`    | The Link   | Connect table to columns| `users:'id`                |
| `'`    | The Label  | Mark a column           | `'email'name`              |
| `'_`   | The Wildcard| All columns            | `users:'_`                 |
| `[ ]`  | The Cage   | Constraints block       | `[ 'active == true ]`      |
| `==`   | The Equal  | Equality check          | `'status == "active"`      |
| `~`    | The Fuse   | Fuzzy match (ILIKE)     | `'name ~ "john"`           |
| `\|`   | The Split  | Logical OR              | `'a == 1 \| 'b == 2`       |
| `&`    | The Bind   | Logical AND             | `'a == 1 & 'b == 2`        |
| `+`/`-`| Sort Order | ASC/DESC                | `-created_at`              |
| `N..M` | The Range  | Pagination              | `0..10`                    |
| `$`    | The Var    | Parameter placeholder   | `$1`                       |
| `!`    | The Unique | DISTINCT                | `get!::`                   |
| `<-`   | Left Join  | LEFT JOIN               | `users<-profiles`          |
| `->`   | Inner Join | INNER JOIN              | `users->orders`            |

---

## 📚 Examples

### Basic SELECT

```bash
get::users:'id'email [ 'active == true ]
# → SELECT id, email FROM users WHERE active = true
```

### All Columns

```bash
get::users:'_
# → SELECT * FROM users
```

### Sorting & Pagination

```bash
get::users:'_ [ -created_at, 0..10 ]
# → SELECT * FROM users ORDER BY created_at DESC LIMIT 10
```

### Fuzzy Search

```bash
get::users:'id'name [ 'name ~ "john" ]
# → SELECT id, name FROM users WHERE name ILIKE '%john%'
```

### UPDATE

```bash
set::users:[ status = "active" ] [ 'id == $1 ]
# → UPDATE users SET status = 'active' WHERE id = $1
```

### DELETE

```bash
del::users:[ 'id == $1 ]
# → DELETE FROM users WHERE id = $1
```

### JOINs

```bash
get::users<-profiles:'name'avatar
# → SELECT name, avatar FROM users LEFT JOIN profiles ON ...
```

---

## 🌐 One Syntax. Every Stack.

QAIL works alongside your existing tools:

| Language | QAIL Package | Works With |
|----------|--------------|------------|
| **Rust** | `qail-core` | SQLx, Diesel, tokio-postgres |
| **Node.js** | `qail-wasm` | pg, mysql2, better-sqlite3 |
| **Browser** | `qail-wasm` | Any REST/GraphQL client |
| **Python** | `qail-py` (coming) | psycopg2, SQLAlchemy |
| **Go** | `qail-go` (coming) | database/sql, GORM |

**Same syntax. Same validation. Any driver.**

---

## 🤝 Contributing

We welcome contributions!

```bash
git clone https://github.com/qail-rs/qail.git
cd qail
cargo test
```

---

## 📄 License

MIT © 2025 QAIL Contributors

---

<p align="center">
  <strong>Built with 🦀 Rust</strong><br>
  <a href="https://qail.rs">qail.rs</a>
</p>
