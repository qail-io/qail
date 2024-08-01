# 🪝 QAIL — The Universal Query Transpiler

> **SQL is Assembly. Stop writing Assembly. Write Qail. Compile to Safety.**

[![Crates.io](https://img.shields.io/badge/crates.io-qail-orange)](https://crates.io/crates/qail)
[![npm](https://img.shields.io/badge/npm-qail--wasm-red)](https://www.npmjs.com/package/qail-wasm)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

---

## What is QAIL?

QAIL is not an ORM. It is not a Query Builder.

**QAIL is a Query Transpiler.**

Write high-density, logic-focused queries in QAIL, and it compiles them instantly into Safe, Optimized, Raw SQL with zero runtime overhead.

```sql
-- SQL (Assembly)
SELECT id, email FROM users WHERE active = true LIMIT 10;
```

```bash
# QAIL
get::users:'id'email [ 'active == true, 0..10 ]
```

One line. Zero ceremony. **Runs everywhere.**

---

## 🚀 Installation

### Rust / CLI

```bash
cargo install qail
```

### Rust Library

```toml
[dependencies]
qail-core = "0.5.0"
```

### JavaScript / Browser (WASM)

```bash
npm i qail-wasm
```

---

## 💡 Usage

### Rust

```rust
use qail_macro::qail;

// Compile-time validated query
let sql = qail!("get::users:'id'email [ 'active == true ]");
// Returns: "SELECT id, email FROM users WHERE active = true"
```

### JavaScript / TypeScript

```javascript
import { parseAndTranspile } from 'qail-wasm';

const sql = parseAndTranspile("get::users:'id'email [ 'active == true ]");
// Returns: "SELECT id, email FROM users WHERE active = true"
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
| `~`    | The Fuse   | Fuzzy match             | `'name ~ "john"`           |
| `\|`   | The Split  | Logical OR              | `'a == 1 \| 'b == 2`       |
| `&`    | The Bind   | Logical AND             | `'a == 1 & 'b == 2`        |
| `+`/`-`| Sort Order | ASC/DESC                | `-created_at`              |
| `N..M` | The Range  | Pagination              | `0..10`                    |
| `$`    | The Var    | Parameter               | `$1`                       |
| `!`    | The Unique | Distinct                | `get!::`                   |
| `<-`   | The Left   | Left Join               | `users<-profiles`          |
| `->`   | The Right  | Inner Join              | `users->orders`            |

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

### Sorting

```bash
get::users:'_ [ -created_at ]
# → SELECT * FROM users ORDER BY created_at DESC
```

### Pagination

```bash
get::users:'_ [ 20..30 ]
# → SELECT * FROM users LIMIT 10 OFFSET 20
```

### Fuzzy Search

```bash
get::users:'id'name [ 'name ~ "john" ]
# → SELECT id, name FROM users WHERE name ILIKE '%john%'
```

### UPDATE

```bash
set::users:[ verified = true ][ 'id == $1 ]
# → UPDATE users SET verified = true WHERE id = $1
```

### DELETE

```bash
del::users:[ 'id == $1 ]
# → DELETE FROM users WHERE id = $1
```

### DISTINCT

```bash
get!::users:'role
# → SELECT DISTINCT role FROM users
```

### JOINs

```bash
get::users<-profiles:'name'avatar
# → SELECT name, avatar FROM users LEFT JOIN profiles ON ...
```

---

## 🌐 One Language. Everywhere.

QAIL works in:

- **Rust** — `qail-core` + `qail!` macro (compile-time)
- **Node.js** — `qail-wasm` (runtime)
- **Browser** — `qail-wasm` (~50KB)

Same syntax. Same validation. Any stack.

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
