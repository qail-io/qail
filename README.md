# 🪝 QAIL — The Horizontal Query Language

> **Stop writing strings. Hook your data.**

[![Crates.io](https://img.shields.io/badge/crates.io-qail-orange)](https://crates.io/crates/qail)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75+-blueviolet)](https://www.rust-lang.org/)

---

## The Manifesto

SQL is **vertical**, verbose, and clunky inside modern codebases.

QAIL is **horizontal**, dense, and composable. It treats database queries like a pipeline, using symbols to **hook** data and pull it into your application.

```sql
-- The Old Way (SQL)
SELECT id, email, role FROM users WHERE active = true LIMIT 1;
```

```bash
# The QAIL Way
get::users•@id@email@role[active=true][lim=1]
```

One line. Zero ceremony. **Maximum velocity.**

---

## 📖 Quick Reference

| Symbol | Name       | Function                | SQL Equivalent           |
|--------|------------|-------------------------|--------------------------|
| `::`   | The Gate   | Defines the action      | `SELECT`, `INSERT`, `UPDATE` |
| `•`    | The Pivot  | Connects action to table| `FROM table`             |
| `@`    | The Hook   | Selects specific columns| `col1, col2`             |
| `[]`   | The Cage   | Constraints & Filters   | `WHERE`, `LIMIT`, `SET`  |
| `~`    | The Fuse   | Fuzzy / Partial Match   | `ILIKE '%val%'`          |
| `\|`   | The Split  | Logical OR              | `OR`                     |
| `&`    | The Bind   | Logical AND             | `AND`                    |
| `^!`   | The Peak   | Sort Descending         | `ORDER BY ... DESC`      |
| `^`    | The Rise   | Sort Ascending          | `ORDER BY ... ASC`       |
| `*`    | The Star   | All / Wildcard          | `*`                      |
| `[*]`  | The Deep   | Array Unnest            | `UNNEST(arr)`            |
| `$`    | The Var    | Parameter Injection     | `$1`, `$2`               |

---

## 🚀 Installation

### CLI (Recommended)

```bash
cargo install qail
```

### As a Library

```toml
# Cargo.toml
[dependencies]
qail = "0.1"
```

---

## 💡 Usage

### CLI — The `qail` Command

```bash
# Fetch all users
qail 'get::users•@*'

# Get specific columns with filter
qail 'get::orders•@id@total@status[user_id=$1][lim=10]' --bind 42

# Update a record
qail 'set::users•[verified=true][id=$1]' --bind 7

# Delete with condition
qail 'del::sessions•[expired_at<now]'

# Transpile only (don't execute)
qail 'get::users•@*[active=true]' --dry-run
```

### As a Library

```rust
use qail::prelude::*;

#[tokio::main]
async fn main() -> Result<(), QailError> {
    let db = QailDB::connect("postgres://localhost/mydb").await?;

    // Parse and execute
    let users: Vec<User> = db
        .query("get::users•@id@email@role[active=true][lim=10]")
        .fetch_all()
        .await?;

    // Or use the builder for type-safe composition
    let query = qail::get("users")
        .hook(&["id", "email", "role"])
        .cage("active", true)
        .limit(10);

    let users: Vec<User> = db.run(query).fetch_all().await?;

    Ok(())
}
```

---

## 📚 Syntax Deep Dive

### A. Simple Fetch (`get::`)

```sql
-- SQL
SELECT id, email, role FROM users WHERE active = true LIMIT 1;
```

```bash
# QAIL
get::users•@id@email@role[active=true][lim=1]
```

---

### B. Mutation (`set::`)

```sql
-- SQL
UPDATE user_verifications SET consumed_at = now() WHERE id = $1;
```

```bash
# QAIL
set::user_verifications•[consumed_at=now][id=$1]
```

> **Note:** In `set::` mode, the **first `[]`** is the payload (SET), the **second `[]`** is the filter (WHERE).

---

### C. Deletion (`del::`)

```sql
-- SQL
DELETE FROM sessions WHERE expired_at < now();
```

```bash
# QAIL
del::sessions•[expired_at<now]
```

---

### D. Complex Search with Fuzzy Match

```sql
-- SQL
SELECT * FROM ai_knowledge_base 
WHERE active = true 
AND (topic ILIKE $1 OR question ILIKE $1 OR EXISTS (SELECT 1 FROM unnest(keywords) k WHERE k ILIKE $1))
ORDER BY created_at DESC
LIMIT 5;
```

```bash
# QAIL
get::ai_knowledge_base•@*[active=true][topic~$1|question~$1|keywords[*]~$1][^!created_at][lim=5]

# Or multi-line for readability:
get::ai_knowledge_base•@*
  [active=true]
  [topic~$1 | question~$1 | keywords[*]~$1]
  [^!created_at]
  [lim=5]
```

---

### E. Joins (Coming Soon)

```bash
# Inner join
get::orders•@*>>users.id=orders.user_id•@users.email[status=paid]

# Left join
get::products•@*>|categories.id=products.cat_id•@categories.name
```

---

## ⚙️ Configuration

Create a `.qailrc` or `qail.toml` in your project root:

```toml
[connection]
driver = "postgres"           # postgres | mysql | sqlite
url = "postgres://localhost/mydb"

[output]
format = "table"              # table | json | csv
color = true

[safety]
confirm_mutations = true      # Prompt before UPDATE/DELETE
dry_run_default = false
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      QAIL Pipeline                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   "get::users•@*[active=true]"                              │
│              │                                              │
│              ▼                                              │
│   ┌─────────────────────┐                                   │
│   │   Parser (nom)      │  → Tokenize symbols               │
│   └─────────┬───────────┘                                   │
│             │                                               │
│             ▼                                               │
│   ┌─────────────────────┐                                   │
│   │   AST (QailCmd)     │  → Structured representation      │
│   └─────────┬───────────┘                                   │
│             │                                               │
│             ▼                                               │
│   ┌─────────────────────┐                                   │
│   │ Transpiler (SQL)    │  → Generate valid SQL             │
│   └─────────┬───────────┘                                   │
│             │                                               │
│             ▼                                               │
│   ┌─────────────────────┐                                   │
│   │ Engine (sqlx)       │  → Execute against DB             │
│   └─────────────────────┘                                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Core Structs

```rust
pub struct QailCmd {
    pub action: Action,         // GET, SET, DEL, ADD
    pub table: String,
    pub columns: Vec<Column>,
    pub cages: Vec<Cage>,       // Filters, limits, sorts
    pub bindings: Vec<Value>,
}

pub enum Action {
    Get,    // SELECT
    Set,    // UPDATE
    Del,    // DELETE
    Add,    // INSERT
}

pub struct Cage {
    pub kind: CageKind,         // Filter, Limit, Sort, Payload
    pub conditions: Vec<Condition>,
}

pub struct Condition {
    pub column: String,
    pub op: Operator,           // Eq, Ne, Gt, Lt, Fuzzy, In
    pub value: Value,
}
```

---

## 🗺️ Roadmap

### Phase 1: Parser ✅
- [x] Lexer for QAIL symbols
- [x] `nom` parser combinators
- [x] AST generation

### Phase 2: Transpiler 🚧
- [x] PostgreSQL codegen
- [ ] MySQL codegen
- [ ] SQLite codegen
- [ ] Query optimization hints

### Phase 3: Engine 🔜
- [ ] Async execution (sqlx)
- [ ] Connection pooling
- [ ] Transaction support
- [ ] Prepared statement caching

### Phase 4: Ecosystem 📋
- [ ] VS Code extension (syntax highlighting)
- [ ] Language server (LSP)
- [ ] REPL mode
- [ ] Schema introspection

---

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

```bash
# Clone the repo
git clone https://github.com/your-username/qail.git
cd qail

# Run tests
cargo test

# Run with example
cargo run -- "get::users•@*[lim=5]" --dry-run
```

---

## 📄 License

MIT © 2025 QAIL Contributors

---

<p align="center">
  <strong>Built with 🦀 Rust and ☕ caffeine</strong><br>
  <a href="https://qail.rs">qail.rs</a>
</p>
