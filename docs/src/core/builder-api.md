# AST Builder API

The recommended way to use QAIL. Build queries as typed Rust structs.

## Query Types

| Method | SQL Equivalent |
|--------|----------------|
| `QailCmd::get()` | SELECT |
| `QailCmd::add()` | INSERT |
| `QailCmd::set()` | UPDATE |
| `QailCmd::del()` | DELETE |

## SELECT Queries

```rust
use qail_core::ast::{QailCmd, Operator, SortOrder};

let cmd = QailCmd::get("users")
    .columns(["id", "email", "name"])
    .filter("active", Operator::Eq, true)
    .order_by("created_at", SortOrder::Desc)
    .limit(10)
    .offset(20);
```

## INSERT Queries

```rust
let cmd = QailCmd::add("users")
    .columns(["email", "name"])
    .values(["alice@example.com", "Alice"])
    .returning(["id", "created_at"]);
```

## UPDATE Queries

```rust
let cmd = QailCmd::set("users")
    .set_value("status", "active")
    .set_value("verified_at", "now()")
    .where_eq("id", 42);
```

## DELETE Queries

```rust
let cmd = QailCmd::del("users")
    .where_eq("id", 42);
```

## Builder Methods

| Method | Description |
|--------|-------------|
| `.columns([...])` | Select specific columns |
| `.select_all()` | SELECT * |
| `.filter(col, op, val)` | WHERE condition |
| `.where_eq(col, val)` | WHERE col = val |
| `.order_by(col, dir)` | ORDER BY |
| `.limit(n)` | LIMIT n |
| `.offset(n)` | OFFSET n |
| `.left_join(table, on_left, on_right)` | LEFT JOIN |
| `.returning([...])` | RETURNING clause |
