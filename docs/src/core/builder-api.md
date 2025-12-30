# AST Builder API

The recommended way to use QAIL. Build queries as typed Rust structs.

## Query Types

| Method | SQL Equivalent |
|--------|----------------|
| `Qail::get()` | SELECT |
| `Qail::add()` | INSERT |
| `Qail::set()` | UPDATE |
| `Qail::del()` | DELETE |

## SELECT Queries

```rust
use qail_core::{Qail, Operator, SortOrder};

let cmd = Qail::get("users")
    .columns(["id", "email", "name"])
    .filter("active", Operator::Eq, true)
    .order_by("created_at", SortOrder::Desc)
    .limit(10)
    .offset(20);
```

## INSERT Queries

```rust
let cmd = Qail::add("users")
    .columns(["email", "name"])
    .values(["alice@example.com", "Alice"])
    .returning(["id", "created_at"]);
```

## UPDATE Queries

```rust
let cmd = Qail::set("users")
    .set_value("status", "active")
    .set_value("verified_at", "now()")
    .where_eq("id", 42);
```

## DELETE Queries

```rust
let cmd = Qail::del("users")
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
