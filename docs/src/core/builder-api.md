# AST Builder API

The recommended way to use QAIL. Build queries as typed Rust structs.

## Query Types

| Method | SQL Equivalent |
|--------|----------------|
| `Qail::get()` | SELECT |
| `Qail::add()` | INSERT |
| `Qail::set()` | UPDATE |
| `Qail::del()` | DELETE |
| `Qail::put()` | UPSERT (INSERT ON CONFLICT) |
| `Qail::make()` | CREATE TABLE |
| `Qail::truncate()` | TRUNCATE |
| `Qail::explain()` | EXPLAIN |
| `Qail::explain_analyze()` | EXPLAIN ANALYZE |
| `Qail::lock()` | LOCK TABLE |
| `Qail::listen()` | LISTEN (Pub/Sub) |
| `Qail::notify()` | NOTIFY (Pub/Sub) |
| `Qail::unlisten()` | UNLISTEN (Pub/Sub) |
| `Qail::export()` | COPY TO |

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

## Pub/Sub (LISTEN/NOTIFY)

```rust
// Subscribe to a channel
let listen = Qail::listen("orders");
// → LISTEN orders

// Send a notification
let notify = Qail::notify("orders", "new_order:123");
// → NOTIFY orders, 'new_order:123'

// Unsubscribe
let unlisten = Qail::unlisten("orders");
// → UNLISTEN orders
```

## Builder Methods

### Column Selection

| Method | Description |
|--------|-------------|
| `.columns([...])` | Select specific columns |
| `.column("col")` | Add a single column |
| `.select_all()` | SELECT * |
| `.column_expr(expr)` | Add an expression as column |
| `.returning([...])` | RETURNING clause |
| `.returning_all()` | RETURNING * |

### Filtering

| Method | Description |
|--------|-------------|
| `.filter(col, op, val)` | WHERE condition |
| `.or_filter(col, op, val)` | OR condition |
| `.where_eq(col, val)` | WHERE col = val |
| `.eq(col, val)` | Shorthand for `= val` |
| `.ne(col, val)` | Shorthand for `!= val` |
| `.gt(col, val)` | `> val` |
| `.gte(col, val)` | `>= val` |
| `.lt(col, val)` | `< val` |
| `.lte(col, val)` | `<= val` |
| `.is_null(col)` | IS NULL |
| `.is_not_null(col)` | IS NOT NULL |
| `.like(col, pattern)` | LIKE pattern |
| `.ilike(col, pattern)` | ILIKE pattern (case-insensitive) |
| `.in_vals(col, [...])` | IN (values) |
| `.filter_cond(condition)` | Add a raw `Condition` struct |

### Sorting & Pagination

| Method | Description |
|--------|-------------|
| `.order_by(col, dir)` | ORDER BY |
| `.order_asc(col)` | ORDER BY col ASC |
| `.order_desc(col)` | ORDER BY col DESC |
| `.limit(n)` | LIMIT n |
| `.offset(n)` | OFFSET n |
| `.fetch_first(n)` | FETCH FIRST n ROWS ONLY |
| `.fetch_with_ties(n)` | FETCH FIRST n ROWS WITH TIES |

### Joins

| Method | Description |
|--------|-------------|
| `.left_join(table, left, right)` | LEFT JOIN |
| `.inner_join(table, left, right)` | INNER JOIN |
| `.left_join_as(table, alias, left, right)` | LEFT JOIN with alias |
| `.inner_join_as(table, alias, left, right)` | INNER JOIN with alias |
| `.join(kind, table, left, right)` | Generic join |
| `.join_on(related)` | Auto-inferred FK join |
| `.join_on_optional(related)` | FK join (no-op if no relation) |

### Grouping & Aggregation

| Method | Description |
|--------|-------------|
| `.group_by([...])` | GROUP BY columns |
| `.having_cond(condition)` | HAVING clause |
| `.distinct_on([...])` | DISTINCT ON columns |
| `.distinct_on_all()` | DISTINCT ON all columns |

### Mutations

| Method | Description |
|--------|-------------|
| `.values([...])` | INSERT values |
| `.set_value(col, val)` | SET col = val (UPDATE) |
| `.default_values()` | INSERT with DEFAULT VALUES |
| `.on_conflict(...)` | ON CONFLICT handling |

### Advanced

| Method | Description |
|--------|-------------|
| `.table_alias(alias)` | FROM table AS alias |
| `.for_update()` | SELECT ... FOR UPDATE |
| `.for_share()` | SELECT ... FOR SHARE |
| `.for_no_key_update()` | FOR NO KEY UPDATE |
| `.for_key_share()` | FOR KEY SHARE |
| `.tablesample_bernoulli(pct)` | TABLESAMPLE BERNOULLI |
| `.tablesample_system(pct)` | TABLESAMPLE SYSTEM |
| `.repeatable(seed)` | REPEATABLE (seed) |
| `.only()` | FROM ONLY table |
| `.overriding_system_value()` | OVERRIDING SYSTEM VALUE |
| `.overriding_user_value()` | OVERRIDING USER VALUE |
| `.update_from([...])` | UPDATE ... FROM tables |
| `.delete_using([...])` | DELETE ... USING tables |
| `.with_ctes(ctes)` | WITH (Common Table Expressions) |
| `.with_rls(&ctx)` | Inject RLS context |

### Materialized Views

```rust
// Create
let view = Qail::create_materialized_view(
    "monthly_stats",
    Qail::get("orders")
        .columns(["date_trunc('month', created_at) AS month", "sum(total)"])
        .group_by(["month"])
);

// Refresh
let refresh = Qail::refresh_materialized_view("monthly_stats");

// Drop
let drop = Qail::drop_materialized_view("monthly_stats");
```
