# Expression Types

QAIL v0.14.2 provides **100% PostgreSQL expression coverage**. All expression types are native AST nodes that encode directly to wire protocol bytes.

## Coverage

| Category | Coverage |
|----------|----------|
| Expressions | 100% |
| DML (SELECT, INSERT, UPDATE, DELETE) | 100% |
| DDL (CREATE, DROP, ALTER) | 100% |

## Basic Expressions

### Column Reference

```rust
use qail_core::ast::Expr;

// Simple column
let expr = Expr::Named("email".to_string());
// → email

// With alias
let expr = Expr::Aliased {
    name: "users.email".to_string(),
    alias: "user_email".to_string(),
};
// → users.email AS user_email
```

### Literals

```rust
use qail_core::ast::{Expr, Value};

let expr = Expr::Literal(Value::Int(42));
// → 42

let expr = Expr::Literal(Value::String("hello".into()));
// → 'hello'
```

## Aggregate Functions

```rust
use qail_core::ast::{Expr, AggregateFunc};

let expr = Expr::Aggregate {
    func: AggregateFunc::Count,
    col: "*".into(),
    distinct: false,
    filter: None,
    alias: Some("total".into()),
};
// → COUNT(*) AS total
```

### With FILTER Clause (v0.14.2+)

```rust
let expr = Expr::Aggregate {
    func: AggregateFunc::Sum,
    col: "amount".into(),
    distinct: false,
    filter: Some(vec![condition]),  // WHERE condition
    alias: Some("filtered_sum".into()),
};
// → SUM(amount) FILTER (WHERE ...) AS filtered_sum
```

## Window Functions

```rust
use qail_core::ast::{Expr, WindowFrame, FrameBound};

let expr = Expr::Window {
    func: "SUM".into(),
    params: vec![Expr::Named("amount".into())],
    partition: vec!["department".into()],
    order: vec![order_spec],
    frame: Some(WindowFrame::Rows {
        start: FrameBound::UnboundedPreceding,
        end: FrameBound::CurrentRow,
    }),
    alias: Some("running_total".into()),
};
// → SUM(amount) OVER (
//     PARTITION BY department
//     ORDER BY date
//     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
//   ) AS running_total
```

## CASE Expressions

```rust
let expr = Expr::Case {
    when_clauses: vec![
        (condition1, Value::String("A".into())),
        (condition2, Value::String("B".into())),
    ],
    else_value: Some(Value::String("C".into())),
    alias: Some("grade".into()),
};
// → CASE WHEN ... THEN 'A' WHEN ... THEN 'B' ELSE 'C' END AS grade
```

## New in v0.14.2

### Array Constructor

```rust
let expr = Expr::ArrayConstructor {
    elements: vec![
        Expr::Named("col1".into()),
        Expr::Named("col2".into()),
    ],
    alias: Some("arr".into()),
};
// → ARRAY[col1, col2] AS arr
```

### Row Constructor

```rust
let expr = Expr::RowConstructor {
    elements: vec![
        Expr::Named("id".into()),
        Expr::Named("name".into()),
    ],
    alias: Some("person".into()),
};
// → ROW(id, name) AS person
```

### Subscript (Array Access)

```rust
let expr = Expr::Subscript {
    expr: Box::new(Expr::Named("tags".into())),
    index: Box::new(Expr::Literal(Value::Int(1))),
    alias: Some("first_tag".into()),
};
// → tags[1] AS first_tag
```

### Collation

```rust
let expr = Expr::Collate {
    expr: Box::new(Expr::Named("name".into())),
    collation: "C".into(),
    alias: None,
};
// → name COLLATE "C"
```

### Field Access (Composite Types)

```rust
let expr = Expr::FieldAccess {
    expr: Box::new(Expr::Named("address".into())),
    field: "city".into(),
    alias: Some("city".into()),
};
// → (address).city AS city
```

## Type Casting

```rust
let expr = Expr::Cast {
    expr: Box::new(Expr::Named("id".into())),
    target_type: "TEXT".into(),
    alias: None,
};
// → id::TEXT
```

## JSON Access

```rust
let expr = Expr::JsonAccess {
    column: "data".into(),
    path_segments: vec![
        ("user".into(), false),  // ->
        ("name".into(), true),   // ->>
    ],
    alias: Some("name".into()),
};
// → data->'user'->>'name' AS name
```

## GROUP BY Modes

| Mode | SQL | Status |
|------|-----|--------|
| `GroupByMode::Simple` | `GROUP BY a, b` | ✓ |
| `GroupByMode::Rollup` | `GROUP BY ROLLUP(a, b)` | ✓ |
| `GroupByMode::Cube` | `GROUP BY CUBE(a, b)` | ✓ |
| `GroupByMode::GroupingSets` | `GROUP BY GROUPING SETS ((a, b), (c))` | ✓ v0.14.2 |

```rust
use qail_core::ast::GroupByMode;

// GROUPING SETS
let mode = GroupByMode::GroupingSets(vec![
    vec!["year".into(), "month".into()],
    vec!["year".into()],
    vec![],  // grand total
]);
// → GROUP BY GROUPING SETS ((year, month), (year), ())
```

## DDL Actions

| Action | SQL | Status |
|--------|-----|--------|
| `Action::Make` | `CREATE TABLE` | ✓ |
| `Action::Drop` | `DROP TABLE` | ✓ |
| `Action::Index` | `CREATE INDEX` | ✓ |
| `Action::CreateView` | `CREATE VIEW AS` | ✓ v0.14.2 |
| `Action::DropView` | `DROP VIEW` | ✓ v0.14.2 |

```rust
use qail_core::ast::{Qail, Action};

// Create view
let mut cmd = Qail::get("orders")
    .columns(["customer_id", "SUM(amount) AS total"])
    .group_by(["customer_id"]);
cmd.action = Action::CreateView;
cmd.table = "customer_totals".into();
// → CREATE VIEW customer_totals AS SELECT ...
```
