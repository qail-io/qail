# Compile-Time Type Safety

> **New in v0.14.20** — Full Diesel-like type checking for QAIL queries

QAIL now supports compile-time type validation through generated schema files, similar to Diesel but with AST-native architecture.

## Quick Start

### 1. Generate Schema

```bash
qail types schema.qail -o src/schema.rs
```

### 2. Use Type-Safe Builders

```rust
use crate::schema::users;

let query = Qail::get(users::TABLE)
    .typed_column(users::id())
    .typed_column(users::email())
    .typed_eq(users::active(), true)  // Compile-time: active must be bool
    .typed_gt(users::age(), 18);       // Compile-time: age must be numeric
```

## Schema Generation

### Input: `schema.qail`

```qail
table users {
    id          uuid primary_key
    email       text not_null unique
    name        text
    active      boolean default(true)
    age         integer
    created_at  timestamptz default(now())
}
```

### Output: `schema.rs`

```rust
pub mod users {
    use qail_core::typed::{TypedColumn, Table};
    
    pub const TABLE: &str = "users";
    
    pub fn id() -> TypedColumn<uuid::Uuid> {
        TypedColumn::new("id")
    }
    
    pub fn email() -> TypedColumn<String> {
        TypedColumn::new("email")
    }
    
    pub fn active() -> TypedColumn<bool> {
        TypedColumn::new("active")
    }
    
    pub fn age() -> TypedColumn<i32> {
        TypedColumn::new("age")
    }
}
```

## Type-Safe Methods

| Method | Description | Example |
|--------|-------------|---------|
| `typed_eq(col, val)` | Type-safe equality | `typed_eq(users::active(), true)` |
| `typed_ne(col, val)` | Type-safe not-equal | `typed_ne(users::status(), "banned")` |
| `typed_gt(col, val)` | Type-safe greater-than | `typed_gt(users::age(), 18)` |
| `typed_lt(col, val)` | Type-safe less-than | `typed_lt(users::balance(), 0.0)` |
| `typed_gte(col, val)` | Greater-than or equal | `typed_gte(users::score(), 100)` |
| `typed_lte(col, val)` | Less-than or equal | `typed_lte(users::priority(), 5)` |
| `typed_column(col)` | Add typed column | `typed_column(users::email())` |

## SQL to Rust Type Mapping

| SQL Type | Rust Type |
|----------|-----------|
| `uuid` | `uuid::Uuid` |
| `text`, `varchar` | `String` |
| `integer`, `int4` | `i32` |
| `bigint`, `int8` | `i64` |
| `smallint`, `int2` | `i16` |
| `boolean`, `bool` | `bool` |
| `real`, `float4` | `f32` |
| `double precision`, `float8` | `f64` |
| `numeric`, `decimal` | `f64` |
| `timestamptz`, `timestamp` | `chrono::DateTime<Utc>` |
| `date` | `chrono::NaiveDate` |
| `jsonb`, `json` | `serde_json::Value` |
| `bytea` | `Vec<u8>` |

## Reserved Keywords

Rust reserved keywords are automatically escaped:

| Column Name | Generated Function |
|-------------|-------------------|
| `type` | `fn r#type()` |
| `fn` | `fn r#fn()` |
| `struct` | `fn r#struct()` |

## Compile-Time Errors

Type mismatches are caught at compile time:

```rust
// ✅ Compiles - active is bool
query.typed_eq(users::active(), true);

// ❌ Compile error - age is i32, not string
query.typed_eq(users::age(), "eighteen");
// error[E0277]: the trait bound `&str: ColumnValue<i32>` is not satisfied
```

## Integration with Existing Code

Type-safe methods can be mixed with dynamic methods:

```rust
let query = Qail::get(users::TABLE)
    .typed_eq(users::active(), true)  // Type-safe
    .filter("created_at", Operator::Gte, "2024-01-01")  // Dynamic
    .typed_column(users::email());
```
