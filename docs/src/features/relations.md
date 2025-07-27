# First-Class Relations

QAIL enables **implicit joins** through schema-defined foreign key relationships. Instead of manually specifying join conditions, define relationships once in `schema.qail` and use `join_on()` to automatically infer the join.

## The Dream

```rust
// ❌ Before: Explicit join conditions
Qail::get("users")
    .left_join("posts", "users.id", "posts.user_id")

// ✅ After: Implicit joins via schema
Qail::get("users")
    .join_on("posts")
```

## 1. Define Relations in Schema

Use `ref:` syntax to declare foreign key relationships:

```qail
table users {
    id UUID primary_key
    email TEXT not_null
}

table posts {
    id UUID primary_key
    user_id UUID ref:users.id
    title TEXT
}
```

The `ref:users.id` annotation tells QAIL that `posts.user_id` references `users.id`.

## 2. Load Relations at Runtime

Before using `join_on()`, load the schema relations:

```rust
use qail_core::schema;

// Load at application startup
schema::load_schema_relations("schema.qail")?;
```

## 3. Use Implicit Joins

```rust
use qail_core::Qail;

// Auto-infers: LEFT JOIN posts ON users.id = posts.user_id
let query = Qail::get("users")
    .columns(["users.id", "users.email", "posts.title"])
    .join_on("posts");

// Forward and reverse relations work automatically
let posts_with_users = Qail::get("posts")
    .join_on("users");  // Infers: LEFT JOIN users ON posts.user_id = users.id
```

## API Reference

### `join_on(table)`

Joins a related table using the schema-defined foreign key. Panics if no relation exists.

```rust
Qail::get("users").join_on("posts")
```

### `join_on_optional(table)`

Same as `join_on()`, but returns `self` unchanged if no relation exists (no panic).

```rust
Qail::get("users").join_on_optional("comments")  // No-op if no relation
```

## How It Works

1. **Schema Parsing**: `build.rs` parses `ref:` annotations and stores them as `ForeignKey` entries
2. **Runtime Registry**: `schema::load_schema_relations()` populates a global `RelationRegistry`
3. **Lookup**: `join_on()` calls `lookup_relation()` to find the join condition
4. **Bidirectional**: Both forward (`posts.user_id → users.id`) and reverse directions are checked
