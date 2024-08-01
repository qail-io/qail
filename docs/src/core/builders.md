# Expression Builders

QAIL provides ergonomic builder functions for constructing AST expressions without verbose struct creation.

## Import

```rust
use qail_core::ast::builders::*;
```

## Column References

```rust
// Named column
col("phone_number")

// Star (*)
star()
```

## Aggregate Functions

```rust
// COUNT(*)
count()

// COUNT(DISTINCT column)
count_distinct("phone_number")

// COUNT(*) FILTER (WHERE ...)
count_filter(vec![
    eq("direction", "outbound"),
    gt("created_at", now_minus("24 hours")),
]).alias("messages_sent_24h")

// Other aggregates
sum("amount")
avg("score")
min("price")
max("quantity")
```

All aggregates support:
- `.distinct()` — Add DISTINCT modifier
- `.filter(conditions)` — Add FILTER clause
- `.alias("name")` — Add AS alias

## Time Functions

```rust
// NOW()
now()

// INTERVAL 'duration'
interval("24 hours")

// NOW() - INTERVAL 'duration' (common pattern)
now_minus("24 hours")

// NOW() + INTERVAL 'duration'
now_plus("7 days")
```

## Type Casting

```rust
// expr::type
cast(col("amount"), "float8")

// With alias
cast(col("amount"), "float8").alias("amount_f")
```

## CASE WHEN Expressions

```rust
// Simple CASE
case_when(gt("score", 80), text("A"))
    .otherwise(text("F"))
    .alias("grade")

// Multiple WHEN clauses
case_when(gt("score", 90), text("A"))
    .when(gt("score", 80), text("B"))
    .when(gt("score", 70), text("C"))
    .otherwise(text("F"))
    .alias("grade")
```

## Condition Helpers

```rust
// Equality
eq("status", "active")      // status = 'active'
ne("status", "deleted")     // status != 'deleted'

// Comparisons
gt("created_at", now_minus("24 hours"))   // created_at > NOW() - INTERVAL '24 hours'
gte("age", 18)              // age >= 18
lt("price", 100)            // price < 100
lte("quantity", 10)         // quantity <= 10

// IN / NOT IN
is_in("status", ["delivered", "read"])    // status IN ('delivered', 'read')
not_in("type", ["spam", "junk"])          // type NOT IN ('spam', 'junk')

// NULL checks
is_null("deleted_at")       // deleted_at IS NULL
is_not_null("email")        // email IS NOT NULL

// Pattern matching
like("name", "John%")       // name LIKE 'John%'
ilike("email", "%@gmail%")  // email ILIKE '%@gmail%'
```

## Function Calls

```rust
// Generic function
func("MY_FUNC", vec![col("a"), col("b")])

// COALESCE
coalesce(vec![col("nickname"), col("name"), text("Anonymous")])

// NULLIF
nullif(col("value"), int(0))
```

## Binary Expressions

```rust
// Arithmetic
binary(col("price"), BinaryOp::Mul, col("quantity"))

// With alias
binary(
    cast(col("success"), "float8"),
    BinaryOp::Div,
    cast(col("total"), "float8")
).alias("success_rate")
```

## Literals

```rust
int(42)           // Integer literal
float(3.14)       // Float literal
text("hello")     // String literal (quoted)
```

## Complete Example

Here's a complex analytics query using all the builders:

```rust
use qail_core::ast::builders::*;

let stats = QailCmd::get("whatsapp_messages")
    .columns([
        count_distinct("phone_number").alias("total_contacts"),
        count().alias("total_messages"),
        count_filter(vec![
            eq("direction", "outbound"),
            gt("created_at", now_minus("24 hours")),
        ]).alias("messages_sent_24h"),
        count_filter(vec![
            eq("direction", "inbound"),
            eq("status", "received"),
        ]).alias("unread_messages"),
    ]);

let cmd = QailCmd::get("stats")
    .with_cte("stats", stats)
    .columns([
        col("total_contacts"),
        col("total_messages"),
        case_when(gt("messages_sent_24h", 0),
            binary(
                cast(col("successful"), "float8"),
                BinaryOp::Div,
                cast(col("messages_sent_24h"), "float8")
            )
        ).otherwise(float(0.0)).alias("delivery_rate"),
    ]);
```

This replaces 40+ lines of raw SQL with type-safe, compile-time checked Rust code.
