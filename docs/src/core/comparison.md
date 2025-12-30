# QAIL: The AST-Native Approach

QAIL takes a unique approach to building SQL queries: instead of strings or macros, queries are constructed as a typed Abstract Syntax Tree.

## The AST-Native Difference

| Approach | How Queries Work |
|----------|------------------|
| **String-based** | SQL written as text, parameterized at runtime |
| **Macro-based** | DSL macros expand to SQL at compile time |
| **AST-Native** | Typed AST compiles directly to wire protocol |

## What QAIL Enables

### Native PostgreSQL Features

```rust
use qail_core::{Qail, builders::*};

// Native JSON operators (->, ->>)
json_path("metadata", ["vessel_bookings", "0", "key"])

// COALESCE with type safety
coalesce([col("booking_number"), text("N/A")])

// String concatenation
concat([col("first_name"), text(" "), col("last_name")])

// Type casting  
cast(col("total_fare"), "float")

// CASE WHEN expressions
case_when(gt("score", 80), text("pass"))
    .otherwise(text("fail"))
```

### Full Query Example

A production WhatsApp integration query with JSON access, string concat, and type casts:

```rust
use qail_core::{Qail, Operator, builders::*};

let route = coalesce([
    concat([
        json_path("o.metadata", ["vessel_bookings", "0", "depart_departure_loc"]),
        text(" → "),
        json_path("o.metadata", ["vessel_bookings", "0", "depart_arrival_loc"]),
    ]),
    text("Route"),
]).alias("route");

let cmd = Qail::get("orders")
    .table_alias("o")
    .column_expr(col("o.id"))
    .column_expr(coalesce([col("o.booking_number"), text("N/A")]).alias("booking_number"))
    .column_expr(cast(col("o.status"), "text").alias("status"))
    .column_expr(route)
    .column_expr(coalesce([
        json_path("o.metadata", ["vessel_bookings", "0", "depart_travel_date"]),
        text("TBD")
    ]).alias("travel_date"))
    .filter_cond(cond(json("o.contact_info", "phone"), Operator::Eq, param(1)))
    .or_filter_cond(cond(
        replace(json("o.contact_info", "phone"), text("+"), text("")),
        Operator::Eq, 
        param(1)
    ))
    .order_desc("o.created_at")
    .limit(10);

let orders = pool.fetch_all::<OrderRow>(&cmd).await?;
```

## QAIL Highlights

| Feature | QAIL Approach |
|---------|---------------|
| **Safety** | Structural - no SQL strings to inject |
| **JSON** | Native `json()`, `json_path()` operators |
| **Expressions** | `coalesce()`, `concat()`, `cast()` builders |
| **CTEs** | `with_cte()` for complex queries |
| **Async** | Full async/await support |
| **Type Validation** | `ColumnType` enum with compile-time checks |

### ColumnType Validation

QAIL validates types at build time:

```rust
pub enum ColumnType {
    Uuid, Text, Varchar(Option<u16>), Int, BigInt, 
    Serial, BigSerial, Bool, Float, Decimal(Option<(u8,u8)>),
    Jsonb, Timestamp, Timestamptz, Date, Time, Bytea,
}

// Compile-time validation
ColumnType::Uuid.can_be_primary_key()     // true
ColumnType::Jsonb.can_be_primary_key()    // false - caught at build time
ColumnType::Jsonb.supports_indexing()     // false - warned before migration
```

## When to Use QAIL

QAIL shines for:
- **Complex PostgreSQL queries** with JSON, CTEs, aggregates
- **Type-safe query building** with IDE support
- **Production systems** where safety is critical
- **Projects** that need advanced SQL features without string literals
