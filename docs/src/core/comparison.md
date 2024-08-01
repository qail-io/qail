# QAIL vs Other Query Builders

This page compares QAIL's approach to building complex SQL queries against SQLx, Diesel, and SeaORM.

## The Example Query

A real-world production query from a WhatsApp integration that:
- SELECTs 7 columns with aliases
- Uses COALESCE for fallbacks
- Accesses nested JSON (`metadata->'vessel_bookings'->0->>'key'`)
- Concatenates strings (`||`)
- Casts types (`::text`, `::float`)
- Filters on JSON with OR
- Limits results

---

## SQLx (Raw SQL Strings)

```rust
sqlx::query_as::<_, OrderRow>(
    r#"
    SELECT o.id,
           COALESCE(o.booking_number, 'N/A') AS booking_number,
           o.status::text AS status,
           COALESCE(
               (o.metadata->'vessel_bookings'->0->>'depart_departure_loc') || ' → ' ||
               (o.metadata->'vessel_bookings'->0->>'depart_arrival_loc'),
               'Route'
           ) AS route,
           COALESCE(o.metadata->'vessel_bookings'->0->>'depart_travel_date', 'TBD') AS travel_date,
           COALESCE((o.total_fare::float / 100.0), 0) AS total_amount,
           COALESCE(o.currency, 'IDR') AS currency
    FROM orders o
    WHERE o.contact_info->>'phone' = $1 
       OR REPLACE(o.contact_info->>'phone', '+', '') = $1
    ORDER BY o.created_at DESC
    LIMIT 10
    "#,
).bind(phone).fetch_all(&pool).await?
```

| Pros | Cons |
|------|------|
| Full SQL power | ⚠️ SQL injection possible if not careful |
| No abstraction overhead | ❌ No compile-time column checks |
| | ❌ String interpolation risk |

---

## Diesel (ORM with DSL)

```rust
// First, define schema.rs...
table! {
    orders (id) {
        id -> Uuid,
        booking_number -> Nullable<Text>,
        metadata -> Jsonb,
        contact_info -> Jsonb,
        status -> Text,
        total_fare -> Int8,
        currency -> Nullable<Text>,
        created_at -> Timestamptz,
    }
}

// Query - but JSON operators are NOT natively supported!
orders::table
    .select((
        orders::id,
        coalesce(orders::booking_number, "N/A"),
        orders::status,
        // ❌ CANNOT DO: metadata->'vessel_bookings'->0->>'key'
        // ❌ CANNOT DO: string concatenation with ||
    ))
    .filter(/* JSON access not supported without extension */)
    .order(orders::created_at.desc())
    .limit(10)
    .load::<OrderRow>(&mut conn)?
```

| Pros | Cons |
|------|------|
| ✅ Compile-time schema validation | ❌ **No native JSON operators** |
| ✅ Type-safe | ❌ No async support |
| | ❌ Must use `sql_query()` for this query |
| | ❌ Heavy proc-macro compile times |

---

## SeaORM (ORM with Entity)

```rust
// First, define entity...
#[derive(Clone, Debug, DeriveEntityModel)]
#[sea_orm(table_name = "orders")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: Uuid,
    pub booking_number: Option<String>,
    pub metadata: Json<Value>,  // Opaque JSON
    pub contact_info: Json<Value>,
    pub status: String,
    pub total_fare: i64,
    pub currency: Option<String>,
    pub created_at: DateTimeUtc,
}

// Query - JSON access NOT supported!
Order::find()
    .filter(/* ❌ Cannot filter on contact_info->>'phone' */)
    .order_by_desc(Column::CreatedAt)
    .limit(10)
    .all(&pool)
    .await?

// Must fall back to raw SQL:
Order::find()
    .from_raw_sql(Statement::from_string(
        DatabaseBackend::Postgres,
        "SELECT ... the whole query ...".to_string()
    ))
```

| Pros | Cons |
|------|------|
| ✅ Clean API for simple queries | ❌ **No JSON operators** |
| ✅ Async support | ❌ No COALESCE builder |
| | ❌ Must use raw SQL for complex queries |

---

## QAIL (AST-Native)

```rust
use qail_core::ast::{QailCmd, Operator, builders::*};

let route = coalesce([
    concat([
        json_path("o.metadata", ["vessel_bookings", "0", "depart_departure_loc"]),
        text(" → "),
        json_path("o.metadata", ["vessel_bookings", "0", "depart_arrival_loc"]),
    ]),
    text("Route"),
]).alias("route");

let cmd = QailCmd::get("orders")
    .table_alias("o")
    // SELECT columns
    .column_expr(col("o.id"))
    .column_expr(coalesce([col("o.booking_number"), text("N/A")]).alias("booking_number"))
    .column_expr(cast(col("o.status"), "text").alias("status"))
    .column_expr(route)
    .column_expr(coalesce([
        json_path("o.metadata", ["vessel_bookings", "0", "depart_travel_date"]),
        text("TBD")
    ]).alias("travel_date"))
    .column_expr(coalesce([
        binary(cast(col("o.total_fare"), "float"), BinaryOp::Div, float(100.0)),
        int(0)
    ]).alias("total_amount"))
    .column_expr(coalesce([col("o.currency"), text("IDR")]).alias("currency"))
    // WHERE with JSON
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

| Pros | Cons |
|------|------|
| ✅ SQL injection **impossible** | Learning curve |
| ✅ Full JSON operator support | More verbose than raw SQL |
| ✅ COALESCE, CASE WHEN, CTEs | |
| ✅ String concat, type casting | |
| ✅ Async + connection pooling | |
| ✅ 28% faster than asyncpg (COPY) | |

---

## Summary

| Feature | SQLx | Diesel | SeaORM | **QAIL** |
|---------|------|--------|--------|----------|
| SQL Injection | ⚠️ Possible | ✅ Safe | ✅ Safe | ✅ **Impossible** |
| JSON Operators | ✅ String | ❌ Extension | ❌ Raw SQL | ✅ **Native json()** |
| COALESCE | ✅ String | ⚠️ Limited | ❌ Raw SQL | ✅ **coalesce()** |
| String Concat | ✅ String | ❌ Raw SQL | ❌ Raw SQL | ✅ **concat()** |
| CTEs | ✅ String | ❌ Raw SQL | ❌ Raw SQL | ✅ **with_cte()** |
| Async | ✅ Yes | ❌ No | ✅ Yes | ✅ **Yes** |
| Type Validation | ⚠️ DB/cache | ✅ schema! | ✅ Entity | ✅ **ColumnType enum** |
| PK Validation | ❌ Runtime | ⚠️ Schema | ⚠️ Schema | ✅ **can_be_primary_key()** |
| IDE Support | ⚠️ Limited | ⚠️ Limited | ⚠️ Limited | ✅ **qail-lsp** |

### QAIL's ColumnType Enum

QAIL provides compile-time type validation through the `ColumnType` enum:

```rust
pub enum ColumnType {
    Uuid, Text, Varchar(Option<u16>), Int, BigInt, 
    Serial, BigSerial, Bool, Float, Decimal(Option<(u8,u8)>),
    Jsonb, Timestamp, Timestamptz, Date, Time, Bytea,
}

// Compile-time validation methods
ColumnType::Uuid.can_be_primary_key()     // ✅ true
ColumnType::Jsonb.can_be_primary_key()    // ❌ false - caught at compile time!
ColumnType::Jsonb.supports_indexing()     // ❌ false - warned before migration
```

**QAIL's Sweet Spot:** Complex PostgreSQL queries with JSON, CTEs, and advanced SQL features—all type-safe with `ColumnType` validation, without falling back to raw SQL strings.
