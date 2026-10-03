# Type System

QAIL provides type conversion between Rust and PostgreSQL.

## Supported Types

| Rust Type | PostgreSQL Type | Notes |
|-----------|-----------------|-------|
| `String` | TEXT, VARCHAR | UTF-8 |
| `i32` | INT4 | 32-bit integer |
| `i64` | INT8, BIGINT | 64-bit integer |
| `f64` | FLOAT8 | Double precision |
| `bool` | BOOLEAN | |
| `Uuid` | UUID | 128-bit |
| `Timestamp` | TIMESTAMP | Microsecond precision |
| `Date` | DATE | |
| `Time` | TIME | |
| `Json` | JSON, JSONB | |
| `Numeric` | NUMERIC | Arbitrary precision, kept as text |
| `Vec<u8>` | BYTEA | Text results in hex or escape `bytea_output` |
| `Vec<String>`, `Vec<i64>` | one-dimensional arrays | No NULL elements, lower bound 1 |
| `PgArray<T>` | any array | Dimensions, lower bounds, NULL elements |

### Special values

Text and binary results decode to the same value:

- `Numeric` keeps `NaN`, `Infinity`, `-Infinity` as those strings.
- `Timestamp::INFINITY` / `NEG_INFINITY` and `Date::INFINITY` / `NEG_INFINITY`
  are PostgreSQL's `infinity` sentinels; check `is_finite()` before
  converting (`Timestamp::try_to_unix_usec` refuses infinity).
  `chrono::DateTime<Utc>` cannot hold infinity and returns an error.
- `Time::END_OF_DAY` is `24:00:00`.
- `BC` dates use astronomical years (1 BC is year 0).
- Text temporal results decode from DateStyle ISO and German. SQL and Postgres
  DateStyles, and zone abbreviations, are errors: the value alone does not say
  day/month order or offset. Use DateStyle ISO or binary results.
- `Vec<String>` / `Vec<i64>` refuse multidimensional arrays, NULL elements and
  explicit bounds instead of flattening them; use `PgArray<T>`.

The AST `Value::Float` is finite-only: the native encoder rejects NaN and
infinity and the SQL preview renders them as an error marker. Write special
float values as a cast text literal, e.g. `'Infinity'::float8`.

## Compile-Time Type Safety

QAIL uses the `ColumnType` enum for compile-time validation in schema definitions:

```rust
use qail_core::migrate::{Column, ColumnType};

// ✅ Compile-time enforced - no typos possible
Column::new("id", ColumnType::Uuid).primary_key()
Column::new("name", ColumnType::Text).not_null()
Column::new("email", ColumnType::Varchar(Some(255))).unique()

// Available types:
// Uuid, Text, Varchar, Int, BigInt, Serial, BigSerial,
// Bool, Float, Decimal, Jsonb, Timestamp, Timestamptz, Date, Time, Bytea
```

**Validation at compile time:**
- `primary_key()` validates the type can be a PK (UUID, INT, SERIAL)
- `unique()` validates the type supports indexing (not JSONB, BYTEA)

## Usage

### Reading Values

```rust
use qail_pg::types::{Timestamp, Uuid, Json};

for row in rows {
    let id: i32 = row.get("id")?;
    let uuid: Uuid = row.get("uuid")?;
    let created: Timestamp = row.get("created_at")?;
    let data: Json = row.get("metadata")?;
}
```

### Temporal Types

```rust
use qail_pg::types::{Timestamp, Date, Time};

// Timestamp with microsecond precision
let ts = Timestamp::from_micros(1703520000000000);

// Date only
let date = Date::from_ymd(2024, 1, 15);

// Time only
let time = Time::from_hms(14, 30, 0);
```

### JSON

```rust
use qail_pg::types::Json;

let json = Json("{"key": "value"}".to_string());
```

## Custom Types

Implement `FromPg` and `ToPg` for custom types:

```rust
use qail_pg::types::{FromPg, ToPg, TypeError};

impl FromPg for MyType {
    fn from_pg(bytes: &[u8], oid: u32, format: i16) -> Result<Self, TypeError> {
        // Decode from wire format
    }
}

impl ToPg for MyType {
    fn to_pg(&self) -> (Vec<u8>, u32, i16) {
        // Encode to wire format
        (bytes, oid, format)
    }
}
```
