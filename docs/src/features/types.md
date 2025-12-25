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
| `Decimal` | NUMERIC | Arbitrary precision |

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
