# Quick Start

## Connect to PostgreSQL

```rust
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect with password (SCRAM-SHA-256)
    let mut driver = PgDriver::connect_with_password(
        "localhost", 5432, "user", "database", "password"
    ).await?;

    // Or with SSL/TLS
    let mut driver = PgDriver::connect(
        "localhost", 5432, "user", "database"
    ).await?;

    Ok(())
}
```

## Execute Your First Query

```rust
use qail_core::Qail;
use qail_core::ast::Operator;

// Build a SELECT query
let cmd = Qail::get("users")
    .columns(["id", "email"])
    .filter("active", Operator::Eq, true)
    .limit(10);

// Execute
let rows = driver.fetch_all(&cmd).await?;

for row in rows {
    let id: i32 = row.get("id")?;
    let email: String = row.get("email")?;
    println!("{}: {}", id, email);
}
```

## Use Connection Pooling

```rust
use qail_pg::driver::{PgPool, PoolConfig};
use qail_core::Qail;

let config = PoolConfig::new("localhost", 5432, "user", "db")
    .password("secret")
    .max_connections(20);

let pool = PgPool::connect(config).await?;

// Acquire connection from pool
let mut conn = pool.acquire().await?;
let probe = Qail::get("users").columns(["id"]).limit(1);
let _rows = conn.fetch_all(&probe).await?;
// Connection automatically returned when dropped
```

## Run Migrations

```bash
# Pull current schema from database
qail pull postgres://user:pass@localhost/db > schema.qail

# Create a new version with changes
# (edit schema.qail manually)

# Diff and apply
qail diff old.qail new.qail
qail migrate up old.qail:new.qail postgres://...
```

## Schema Layout Examples (Single vs Modular)

Use the built-in samples in this repository:

- Single file: `examples/schema/single/schema.qail`
- Modular directory: `examples/schema/modular/schema/`

Try them:

```bash
qail check examples/schema/single/schema.qail
qail check examples/schema/modular/schema
qail check examples/schema/modular/schema.qail
```

The modular sample includes `schema/_order.qail` with strict manifest mode:

- `-- qail: strict-manifest`
- every discovered module must be listed (directly or through listed directories)
