# Quick Start

## Connect to PostgreSQL

```rust
use qail_pg::driver::PgDriver;

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
use qail_core::ast::{QailCmd, Operator};

// Build a SELECT query
let cmd = QailCmd::get("users")
    .columns(["id", "email"])
    .filter("active", Operator::Eq, true)
    .limit(10);

// Execute
let rows = driver.query(&cmd).await?;

for row in rows {
    let id: i32 = row.get("id")?;
    let email: String = row.get("email")?;
    println!("{}: {}", id, email);
}
```

## Use Connection Pooling

```rust
use qail_pg::driver::{PgPool, PoolConfig};

let config = PoolConfig::new("localhost", 5432, "user", "db")
    .password("secret")
    .max_connections(20);

let pool = PgPool::connect(config).await?;

// Acquire connection from pool
let mut conn = pool.acquire().await?;
conn.simple_query("SELECT 1").await?;
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
