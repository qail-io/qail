# PostgreSQL Driver

The `qail-pg` crate provides a native PostgreSQL driver with SSL/TLS and SCRAM-SHA-256 authentication.

## Connection

```rust
use qail_pg::driver::PgDriver;

// Trust mode (no password)
let driver = PgDriver::connect("localhost", 5432, "user", "db").await?;

// With password (auto-detects MD5 or SCRAM-SHA-256)
let driver = PgDriver::connect_with_password(
    "localhost", 5432, "user", "db", "password"
).await?;
```

## SSL/TLS

SSL/TLS is automatically negotiated during connection. Requires your PostgreSQL server to have SSL enabled.

## Queries

### Simple Query

```rust
let rows = driver.simple_query("SELECT 1 as num").await?;
```

### AST Query

```rust
let cmd = QailCmd::get("users").select_all().limit(10);
let rows = driver.query(&cmd).await?;
```

### Query Pipeline (Batch)

Send multiple queries in one network round-trip:

```rust
let results = driver.query_pipeline(&[
    QailCmd::get("users").select_all(),
    QailCmd::get("orders").select_all(),
]).await?;
```

## Prepared Statements

```rust
let stmt = driver.prepare("SELECT * FROM users WHERE id = $1").await?;
let rows = driver.execute_prepared(&stmt, &[&42]).await?;
```

## Row Decoding

```rust
for row in rows {
    let id: i32 = row.get("id")?;
    let email: String = row.get("email")?;
    let created_at: Timestamp = row.get("created_at")?;
}
```
