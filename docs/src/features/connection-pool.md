# Connection Pooling

Efficient connection reuse with `PgPool`.

## Configuration

```rust
use qail_pg::driver::{PgPool, PoolConfig};

let config = PoolConfig::new("localhost", 5432, "user", "database")
    .password("secret")
    .max_connections(20)
    .min_connections(5);
```

## Creating a Pool

```rust
let pool = PgPool::connect(config).await?;
```

## Acquiring Connections

```rust
// This waits if all connections are in use
let mut conn = pool.acquire().await?;

// Use the connection
conn.simple_query("SELECT 1").await?;

// Connection automatically returned to pool when dropped
```

## Pool Stats

```rust
// Current idle connections
let idle = pool.idle_count().await;

// Maximum configured connections
let max = pool.max_connections();
```

## Best Practices

1. **Create pool once** at application startup
2. **Share via `Arc`** across threads/tasks
3. **Don't hold connections** longer than needed
4. **Set appropriate pool size** (CPU cores × 2 is a good start)

```rust
use std::sync::Arc;

let pool = Arc::new(PgPool::connect(config).await?);

// Clone Arc for each task
let pool_clone = pool.clone();
tokio::spawn(async move {
    let conn = pool_clone.acquire().await?;
    // ...
});
```
