# Connection Pooling

Efficient connection reuse with built-in multi-tenant safety.

## Configuration

```rust
use qail_pg::{PgPool, PoolConfig};

let config = PoolConfig::new("localhost", 5432, "user", "database")
    .password("secret")
    .max_connections(20)
    .min_connections(5);
```

Or load from `qail.toml`:

```rust
let pool = PgPool::from_config().await?;
```

## Acquiring Connections

Always use RLS-aware methods for tenant queries:

```rust
use qail_core::rls::RlsContext;

// Tenant-scoped connection — RLS is set before any query runs
let ctx = RlsContext::operator(operator_id);
let mut conn = pool.acquire_with_rls(ctx).await?;

// With custom statement timeout (milliseconds)
let mut conn = pool.acquire_with_rls_timeout(ctx, 30_000).await?;

// System connection — no tenant context (for schema introspection, migrations)
let mut conn = pool.acquire_system().await?;
```

> **Warning:** Never use `acquire_raw()` for tenant queries. It returns a connection with **no RLS context**, bypassing row-level security. This method is crate-internal only.

## Connection Lifecycle

Every connection follows a strict lifecycle:

```
acquire_with_rls(ctx)
  → set_config('app.current_operator_id', '...', false)
  → set_config('app.is_super_admin', '...', false)
  → execute queries (RLS policies filter rows automatically)
  → release()
      → DISCARD ALL (clears ALL server-side state)
      → clear client-side caches
      → return to pool
```

`DISCARD ALL` destroys prepared statements, temp tables, GUCs, and all session state. This guarantees zero state leakage between tenants sharing the same physical connection.

## Pool Stats

```rust
let stats = pool.stats();
println!("Active: {}, Idle: {}", stats.active, stats.idle);
```

## Best Practices

1. **Create pool once** at application startup
2. **Share via `Arc`** across threads/tasks
3. **Don't hold connections** longer than needed
4. **Always use `acquire_with_rls()`** for tenant queries — never `acquire_raw()`
5. **Set appropriate pool size** — CPU cores × 2 is a good start

```rust
use std::sync::Arc;

let pool = Arc::new(PgPool::connect(config).await?);

// Clone Arc for each task
let pool_clone = pool.clone();
tokio::spawn(async move {
    let ctx = RlsContext::operator(op_id);
    let conn = pool_clone.acquire_with_rls(ctx).await?;
    // ...
});
```
