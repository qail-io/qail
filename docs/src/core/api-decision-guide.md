# API Decision Guide

Quick reference: "I want to do X → use Y."

---

## Connecting

| I want to... | Use |
|--------------|-----|
| Connect with just host/port/user/db | `PgDriver::connect()` |
| Connect with a password | `PgDriver::connect_with_password()` |
| Use `DATABASE_URL` from env | `PgDriver::connect_env()` |
| Connect over TLS (cloud DB) | `PgConnection::connect_tls()` |
| Use client certificates (mTLS) | `PgConnection::connect_mtls()` |
| Connect via Unix socket | `PgConnection::connect_unix()` |
| Customize everything | `PgDriver::builder()` |
| **Run a production server** | **`PgPool::connect(config)`** |

---

## Querying

| I want to... | Use |
|--------------|-----|
| Get rows back | `driver.fetch_all(&cmd)` |
| Get exactly one row | `driver.fetch_one(&cmd)` |
| Run a mutation (INSERT/UPDATE/DELETE) | `driver.execute(&cmd)` |
| Use SQL text anyway (outside `qail-pg`) | `tokio-postgres` / `sqlx` for that service |

---

## Bulk Operations

| I want to... | Use |
|--------------|-----|
| Insert thousands of rows fast | `driver.copy_bulk(&cmd, &rows)` (COPY protocol) |
| Pipeline many inserts (no results needed) | `driver.pipeline_ast_fast(&commands)` |
| Pipeline inserts (need the inserted rows) | `driver.pipeline_ast(&commands)` |
| Pipeline the same query shape with many param sets | `driver.pipeline_ast_cached(&commands)` |

---

## Connection Pool

| I want to... | Use |
|--------------|-----|
| Get a pooled connection | `pool.acquire()` |
| Get a connection with RLS tenant context | `pool.acquire_with_rls(&ctx)` |
| Check pool status | `pool.idle_count()`, `pool.stats()` |

---

## Transactions

| I want to... | Use |
|--------------|-----|
| Start a transaction | `conn.begin_transaction()` |
| Commit | `conn.commit()` |
| Roll back | `conn.rollback()` |

---

## Multi-Tenant (RLS)

| I want to... | Use |
|--------------|-----|
| Set tenant context on connection | `pool.acquire_with_rls(&ctx)` |
| Manually set RLS | `driver.set_rls_context(&ctx)` |
| Clear RLS context | `driver.clear_rls_context()` |
| Define RLS policies in schema | `policy name on table for select using $$ ... $$` in `.qail` |
| Generate RLS setup SQL | `rls_setup_sql(&table, &policy)` |

---

## Performance Tips

1. **Use the pool** — `PgPool` reuses connections and caches prepared statements.
2. **Use `pipeline_ast_fast()`** for bulk mutations — one round-trip instead of N.
3. **Use `copy_bulk()`** for truly massive inserts (>10K rows) — 10x faster than pipelining.
4. **Use `fetch_all()` (cached)** not `fetch_all_uncached()` — statement caching gives ~2x speedup.
5. **Use `acquire_with_rls()`** in multi-tenant apps — auto-clears on Drop, prevents cross-tenant leaks.
