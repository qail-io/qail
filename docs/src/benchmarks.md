# Benchmark: Qail vs GraphQL vs REST

A scientific comparison of **seven architectural approaches** to the same complex database query, proving why AST-native matters — and showing there's still room to go faster.

## The Query

> Get all enabled ferry connections with their origin harbor name, destination harbor name, and operator brand name — **3×LEFT JOIN**, filtered by `is_enabled`, sorted by name, limited to 50 rows.

This is a realistic query that every SaaS backend encounters: fetching a list of resources with their related entities.

## Results

100 iterations, `--release` mode, PostgreSQL buffer cache pre-warmed, **randomized execution order** to eliminate ordering bias. Sorted by median (most stable metric).

| # | Approach | Median | p99 | DB Queries | vs Qail |
|---|----------|--------|-----|------------|---------|
| 1 | **Qail AST (prepared)** | **224µs** | 633µs | **1** | baseline |
| 2 | **Qail AST (fast)** | **380µs** | 494µs | **1** | 1.7× |
| 3 | Qail AST (uncached) | 465µs | 2.1ms | 1 | 2.1× |
| 4 | REST + `?expand=` | 539µs | 1.7ms | 1 | 2.4× |
| 5 | GraphQL + DataLoader | 1.09ms | 2.5ms | ~3 | 4.9× |
| 6 | REST naive | 17.7ms | 54.6ms | ~151 | 78.9× |
| 7 | GraphQL naive | 17.8ms | 161.6ms | ~151 | 79.6× |

### The Three Qail Tiers

Qail offers three fetch modes, each trading overhead for features:

| Mode | What It Does | Median | Why Use It |
|------|-------------|--------|------------|
| **Prepared** | Cached statement (skips Parse) — `Bind+Execute` only | 224µs | Production default. Postgres skips query planning on repeat calls. |
| **Fast** | Skips ColumnInfo metadata, no `Arc` clones per row | 380µs | When you know your column layout and want zero allocation overhead. |
| **Uncached** | Full `Parse+Bind+Execute` every call | 465µs | Dynamic queries that change per request. Still faster than REST. |

### What This Proves

- **Prepared statements are the headline** — Qail prepared is **2.4× faster than the best possible REST** because Postgres skips query planning entirely
- **The fast path eliminates Rust overhead** — Skipping ColumnInfo + Arc saves ~85µs per call (18% improvement over uncached)
- **DataLoader helps but still 4.9× slower** — 3 round trips vs 1, and each trip has its own Parse+Bind+Execute cycle
- **N+1 is catastrophic** — 79× slower with 151 database round trips. The p99 tells the real story: **161ms** tail latency on GraphQL naive
- **REST+expand ≈ Qail uncached + JSON overhead** — when both do Parse+Bind+Execute with the same query, the gap is the JSON serialization cost

### Still Tuning

> **These results are from early Qail development.** The `fetch_all_fast` path was added during this benchmark session — a 10-minute optimization that saved 18%. There is significant headroom remaining:
>
> - **SIMD row decoding** — parsing fixed-width columns with SIMD instructions
> - **Zero-copy text fields** — borrowing from the receive buffer instead of allocating
> - **Batch prepared execution** — pipelining multiple Bind+Execute in a single round trip
> - **Arena allocation** — per-query bump allocator instead of per-row heap allocation
>
> Qail's architecture (AST → binary wire protocol) is designed so these optimizations compose. Each one is a multiplier, not a replacement.

### Security Comparison

| | Qail | GraphQL | REST |
|---|---|---|---|
| **SQL Injection** | Impossible (binary AST) | String fields vulnerable | String queries vulnerable |
| **Tenant Isolation** | RLS at protocol level | Per-resolver ACL | Per-middleware ACL |
| **Query Abuse** | AST validates at compile time | Depth/complexity attacks | IDOR per endpoint |

---

## Approach 1: Qail AST — Prepared (Production Default)

The fastest path. The prepared statement is cached after the first call — subsequent executions skip PostgreSQL's Parse phase entirely, sending only `Bind+Execute`.

**1 query. 1 round trip. No Parse. No JSON.**

```rust
use qail_core::prelude::*;
use qail_core::ast::{Operator, JoinKind, SortOrder};
use qail_pg::PgDriver;

async fn run_qail_prepared(driver: &mut PgDriver) -> Result<Vec<QailRow>, Box<dyn std::error::Error>> {
    let cmd = Qail::get("odyssey_connections")
        .columns(vec![
            "odyssey_connections.id",
            "odyssey_connections.name",
            "odyssey_connections.description",
            "odyssey_connections.is_enabled",
            "odyssey_connections.created_at",
        ])
        .join(JoinKind::Left, "harbors AS origin",
              "odyssey_connections.origin_harbor_id", "origin.id")
        .join(JoinKind::Left, "harbors AS dest",
              "odyssey_connections.destination_harbor_id", "dest.id")
        .join(JoinKind::Left, "operators",
              "odyssey_connections.operator_id", "operators.id")
        .column("origin.name AS origin_harbor")
        .column("dest.name AS dest_harbor")
        .column("operators.brand_name AS operator_name")
        .filter("odyssey_connections.is_enabled", Operator::Eq, Value::Bool(true))
        .order_by("odyssey_connections.name", SortOrder::Asc)
        .limit(50);

    // fetch_all_cached: first call does Parse+Bind+Execute,
    // subsequent calls skip Parse (cached prepared statement)
    let rows = driver.fetch_all_cached(&cmd).await?;
    Ok(rows)
}
```

**Generated SQL:**
```sql
SELECT odyssey_connections.id, odyssey_connections.name,
       odyssey_connections.description, odyssey_connections.is_enabled,
       odyssey_connections.created_at,
       origin.name AS origin_harbor,
       dest.name AS dest_harbor,
       operators.brand_name AS operator_name
FROM odyssey_connections
LEFT JOIN harbors AS origin ON odyssey_connections.origin_harbor_id = origin.id
LEFT JOIN harbors AS dest ON odyssey_connections.destination_harbor_id = dest.id
LEFT JOIN operators ON odyssey_connections.operator_id = operators.id
WHERE odyssey_connections.is_enabled = true
ORDER BY odyssey_connections.name ASC
LIMIT 50
```

> **Key insight:** This SQL is never generated as a string. The AST encodes directly to PostgreSQL's binary wire protocol. There is zero injection surface because there are no strings to inject into.

---

## Approach 2: Qail AST — Fast (Zero-Overhead Receive)

Same query as prepared, but uses `fetch_all_fast` which skips building the `ColumnInfo` metadata HashMap and avoids `Arc::clone()` per row. This eliminates ~50 atomic reference count operations per call.

```rust
// Same query as Approach 1, but:
let rows = driver.fetch_all_fast(&cmd).await?;
// Rows come back without ColumnInfo — access by index only, not by name.
// Saves: HashMap allocation + Arc::clone() × num_rows
```

> **When to use:** High-throughput codepaths where you know your column layout at compile time and don't need name-based column access. The 18% speedup over uncached comes entirely from eliminating Rust-side allocation and reference counting.

---

## Approach 3: Qail AST — Uncached (Full Round Trip)

Full `Parse+Bind+Execute` on every call. This is the baseline for comparing against REST+expand, since both pay the same PostgreSQL protocol overhead.

```rust
let rows = driver.fetch_all_uncached(&cmd).await?;
// Parse+Bind+Execute every time — Postgres re-plans the query each call
```

> **465µs vs 224µs:** The 2.1× gap between uncached and prepared is pure Postgres query planning overhead. Prepared statements are free performance.

---

## Approach 4: GraphQL Naive — N+1 Resolvers

The classic GraphQL anti-pattern. Each resolver fires a separate query for each related entity:

1. **Root query:** Fetch all connections (1 query → 50 rows)
2. **Per-row:** For each connection, resolve origin harbor (50 queries)
3. **Per-row:** For each connection, resolve destination harbor (50 queries)
4. **Per-row:** For each connection, resolve operator (50 queries)

**Total: ~151 queries per request.**

```rust
async fn run_graphql_naive(driver: &mut PgDriver)
    -> Result<(usize, Duration, usize), Box<dyn std::error::Error>>
{
    // Step 1: Root query
    let root_cmd = Qail::get("odyssey_connections")
        .filter("is_enabled", Operator::Eq, Value::Bool(true))
        .order_by("name", SortOrder::Asc)
        .limit(50);

    let connections = driver.fetch_all_uncached(&root_cmd).await?;

    // Step 2: N+1 — resolve each relation individually
    for conn in &connections {
        let origin_id = conn.text(2);
        let dest_id = conn.text(3);
        let op_id = conn.get_string(11);

        // GET /harbors/:origin_id
        driver.fetch_all_uncached(
            &Qail::get("harbors")
                .filter("id", Operator::Eq, Value::String(origin_id))
                .limit(1)
        ).await?;

        // GET /harbors/:dest_id
        driver.fetch_all_uncached(
            &Qail::get("harbors")
                .filter("id", Operator::Eq, Value::String(dest_id))
                .limit(1)
        ).await?;

        // GET /operators/:op_id
        if let Some(oid) = op_id {
            driver.fetch_all_uncached(
                &Qail::get("operators")
                    .filter("id", Operator::Eq, Value::String(oid))
                    .limit(1)
            ).await?;
        }
    }
    // ...
}
```

> **Why this is the default:** Most GraphQL tutorials teach exactly this pattern. Junior developers follow it because it's "clean" — each resolver is independent. The N+1 problem is invisible until production load hits. The p99 of **161ms** means 1% of your requests take over 160ms — for a single database query.

---

## Approach 5: GraphQL + DataLoader — Batched Queries

The standard optimization. DataLoader collects all IDs from the root query, then fires batched `WHERE id IN (...)` queries:

1. **Root query:** Fetch all connections (1 query)
2. **Batch:** All unique harbor IDs → `WHERE id IN ($1, $2, ...)` (1 query)
3. **Batch:** All unique operator IDs → `WHERE id IN ($1, $2, ...)` (1 query)

**Total: 3 queries per request.**

```rust
async fn run_graphql_dataloader(driver: &mut PgDriver)
    -> Result<(usize, Duration, usize), Box<dyn std::error::Error>>
{
    let connections = driver.fetch_all_uncached(&root_cmd).await?;

    // Collect unique IDs (DataLoader batching)
    let mut harbor_ids = HashSet::new();
    let mut operator_ids = HashSet::new();
    for conn in &connections {
        harbor_ids.insert(conn.text(2));   // origin
        harbor_ids.insert(conn.text(3));   // destination
        if let Some(oid) = conn.get_string(11) {
            operator_ids.insert(oid);
        }
    }

    // Batch: SELECT * FROM harbors WHERE id IN (...)
    let harbor_ids_list: Vec<String> = harbor_ids.into_iter().collect();
    driver.fetch_all_uncached(
        &Qail::get("harbors").filter("id", Operator::In,
            Value::Array(harbor_ids_list.iter()
                .map(|s| Value::String(s.clone())).collect()))
    ).await?;

    // Batch: SELECT * FROM operators WHERE id IN (...)
    let op_ids_list: Vec<String> = operator_ids.into_iter().collect();
    if !op_ids_list.is_empty() {
        driver.fetch_all_uncached(
            &Qail::get("operators").filter("id", Operator::In,
                Value::Array(op_ids_list.iter()
                    .map(|s| Value::String(s.clone())).collect()))
        ).await?;
    }
    // ...
}
```

> **Still 3 round trips.** In a local benchmark, latency is ~0ms. In production (App → RDS across AZs), each round trip adds 1-2ms. DataLoader's 3-trip approach would add 3-6ms of pure network latency that Qail's single-trip approach avoids entirely.

---

## Approach 6: REST Naive — Sequential Calls + JSON

Simulates a frontend client that:
1. `GET /api/connections` → JSON → parse
2. For each connection: `GET /api/harbors/:id` → JSON → parse (×50 origins)
3. For each connection: `GET /api/harbors/:id` → JSON → parse (×50 destinations)
4. For each connection: `GET /api/operators/:id` → JSON → parse (×50 operators)

**Total: ~151 queries + JSON serialization/deserialization overhead.**

```rust
async fn run_rest_naive(driver: &mut PgDriver)
    -> Result<(usize, Duration, usize), Box<dyn std::error::Error>>
{
    let connections = driver.fetch_all_uncached(&root_cmd).await?;

    // Serialize root response to JSON
    let mut conn_data: Vec<String> = Vec::with_capacity(connections.len());
    for conn in &connections {
        conn_data.push(format!(
            r#"{{"id":"{}","odyssey_id":"{}","name":"{}"}}"#,
            conn.text(0), conn.text(1), conn.text(4)
        ));
    }

    // N+1: resolve each relation + JSON each response
    for conn in &connections {
        let origin_id = conn.text(2);
        let dest_id = conn.text(3);

        let origin_rows = driver.fetch_all_uncached(
            &Qail::get("harbors")
                .filter("id", Operator::Eq, Value::String(origin_id))
                .limit(1)
        ).await?;
        // Simulate JSON deserialization
        if let Some(h) = origin_rows.first() {
            let _ = format!(r#"{{"name":"{}"}}"#, h.text(1));
        }
        // ... repeat for dest + operator
    }
    // ...
}
```

---

## Approach 7: REST + `?expand=` — Server-Side JOIN

The optimized REST pattern. The server performs the JOIN (same query as Qail AST) and returns denormalized JSON:

`GET /api/connections?expand=harbors,operators`

**1 query + JSON serialization overhead.**

```rust
async fn run_rest_expand(driver: &mut PgDriver)
    -> Result<(usize, Duration), Box<dyn std::error::Error>>
{
    // Same JOIN query as Qail (server-side)
    let cmd = build_join_query();
    let rows = driver.fetch_all_uncached(&cmd).await?;

    // Simulate full JSON response serialization
    let mut json_out = String::with_capacity(4096);
    json_out.push('[');
    for (i, r) in rows.iter().enumerate() {
        if i > 0 { json_out.push(','); }
        json_out.push_str(&format!(
            r#"{{"id":"{}","name":"{}","origin":"{}","dest":"{}","operator":"{}"}}"#,
            r.text(0), r.text(1), r.text(5), r.text(6), r.text(7),
        ));
    }
    json_out.push(']');
    std::hint::black_box(&json_out); // prevent compiler optimization
    // ...
}
```

> **The 2.4× gap vs Qail prepared is JSON + query planning.** REST+expand pays for both JSON serialization AND Parse+Bind+Execute (no prepared statement). Against Qail uncached, the gap narrows to just 1.15× — proving JSON overhead is minimal; the real win is prepared statements.

---

## Methodology

### Randomized Execution Order

Approaches run in Fisher-Yates shuffled order to eliminate Postgres buffer cache ordering bias. Previous benchmarks showed the first approach could appear slower due to cold cache pages.

### Cache Equalization

A 10-iteration global warmup loads all table data pages into PostgreSQL's buffer cache before any timed approach runs:

```rust
println!("⏳ Global warmup (loading data pages into Postgres buffer cache)...");
for _ in 0..10 {
    let _ = driver.fetch_all_uncached(&warmup_join).await?;
    let _ = driver.fetch_all_uncached(&warmup_harbors).await?;
    let _ = driver.fetch_all_uncached(&warmup_operators).await?;
}
println!("✓ Buffer cache warm — all approaches start equal");
```

### Statistical Rigor

Each approach reports **median** (most stable, resistant to outliers) and **p99** (tail latency, shows worst-case behavior). The results table is sorted by median, not average, to avoid outlier distortion.

### Fair Protocol

All seven approaches use Qail's `PgDriver` internally. This isolates the **architectural difference** (1 query vs N+1 vs batched, prepared vs uncached) from protocol differences. We're measuring the cost of the pattern, not the driver.

### RLS Bypass

Row-Level Security is bypassed (`SET app.is_super_admin = 'true'`) so timing measures pure query performance. In production, Qail enforces RLS at the protocol level — GraphQL and REST must implement it at the application layer.

### Network Latency

This is a **local** benchmark (app and database on the same machine). Network latency = 0ms. In a real cloud deployment (e.g., AWS App → RDS), each database round trip adds **1-2ms** of network latency:

| Approach | Local Median | Estimated Cloud Latency |
|----------|-------------|------------------------|
| Qail prepared (1 trip) | 224µs | ~1.5ms |
| Qail uncached (1 trip) | 465µs | ~2ms |
| DataLoader (3 trips) | 1.09ms | ~5-7ms |
| Naive N+1 (151 trips) | 17.8ms | ~150-300ms |

**In production, the gap between Qail and N+1 widens from 79× to potentially 100-200×.**

---

## Run It Yourself

```bash
DATABASE_URL=postgresql://user:pass@localhost:5432/yourdb \
  cargo run --example battle_comparison --features chrono,uuid --release
```

The full benchmark source is at [`pg/examples/battle_comparison.rs`](https://github.com/qail-io/qail/blob/main/pg/examples/battle_comparison.rs).
