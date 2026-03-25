# Benchmark: Qail vs GraphQL vs REST Patterns

This benchmark measures one endpoint objective executed with different data-access patterns:

- Single query with server-side JOIN
- Batched lookup pattern (DataLoader style)
- Naive N+1 resolver/call pattern

All approaches must return the same canonical payload shape before timing:
`{ id, name, origin_harbor, dest_harbor }`.

## Workload

- Dataset: current `swb_staging_local` PostgreSQL schema
- Primary table: `odyssey_connections`
- Related table: `harbors`
- Query shape for single-query variants: 2x `LEFT JOIN`
- Filter: `odyssey_connections.is_enabled = true`
- Sort: `odyssey_connections.name ASC`
- Limit: `50`

## Methodology

- Build profile: `--release`
- Iterations: `200`
- Per-approach warmup: `15`
- Global warmup: `15`
- Execution order: randomized each run
- RLS: bypassed uniformly (`SET app.is_super_admin = 'true'`) to isolate pattern cost
- Correctness guard: benchmark aborts if payload equivalence check fails
- Optional RTT injection: `BATTLE_SIMULATED_RTT_US` (applied per query dispatch in harness)
- Stats reported: median, p95, avg (avg in raw log), query-count/request

## Results (Snapshot: March 25, 2026)

### Loopback (`BATTLE_SIMULATED_RTT_US=0`)

| Approach | Median | p95 | DB Queries / request |
|---|---:|---:|---:|
| GraphQL + DataLoader | 146.8us | 168.0us | 2 |
| Qail AST (uncached) | 146.9us | 163.7us | 1 |
| REST + `?expand=` | 164.5us | 186.3us | 1 |
| Qail AST (prepared) | 347.6us | 372.0us | 1 |
| REST naive (N+1 + JSON) | 4.71ms | 4.86ms | 101 |
| GraphQL naive (N+1) | 4.74ms | 4.88ms | 101 |

### Simulated RTT (`BATTLE_SIMULATED_RTT_US=250`)

| Approach | Median | p95 | DB Queries / request |
|---|---:|---:|---:|
| Qail AST (uncached) | 475.4us | 491.2us | 1 |
| REST + `?expand=` | 486.4us | 499.9us | 1 |
| Qail AST (prepared) | 660.2us | 688.5us | 1 |
| GraphQL + DataLoader | 779.4us | 802.5us | 2 |
| REST naive (N+1 + JSON) | 35.00ms | 35.39ms | 101 |
| GraphQL naive (N+1) | 35.13ms | 35.52ms | 101 |

### Simulated RTT (`BATTLE_SIMULATED_RTT_US=1000`)

| Approach | Median | p95 | DB Queries / request |
|---|---:|---:|---:|
| Qail AST (uncached) | 1237.8us | 1252.5us | 1 |
| REST + `?expand=` | 1248.4us | 1262.5us | 1 |
| Qail AST (prepared) | 1414.6us | 1448.4us | 1 |
| GraphQL + DataLoader | 2287.0us | 2507.0us | 2 |
| REST naive (N+1 + JSON) | 111.46ms | 112.40ms | 101 |
| GraphQL naive (N+1) | 111.56ms | 112.19ms | 101 |

## Technical Takeaways

- The dominant cost shift is query fan-out, not framework branding.
- On loopback, single-query and DataLoader are effectively tied.
- As RTT rises, single-query patterns pull ahead because they pay one round trip.
- Naive N+1 inflates round trips from `1-2` to `101`, producing order-of-magnitude latency growth.
- Prepared vs uncached remains workload-sensitive in this harness and query shape.

## Reproduce

```bash
DATABASE_URL="postgresql://orion@localhost:5432/swb_staging_local?sslmode=disable" \
BATTLE_ITERATIONS=200 \
BATTLE_WARMUP=15 \
BATTLE_GLOBAL_WARMUP=15 \
BATTLE_SIMULATED_RTT_US=1000 \
cargo run -p qail-pg --example battle_comparison \
  --features chrono,uuid,legacy-raw-examples --release
```
