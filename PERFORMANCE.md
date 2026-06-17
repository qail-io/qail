# Performance and Benchmarking

This file is a stable entry point for QAIL performance work. Detailed benchmark
writeups live in the docs and benchmark harnesses live beside the crates they
exercise.

## Current Benchmark Docs

- `docs/src/benchmarks.md` documents the current QAIL vs REST/GraphQL access
  pattern benchmark and its methodology.
- `gateway/docs/slo-runbook.md` documents production SLO targets and Prometheus
  queries for the gateway.
- `gateway/docs/grafana-dashboard.json` is the maintained Grafana dashboard for
  gateway latency, throughput, cache, pool, RPC, batch, and WebSocket metrics.
- `soak/` contains the local soak-test stack, including Grafana provisioning.

## Runnable Harnesses

PostgreSQL driver:

```bash
cargo run --release -p qail-pg --example benchmark
cargo run --release -p qail-pg --example db_benchmark
cargo run --release -p qail-pg --example encoding_benchmark
```

Gateway:

```bash
cargo run --release -p qail-gateway --example gateway_benchmark
```

Qdrant:

```bash
cargo run --release -p qail-qdrant --example batch_benchmark
cargo run --release -p qail-qdrant --example pool_benchmark
```

Official Qdrant client comparisons are optional and require the benchmark
feature:

```bash
cargo run --release -p qail-qdrant --features official-client-bench --example fair_benchmark
```

## Rules For New Numbers

- Include hardware, OS, database/vector-store version, dataset size, warmup,
  iteration count, and whether loopback or simulated RTT was used.
- Prefer median and p95/p99 over a single average.
- Keep correctness guards in the benchmark harness; performance numbers without
  payload equivalence are not useful.
- Do not overwrite current docs with one-off local results. Add new dated
  sections to `docs/src/benchmarks.md` or a crate-specific benchmark note.

## Historical Note

Older `v0.14.x` microbenchmarks were removed from the repository root because
they no longer describe the current `1.3.x` architecture. The changelog keeps
the historical release context; this file stays focused on current benchmark
entry points.
