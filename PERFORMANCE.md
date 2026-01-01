# Performance History

Historical benchmark results for QAIL PostgreSQL driver.

**Hardware:** MacBook Pro M-series, PostgreSQL 16  
**Benchmark:** `cargo run --release --example fair_benchmark`

---

## v0.14.13 (2026-01-02)

Unified Qail AST for Redis added - **no regression, actually faster!**

| Driver | μs/query | QPS | vs QAIL |
|--------|----------|-----|---------|
| **QAIL** | 39.1 | 25,554 | - |
| SeaORM | 71.0 | 14,093 | 81% slower |
| SQLx | 97.8 | 10,228 | 150% slower |

*+14% faster than v0.14.9 (44.8μs → 39.1μs)*

---

## v0.14.9 (2026-01-01)

NULL byte protection added - no regression.

| Driver | μs/query | QPS | vs QAIL |
|--------|----------|-----|---------|
| **QAIL** | 44.8 | 22,313 | - |
| SeaORM | 69.4 | 14,411 | 55% slower |
| SQLx | 93.0 | 10,758 | 107% slower |

---

## v0.14.8 (2026-01-01)

Pool overhead measured: **9.5μs/checkout**

| Metric | Value |
|--------|-------|
| Pool checkout | 9.5μs |
| Statement cache hit | ~5μs |

---

## v0.14.4 (2025-12-30)

AST hash + LRU cache optimization.

| Driver | μs/query | QPS |
|--------|----------|-----|
| QAIL | 45.2 | 22,124 |
| SQLx | 91.8 | 10,893 |

---

## Bulk Operations

### COPY Protocol (v0.14.9)

100 million rows benchmark:

| Metric | Value |
|--------|-------|
| **Rows/sec** | **1,633,220** |
| Per row | 612ns |
| Total time | 61.2s |
| Batch size | 10,000 rows |

| Operation | Rows/sec | Notes |
|-----------|----------|-------|
| COPY bulk insert | 1.63M | Native COPY |
| Pipelined INSERT | 180K | Extended Query |
| Single INSERT | 22K | Per-statement |

---

## Qdrant Driver (v0.14.10)

**Hardware:** MacBook Pro M-series, Qdrant 1.12.1  
**Vector Size:** 1536 dimensions (OpenAI embeddings)

### Single Query Performance

QAIL vs Official qdrant-client (1000 queries):

| Driver | μs/query | QPS | vs Official |
|--------|----------|-----|-------------|
| **QAIL gRPC** | **140.3** | **7,126** | **1.17x faster** |
| Official client | 164.0 | 6,096 | baseline |

**Optimizations:**
- Zero-copy buffer pooling (`.split()` vs `.clone()`)
- Direct h2 transport (no Tonic overhead)
- Pre-computed protobuf tags
- `unsafe` memcpy for vector floats (1536 floats → 1 memcpy)

### Connection Pool Performance

100 concurrent searches with pool size 10:

| Approach | Total Time | vs Sequential |
|----------|------------|---------------|
| **Pool (10 conns)** | **16.2ms** | **1.46x faster** |
| Single sequential | 23.6ms | baseline |

### Batch Performance (HTTP/2 Pipelining)

50 queries with HTTP/2 multiplexing:

| Approach | Total Time | Per Query | Speedup |
|----------|------------|-----------|---------|
| **HTTP/2 batch** | **4.8ms** | **95μs** | **4.00x faster** |
| Sequential | 19.0ms | 380μs | baseline |

**Key technique:** `search_batch()` sends all requests concurrently over a single h2 connection using `futures::join_all()`.

---

## Notes

- All benchmarks use parameterized queries with statement caching
- QAIL uses AST hashing to avoid re-encoding identical queries
- Times include network round-trip to local PostgreSQL
