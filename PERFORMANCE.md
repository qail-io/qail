# Performance History

Historical benchmark results for QAIL PostgreSQL driver.

**Hardware:** MacBook Pro M-series, PostgreSQL 16  
**Benchmark:** `cargo run --release --example fair_benchmark`

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

## Notes

- All benchmarks use parameterized queries with statement caching
- QAIL uses AST hashing to avoid re-encoding identical queries
- Times include network round-trip to local PostgreSQL
