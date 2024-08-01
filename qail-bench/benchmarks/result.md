# QAIL Benchmark Results

Comprehensive benchmark comparison of qail-py (PyO3 + Tokio) against asyncpg and native Rust qail-pg.

## Test Environment

- **Database**: PostgreSQL (local)
- **Connection**: Single connection (no pooling)
- **Python**: 3.13.7
- **Rust**: stable
- **Hardware**: macOS (arm64)

## Benchmark 1: Pipelined Query Execution

### Test Configuration
- **Total queries**: 50,000,000
- **Batch size**: 10,000 queries per batch
- **Query type**: `SELECT id, name FROM harbors LIMIT 10`
- **Protocol**: PostgreSQL pipelined batch execution

### Results

| Driver | Queries/Second | Time (50M queries) | Notes |
|--------|----------------|-------------------|-------|
| **qail-py (PyO3 + Tokio)** | **122,352** 🏆 | 408.7s | GIL released, Rust owns TCP |
| qail-py (ctypes FFI) | ~70,000 | ~714s | Pre-encoded batches |
| Native Rust qail-pg | **353,638** 🥇 | 141.4s | Baseline (no Python) |
| asyncpg | ~16,000 | ~3,125s | Sequential queries |

**Winner**: qail-py is **7.6x faster** than asyncpg for pipelined queries.

### Performance Analysis
- qail-py achieves **35% of native Rust performance** (impressive for Python!)
- **75% improvement** over ctypes approach (70k → 122k q/s)
- asyncpg's sequential query execution is slower than pipelined approaches

---

## Benchmark 2: COPY Bulk Insert

### Test Configuration
- **Total rows**: 2,600,000
- **Batch size**: 10,000 rows per COPY operation
- **COPY operations**: 260
- **Table**: `_test(a int, b int, c int, d text, e float, f int, g text)`
- **Protocol**: PostgreSQL COPY FROM STDIN

### Results

| Driver | Rows/Second | Time (2.6M rows) | Implementation |
|--------|-------------|------------------|----------------|
| **asyncpg** | **1,015,759** 🥇 | 2.6s | C extensions (Cython) |
| Native Rust qail-pg | 405,766 | 6.4s | Pure Rust Tokio |
| qail-py (PyO3 + Tokio) | 269,165 | 9.7s | Rust + PyO3 |

**Winner**: asyncpg is **2.5x faster** than native Rust for COPY operations.

### Performance Analysis
- asyncpg uses **specialized C extensions** (Cython-compiled) for COPY protocol
- qail-py achieves **66% of native Rust COPY performance**
- asyncpg's exceptional COPY speed comes from highly optimized C code

---

## Summary Matrix

| Driver | Pipeline Queries | COPY Bulk Insert | Specialty |
|--------|------------------|------------------|-----------|
| **asyncpg** | 16k q/s | **1.02M rows/s** 🏆 | COPY specialist |
| **Native Rust** | **354k q/s** 🥇 | 406k rows/s | Balanced champion |
| **qail-py** | **122k q/s** 🏆 | 269k rows/s | Query pipeline star |

## Key Insights

1. **qail-py excels at high-volume query pipelines**
   - 7.6x faster than asyncpg for pipelined queries
   - 35% of native Rust speed (excellent for Python!)
   - PyO3 + Tokio with GIL release is highly effective

2. **asyncpg dominates COPY operations**
   - Specialized C extensions outperform even native Rust by 2.5x
   - Highly optimized for PostgreSQL COPY protocol specifically
   - Less optimized for general query pipelines

3. **Native Rust qail-pg is the balanced champion**
   - Best overall query performance (354k q/s)
   - Solid COPY performance (406k rows/s)
   - No Python overhead

## Use Case Recommendations

### Use **qail-py** when:
- High-volume query pipelines (APIs, data processing)
- Need AST-native query building in Python
- Want excellent performance without C extensions

### Use **asyncpg** when:
- Bulk data loading (ETL, migrations)
- COPY operations are primary workload
- Need maximum COPY throughput

### Use **Native Rust qail-pg** when:
- Building Rust services
- Need maximum query throughput
- Want balanced performance across all operations

## Architecture Details

### qail-py (PyO3 + Tokio)
```
Python call → Rust (owns TCP via Tokio) → PostgreSQL
              ↓
         GIL released (py.detach)
         All I/O in Rust
```

- **Key optimization**: Blocking API with GIL release
- **Tokio runtime**: Global, multi-threaded, shared across connections
- **No SQL generation**: AST → wire protocol directly

### asyncpg (C Extensions)
```
Python call → Cython C code → PostgreSQL
              ↓
         Native C performance
         Specialized COPY protocol
```

- **Key optimization**: Compiled C extensions for critical paths
- **COPY protocol**: Hand-optimized C implementation
- **Trade-off**: Slower for general queries

## Benchmark Scripts

All benchmarks are reproducible:

### Pipeline Queries
- `qail_pyo3_bench.py` - qail-py (PyO3 + Tokio)
- `qail_optimized_bench.py` - qail-py (ctypes)
- `asyncpg_bench.py` - asyncpg sequential
- Native Rust: Use `fifty_million_benchmark` binary

### COPY Bulk Insert
- `qail_copy_bench.py` - qail-py
- `asyncpg_copy_bench.py` - asyncpg
- `copy_bench` - Native Rust qail-pg

## Conclusion

**qail-py achieved its performance goal**: 122k queries/second is **outstanding** for a Python database driver, making it ideal for high-volume query workloads. While asyncpg dominates bulk COPY operations with its specialized C extensions, qail-py's **7.6x advantage** in pipelined queries makes it the superior choice for real-world API and data processing scenarios.

The PyO3 + Tokio architecture with GIL release proved highly effective, achieving **35% of native Rust performance** - a remarkable result that demonstrates the power of Rust-Python integration.

---

## Benchmark 3: PHP Extension (Pipeline Mode)

### Test Configuration
- **Total queries**: 10,000
- **Batch size**: 100 queries per pipeline
- **Query type**: `SELECT id, name FROM harbors LIMIT n`
- **Implementation**: Native PHP C extension wrapping Rust static library

### Results

| Driver | Queries/Second | Notes |
|--------|----------------|-------|
| **qail-php (Pipeline)** | **232,024** 🏆 | Rust encoding + raw socket |
| Raw PDO | 29,362 | Prepared statements |
| Eloquent-like | 11,537 | PDO + model hydration |

**Winner**: qail-php is **7.9x faster than raw PDO** and **20x faster than Eloquent**.

### Performance Analysis
- Pipeline mode sends 100 queries in 1 network round-trip
- Direct PostgreSQL wire protocol eliminates PDO/libpq overhead
- Rust encoding at 3.9M ops/sec, I/O is the bottleneck
- PHP extension overhead: ~0.36µs vs FFI at ~0.65µs

---

## Benchmark 4: Go Driver (CGO)

### Test Configuration
- **Total queries**: 100,000
- **Batch size**: 1,000 queries per batch
- **Query type**: `SELECT id, name FROM harbors LIMIT n`
- **Implementation**: CGO calling Rust static library

### Results

| Driver | Queries/Second | Notes |
|--------|----------------|-------|
| **qail-go** | **126,000** | Rust encoding, Go I/O |
| pgx | 239,000 | Pure Go, highly optimized |
| GORM | 27,000 | ORM overhead |

**Winner**: qail-go is **4.2x faster than GORM** (still 53% of pgx).

### Performance Analysis
- CGO overhead limits performance vs pure Go pgx
- Prepared batch encoding: one CGO call for 1000 queries
- Buffered I/O (16KB buffers) reduces syscalls
- Value proposition: **type-safe AST queries** + **4x ORM speed**

---

## Cross-Language Performance Summary

| Driver | Query Speed | vs ORM | vs Best Native |
|--------|-------------|--------|----------------|
| **qail-php (pipeline)** | 232K q/s | 20x Eloquent | 7.9x PDO |
| **qail-py (PyO3)** | 122K q/s | 7.6x asyncpg | 35% Rust |
| **qail-go (CGO)** | 126K q/s | 4.2x GORM | 53% pgx |
| Native Rust | 354K q/s | N/A | baseline |

