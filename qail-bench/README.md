# QAIL Cross-Language Benchmarks

Reproducible benchmarks comparing QAIL (Rust) against C libpq and Go pgx.

## Results

### 50 Million Queries (Skip Results - Raw Throughput)

| Driver | Language | Queries/sec |
|--------|----------|-------------|
| **QAIL** | Rust 🥇 | **353,638** |
| libpq | C 🥈 | 339,649 |
| pgx | Go 🥉 | 303,844 |

### 10 Million Queries (With Result Consumption)

Run these benchmarks yourself to compare with full row parsing:

```bash
# QAIL (Rust)
cargo run --release --bin fifty_million_consume

# C libpq (with max optimization)
cd benchmarks && ./fifty_million_consume

# Go pgx
cd benchmarks && go run fifty_million_consume.go
```

## Configuration

Set environment variables:

```bash
export PG_HOST=127.0.0.1
export PG_PORT=5432
export PG_USER=postgres
export PG_DATABASE=postgres
```

## Benchmark Descriptions

### `fifty_million_benchmark` (50M Queries - Skip Results)
- Measures raw driver throughput
- Skips row parsing (like batch inserts)
- Tests encoding/decoding efficiency

### `fifty_million_consume` (10M Queries - With Results)
- Actually reads and parses row data
- More realistic for SELECT workloads
- Tests end-to-end performance

## Build Instructions

### QAIL (Rust)

```bash
cargo run --release --bin fifty_million_benchmark
cargo run --release --bin fifty_million_consume
```

### C libpq (Maximum Optimization)

```bash
cd benchmarks

# Standard optimization
gcc -O3 -o fifty_million_libpq fifty_million_libpq.c \
    -I$(pg_config --includedir) -L$(pg_config --libdir) -lpq

# Maximum optimization (for fair comparison)
gcc -O3 -march=native -flto -ffast-math \
    -o fifty_million_consume fifty_million_consume.c \
    -I$(pg_config --includedir) -L$(pg_config --libdir) -lpq
```

### Go pgx

```bash
cd benchmarks
go mod init bench 2>/dev/null || true
go mod tidy
go run fifty_million_pgx.go
go run fifty_million_consume.go
```

## Methodology

All benchmarks ensure fair comparison:

- ✅ **Same SQL query**: `SELECT id, name FROM harbors LIMIT $1`
- ✅ **Same prepared statements**: Pre-compiled for maximum throughput
- ✅ **Same pre-built parameters**: Parameters built once, reused for all batches
- ✅ **Same batch size**: Consistent batch sizes across languages
- ✅ **Same pipelining**: PostgreSQL 14+ pipelining enabled
- ✅ **Same machine**: All tests run on the same hardware

### C Optimization Flags

For the "consume" benchmark, C uses maximum optimization:
- `-O3`: Full optimization
- `-march=native`: Use native CPU instructions
- `-flto`: Link-time optimization
- `-ffast-math`: Fast floating point (if applicable)

## Requirements

- PostgreSQL 14+ (for pipelining support)
- A table named `harbors` with `id` and `name` columns
- Rust 1.75+, GCC with LTO support, Go 1.21+

## Creating Test Table

```sql
CREATE TABLE IF NOT EXISTS harbors (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

INSERT INTO harbors (name) 
SELECT 'Harbor ' || i 
FROM generate_series(1, 100) AS i;
```
