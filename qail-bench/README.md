# QAIL Cross-Language Benchmarks

Reproducible benchmarks comparing QAIL (Rust) against C libpq and Go pgx.

## Results (50 Million Queries)

| Driver | Language | Queries/sec |
|--------|----------|-------------|
| **QAIL** | Rust 🥇 | **353,638** |
| libpq | C 🥈 | 339,649 |
| pgx | Go 🥉 | 303,844 |

## Configuration

Set environment variables:

```bash
export PG_HOST=127.0.0.1
export PG_PORT=5432
export PG_USER=postgres
export PG_DATABASE=postgres
```

## Run Benchmarks

### QAIL (Rust)

```bash
cargo run --release --bin fifty_million_benchmark
```

### C libpq

```bash
cd benchmarks
gcc -O3 -o fifty_million_libpq fifty_million_libpq.c \
    -I$(pg_config --includedir) -L$(pg_config --libdir) -lpq
./fifty_million_libpq
```

### Go pgx

```bash
cd benchmarks
go mod init bench && go mod tidy
go run fifty_million_pgx.go
```

## Methodology

All benchmarks use:

- ✅ **Same SQL query**: `SELECT id, name FROM harbors LIMIT $1`
- ✅ **Same prepared statements**: Pre-compiled for maximum throughput
- ✅ **Same pre-built parameters**: Parameters built once, reused for all batches
- ✅ **Same batch size**: 10,000 queries per batch
- ✅ **Same pipelining**: PostgreSQL 14+ pipelining enabled
- ✅ **Same machine**: All tests run on the same hardware

## Requirements

- PostgreSQL 14+ (for pipelining support)
- A table named `harbors` with `id` and `name` columns
- Rust 1.75+, GCC, Go 1.21+
