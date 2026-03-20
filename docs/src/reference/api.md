# API Reference

Full API documentation is generated from source code.

## Rust Crates

| Crate | Description | Docs |
|-------|-------------|------|
| `qail-core` | AST, Builder, Parser | [docs.rs](https://docs.rs/qail-core) |
| `qail-pg` | PostgreSQL driver | [docs.rs](https://docs.rs/qail-pg) |
| `qail-gateway` | Auto-REST gateway | [docs.rs](https://docs.rs/qail-gateway) |
| `qail` | CLI and tooling | [docs.rs](https://docs.rs/qail) |

## SDKs

| SDK | Status | Distribution |
|-----|--------|--------------|
| TypeScript (`@qail/client`) | Supported | npm |
| Swift (`sdk/swift`) | Supported | Source package |
| Kotlin (`sdk/kotlin`) | Supported | Source module |
| Node.js native binding | Deferred | Not published |
| WASM binding | Deferred | Not published |

## Generate Local Docs

```bash
cargo doc --no-deps --open
```

## Key Types

### qail-core

- `QailCmd` - Query command builder
- `Operator` - Comparison operators
- `SortOrder` - ASC/DESC
- `Expr` - Expression AST nodes

### qail-pg

- `PgDriver` - Database connection
- `PgPool` - Connection pool
- `PgRow` - Result row
- `PgError` - Error types

## Source Code

View the source on GitHub:

- [qail-core](https://github.com/qail-io/qail/tree/main/core)
- [qail-pg](https://github.com/qail-io/qail/tree/main/pg)
- [qail-gateway](https://github.com/qail-io/qail/tree/main/gateway)
- [qail-cli](https://github.com/qail-io/qail/tree/main/cli)
- [qail-sdk](https://github.com/qail-io/qail/tree/main/sdk)
