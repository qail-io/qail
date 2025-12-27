# API Reference

Full API documentation is generated from source code.

## Crates

| Crate | Description | Docs |
|-------|-------------|------|
| `qail-core` | AST, Builder, Parser | [docs.rs](https://docs.rs/qail-core) |
| `qail-pg` | PostgreSQL driver | [docs.rs](https://docs.rs/qail-pg) |
| `qail-wasm` | WASM bindings | [npm](https://npmjs.com/package/qail-wasm) |

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

- [qail-core](https://github.com/qail-io/qail/tree/main/qail-core)
- [qail-pg](https://github.com/qail-io/qail/tree/main/qail-pg)
- [qail-cli](https://github.com/qail-io/qail/tree/main/qail-cli)
