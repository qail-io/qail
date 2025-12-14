# Contributing to Qail

Thank you for your interest in contributing to Qail — the AST-native data layer for PostgreSQL.

## Getting Started

### Prerequisites

- **Rust** ≥ 1.75 (stable)
- **PostgreSQL** ≥ 14 (for integration tests)
- **Cargo** (comes with Rust)

### Clone & Build

```bash
git clone https://github.com/qail-io/qail.git
cd qail
cargo build --workspace
```

### Run Tests

```bash
cargo test --workspace --lib     # Unit tests (no DB required)
cargo clippy --workspace         # Lint checks
```

## Project Structure

| Crate | Path | Purpose |
|-------|------|---------|
| `qail-core` | `core/` | Parser, AST, transpiler, schema validation |
| `qail-pg` | `pg/` | PostgreSQL wire protocol driver |
| `qail-gateway` | `gateway/` | HTTP/WS gateway (Axum) |
| `qail-qdrant` | `qdrant/` | Qdrant vector database adapter |
| `qail` (CLI) | `cli/` | Command-line tooling |

## Code Style

- Use `snake_case` for variables, functions, and database columns.
- Prefer `Result<T, AppError>` over `.unwrap()` in production code.
- Use `tracing` macros (`tracing::info!`, `tracing::warn!`, etc.) instead of `println!`/`eprintln!`.
- Run `cargo clippy --workspace` before submitting — zero warnings required.

## Submitting Changes

1. **Fork** the repository and create a feature branch.
2. **Write tests** for new functionality.
3. **Ensure CI passes**: `cargo build --workspace && cargo test --workspace --lib && cargo clippy --workspace`.
4. **Open a Pull Request** with a clear description of your changes.

## Reporting Issues

- Use [GitHub Issues](https://github.com/qail-io/qail/issues) for bug reports and feature requests.
- Include reproduction steps, expected behavior, and actual behavior.

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
