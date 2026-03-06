# Installation

## Rust (Recommended)

Add QAIL to your `Cargo.toml`:

```toml
[dependencies]
qail-core = "0.9"    # AST and Builder
qail-pg = "0.9"      # PostgreSQL driver
```

## CLI

Install the QAIL command-line tool:

```bash
cargo install qail
```

## JavaScript/TypeScript (WASM)

WASM packaging is currently deferred until the platform reaches production-ready status.
Use Rust crates and CLI for now.

## Verify Installation

```bash
qail --version
# qail 0.9.5
```
