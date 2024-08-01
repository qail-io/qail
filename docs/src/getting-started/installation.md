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

```bash
npm install qail-wasm
```

## Verify Installation

```bash
qail --version
# qail 0.9.5
```
