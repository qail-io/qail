# qail-lsp

Language Server Protocol server for QAIL.

[![Crates.io](https://img.shields.io/crates/v/qail-lsp.svg)](https://crates.io/crates/qail-lsp)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Installation

```bash
cargo install qail-lsp
```

## What It Provides

- Syntax diagnostics for QAIL files
- Completion and hover support for QAIL language constructs
- Semantic diagnostics powered by `qail-core`
- Workspace schema discovery for `schema.qail` and modular `schema/` directories
- RLS/schema/N+1 quick fixes for Rust query call sites

## Editor Integration

Configure your editor's LSP client to run:

```bash
qail-lsp
```

The server searches upward from the current file for `schema.qail` first, then
for a modular `schema/` directory. Modular schemas use the same `_order.qail`
and `qail.toml` strict-manifest behavior as the CLI.

## License

Apache-2.0
