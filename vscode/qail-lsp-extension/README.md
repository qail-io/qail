# QAIL LSP Extension

This extension connects VS Code to the `qail-lsp` language server.

## Setup

1. Build/install `qail-lsp` so the binary is available on your machine.
2. Install this extension `.vsix`.
3. Open a workspace containing `.qail`, `schema.qail`, or Rust files.

## Configuration

- `qailLsp.serverPath`: path to `qail-lsp` binary (default: `qail-lsp` in PATH)
- `qailLsp.serverArgs`: extra arguments passed to the server

## Command

- `QAIL: Restart Language Server`
