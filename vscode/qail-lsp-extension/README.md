# QAIL LSP Extension

VS Code extension for the `qail-lsp` language server.

## Features

- LSP diagnostics for `.qail`, `schema.qail`, and Rust files
- Hover previews for embedded QAIL/SQL queries
- Context-aware completion
- Code actions
- Document formatting (raw `.qail` documents)
- Restart command for the language server

## Setup

1. Build or install `qail-lsp` so the binary is available on your machine.
2. Install this extension (`.vsix`).
3. Open a workspace containing `.qail`, `schema.qail`, or Rust files.

Default server discovery order:
1. `qail-lsp` found in `PATH`
2. `<workspace>/target/debug/qail-lsp`
3. `<workspace>/target/release/qail-lsp`

## Configuration

- `qailLsp.serverPath`: path to the `qail-lsp` binary. Relative paths resolve from the first workspace folder.
- `qailLsp.serverArgs`: extra command-line arguments passed to `qail-lsp`

## Command

- `QAIL: Restart Language Server`

## Usage

Create a `.qail` file and start typing, for example:

`get users fields id, email where active = true`

## Contributing

Contribute in the main repository:

- <https://github.com/qail-io/qail>
