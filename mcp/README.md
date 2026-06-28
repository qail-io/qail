# qail-mcp

Model Context Protocol server for the QAIL AST Kernel.

`qail-mcp` lets LLM clients discover QAIL syntax, parse queries into the typed
AST, format QAIL source, transpile QAIL to SQL, and summarize `schema.qail`
files without connecting to a database.

## Install

```bash
cargo install --path mcp
```

## Run

MCP clients should launch the binary over stdio:

```bash
qail-mcp
```

Example client configuration:

```json
{
  "mcpServers": {
    "qail": {
      "command": "qail-mcp"
    }
  }
}
```

During local development, point the command at Cargo:

```json
{
  "mcpServers": {
    "qail": {
      "command": "cargo",
      "args": ["run", "-p", "qail-mcp"]
    }
  }
}
```

## Exposed Capabilities

Tools:

- `qail_parse_query`: parse QAIL v2 syntax into AST, formatted QAIL, and SQL.
- `qail_format_query`: parse and pretty-print a QAIL query.
- `qail_transpile_query`: transpile QAIL to PostgreSQL or SQLite SQL.
- `qail_explain_query`: summarize the parsed AST in LLM-friendly terms.
- `qail_schema_summary`: parse and summarize a `schema.qail` document.
- `qail_builder_cookbook`: return focused AST builder examples.

Resources:

- `qail://guide/ast-kernel`
- `qail://guide/query-syntax`
- `qail://guide/schema`
- `qail://guide/llm-usage`

Prompts:

- `learn_qail_ast`
- `explain_qail_query`
- `write_qail_builder`

