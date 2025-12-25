# CLI Commands

The `qail` command-line tool.

## Installation

```bash
cargo install qail
```

## Commands

### `qail parse`

Parse QAIL text syntax to SQL:

```bash
qail parse "get users fields * where active = true"
# SELECT * FROM users WHERE active = true
```

### `qail pull`

Extract schema from database:

```bash
qail pull postgres://user:pass@localhost/db > schema.qail
```

### `qail diff`

Compare two schemas and show migration commands:

```bash
qail diff old.qail new.qail
```

### `qail migrate up`

Apply migrations:

```bash
qail migrate up old.qail:new.qail postgres://...
```

### `qail migrate down`

Rollback migrations:

```bash
qail migrate down old.qail:new.qail postgres://...
```

### `qail fmt`

Format QAIL text:

```bash
qail fmt "get users fields *" --indent
```

## Options

| Flag | Description |
|------|-------------|
| `-d, --dialect` | Target SQL dialect (pg, mysql) |
| `-f, --format` | Output format (sql, ast, json) |
| `-v, --verbose` | Verbose output |
| `--version` | Show version |
| `--help` | Show help |
