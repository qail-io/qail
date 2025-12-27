# 🪝 QAIL — The AST-Native Query Language

> **Write queries as data. Send them as bytes. No SQL strings.**

[![Crates.io](https://img.shields.io/badge/crates.io-qail-orange)](https://crates.io/crates/qail)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

---

## The Vision

QAIL is not a query transpiler. **QAIL is a data language.**

Instead of passing SQL strings through your stack, you work directly with a typed AST (Abstract Syntax Tree). This AST compiles to database wire protocol bytes — no string interpolation, no SQL injection, no parsing at runtime.

```
┌─────────────────────────────────────────────────────────────────┐
│  Layer 1: Intent                                                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │   let cmd = QailCmd::get("users").filter("id", Eq, 42); │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  Layer 2: Brain (Pure Logic - NO ASYNC)                         │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │   let bytes = PgEncoder::encode(&cmd);  // → BytesMut   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  Layer 3: Muscle (Async I/O - Tokio)                            │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │   stream.write_all(&bytes).await?;                      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  Layer 4: Database                                              │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │   PostgreSQL / MySQL / etc.                             │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

### Why This Matters

| Old Way (SQL Strings) | QAIL Way (AST-Native) |
|-----------------------|-----------------------|
| `"SELECT * FROM users WHERE id = $1"` | `QailCmd::get("users").filter("id", Eq, id)` |
| String concatenation risk | Typed at compile time |
| Parse SQL at runtime | Compile to bytes directly |
| Locked to one driver (sqlx, pg) | Runtime-agnostic |

---

## Architecture

```
qail.rs/
├── qail-core/          # Layer 1: AST + Parser
│   ├── ast/            #   QailCmd, Expr, Value
│   ├── parser/         #   Text → AST (for CLI, LSP)
│   └── transpiler/     #   AST → SQL text (legacy path)
│
├── qail-pg/            # PostgreSQL Driver (Rust)
│   ├── protocol/       #   Layer 2: AST → BytesMut (pure, sync)
│   └── driver/         #   Layer 3: Async I/O (tokio)
│
├── qail-encoder/       # Lightweight FFI (no tokio/TLS)
│                       #   For language bindings: Zig, Go, etc.
│
├── qail-cli/           # Command-line tool
├── qail-lsp/           # Language server
├── qail-wasm/          # Browser playground
└── qail-ffi/           # C-API for other languages
```

---

## Quick Start

### Rust

```rust
use qail_core::ast::QailCmd;
use qail_pg::{PgEncoder, PgDriver};

// Layer 1: Express intent as AST
let cmd = QailCmd::get("users")
    .columns(vec!["id", "email"])
    .filter("active", Operator::Eq, true);

// Layer 2: Compile to wire protocol (pure, sync)
let bytes = PgEncoder::encode_simple_query(&cmd);

// Layer 3: Send over network (async)
let mut driver = PgDriver::connect("localhost", 5432, "user", "db").await?;
let rows = driver.fetch_all(&cmd).await?;
```

### CLI (for migration / debugging)

```bash
# Install
cargo install qail

# Transpile QAIL text to SQL (legacy mode)
qail "get users fields id, email where active = true"
# → SELECT id, email FROM users WHERE active = true
```

---

## The Three Layers

### Layer 2: The Brain (Pure Logic)

This is the key innovation. The encoder:
- Takes a `QailCmd` (AST)
- Returns `BytesMut` (wire protocol bytes)
- Has **zero async**, **zero I/O**, **zero tokio**

```rust
// This is PURE computation - can compile to WASM
let bytes = PgEncoder::encode_simple_query(&cmd);
```

### Layer 3: The Muscle (Async Runtime)

The only place where tokio lives. If a better runtime emerges, only this layer changes:

```rust
// Currently uses tokio - swappable in the future
let mut driver = PgDriver::connect(...).await?;
driver.send(&bytes).await?;
```

---

## Supported Databases

| Database | Status | Crate |
|----------|--------|-------|
| PostgreSQL | 🔄 In Progress | `qail-pg` |
| MySQL | 📋 Planned | `qail-mysql` |
| SQLite | 📋 Planned | `qail-sqlite` |

Each database has its own wire protocol, so each gets its own encoder.

---

## Contributing

```bash
git clone https://github.com/qail-io/qail.git
cd qail
cargo test
```

---

## License

MIT © 2025 QAIL Contributors

---

<p align="center">
  <strong>Built with 🦀 Rust</strong><br>
  <a href="https://qail.rs">qail.rs</a>
</p>
