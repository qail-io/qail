# 🪝 QAIL — Native AST PostgreSQL Driver

> **The world's first AST-native PostgreSQL driver. No SQL strings. No ORM. Just bytes.**

[![Crates.io](https://img.shields.io/badge/crates.io-qail-orange)](https://crates.io/crates/qail)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

---

## The Vision

QAIL is not a query transpiler or ORM. **QAIL is a native AST PostgreSQL driver.**

Instead of passing SQL strings through your stack, you work directly with a typed AST (Abstract Syntax Tree). This AST compiles directly to PostgreSQL wire protocol bytes — no string interpolation, no SQL injection, no parsing at runtime.

```
┌─────────────────────────────────────────────────────────────────┐
│  Layer 1: Intent                                                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │   let cmd = Qail::get("users").filter("id", Eq, 42); │    │
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
| `"SELECT * FROM users WHERE id = $1"` | `Qail::get("users").filter("id", Eq, id)` |
| String concatenation risk | Typed at compile time |
| Parse SQL at runtime | Compile to bytes directly |
| Locked to one driver (sqlx, pg) | Runtime-agnostic |

---

## Architecture

```
qail.rs/
├── core/               # Layer 1: AST + Parser
│   ├── ast/            #   Qail, Expr, Value
│   ├── parser/         #   Text → AST (for CLI, LSP)
│   └── transpiler/     #   AST → SQL text (legacy path)
│
├── pg/                 # PostgreSQL Driver (Rust)
│   ├── protocol/       #   Layer 2: AST → BytesMut (pure, sync)
│   └── driver/         #   Layer 3: Async I/O (tokio)
│
├── encoder/            # Lightweight FFI (no tokio/TLS)
│                       #   For language bindings: Zig, Go, etc.
│
├── cli/                # Command-line tool
├── lsp/                # Language server
├── wasm/               # Browser playground
└── ffi/                # C-API for other languages
```

---

## Quick Start

### Rust

```rust
use qail_core::Qail;
use qail_pg::{PgEncoder, PgDriver};

// Layer 1: Express intent as AST
let cmd = Qail::get("users")
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
- Takes a `Qail` (AST)
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

## Performance

QAIL's AST-native architecture and wire-level pipelining deliver exceptional performance:

| Benchmark | QAIL | tokio-postgres | SQLx | QAIL Advantage |
|-----------|------|----------------|------|----------------|
| **Sequential** | 33K q/s | 25K q/s | 11K q/s | **1.3x - 3x** |
| **Pipeline (10K batch)** | **347K q/s** | 27K q/s | N/A | **12.8x** |

### Why QAIL is Faster

1. **Wire-level Pipelining**: Batch 10,000+ queries in a single TCP write
2. **AST-native Encoding**: No SQL string generation in hot path
3. **Zero-allocation Encoders**: Pre-computed buffer sizes
4. **Prepared Statement Caching**: Hash-based auto-caching

## Supported Databases

| Database | Status | Crate |
|----------|--------|-------|
| PostgreSQL | ✅ Production | `qail-pg` |
| MySQL | � In Progress | `qail-mysql` |
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

## 🤝 Contributing & Support

We welcome issue reports on GitHub! Please provide detailed descriptions to help us reproduce and fix the problem. We aim to address critical issues within 1-5 business days.

> [!CAUTION]
> **Alpha Software**: QAIL is currently in **alpha**. While we strive for stability, the API is evolving to ensure it remains ergonomic and truly AST-native. **Do not use in production environments yet.**
