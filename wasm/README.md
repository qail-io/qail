# qail-wasm

WebAssembly bindings for QAIL - the AST-native query language.

[![npm](https://img.shields.io/npm/v/qail-wasm)](https://www.npmjs.com/package/qail-wasm)
[![License](https://img.shields.io/badge/license-MIT-blue)](../LICENSE)

## Features

- 🚀 **Zero runtime dependencies** - Pure WebAssembly
- 🔒 **Type-safe SQL generation** - From AST, not strings
- 🌐 **Browser & Node.js compatible**
- ⚡ **Fast** - Native-speed query parsing

## Installation

> [!CAUTION]
> **Alpha Software**: QAIL is currently in **alpha**. While we strive for stability, the API is evolving to ensure it remains ergonomic and truly AST-native. **Do not use in production environments yet.**

```bash
npm install qail-wasm
```

## Usage

```javascript
import init, { parse_and_transpile } from 'qail-wasm';

await init();

// Parse QAIL syntax and transpile to SQL
const sql = parse_and_transpile("get users : id, name [ active = true ]", "postgres");
console.log(sql);
// → SELECT id, name FROM users WHERE active = true
```

## Keywords

- **qail** - Query AST Intermediate Language
- **sql** - Structured Query Language transpilation
- **wasm** - WebAssembly bindings
- **ast** - Abstract Syntax Tree based queries
- **query-builder** - Type-safe query construction
- **database** - Multi-database support
- **postgresql** - PostgreSQL SQL generation
- **mysql** - MySQL SQL generation (planned)
- **sqlite** - SQLite SQL generation (planned)
- **transpiler** - Query language transpilation
- **parser** - QAIL syntax parsing
- **no-sql-injection** - Safe by construction

## Related Packages

- [qail-core](https://crates.io/crates/qail-core) - Rust AST and parser
- [qail-pg](https://crates.io/crates/qail-pg) - PostgreSQL driver
- [qail-cli](https://crates.io/crates/qail-cli) - Command-line tool

## License

MIT © 2025 QAIL Contributors

---

<p align="center">
  <a href="https://qail.rs">qail.rs</a>
</p>

## 🤝 Contributing & Support

We welcome issue reports on GitHub! Please provide detailed descriptions to help us reproduce and fix the problem. We aim to address critical issues within 1-5 business days.
