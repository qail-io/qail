# Installation

## Rust (Recommended)

Add QAIL to your `Cargo.toml`:

```toml
[dependencies]
qail-core = "0.27.10"    # AST and builder
qail-pg = "0.27.10"      # PostgreSQL driver
```

## CLI

Install the QAIL command-line tool:

```bash
cargo install qail
```

## TypeScript SDK

```bash
npm install @qail/client
```

## Swift SDK

Use the source package in this repository:

- `sdk/swift/Package.swift`

## Kotlin SDK

Use the Gradle module in this repository:

- `sdk/kotlin/build.gradle.kts`

## Deferred Bindings

- Node.js native binding: deferred
- WASM packaging: deferred

## Verify Installation

```bash
qail --version
# qail 0.27.x
```
