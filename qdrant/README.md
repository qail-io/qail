# qail-qdrant

QAIL driver for Qdrant vector database.

> ⚠️ **BETA** - This crate is under active development. API may change.

## Overview

AST-native Rust driver for Qdrant vector search over gRPC.

The driver uses:
- direct protobuf wire encoding (no tonic-generated request structs)
- HTTP/2 transport with reconnect + timeout handling
- QAIL AST integration for search filters

> This crate does not use SQL. "SQL bytes vs SQL strings" terminology only applies to PostgreSQL crates (`qail-core` + `qail-pg`).

## Features

- 🔍 **Vector similarity search** with filters
- 📦 **Upsert points** with payload metadata
- 🗑️ **Delete points** by ID
- 📁 **Collection management** (create, delete)
- 🚀 **Zero-copy protobuf encoding** for gRPC

## Quick Start

```ignore
use qail_qdrant::{Distance, Point, QdrantDriver};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to Qdrant gRPC (default: 6334)
    let mut driver = QdrantDriver::connect("localhost", 6334).await?;

    // Create collection
    driver
        .create_collection("products", 384, Distance::Cosine, false)
        .await?;

    // Upsert points
    let point = Point::new("p1", vec![0.1, 0.2, 0.3]).with_payload("name", "iPhone 15");
    driver.upsert("products", &[point], false).await?;

    // Search
    let embedding = vec![0.1, 0.2, 0.3];
    let results = driver.search("products", &embedding, 10, None).await?;

    Ok(())
}
```

## Requirements

- Qdrant server running with gRPC enabled (default gRPC port: 6334)
- Rust 2024 edition

## License

Apache-2.0
