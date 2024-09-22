# qail-qdrant

QAIL driver for Qdrant vector database.

> ⚠️ **ALPHA** - This crate is under active development. API may change.

## Overview

AST-native Rust driver for Qdrant vector search. Uses the same QAIL builder pattern as `qail-pg`, encoding directly to Qdrant's REST API.

## Features

- 🔍 **Vector similarity search** with filters
- 📦 **Upsert points** with payload metadata
- 🗑️ **Delete points** by ID
- 📁 **Collection management** (create, delete, list)
- 🚀 **Zero-copy encoding** to JSON

## Quick Start

```rust
use qail_qdrant::{QdrantDriver, Point, Distance};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect
    let driver = QdrantDriver::connect("localhost", 6333).await?;

    // Create collection
    driver.create_collection("products", 384, Distance::Cosine).await?;

    // Upsert points
    driver.upsert("products", &[
        Point::new("p1", vec![0.1, 0.2, 0.3, /* ... */])
            .with_payload("name", "iPhone 15"),
    ]).await?;

    // Search
    let embedding = vec![0.1, 0.2, 0.3, /* ... */];
    let results = driver.search(&Qail::search("products")
        .vector(embedding)
        .limit(10)
    ).await?;

    Ok(())
}
```

## Requirements

- Qdrant server running (default port: 6333)
- Rust 2024 edition

## License

MIT
