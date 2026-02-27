//! Vector database operations for QAIL CLI
//!
//! Supports Qdrant collection management:
//! - `qail vector create` - Create collection
//! - `qail vector drop` - Delete collection

use crate::colors::*;
use anyhow::Result;

/// Create a vector collection in Qdrant.
pub async fn vector_create(collection: &str, size: u64, distance: &str, url: &str) -> Result<()> {
    use qail_core::ast::Distance;
    use qail_qdrant::QdrantDriver;

    // Parse distance metric
    let dist = match distance.to_lowercase().as_str() {
        "cosine" => Distance::Cosine,
        "euclid" | "euclidean" => Distance::Euclid,
        "dot" | "dotproduct" => Distance::Dot,
        _ => {
            anyhow::bail!(
                "Invalid distance metric: {}. Use cosine, euclid, or dot",
                distance
            );
        }
    };

    println!(
        "{} Creating collection: {}",
        "→".cyan(),
        collection.yellow()
    );
    println!("  Size: {} dimensions", size);
    println!("  Distance: {:?}", dist);
    println!("  URL: {}", url);

    // Parse URL to extract host and port
    let (_scheme, host, port, _path) = crate::util::parse_url_parts(url)?;
    let port = if port == 5432 { 6334 } else { port };

    let mut driver = QdrantDriver::connect(&host, port).await?;
    driver
        .create_collection(collection, size, dist, false)
        .await?;

    println!(
        "{} Collection '{}' created successfully!",
        "✓".green(),
        collection
    );
    Ok(())
}

/// Drop a vector collection in Qdrant.
pub async fn vector_drop(collection: &str, url: &str) -> Result<()> {
    use qail_qdrant::QdrantDriver;

    println!(
        "{} Dropping collection: {}",
        "→".cyan(),
        collection.yellow()
    );
    println!("  URL: {}", url);

    // Parse URL to extract host and port
    let (_scheme, host, port, _path) = crate::util::parse_url_parts(url)?;
    let port = if port == 5432 { 6334 } else { port };

    let mut driver = QdrantDriver::connect(&host, port).await?;
    driver.delete_collection(collection).await?;

    println!(
        "{} Collection '{}' dropped successfully!",
        "✓".green(),
        collection
    );
    Ok(())
}
