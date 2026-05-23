//! Vector database operations for QAIL CLI
//!
//! Supports Qdrant collection management:
//! - `qail vector create` - Create collection
//! - `qail vector drop` - Delete collection

use crate::colors::*;
use crate::util::redact_url;
use anyhow::Result;

/// Create a vector collection in Qdrant.
pub async fn vector_create(collection: &str, size: u64, distance: &str, url: &str) -> Result<()> {
    use qail_qdrant::{Distance, QdrantDriver};

    // Parse distance metric
    let dist = match distance.to_lowercase().as_str() {
        "cosine" => Distance::Cosine,
        "euclidean" => Distance::Euclidean,
        "dot" => Distance::Dot,
        _ => {
            anyhow::bail!(
                "Invalid distance metric: {}. Use cosine, euclidean, or dot",
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
    println!("  URL: {}", redact_url(url));

    let (host, port) = qdrant_grpc_endpoint(url)?;

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
    println!("  URL: {}", redact_url(url));

    let (host, port) = qdrant_grpc_endpoint(url)?;

    let mut driver = QdrantDriver::connect(&host, port).await?;
    driver.delete_collection(collection).await?;

    println!(
        "{} Collection '{}' dropped successfully!",
        "✓".green(),
        collection
    );
    Ok(())
}

fn qdrant_grpc_endpoint(url: &str) -> Result<(String, u16)> {
    let (_scheme, host, port, _path) = crate::util::parse_url_parts(url)?;
    let port = if port == 6333 { 6334 } else { port };
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::{qdrant_grpc_endpoint, redact_url};

    #[test]
    fn qdrant_grpc_endpoint_maps_rest_port_to_grpc() {
        assert_eq!(
            qdrant_grpc_endpoint("http://localhost:6333").unwrap(),
            ("localhost".to_string(), 6334)
        );
    }

    #[test]
    fn qdrant_grpc_endpoint_preserves_explicit_grpc_port() {
        assert_eq!(
            qdrant_grpc_endpoint("http://localhost:6334").unwrap(),
            ("localhost".to_string(), 6334)
        );
    }

    #[test]
    fn qdrant_grpc_endpoint_preserves_custom_port() {
        assert_eq!(
            qdrant_grpc_endpoint("http://localhost:7000").unwrap(),
            ("localhost".to_string(), 7000)
        );
    }

    #[test]
    fn vector_display_url_redacts_credentials() {
        assert_eq!(
            redact_url("http://qdrant:s3cret@localhost:6333"),
            "http://qdrant:***@localhost:6333/"
        );
    }
}
