//! Debug test for upsert - checks actual gRPC response

use qail_qdrant::{Point, PointId, QdrantDriver};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Upsert Debug Test");

    let mut driver = QdrantDriver::connect("localhost", 6334).await?;

    // Create test collection
    let collection = "upsert_debug_test";
    println!("Creating collection: {}", collection);

    // Delete if exists
    let _ = driver.delete_collection(collection).await;

    // Create fresh
    driver
        .create_collection(collection, 4, qail_qdrant::Distance::Cosine, false)
        .await?;
    println!("✓ Collection created");

    // Create a simple point
    let point = Point {
        id: PointId::Num(1),
        vector: vec![0.1, 0.2, 0.3, 0.4],
        payload: HashMap::new(),
    };

    println!("Upserting 1 point...");
    match driver.upsert(collection, &[point], true).await {
        Ok(_) => println!("✓ Upsert succeeded!"),
        Err(e) => println!("✗ Upsert failed: {:?}", e),
    }

    // Try search to verify
    println!("Searching...");
    let query = vec![0.1, 0.2, 0.3, 0.4];
    match driver.search(collection, &query, 10, None).await {
        Ok(results) => println!("✓ Search returned {} results", results.len()),
        Err(e) => println!("✗ Search failed: {:?}", e),
    }

    // Cleanup
    driver.delete_collection(collection).await?;
    println!("✓ Cleanup done");

    Ok(())
}
