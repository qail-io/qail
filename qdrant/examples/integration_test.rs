//! Integration example: Test qail-qdrant against a live Qdrant server.
//!
//! Prerequisites:
//! 1. Run Qdrant: `docker run -p 6333:6333 qdrant/qdrant`
//! 2. Run this example: `cargo run -p qail-qdrant --example integration_test`

use qail_qdrant::prelude::Distance;
use qail_qdrant::{Point, QdrantDriver};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 Connecting to Qdrant...");
    
    let mut driver = match QdrantDriver::connect("localhost", 6333).await {
        Ok(d) => {
            println!("✅ Connected to Qdrant");
            d
        }
        Err(e) => {
            println!("❌ Failed to connect: {}", e);
            println!("\n💡 Make sure Qdrant is running:");
            println!("   docker run -p 6333:6333 qdrant/qdrant");
            return Ok(());
        }
    };

    let collection = "qail_test";

    // --- Clean up from previous runs ---
    println!("\n🗑️ Cleaning up...");
    let _ = driver.delete_collection(collection).await;

    // --- Create collection ---
    println!("📁 Creating collection '{}'...", collection);
    driver
        .create_collection(collection, 4, Distance::Cosine, false)
        .await?;
    println!("✅ Collection created");

    // --- Upsert points ---
    println!("\n📤 Upserting points...");
    let points = vec![
        Point::new_num(1, vec![0.9, 0.1, 0.0, 0.0])
            .with_payload("category", "electronics")
            .with_payload("price", 999),
        Point::new_num(2, vec![0.8, 0.2, 0.0, 0.0])
            .with_payload("category", "electronics")
            .with_payload("price", 499),
        Point::new_num(3, vec![0.1, 0.9, 0.0, 0.0])
            .with_payload("category", "clothing")
            .with_payload("price", 79),
        Point::new_num(4, vec![0.0, 0.1, 0.9, 0.0])
            .with_payload("category", "food")
            .with_payload("price", 15),
    ];
    driver.upsert(collection, &points, true).await?;
    println!("✅ Upserted {} points", points.len());

    // --- Search without filter ---
    println!("\n🔍 Search: Similar to [0.85, 0.15, 0.0, 0.0]...");
    let query_vector = vec![0.85, 0.15, 0.0, 0.0];
    
    let results = driver.search(collection, &query_vector, 3, None).await?;
    println!("   Found {} results:", results.len());
    for r in &results {
        println!("   - {:?} (score: {:.3})", r.id, r.score);
    }

    // --- Search with score threshold ---
    println!("\n🔍 Search with score_threshold > 0.9...");
    let threshold_results = driver.search(collection, &query_vector, 10, Some(0.9)).await?;
    println!("   Found {} results with score > 0.9:", threshold_results.len());
    for r in &threshold_results {
        println!("   - {:?} (score: {:.3})", r.id, r.score);
    }

    // --- Cleanup ---
    println!("\n🧹 Cleaning up...");
    driver.delete_collection(collection).await?;
    println!("✅ Done!");

    Ok(())
}
