//! Integration example: Test qail-qdrant against a live Qdrant server.
//!
//! Prerequisites:
//! 1. Run Qdrant: `docker run -p 6333:6333 qdrant/qdrant`
//! 2. Run this example: `cargo run -p qail-qdrant --example integration_test`

use qail_core::prelude::*;
use qail_qdrant::{Distance, Point, QdrantDriver};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 Connecting to Qdrant...");
    
    let driver = match QdrantDriver::connect("localhost", 6333).await {
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
        .create_collection(collection, 4, Distance::Cosine)
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
    driver.upsert(collection, &points).await?;
    println!("✅ Upserted {} points", points.len());

    // --- Search without filter ---
    println!("\n🔍 Search: Similar to [0.85, 0.15, 0.0, 0.0]...");
    let query_vector = vec![0.85, 0.15, 0.0, 0.0];
    
    let cmd = Qail::search(collection)
        .vector(query_vector.clone())
        .limit(3);
    
    let results = driver.search(&cmd).await?;
    println!("   Found {} results:", results.len());
    for r in &results {
        println!("   - {:?} (score: {:.3})", r.id, r.score);
    }

    // --- Search with filter ---
    println!("\n🔍 Search with filter: category = 'electronics'...");
    let cmd_filtered = Qail::search(collection)
        .vector(query_vector.clone())
        .filter("category", Operator::Eq, "electronics")
        .limit(10);
    
    let filtered = driver.search(&cmd_filtered).await?;
    println!("   Found {} results (electronics only):", filtered.len());
    for r in &filtered {
        println!("   - {:?} (score: {:.3})", r.id, r.score);
    }

    // --- Search with score threshold ---
    println!("\n🔍 Search with score_threshold > 0.9...");
    let cmd_threshold = Qail::search(collection)
        .vector(query_vector)
        .score_threshold(0.9)
        .limit(10);
    
    let threshold_results = driver.search(&cmd_threshold).await?;
    println!("   Found {} results with score > 0.9:", threshold_results.len());
    for r in &threshold_results {
        println!("   - {:?} (score: {:.3})", r.id, r.score);
    }

    // --- List collections ---
    println!("\n📋 Listing collections...");
    let collections = driver.list_collections().await?;
    println!("   Collections: {:?}", collections);

    // --- Cleanup ---
    println!("\n🧹 Cleaning up...");
    driver.delete_collection(collection).await?;
    println!("✅ Done!");

    Ok(())
}
