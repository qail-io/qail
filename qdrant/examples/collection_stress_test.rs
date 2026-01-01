//! Stress test for qail vector CLI commands
//!
//! Tests create/drop collection operations against a real Qdrant instance.

use qail_core::ast::Distance;
use qail_qdrant::QdrantDriver;
use std::time::Instant;

const QDRANT_HOST: &str = "localhost";
const QDRANT_PORT: u16 = 6334;
const NUM_COLLECTIONS: usize = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔥 Qdrant Collection Stress Test");
    println!("================================");
    println!("Host: {}:{}", QDRANT_HOST, QDRANT_PORT);
    println!("Collections to create: {}", NUM_COLLECTIONS);
    println!();

    // Connect to Qdrant
    let mut driver = match QdrantDriver::connect(QDRANT_HOST, QDRANT_PORT).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("❌ Failed to connect to Qdrant: {}", e);
            eprintln!("   Make sure Qdrant is running on {}:{}", QDRANT_HOST, QDRANT_PORT);
            return Ok(());
        }
    };

    println!("✓ Connected to Qdrant\n");

    // Test 1: Create collections
    println!("📦 Creating {} collections...", NUM_COLLECTIONS);
    let create_start = Instant::now();
    let mut created = 0;

    for i in 0..NUM_COLLECTIONS {
        let name = format!("stress_test_{}", i);
        let start = Instant::now();
        
        match driver.create_collection(&name, 1536, Distance::Cosine, false).await {
            Ok(_) => {
                created += 1;
                println!("  ✓ {} ({:.2}ms)", name, start.elapsed().as_secs_f64() * 1000.0);
            }
            Err(e) => {
                eprintln!("  ✗ {} - {}", name, e);
            }
        }
    }

    let create_elapsed = create_start.elapsed();
    println!();
    println!("Created {}/{} collections in {:.2}ms", created, NUM_COLLECTIONS, create_elapsed.as_secs_f64() * 1000.0);
    println!("Avg: {:.2}ms per collection", create_elapsed.as_secs_f64() * 1000.0 / created as f64);
    println!();

    // Test 2: Delete collections
    println!("🗑️  Deleting {} collections...", NUM_COLLECTIONS);
    let delete_start = Instant::now();
    let mut deleted = 0;

    for i in 0..NUM_COLLECTIONS {
        let name = format!("stress_test_{}", i);
        let start = Instant::now();
        
        match driver.delete_collection(&name).await {
            Ok(_) => {
                deleted += 1;
                println!("  ✓ {} ({:.2}ms)", name, start.elapsed().as_secs_f64() * 1000.0);
            }
            Err(e) => {
                eprintln!("  ✗ {} - {}", name, e);
            }
        }
    }

    let delete_elapsed = delete_start.elapsed();
    println!();
    println!("Deleted {}/{} collections in {:.2}ms", deleted, NUM_COLLECTIONS, delete_elapsed.as_secs_f64() * 1000.0);
    println!("Avg: {:.2}ms per collection", delete_elapsed.as_secs_f64() * 1000.0 / deleted as f64);
    println!();

    // Summary
    println!("================================");
    println!("📊 Summary");
    println!("  Create: {:.2}ms total, {:.2}ms avg", 
        create_elapsed.as_secs_f64() * 1000.0,
        create_elapsed.as_secs_f64() * 1000.0 / created.max(1) as f64);
    println!("  Delete: {:.2}ms total, {:.2}ms avg",
        delete_elapsed.as_secs_f64() * 1000.0,
        delete_elapsed.as_secs_f64() * 1000.0 / deleted.max(1) as f64);

    Ok(())
}
