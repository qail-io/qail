//! Heavy stress test for Qdrant driver
//!
//! Tests:
//! - 50 collections create/drop
//! - Concurrent point upserts
//! - Batch search operations

use qail_core::ast::Distance;
use qail_qdrant::{QdrantDriver, Point, PointId};
use qail_qdrant::point::PayloadValue;
use std::collections::HashMap;
use std::time::Instant;

const QDRANT_HOST: &str = "localhost";
const QDRANT_PORT: u16 = 6334;
const NUM_COLLECTIONS: usize = 50;
const POINTS_PER_COLLECTION: usize = 100;
const VECTOR_DIM: usize = 1536;

fn random_vector(dim: usize) -> Vec<f32> {
    (0..dim).map(|i| ((i * 7 + 13) % 100) as f32 / 100.0).collect()
}

fn random_points(count: usize, dim: usize) -> Vec<Point> {
    (0..count)
        .map(|i| {
            let mut payload = HashMap::new();
            payload.insert("index".to_string(), PayloadValue::Integer(i as i64));
            payload.insert("category".to_string(), PayloadValue::String(format!("cat_{}", i % 10)));
            
            Point {
                id: PointId::Uuid(uuid::Uuid::new_v4().to_string()),
                vector: random_vector(dim),
                payload,
            }
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔥🔥🔥 HEAVY Qdrant Stress Test 🔥🔥🔥");
    println!("=====================================");
    println!("Host: {}:{}", QDRANT_HOST, QDRANT_PORT);
    println!("Collections: {}", NUM_COLLECTIONS);
    println!("Points per collection: {}", POINTS_PER_COLLECTION);
    println!("Vector dimensions: {}", VECTOR_DIM);
    println!();

    let mut driver = match QdrantDriver::connect(QDRANT_HOST, QDRANT_PORT).await {
        Ok(d) => d,
        Err(e) => {
            eprintln!("❌ Failed to connect: {}", e);
            return Ok(());
        }
    };

    let total_start = Instant::now();

    // Phase 1: Create collections
    println!("📦 Phase 1: Creating {} collections...", NUM_COLLECTIONS);
    let create_start = Instant::now();
    let mut created = 0;

    for i in 0..NUM_COLLECTIONS {
        let name = format!("heavy_stress_{}", i);
        if driver.create_collection(&name, VECTOR_DIM as u64, Distance::Cosine, false).await.is_ok() {
            created += 1;
            if (i + 1) % 10 == 0 {
                println!("  Created {}/{}", i + 1, NUM_COLLECTIONS);
            }
        }
    }

    let create_elapsed = create_start.elapsed();
    println!("✓ Created {}/{} in {:.2}s ({:.1}ms avg)\n", 
        created, NUM_COLLECTIONS, 
        create_elapsed.as_secs_f64(),
        create_elapsed.as_secs_f64() * 1000.0 / created as f64);

    // Phase 2: Upsert points
    println!("📝 Phase 2: Upserting {} points each to {} collections...", 
        POINTS_PER_COLLECTION, NUM_COLLECTIONS);
    let upsert_start = Instant::now();
    let mut upserted = 0;

    for i in 0..NUM_COLLECTIONS {
        let name = format!("heavy_stress_{}", i);
        let points = random_points(POINTS_PER_COLLECTION, VECTOR_DIM);
        
        if driver.upsert(&name, &points, true).await.is_ok() {
            upserted += POINTS_PER_COLLECTION;
            if (i + 1) % 10 == 0 {
                println!("  Upserted to {}/{} collections", i + 1, NUM_COLLECTIONS);
            }
        }
    }

    let upsert_elapsed = upsert_start.elapsed();
    println!("✓ Upserted {} points in {:.2}s ({:.0} points/sec)\n", 
        upserted, 
        upsert_elapsed.as_secs_f64(),
        upserted as f64 / upsert_elapsed.as_secs_f64());

    // Phase 3: Search
    println!("🔍 Phase 3: Searching across {} collections...", NUM_COLLECTIONS);
    let search_start = Instant::now();
    let mut searches = 0;
    let query_vector = random_vector(VECTOR_DIM);

    for i in 0..NUM_COLLECTIONS {
        let name = format!("heavy_stress_{}", i);
        if driver.search(&name, &query_vector, 10, None).await.is_ok() {
            searches += 1;
        }
    }

    let search_elapsed = search_start.elapsed();
    println!("✓ Searched {} collections in {:.2}ms ({:.2}ms avg)\n", 
        searches, 
        search_elapsed.as_secs_f64() * 1000.0,
        search_elapsed.as_secs_f64() * 1000.0 / searches as f64);

    // Phase 4: Cleanup
    println!("🗑️  Phase 4: Deleting {} collections...", NUM_COLLECTIONS);
    let delete_start = Instant::now();
    let mut deleted = 0;

    for i in 0..NUM_COLLECTIONS {
        let name = format!("heavy_stress_{}", i);
        if driver.delete_collection(&name).await.is_ok() {
            deleted += 1;
        }
    }

    let delete_elapsed = delete_start.elapsed();
    println!("✓ Deleted {} in {:.2}ms ({:.1}ms avg)\n", 
        deleted, 
        delete_elapsed.as_secs_f64() * 1000.0,
        delete_elapsed.as_secs_f64() * 1000.0 / deleted as f64);

    // Summary
    let total_elapsed = total_start.elapsed();
    println!("=====================================");
    println!("📊 SUMMARY");
    println!("  Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!("  Collections: {} create, {} delete", created, deleted);
    println!("  Points: {} upserted ({:.0}/sec)", upserted, upserted as f64 / upsert_elapsed.as_secs_f64());
    println!("  Searches: {} ({:.2}ms avg)", searches, search_elapsed.as_secs_f64() * 1000.0 / searches.max(1) as f64);
    println!();
    println!("  Create:  {:.2}s ({:.1}ms/op)", create_elapsed.as_secs_f64(), create_elapsed.as_secs_f64() * 1000.0 / created.max(1) as f64);
    println!("  Upsert:  {:.2}s ({:.1}ms/batch)", upsert_elapsed.as_secs_f64(), upsert_elapsed.as_secs_f64() * 1000.0 / NUM_COLLECTIONS as f64);
    println!("  Search:  {:.2}ms ({:.2}ms/op)", search_elapsed.as_secs_f64() * 1000.0, search_elapsed.as_secs_f64() * 1000.0 / searches.max(1) as f64);
    println!("  Delete:  {:.2}ms ({:.1}ms/op)", delete_elapsed.as_secs_f64() * 1000.0, delete_elapsed.as_secs_f64() * 1000.0 / deleted.max(1) as f64);

    Ok(())
}
