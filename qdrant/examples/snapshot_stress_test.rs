//! Stress test for Qdrant snapshot backup/restore operations

use anyhow::Result;
use reqwest::Client;
use serde::Deserialize;
use std::time::Instant;

const QDRANT_REST: &str = "http://localhost:6333";
const QDRANT_GRPC: &str = "localhost";
const QDRANT_PORT: u16 = 6334;
const NUM_COLLECTIONS: usize = 5;
const POINTS_PER_COLLECTION: usize = 1000;
const VECTOR_DIM: usize = 128;

#[derive(Debug, Deserialize)]
struct SnapshotInfo {
    name: String,
    size: u64,
}

#[derive(Debug, Deserialize)]
struct SnapshotCreateResponse {
    result: SnapshotInfo,
}

#[derive(Debug, Deserialize)]
struct SnapshotListResponse {
    result: Vec<SnapshotInfo>,
}

use qail_core::ast::Distance;
use qail_qdrant::{QdrantDriver, Point, PointId};
use std::collections::HashMap;

fn random_points(count: usize, dim: usize) -> Vec<Point> {
    (0..count)
        .map(|i| Point {
            id: PointId::Num(i as u64),
            vector: (0..dim).map(|j| ((i * 7 + j * 13) % 100) as f32 / 100.0).collect(),
            payload: HashMap::new(),
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔥 Snapshot Stress Test");
    println!("========================");
    println!("Collections: {}", NUM_COLLECTIONS);
    println!("Points per collection: {}", POINTS_PER_COLLECTION);
    println!("REST API: {}", QDRANT_REST);
    println!();

    let client = Client::new();
    let mut driver = QdrantDriver::connect(QDRANT_GRPC, QDRANT_PORT).await?;
    let total_start = Instant::now();

    // Phase 1: Create collections and insert data
    println!("📦 Phase 1: Creating {} collections with data...", NUM_COLLECTIONS);
    let create_start = Instant::now();
    
    for i in 0..NUM_COLLECTIONS {
        let name = format!("snapshot_stress_{}", i);
        
        // Create collection
        driver.create_collection(&name, VECTOR_DIM as u64, Distance::Cosine, false).await?;
        
        // Insert points
        let points = random_points(POINTS_PER_COLLECTION, VECTOR_DIM);
        driver.upsert(&name, &points, true).await?;
        
        println!("  ✓ {} ({} points)", name, POINTS_PER_COLLECTION);
    }
    
    let create_elapsed = create_start.elapsed();
    println!("Created {} collections in {:.2}s\n", NUM_COLLECTIONS, create_elapsed.as_secs_f64());

    // Phase 2: Create snapshots
    println!("📸 Phase 2: Creating snapshots...");
    let snapshot_start = Instant::now();
    let mut snapshots = Vec::new();
    
    for i in 0..NUM_COLLECTIONS {
        let name = format!("snapshot_stress_{}", i);
        let start = Instant::now();
        
        let url = format!("{}/collections/{}/snapshots", QDRANT_REST, name);
        let response: SnapshotCreateResponse = client.post(&url).send().await?.json().await?;
        
        let elapsed = start.elapsed();
        println!("  ✓ {} -> {} ({:.2}ms, {} bytes)", 
            name, response.result.name, 
            elapsed.as_secs_f64() * 1000.0,
            response.result.size);
        
        snapshots.push((name, response.result.name));
    }
    
    let snapshot_elapsed = snapshot_start.elapsed();
    println!("Created {} snapshots in {:.2}s ({:.0}ms avg)\n", 
        NUM_COLLECTIONS, 
        snapshot_elapsed.as_secs_f64(),
        snapshot_elapsed.as_secs_f64() * 1000.0 / NUM_COLLECTIONS as f64);

    // Phase 3: List snapshots
    println!("📋 Phase 3: Listing snapshots...");
    let list_start = Instant::now();
    let mut total_size = 0u64;
    
    for i in 0..NUM_COLLECTIONS {
        let name = format!("snapshot_stress_{}", i);
        let url = format!("{}/collections/{}/snapshots", QDRANT_REST, name);
        let response: SnapshotListResponse = client.get(&url).send().await?.json().await?;
        
        for s in &response.result {
            total_size += s.size;
        }
    }
    
    let list_elapsed = list_start.elapsed();
    println!("Listed snapshots in {:.2}ms (total size: {} MB)\n", 
        list_elapsed.as_secs_f64() * 1000.0,
        total_size / 1024 / 1024);

    // Phase 4: Cleanup - Delete collections
    println!("🗑️  Phase 4: Cleanup...");
    let delete_start = Instant::now();
    
    for i in 0..NUM_COLLECTIONS {
        let name = format!("snapshot_stress_{}", i);
        driver.delete_collection(&name).await?;
    }
    
    let delete_elapsed = delete_start.elapsed();
    println!("Deleted {} collections in {:.2}ms\n", NUM_COLLECTIONS, delete_elapsed.as_secs_f64() * 1000.0);

    // Summary
    let total_elapsed = total_start.elapsed();
    println!("========================");
    println!("📊 SUMMARY");
    println!("  Total time: {:.2}s", total_elapsed.as_secs_f64());
    println!("  Collections: {}", NUM_COLLECTIONS);
    println!("  Points: {}", NUM_COLLECTIONS * POINTS_PER_COLLECTION);
    println!("  Snapshot size: {} MB", total_size / 1024 / 1024);
    println!();
    println!("  Create+Insert: {:.2}s", create_elapsed.as_secs_f64());
    println!("  Snapshot: {:.2}s ({:.0}ms/collection)", 
        snapshot_elapsed.as_secs_f64(),
        snapshot_elapsed.as_secs_f64() * 1000.0 / NUM_COLLECTIONS as f64);
    println!("  List: {:.2}ms", list_elapsed.as_secs_f64() * 1000.0);
    println!("  Delete: {:.2}ms", delete_elapsed.as_secs_f64() * 1000.0);

    Ok(())
}
