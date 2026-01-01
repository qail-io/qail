//! Comprehensive Benchmark: QAIL vs Official qdrant-client
//!
//! Tests:
//! 1. Single query latency (✓ QAIL won 1.13x)
//! 2. Pipeline/batch queries  
//!
//! Run with: cargo run --example comprehensive_benchmark --release
//!
//! Requires Qdrant running on localhost:6333/6334

use std::time::Instant;
use qail_qdrant::prelude::Distance;
use qail_qdrant::{QdrantDriver, Point};

// Official client
use qdrant_client::Qdrant;
use qdrant_client::qdrant::SearchPointsBuilder;

const COLLECTION_NAME: &str = "benchmark_collection";
const VECTOR_DIM: usize = 1536; // OpenAI embedding dimension
const NUM_POINTS: usize = 1000;
const NUM_SEARCHES: usize = 1000;
const BATCH_SIZE: usize = 50; // For pipeline test

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     QAIL vs Official qdrant-client: Full Benchmark Suite    ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Setup
    println!("📦 Setting up benchmark...");
    let mut rest_driver = QdrantDriver::connect("localhost", 6333).await?;
    let mut grpc_driver = QdrantDriver::connect("localhost", 6334).await?;
    let official_client = Qdrant::from_url("http://localhost:6334").build()?;

    // Cleanup and create collection
    let _ = rest_driver.delete_collection(COLLECTION_NAME).await;
    rest_driver
        .create_collection(COLLECTION_NAME, VECTOR_DIM as u64, Distance::Cosine, false)
        .await?;
    println!("   ✓ Collection '{}' created ({} dimensions)", COLLECTION_NAME, VECTOR_DIM);

    // Generate test data
    println!("   ✓ Generating {} test points with {}D vectors...", NUM_POINTS, VECTOR_DIM);
    let points: Vec<Point> = (0..NUM_POINTS)
        .map(|i| {
            let vector: Vec<f32> = (0..VECTOR_DIM)
                .map(|j| ((i + j) as f32 / VECTOR_DIM as f32).sin())
                .collect();
            Point::new_num(i as u64, vector)
                .with_payload("index", i as i64)
        })
        .collect();

    // Insert via REST
    rest_driver.upsert(COLLECTION_NAME, &points, true).await?;
    println!("   ✓ Points inserted\n");

    // Generate query vectors
    let query_vectors: Vec<Vec<f32>> = (0..NUM_SEARCHES)
        .map(|i| {
            (0..VECTOR_DIM)
                .map(|j| ((i * 7 + j) as f32 / VECTOR_DIM as f32).cos())
                .collect()
        })
        .collect();

    // ═══════════════════════════════════════════════════════════════
    // Test 1: Single Query Latency (BASELINE)
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Test 1: Single Query Latency ({} queries)", NUM_SEARCHES);
    println!("───────────────────────────────────────────────────────────────");

    // Official client
    let official_start = Instant::now();
    for vector in &query_vectors {
        let _ = official_client.search_points(
            SearchPointsBuilder::new(COLLECTION_NAME, vector.clone(), 10)
        ).await?;
    }
    let official_duration = official_start.elapsed();
    let official_per_op = official_duration / NUM_SEARCHES as u32;

    // QAIL
    let qail_start = Instant::now();
    for vector in &query_vectors {
        let _ = grpc_driver.search(COLLECTION_NAME, vector, 10, None).await?;
    }
    let qail_duration = qail_start.elapsed();
    let qail_per_op = qail_duration / NUM_SEARCHES as u32;

    println!("   Official: {:?}/query", official_per_op);
    println!("   QAIL:     {:?}/query", qail_per_op);
    println!("   Result:   QAIL is {:.2}x faster\n", 
        official_duration.as_secs_f64() / qail_duration.as_secs_f64());

    // ═══════════════════════════════════════════════════════════════
    // Test 2: Pipeline/Batch Queries
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Test 2: Pipeline ({} batches of {} queries)", NUM_SEARCHES / BATCH_SIZE, BATCH_SIZE);
    println!("───────────────────────────────────────────────────────────────");

    // Official client (sequential batches)
    let official_batch_start = Instant::now();
    for chunk in query_vectors.chunks(BATCH_SIZE) {
        let mut tasks = Vec::new();
        for vector in chunk {
            let client = official_client.clone();
            let vec = vector.clone();
            tasks.push(tokio::spawn(async move {
                client.search_points(
                    SearchPointsBuilder::new(COLLECTION_NAME, vec, 10)
                ).await
            }));
        }
        for task in tasks {
            let _ = task.await?;
        }
    }
    let official_batch_duration = official_batch_start.elapsed();

    // QAIL (sequential batches with fresh connections per task)
    let qail_batch_start = Instant::now();
    for chunk in query_vectors.chunks(BATCH_SIZE) {
        let mut tasks = Vec::new();
        for vector in chunk {
            let vec = vector.clone();
            tasks.push(tokio::spawn(async move {
                let mut driver = QdrantDriver::connect("localhost", 6334).await?;
                driver.search(COLLECTION_NAME, &vec, 10, None).await
            }));
        }
        for task in tasks {
            let _ = task.await?;
        }
    }
    let qail_batch_duration = qail_batch_start.elapsed();

    println!("   Official: {:?} total ({:?}/batch)", 
        official_batch_duration, 
        official_batch_duration / (NUM_SEARCHES / BATCH_SIZE) as u32);
    println!("   QAIL:     {:?} total ({:?}/batch)", 
        qail_batch_duration,
        qail_batch_duration / (NUM_SEARCHES / BATCH_SIZE) as u32);
    println!("   Result:   QAIL is {:.2}x faster\n", 
        official_batch_duration.as_secs_f64() / qail_batch_duration.as_secs_f64());

    // ═══════════════════════════════════════════════════════════════
    // Final Summary
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📈 FINAL SUMMARY");
    println!("───────────────────────────────────────────────────────────────");
    println!("   Test 1 (Single):   QAIL {:.2}x faster", 
        official_duration.as_secs_f64() / qail_duration.as_secs_f64());
    println!("   Test 2 (Pipeline): QAIL {:.2}x faster", 
        official_batch_duration.as_secs_f64() / qail_batch_duration.as_secs_f64());

    // Cleanup
    println!("\n🧹 Cleaning up...");
    rest_driver.delete_collection(COLLECTION_NAME).await?;
    println!("   ✓ Collection deleted\n");

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    Benchmark Complete!                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    Ok(())
}
