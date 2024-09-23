//! Batch Benchmark: Sequential vs HTTP/2 Pipelined
//!
//! Prerequisites: Run seed_qdrant.py first!
//! Run: cargo run --example batch_benchmark --release

use std::time::Instant;
use qail_qdrant::QdrantDriver;

const COLLECTION_NAME: &str = "benchmark_collection";
const VECTOR_DIM: usize = 1536;
const NUM_POINTS: usize = 1000;
const BATCH_SIZE: usize = 50;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     Batch Benchmark: Sequential vs HTTP/2 Pipelining        ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("⚠️  Assumes '{}' is seeded (run seed_qdrant.py)\n", COLLECTION_NAME);

    // Generate query vectors
    println!("📊 Generating {} query vectors...", BATCH_SIZE);
    let query_vectors: Vec<Vec<f32>> = (0..BATCH_SIZE)
        .map(|i| {
            let base_idx = (i * 13) % NUM_POINTS;
            let mut vector: Vec<f32> = (0..VECTOR_DIM)
                .map(|j| {
                    let seed = (base_idx * 31 + j * 17) as f32;
                    let base = seed.sin() * 0.5 + (seed / 100.0).cos() * 0.3 + (seed / 1000.0).sin() * 0.2;
                    base + ((i + j) as f32 / 10000.0).sin() * 0.01
                })
                .collect();
            
            let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                vector.iter_mut().for_each(|x| *x /= norm);
            }
            vector
        })
        .collect();
    println!("   ✓ Generated\n");

    // Connect
    let mut driver = QdrantDriver::connect("localhost", 6334).await?;

    // ═══════════════════════════════════════════════════════════════
    // Test 1: Sequential Searches
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Test 1: Sequential Searches ({} queries)", BATCH_SIZE);
    println!("───────────────────────────────────────────────────────────────");

    let sequential_start = Instant::now();
    let mut sequential_results = 0;
    for vector in &query_vectors {
        let results = driver.search(COLLECTION_NAME, vector, 10, None).await?;
        sequential_results += results.len();
    }
    let sequential_duration = sequential_start.elapsed();
    
    println!("   Total time:    {:?}", sequential_duration);
    println!("   Per query:     {:?}", sequential_duration / BATCH_SIZE as u32);
    println!("   Total results: {}\n", sequential_results);

    // ═══════════════════════════════════════════════════════════════
    // Test 2: HTTP/2 Pipelined Batch
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Test 2: HTTP/2 Pipelined Batch ({} queries)", BATCH_SIZE);
    println!("───────────────────────────────────────────────────────────────");

    let batch_start = Instant::now();
    let batch_results = driver.search_batch(COLLECTION_NAME, &query_vectors, 10, None).await?;
    let batch_duration = batch_start.elapsed();
    
    let batch_total_results: usize = batch_results.iter().map(|r| r.len()).sum();
    
    println!("   Total time:    {:?}", batch_duration);
    println!("   Per query:     {:?}", batch_duration / BATCH_SIZE as u32);
    println!("   Total results: {}\n", batch_total_results);

    // ═══════════════════════════════════════════════════════════════
    // Summary
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📈 RESULTS");
    println!("───────────────────────────────────────────────────────────────");
    
    let speedup = sequential_duration.as_secs_f64() / batch_duration.as_secs_f64();
    
    println!("   Sequential:    {:?} total", sequential_duration);
    println!("   HTTP/2 batch:  {:?} total", batch_duration);
    println!("   ────────────────────────────");
    println!("   🚀 Pipelining is {:.2}x faster", speedup);
    println!("   💾 Saved: {:?}\n", sequential_duration - batch_duration);

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    Benchmark Complete!                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    Ok(())
}
