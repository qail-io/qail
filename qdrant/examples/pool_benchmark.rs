//! Pool Benchmark: Single Connection vs Pooled Connections
//!
//! Prerequisites: Run seed_qdrant.py first!
//! Run: cargo run --example pool_benchmark --release

use std::time::Instant;
use qail_qdrant::{QdrantDriver, QdrantPool, PoolConfig};

const COLLECTION_NAME: &str = "benchmark_collection";
const VECTOR_DIM: usize = 1536;
const NUM_POINTS: usize = 1000;
const CONCURRENT_REQUESTS: usize = 100;
const POOL_SIZE: usize = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║       Pool Benchmark: Single vs Pooled Connections          ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!("⚠️  Assumes '{}' is seeded (run seed_qdrant.py)\n", COLLECTION_NAME);

    // Generate query vectors
    println!("📊 Generating {} query vectors...", CONCURRENT_REQUESTS);
    let query_vectors: Vec<Vec<f32>> = (0..CONCURRENT_REQUESTS)
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

    // ═══════════════════════════════════════════════════════════════
    // Test 1: Single Connection (Sequential)
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Test 1: Single Connection ({} requests sequential)", CONCURRENT_REQUESTS);
    println!("───────────────────────────────────────────────────────────────");

    let mut driver = QdrantDriver::connect("localhost", 6334).await?;
    
    let single_start = Instant::now();
    for vector in &query_vectors {
        let _ = driver.search(COLLECTION_NAME, vector, 10, None).await?;
    }
    let single_duration = single_start.elapsed();
    
    println!("   Total time:    {:?}", single_duration);
    println!("   Per request:   {:?}", single_duration / CONCURRENT_REQUESTS as u32);
    println!("   Throughput:    {:.0} req/sec\n", CONCURRENT_REQUESTS as f64 / single_duration.as_secs_f64());

    // ═══════════════════════════════════════════════════════════════
    // Test 2: Connection Pool (Concurrent)
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Test 2: Connection Pool ({} concurrent, {} pool size)", CONCURRENT_REQUESTS, POOL_SIZE);
    println!("───────────────────────────────────────────────────────────────");

    let pool = QdrantPool::new(
        PoolConfig::new("localhost", 6334).max_connections(POOL_SIZE)
    ).await?;
    
    let pool_start = Instant::now();
    let mut tasks = Vec::new();
    for vector in &query_vectors {
        let pool_clone = pool.clone();
        let vec = vector.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = pool_clone.get().await?;
            conn.search(COLLECTION_NAME, &vec, 10, None).await
        }));
    }
    
    for task in tasks {
        let _ = task.await?;
    }
    let pool_duration = pool_start.elapsed();
    
    println!("   Total time:    {:?}", pool_duration);
    println!("   Per request:   {:?}", pool_duration / CONCURRENT_REQUESTS as u32);
    println!("   Throughput:    {:.0} req/sec\n", CONCURRENT_REQUESTS as f64 / pool_duration.as_secs_f64());

    // ═══════════════════════════════════════════════════════════════
    // Summary
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📈 RESULTS");
    println!("───────────────────────────────────────────────────────────────");
    
    let speedup = single_duration.as_secs_f64() / pool_duration.as_secs_f64();
    
    println!("   Single connection:  {:?} total", single_duration);
    println!("   Pooled ({}):        {:?} total", POOL_SIZE, pool_duration);
    println!("   ────────────────────────────");
    println!("   🚀 Pool is {:.2}x faster", speedup);
    println!("   💾 Saved: {:?}\n", single_duration - pool_duration);

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    Benchmark Complete!                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    Ok(())
}
