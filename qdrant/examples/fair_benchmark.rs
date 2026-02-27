//! Fair Benchmark: QAIL vs Official qdrant-client
//!
//! Prerequisites: Run seed_qdrant.py first to seed data!
//! Run: cargo run --example fair_benchmark --release

use qail_qdrant::QdrantDriver;
use std::time::Instant;

// Official client
use qdrant_client::Qdrant;
use qdrant_client::qdrant::SearchPointsBuilder;

const COLLECTION_NAME: &str = "benchmark_collection";
const VECTOR_DIM: usize = 1536;
const NUM_SEARCHES: usize = 1000;
const NUM_POINTS: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║         QAIL vs Official qdrant-client Benchmark            ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    println!(
        "⚠️  This benchmark assumes '{}' is already seeded!",
        COLLECTION_NAME
    );
    println!("   Run: python3 seed_qdrant.py\n");

    // Connect
    println!("🔌 Connecting...");
    let mut qail_driver = QdrantDriver::connect("localhost", 6334).await?;
    let official_client = Qdrant::from_url("http://localhost:6334").build()?;
    println!("   ✓ Connected\n");

    // Generate query vectors matching seeded data
    println!("📊 Generating {} query vectors...", NUM_SEARCHES);
    let query_vectors: Vec<Vec<f32>> = (0..NUM_SEARCHES)
        .map(|i| {
            let base_idx = (i * 13) % NUM_POINTS;
            let mut vector: Vec<f32> = (0..VECTOR_DIM)
                .map(|j| {
                    let seed = (base_idx * 31 + j * 17) as f32;
                    let base =
                        seed.sin() * 0.5 + (seed / 100.0).cos() * 0.3 + (seed / 1000.0).sin() * 0.2;
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
    // Benchmark: Official qdrant-client
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Official qdrant-client ({} searches)", NUM_SEARCHES);
    println!("───────────────────────────────────────────────────────────────");

    // Warmup
    for vector in query_vectors.iter().take(10) {
        let _ = official_client
            .search_points(SearchPointsBuilder::new(
                COLLECTION_NAME,
                vector.clone(),
                10,
            ))
            .await;
    }

    let official_start = Instant::now();
    let mut official_results = 0;
    for vector in &query_vectors {
        let results = official_client
            .search_points(SearchPointsBuilder::new(
                COLLECTION_NAME,
                vector.clone(),
                10,
            ))
            .await?;
        official_results += results.result.len();
    }
    let official_duration = official_start.elapsed();

    let official_per_op = official_duration / NUM_SEARCHES as u32;
    let official_ops_per_sec = NUM_SEARCHES as f64 / official_duration.as_secs_f64();

    println!("   Total time:    {:?}", official_duration);
    println!("   Per operation: {:?}", official_per_op);
    println!("   Throughput:    {:.0} ops/sec", official_ops_per_sec);
    println!("   Total results: {}\n", official_results);

    // ═══════════════════════════════════════════════════════════════
    // Benchmark: QAIL gRPC
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 QAIL gRPC Zero-Copy ({} searches)", NUM_SEARCHES);
    println!("───────────────────────────────────────────────────────────────");

    // Warmup
    for vector in query_vectors.iter().take(10) {
        let _ = qail_driver.search(COLLECTION_NAME, vector, 10, None).await;
    }

    let qail_start = Instant::now();
    let mut qail_results = 0;
    for vector in &query_vectors {
        let results = qail_driver
            .search(COLLECTION_NAME, vector, 10, None)
            .await?;
        qail_results += results.len();
    }
    let qail_duration = qail_start.elapsed();

    let qail_per_op = qail_duration / NUM_SEARCHES as u32;
    let qail_ops_per_sec = NUM_SEARCHES as f64 / qail_duration.as_secs_f64();

    println!("   Total time:    {:?}", qail_duration);
    println!("   Per operation: {:?}", qail_per_op);
    println!("   Throughput:    {:.0} ops/sec", qail_ops_per_sec);
    println!("   Total results: {}\n", qail_results);

    // ═══════════════════════════════════════════════════════════════
    // Summary
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📈 FINAL RESULTS");
    println!("───────────────────────────────────────────────────────────────");

    let speedup = official_duration.as_secs_f64() / qail_duration.as_secs_f64();

    println!(
        "   Official client: {:?}/op ({:.0} ops/sec)",
        official_per_op, official_ops_per_sec
    );
    println!(
        "   QAIL zero-copy:  {:?}/op ({:.0} ops/sec)",
        qail_per_op, qail_ops_per_sec
    );
    println!("   ────────────────────────────");

    if speedup > 1.0 {
        println!("   🚀 QAIL is {:.2}x faster", speedup);
    } else {
        println!("   ⚠️  Official is {:.2}x faster", 1.0 / speedup);
    }

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║                    Benchmark Complete!                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    Ok(())
}
