//! Benchmark: QAIL Zero-Copy vs Official qdrant-client
//!
//! Measures encoding time, total latency, and throughput.
//!
//! Run with: cargo run --example benchmark --release
//!
//! Requires Qdrant running on localhost:6333/6334

use bytes::BytesMut;
use std::time::Instant;
use qail_qdrant::{GrpcDriver, QdrantDriver, Point, Distance};
use qail_qdrant::proto_encoder;

// Official client
use qdrant_client::Qdrant;
use qdrant_client::qdrant::SearchPointsBuilder;

const COLLECTION_NAME: &str = "benchmark_collection";
const VECTOR_DIM: usize = 1536; // OpenAI embedding dimension
const NUM_POINTS: usize = 1000;
const NUM_SEARCHES: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  QAIL Zero-Copy vs Official qdrant-client Benchmark          ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Setup
    println!("📦 Setting up benchmark...");
    let rest_driver = QdrantDriver::connect("localhost", 6333).await?;
    let mut grpc_driver = GrpcDriver::connect("localhost", 6334).await?;
    let official_client = Qdrant::from_url("http://localhost:6334").build()?;

    // Cleanup and create collection
    let _ = rest_driver.delete_collection(COLLECTION_NAME).await;
    rest_driver
        .create_collection(COLLECTION_NAME, VECTOR_DIM as u64, Distance::Cosine)
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
    rest_driver.upsert(COLLECTION_NAME, &points).await?;
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
    // Benchmark 1: Encoding Speed (proto_encoder only)
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Benchmark 1: QAIL Proto Encoding Speed ({} iterations)", NUM_SEARCHES);
    println!("───────────────────────────────────────────────────────────────");

    let mut buffer = BytesMut::with_capacity(VECTOR_DIM * 4 + 256);
    
    let encode_start = Instant::now();
    for vector in &query_vectors {
        proto_encoder::encode_search_proto(
            &mut buffer,
            COLLECTION_NAME,
            vector,
            10,
            None,
            None,
        );
    }
    let encode_duration = encode_start.elapsed();
    
    let encode_per_op = encode_duration / NUM_SEARCHES as u32;
    let encode_ops_per_sec = NUM_SEARCHES as f64 / encode_duration.as_secs_f64();
    
    println!("   Total time:    {:?}", encode_duration);
    println!("   Per operation: {:?}", encode_per_op);
    println!("   Throughput:    {:.0} ops/sec", encode_ops_per_sec);
    println!("   Buffer size:   {} bytes/request\n", buffer.len());

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 2: Official qdrant-client (gRPC with tonic/prost)
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Benchmark 2: Official qdrant-client ({} iterations)", NUM_SEARCHES);
    println!("───────────────────────────────────────────────────────────────");

    // Warmup
    for vector in query_vectors.iter().take(10) {
        let _ = official_client.search_points(
            SearchPointsBuilder::new(COLLECTION_NAME, vector.clone(), 10)
        ).await;
    }

    let official_start = Instant::now();
    let mut official_results = 0;
    for vector in &query_vectors {
        let results = official_client.search_points(
            SearchPointsBuilder::new(COLLECTION_NAME, vector.clone(), 10)
        ).await?;
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
    // Benchmark 3: QAIL gRPC (Zero-Copy)
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Benchmark 3: QAIL gRPC Zero-Copy ({} iterations)", NUM_SEARCHES);
    println!("───────────────────────────────────────────────────────────────");

    // Warmup
    for vector in query_vectors.iter().take(10) {
        let _ = grpc_driver.search(COLLECTION_NAME, vector, 10, None).await;
    }

    let qail_start = Instant::now();
    let mut qail_results = 0;
    for vector in &query_vectors {
        let results = grpc_driver.search(COLLECTION_NAME, vector, 10, None).await?;
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
    println!("📈 Summary: QAIL vs Official Client");
    println!("───────────────────────────────────────────────────────────────");
    
    let qail_vs_official = official_duration.as_secs_f64() / qail_duration.as_secs_f64();
    
    println!("   Official client: {:?}/op ({:.0} ops/sec)", official_per_op, official_ops_per_sec);
    println!("   QAIL zero-copy:  {:?}/op ({:.0} ops/sec)", qail_per_op, qail_ops_per_sec);
    println!("   ────────────────────────────");
    
    if qail_vs_official > 1.0 {
        println!("   🚀 QAIL is {:.2}x faster than official client", qail_vs_official);
    } else if qail_vs_official > 0.95 {
        println!("   ≈  QAIL is comparable to official client ({:.2}x)", qail_vs_official);
    } else {
        println!("   ⚠️  Official client is {:.2}x faster than QAIL", 1.0 / qail_vs_official);
        println!("      (Room for optimization in transport layer)");
    }
    
    println!("\n   Encoding overhead: {:?} ({:.1}% of QAIL latency)",
        encode_per_op,
        (encode_per_op.as_nanos() as f64 / qail_per_op.as_nanos() as f64) * 100.0
    );

    // Cleanup
    println!("\n🧹 Cleaning up...");
    rest_driver.delete_collection(COLLECTION_NAME).await?;
    println!("   ✓ Collection deleted\n");

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                    Benchmark Complete!                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    Ok(())
}
