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

const COLLECTION_NAME: &str = "benchmark_collection";
const VECTOR_DIM: usize = 1536; // OpenAI embedding dimension
const NUM_POINTS: usize = 1000;
const NUM_SEARCHES: usize = 1000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║     QAIL Zero-Copy gRPC vs REST Benchmark                    ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // Setup
    println!("📦 Setting up benchmark...");
    let rest_driver = QdrantDriver::connect("localhost", 6333).await?;
    let mut grpc_driver = GrpcDriver::connect("localhost", 6334).await?;

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
    println!("📊 Benchmark 1: Proto Encoding Speed ({} iterations)", NUM_SEARCHES);
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
    // Benchmark 2: REST Search (QdrantDriver)
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Benchmark 2: REST Search ({} iterations)", NUM_SEARCHES);
    println!("───────────────────────────────────────────────────────────────");

    // Warmup
    for vector in query_vectors.iter().take(10) {
        let _ = rest_driver.search(
            &qail_core::ast::Qail::search(COLLECTION_NAME)
                .vector(vector.clone())
                .limit(10)
        ).await;
    }

    let rest_start = Instant::now();
    let mut rest_results = 0;
    for vector in &query_vectors {
        let results = rest_driver.search(
            &qail_core::ast::Qail::search(COLLECTION_NAME)
                .vector(vector.clone())
                .limit(10)
        ).await?;
        rest_results += results.len();
    }
    let rest_duration = rest_start.elapsed();
    
    let rest_per_op = rest_duration / NUM_SEARCHES as u32;
    let rest_ops_per_sec = NUM_SEARCHES as f64 / rest_duration.as_secs_f64();
    
    println!("   Total time:    {:?}", rest_duration);
    println!("   Per operation: {:?}", rest_per_op);
    println!("   Throughput:    {:.0} ops/sec", rest_ops_per_sec);
    println!("   Total results: {}\n", rest_results);

    // ═══════════════════════════════════════════════════════════════
    // Benchmark 3: gRPC Search (GrpcDriver with zero-copy)
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📊 Benchmark 3: gRPC Search - Zero Copy ({} iterations)", NUM_SEARCHES);
    println!("───────────────────────────────────────────────────────────────");

    // Warmup
    for vector in query_vectors.iter().take(10) {
        let _ = grpc_driver.search(COLLECTION_NAME, vector, 10, None).await;
    }

    let grpc_start = Instant::now();
    let mut grpc_results = 0;
    for vector in &query_vectors {
        let results = grpc_driver.search(COLLECTION_NAME, vector, 10, None).await?;
        grpc_results += results.len();
    }
    let grpc_duration = grpc_start.elapsed();
    
    let grpc_per_op = grpc_duration / NUM_SEARCHES as u32;
    let grpc_ops_per_sec = NUM_SEARCHES as f64 / grpc_duration.as_secs_f64();
    
    println!("   Total time:    {:?}", grpc_duration);
    println!("   Per operation: {:?}", grpc_per_op);
    println!("   Throughput:    {:.0} ops/sec", grpc_ops_per_sec);
    println!("   Total results: {}\n", grpc_results);

    // ═══════════════════════════════════════════════════════════════
    // Summary
    // ═══════════════════════════════════════════════════════════════
    println!("═══════════════════════════════════════════════════════════════");
    println!("📈 Summary");
    println!("───────────────────────────────────────────────────────────────");
    
    let speedup = rest_duration.as_secs_f64() / grpc_duration.as_secs_f64();
    let latency_reduction_pct = (1.0 - grpc_duration.as_secs_f64() / rest_duration.as_secs_f64()) * 100.0;
    
    println!("   REST latency:     {:?}/op", rest_per_op);
    println!("   gRPC latency:     {:?}/op", grpc_per_op);
    println!("   ────────────────────────────");
    
    if speedup > 1.0 {
        println!("   🚀 gRPC is {:.2}x faster than REST", speedup);
        println!("   📉 Latency reduced by {:.1}%", latency_reduction_pct);
    } else {
        println!("   ⚠️  REST is {:.2}x faster than gRPC", 1.0 / speedup);
    }
    
    println!("\n   Encoding overhead: {:?} ({:.1}% of gRPC latency)",
        encode_per_op,
        (encode_per_op.as_nanos() as f64 / grpc_per_op.as_nanos() as f64) * 100.0
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
