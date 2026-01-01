//! Integration test for gRPC driver against live Qdrant.
//!
//! Run with: cargo run --example grpc_integration_test
//!
//! Requires Qdrant running on localhost:6334 (gRPC port).
//! Start with: docker run -p 6334:6334 qdrant/qdrant

use bytes::BytesMut;
use qail_qdrant::prelude::Distance;
use qail_qdrant::{QdrantDriver, QdrantResult};
use qail_qdrant::encoder;

const COLLECTION_NAME: &str = "grpc_test_collection";

#[tokio::main]
async fn main() -> QdrantResult<()> {
    println!("=== QAIL Qdrant gRPC Integration Test ===\n");

    // 1. Connect via gRPC
    println!("1. Connecting to Qdrant gRPC (localhost:6334)...");
    let mut driver = match QdrantDriver::connect("localhost", 6334).await {
        Ok(d) => {
            println!("   ✓ Connected via gRPC/HTTP2");
            d
        }
        Err(e) => {
            println!("   ✗ Failed to connect: {}", e);
            println!("\n   Make sure Qdrant is running:");
            println!("   docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant");
            return Err(e);
        }
    };

    // 2. Create collection (via REST for now - collection ops use different proto)
    println!("\n2. Creating collection '{}' (via REST)...", COLLECTION_NAME);
    let mut rest_driver = qail_qdrant::QdrantDriver::connect("localhost", 6333).await?;
    
    // Clean up first
    let _ = rest_driver.delete_collection(COLLECTION_NAME).await;
    
    rest_driver
        .create_collection(COLLECTION_NAME, 4, Distance::Cosine, false)
        .await?;
    println!("   ✓ Collection created with 4D vectors, Cosine distance");

    // 3. Upsert points
    println!("\n3. Upserting test points...");
    let points = vec![
        qail_qdrant::Point::new_num(1, vec![0.1, 0.2, 0.3, 0.4])
            .with_payload("name", "Product A"),
        qail_qdrant::Point::new_num(2, vec![0.2, 0.3, 0.4, 0.5])
            .with_payload("name", "Product B"),
        qail_qdrant::Point::new_num(3, vec![0.9, 0.8, 0.7, 0.6])
            .with_payload("name", "Product C"),
    ];
    
    // Use REST for upsert (proto encoder for upsert needs more work)
    rest_driver.upsert(COLLECTION_NAME, &points, true).await?;
    println!("   ✓ Inserted 3 points");

    // 4. Search via gRPC with zero-copy encoding
    println!("\n4. Searching via gRPC (zero-copy encoding)...");
    let query_vector = vec![0.15, 0.25, 0.35, 0.45];
    
    // Encode with zero-copy proto encoder
    let mut buf = BytesMut::with_capacity(1024);
    encoder::encode_search_proto(
        &mut buf,
        COLLECTION_NAME,
        &query_vector,
        3,       // limit
        None,    // no score threshold
        None,    // no vector name
    );
    
    println!("   Encoded {} bytes of protobuf", buf.len());
    println!("   First 20 bytes: {:02x?}", &buf[..20.min(buf.len())]);
    
    // Note: The actual gRPC call will fail with current implementation
    // because we need to implement proper response decoding.
    // This test validates the encoding pipeline.
    
    let results = driver.search(COLLECTION_NAME, &query_vector, 3, None).await?;
    println!("   ✓ Got {} results", results.len());
    
    for (i, point) in results.iter().enumerate() {
        println!("     {}. ID: {:?}, Score: {:.4}", i + 1, point.id, point.score);
    }

    // 5. Cleanup
    println!("\n5. Cleaning up...");
    rest_driver.delete_collection(COLLECTION_NAME).await?;
    println!("   ✓ Collection deleted");

    println!("\n=== All tests passed! ===");
    Ok(())
}
