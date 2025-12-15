//! End-to-end integration test for qail-qdrant driver.
//!
//! Run with: cargo test -p qail-qdrant --test e2e_qdrant -- --nocapture
//!
//! Requires a running Qdrant instance on localhost:6334 (gRPC).
//! Start one with: podman run -d --name qdrant-test -m 256m -p 6333:6333 -p 6334:6334 qdrant/qdrant

use qail_qdrant::prelude::*;
use std::collections::HashMap;

const COLLECTION: &str = "e2e_test_collection";
const VECTOR_DIM: usize = 4;

#[tokio::test]
#[ignore = "Requires live Qdrant server on localhost:6334"]
async fn e2e_qdrant_lifecycle() {
    // ── 1. Connect (plain TCP) ──────────────────────────────────────
    println!("▸ Connecting to Qdrant gRPC on localhost:6334...");
    let mut driver = QdrantDriver::connect("localhost", 6334)
        .await
        .expect("Failed to connect to Qdrant — is it running?");
    println!("  ✓ Connected");

    // ── 2. Cleanup: delete collection if leftover from previous run ─
    println!("▸ Cleanup: deleting '{}' if exists...", COLLECTION);
    let _ = driver.delete_collection(COLLECTION).await; // ignore error
    println!("  ✓ Cleanup done");

    // ── 3. Create collection ────────────────────────────────────────
    println!("▸ Creating collection '{}' (dim={})...", COLLECTION, VECTOR_DIM);
    driver
        .create_collection(COLLECTION, VECTOR_DIM as u64, Distance::Cosine, false)
        .await
        .expect("Failed to create collection");
    println!("  ✓ Collection created");

    // Wait for collection to be ready
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // ── 4. Upsert points ────────────────────────────────────────────
    println!("▸ Upserting 3 points...");
    let points = vec![
        Point {
            id: PointId::Num(1),
            vector: vec![1.0, 0.0, 0.0, 0.0],
            payload: {
                let mut m = HashMap::new();
                m.insert("city".to_string(), PayloadValue::String("Jakarta".to_string()));
                m.insert("score".to_string(), PayloadValue::Integer(95));
                m
            },
        },
        Point {
            id: PointId::Num(2),
            vector: vec![0.0, 1.0, 0.0, 0.0],
            payload: {
                let mut m = HashMap::new();
                m.insert("city".to_string(), PayloadValue::String("Surabaya".to_string()));
                m.insert("score".to_string(), PayloadValue::Integer(88));
                m
            },
        },
        Point {
            id: PointId::Num(3),
            vector: vec![0.9, 0.1, 0.0, 0.0],
            payload: {
                let mut m = HashMap::new();
                m.insert("city".to_string(), PayloadValue::String("Bali".to_string()));
                m.insert("score".to_string(), PayloadValue::Integer(72));
                m
            },
        },
    ];

    driver
        .upsert(COLLECTION, &points, false)
        .await
        .expect("Failed to upsert points");
    println!("  ✓ 3 points upserted");

    // Wait for indexing
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // ── 5. Search ───────────────────────────────────────────────────
    println!("▸ Searching with vector [1.0, 0.0, 0.0, 0.0] (top 3)...");
    let results = driver
        .search(COLLECTION, &[1.0, 0.0, 0.0, 0.0], 3, None)
        .await
        .expect("Search failed");

    println!("  ✓ Got {} results:", results.len());
    for pt in &results {
        println!("    id={:?}, score={:.4}, payload={:?}", pt.id, pt.score, pt.payload);
    }

    assert!(!results.is_empty(), "Search returned no results");
    // Point 1 should be the best match (exact vector)
    assert_eq!(results[0].id, PointId::Num(1), "First result should be point 1 (exact match)");
    // Point 3 should be second (0.9 similarity)
    assert_eq!(results[1].id, PointId::Num(3), "Second result should be point 3 (close match)");
    println!("  ✓ Search ranking verified");

    // ── 6. Scroll ───────────────────────────────────────────────────
    println!("▸ Scrolling all points...");
    let scroll_result = driver
        .scroll(COLLECTION, 10, None, false)
        .await
        .expect("Scroll failed");

    println!("  ✓ Scrolled {} points", scroll_result.points.len());
    assert_eq!(scroll_result.points.len(), 3, "Should have 3 points");
    println!("  ✓ Scroll count verified");

    // ── 7. connect_url auto-detect ──────────────────────────────────
    println!("▸ Testing connect_url('http://localhost:6334')...");
    let mut driver2 = QdrantDriver::connect_url("http://localhost:6334")
        .await
        .expect("connect_url failed");

    let results2 = driver2
        .search(COLLECTION, &[0.0, 1.0, 0.0, 0.0], 1, None)
        .await
        .expect("Search via connect_url failed");

    assert!(!results2.is_empty());
    assert_eq!(results2[0].id, PointId::Num(2), "Should find Surabaya point");
    println!("  ✓ connect_url works, found point 2");

    // ── 8. Pool test ────────────────────────────────────────────────
    println!("▸ Testing connection pool...");
    let pool_config = PoolConfig::new("localhost", 6334);
    let pool = QdrantPool::new(pool_config)
        .await
        .expect("Pool creation failed");

    {
        let mut conn = pool.get().await.expect("Pool get failed");
        let results3 = conn
            .search(COLLECTION, &[1.0, 0.0, 0.0, 0.0], 2, None)
            .await
            .expect("Pool search failed");
        assert!(!results3.is_empty());
        println!("  ✓ Pool connection works, got {} results", results3.len());
    }

    // ── 9. Delete collection ────────────────────────────────────────
    println!("▸ Deleting collection '{}'...", COLLECTION);
    driver
        .delete_collection(COLLECTION)
        .await
        .expect("Delete collection failed");
    println!("  ✓ Collection deleted");

    println!("\n═══════════════════════════════════════════");
    println!("  ALL E2E TESTS PASSED ✓");
    println!("═══════════════════════════════════════════");
}
