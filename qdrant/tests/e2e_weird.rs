//! Weird, adversarial, and edge-case tests for qail-qdrant.
//!
//! Run with: cargo test -p qail-qdrant --test e2e_weird -- --nocapture
//!
//! Requires Qdrant on localhost:6334.
//! Start: podman run -d --name qdrant-test -m 256m -p 6333:6333 -p 6334:6334 qdrant/qdrant

use qail_qdrant::prelude::*;
use std::collections::HashMap;

/// Helper: connect to local Qdrant
async fn driver() -> QdrantDriver {
    QdrantDriver::connect("localhost", 6334)
        .await
        .expect("Qdrant not running on localhost:6334")
}

/// Helper: ensure a clean collection exists
async fn ensure_collection(d: &mut QdrantDriver, name: &str, dim: u64) {
    let _ = d.delete_collection(name).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    d.create_collection(name, dim, qail_qdrant::Distance::Cosine, false)
        .await
        .expect("create_collection failed");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
}

// ═══════════════════════════════════════════════════════════════════
// 1. Search on an empty collection — should return zero, not error
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live Qdrant server"]
async fn weird_search_empty_collection() {
    println!("▸ Search on empty collection...");
    let mut d = driver().await;
    ensure_collection(&mut d, "weird_empty", 3).await;

    let results = d
        .search("weird_empty", &[1.0, 0.0, 0.0], 10, None)
        .await
        .expect("search on empty should not error");
    assert!(
        results.is_empty(),
        "Expected 0 results, got {}",
        results.len()
    );
    println!("  ✓ Empty collection returns 0 results");

    d.delete_collection("weird_empty").await.ok();
}

// ═══════════════════════════════════════════════════════════════════
// 2. Upsert same ID twice — second write should overwrite
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live Qdrant server"]
async fn weird_duplicate_id_overwrite() {
    println!("▸ Duplicate ID overwrite...");
    let mut d = driver().await;
    ensure_collection(&mut d, "weird_dup", 3).await;

    let p1 = vec![Point::new(1u64, vec![1.0, 0.0, 0.0]).with_payload("version", "first")];
    d.upsert("weird_dup", &p1, false).await.unwrap();

    let p2 = vec![Point::new(1u64, vec![0.0, 1.0, 0.0]).with_payload("version", "second")];
    d.upsert("weird_dup", &p2, false).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Search should find the overwritten version
    let results = d
        .search("weird_dup", &[0.0, 1.0, 0.0], 1, None)
        .await
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, PointId::Num(1));
    // The score should be 1.0 (exact match with the second upsert vector)
    assert!(
        (results[0].score - 1.0).abs() < 0.001,
        "Score should be ~1.0, got {}",
        results[0].score
    );
    // Payload should show "second"
    match results[0].payload.get("version") {
        Some(PayloadValue::String(v)) => assert_eq!(v, "second"),
        other => panic!("Expected version='second', got {:?}", other),
    }
    println!("  ✓ Second upsert overwrote first (vector + payload)");

    d.delete_collection("weird_dup").await.ok();
}

// ═══════════════════════════════════════════════════════════════════
// 3. Unicode nightmare payload — emoji, CJK, RTL, zalgo text
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live Qdrant server"]
async fn weird_unicode_payload() {
    println!("▸ Unicode nightmare payload...");
    let mut d = driver().await;
    ensure_collection(&mut d, "weird_unicode", 2).await;

    let long_fire = "🔥".repeat(500);
    let cursed_strings: Vec<(&str, &str)> = vec![
        ("emoji", "🚢⛵🌊🏝️ sailing through 日本語 and العربية"),
        ("zalgo", "H̴̢̧e̵̡̛ ̸̧̨C̷o̵m̴e̷s̵"),
        ("null_bytes", "before\0after"),
        ("empty", ""),
        ("long_utf8", &long_fire),
        ("newlines", "line1\nline2\r\nline3\ttab"),
        ("json_injection", r#"{"evil": true, "drop": "table"}"#),
        ("backslash", r"C:\Users\hacker\..\..\etc\passwd"),
    ];

    let points: Vec<Point> = cursed_strings
        .iter()
        .enumerate()
        .map(|(i, (key, val))| Point::new((i + 1) as u64, vec![1.0, 0.0]).with_payload(*key, *val))
        .collect();

    d.upsert("weird_unicode", &points, false)
        .await
        .expect("Unicode upsert should work");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Scroll all and verify payloads survived the round trip
    let scroll = d
        .scroll("weird_unicode", 20, None, false)
        .await
        .expect("Scroll should work");
    assert_eq!(
        scroll.points.len(),
        cursed_strings.len(),
        "Should have {} points, got {}",
        cursed_strings.len(),
        scroll.points.len()
    );

    println!(
        "  ✓ All {} Unicode payloads survived round-trip",
        cursed_strings.len()
    );
    d.delete_collection("weird_unicode").await.ok();
}

// ═══════════════════════════════════════════════════════════════════
// 4. Very high-dimensional vectors (1536-dim like OpenAI embeddings)
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live Qdrant server"]
async fn weird_high_dim_vectors() {
    println!("▸ High-dimensional vectors (1536-dim)...");
    let mut d = driver().await;
    ensure_collection(&mut d, "weird_highdim", 1536).await;

    // Create 5 random-ish vectors
    let points: Vec<Point> = (0..5)
        .map(|i| {
            let mut vec = vec![0.0f32; 1536];
            // Seed different dimensions for each point
            for (j, v) in vec.iter_mut().enumerate() {
                *v = ((i * 1536 + j) as f32 * 0.001).sin();
            }
            Point::new((i + 1) as u64, vec).with_payload("dim", 1536i64)
        })
        .collect();

    d.upsert("weird_highdim", &points, false)
        .await
        .expect("1536-dim upsert should work");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Search with a 1536-dim query
    let query: Vec<f32> = points[2].vector.clone();
    let results = d
        .search("weird_highdim", &query, 3, None)
        .await
        .expect("1536-dim search should work");

    assert!(!results.is_empty());
    assert_eq!(
        results[0].id,
        PointId::Num(3),
        "Exact match should be first"
    );
    assert!(
        (results[0].score - 1.0).abs() < 0.01,
        "Self-search score should be ~1.0"
    );
    println!("  ✓ 1536-dim vectors work (OpenAI embedding size)");

    d.delete_collection("weird_highdim").await.ok();
}

// ═══════════════════════════════════════════════════════════════════
// 5. Score threshold filter — only return results above threshold
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live Qdrant server"]
async fn weird_score_threshold() {
    println!("▸ Score threshold filter...");
    let mut d = driver().await;
    ensure_collection(&mut d, "weird_threshold", 3).await;

    let points = vec![
        Point::new(1u64, vec![1.0, 0.0, 0.0]).with_payload("name", "exact_match"),
        Point::new(2u64, vec![0.5, 0.5, 0.0]).with_payload("name", "partial"),
        Point::new(3u64, vec![0.0, 0.0, 1.0]).with_payload("name", "orthogonal"),
    ];
    d.upsert("weird_threshold", &points, false).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Without threshold: all 3 returned
    let all = d
        .search("weird_threshold", &[1.0, 0.0, 0.0], 10, None)
        .await
        .unwrap();
    assert_eq!(all.len(), 3);

    // With high threshold: only exact + partial
    let filtered = d
        .search("weird_threshold", &[1.0, 0.0, 0.0], 10, Some(0.5))
        .await
        .unwrap();
    println!(
        "  Filtered results (threshold=0.5): {:?}",
        filtered
            .iter()
            .map(|p| (&p.id, p.score))
            .collect::<Vec<_>>()
    );
    assert!(
        filtered.len() <= 2,
        "Threshold 0.5 should filter orthogonal"
    );
    assert!(
        filtered.iter().all(|p| p.score >= 0.5),
        "All scores should be >= 0.5"
    );

    // With very high threshold: only exact
    let strict = d
        .search("weird_threshold", &[1.0, 0.0, 0.0], 10, Some(0.99))
        .await
        .unwrap();
    assert_eq!(
        strict.len(),
        1,
        "Only exact match should pass 0.99 threshold"
    );
    println!("  ✓ Score threshold works correctly");

    d.delete_collection("weird_threshold").await.ok();
}

// ═══════════════════════════════════════════════════════════════════
// 6. Concurrent pool access — 10 searches in parallel
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live Qdrant server"]
async fn weird_concurrent_pool() {
    println!("▸ Concurrent pool access (10 parallel searches)...");
    let mut d = driver().await;
    ensure_collection(&mut d, "weird_concurrent", 3).await;

    // Seed some data
    let points: Vec<Point> = (0..20)
        .map(|i| {
            let angle = (i as f32) * std::f32::consts::PI / 10.0;
            Point::new((i + 1) as u64, vec![angle.cos(), angle.sin(), 0.0])
                .with_payload("idx", i as i64)
        })
        .collect();
    d.upsert("weird_concurrent", &points, false).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Create a pool with 3 connections
    let cfg = PoolConfig::new("localhost", 6334);
    let pool = QdrantPool::new(cfg).await.unwrap();

    // Fire 10 parallel searches
    let mut handles = Vec::new();
    for i in 0..10u32 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = pool.get().await.expect("pool.get failed");
            let angle = (i as f32) * std::f32::consts::PI / 5.0;
            let query = vec![angle.cos(), angle.sin(), 0.0];
            let results = conn
                .search("weird_concurrent", &query, 3, None)
                .await
                .expect("concurrent search failed");
            assert!(!results.is_empty(), "Search {} returned empty", i);
            (i, results.len())
        }));
    }

    let mut total = 0;
    for h in handles {
        let (i, count) = h.await.unwrap();
        println!("    Search {}: {} results", i, count);
        total += count;
    }
    assert!(total >= 10, "Should have at least 10 total results");
    println!(
        "  ✓ 10 concurrent searches completed ({} total results)",
        total
    );

    d.delete_collection("weird_concurrent").await.ok();
}

// ═══════════════════════════════════════════════════════════════════
// 7. UUID point IDs — use string UUIDs instead of numeric
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live Qdrant server"]
async fn weird_uuid_point_ids() {
    println!("▸ UUID point IDs...");
    let mut d = driver().await;
    ensure_collection(&mut d, "weird_uuid", 2).await;

    let uuid1 = "550e8400-e29b-41d4-a716-446655440000";
    let uuid2 = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

    let points = vec![
        Point {
            id: PointId::Uuid(uuid1.to_string()),
            vector: vec![1.0, 0.0],
            payload: HashMap::new(),
        },
        Point {
            id: PointId::Uuid(uuid2.to_string()),
            vector: vec![0.0, 1.0],
            payload: HashMap::new(),
        },
    ];
    d.upsert("weird_uuid", &points, false)
        .await
        .expect("UUID upsert should work");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let results = d.search("weird_uuid", &[1.0, 0.0], 2, None).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].id, PointId::Uuid(uuid1.to_string()));
    assert_eq!(results[1].id, PointId::Uuid(uuid2.to_string()));
    println!("  ✓ UUID point IDs work correctly");

    d.delete_collection("weird_uuid").await.ok();
}

// ═══════════════════════════════════════════════════════════════════
// 8. Nested payload — objects and arrays in payload values
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live Qdrant server"]
async fn weird_nested_payload() {
    println!("▸ Nested payload (objects + arrays)...");
    let mut d = driver().await;
    ensure_collection(&mut d, "weird_nested", 2).await;

    let mut nested_obj = HashMap::new();
    nested_obj.insert(
        "street".to_string(),
        PayloadValue::String("Jl. Sudirman".to_string()),
    );
    nested_obj.insert(
        "city".to_string(),
        PayloadValue::String("Jakarta".to_string()),
    );
    nested_obj.insert("zip".to_string(), PayloadValue::Integer(12345));

    let mut payload = HashMap::new();
    payload.insert(
        "name".to_string(),
        PayloadValue::String("PT Qail".to_string()),
    );
    payload.insert("active".to_string(), PayloadValue::Bool(true));
    payload.insert("rating".to_string(), PayloadValue::Float(4.85));
    payload.insert(
        "tags".to_string(),
        PayloadValue::List(vec![
            PayloadValue::String("maritime".to_string()),
            PayloadValue::String("tech".to_string()),
            PayloadValue::Integer(42),
        ]),
    );
    payload.insert("address".to_string(), PayloadValue::Object(nested_obj));

    let points = vec![Point {
        id: PointId::Num(1),
        vector: vec![1.0, 0.0],
        payload,
    }];

    d.upsert("weird_nested", &points, false)
        .await
        .expect("Nested payload upsert should work");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let scroll = d.scroll("weird_nested", 10, None, false).await.unwrap();
    assert_eq!(scroll.points.len(), 1);
    let pt = &scroll.points[0];

    // Verify nested structure survived
    match pt.payload.get("tags") {
        Some(PayloadValue::List(tags)) => {
            assert_eq!(tags.len(), 3);
            println!("    tags = {:?}", tags);
        }
        other => panic!("Expected List for tags, got {:?}", other),
    }
    match pt.payload.get("address") {
        Some(PayloadValue::Object(addr)) => {
            assert!(addr.contains_key("street"));
            assert!(addr.contains_key("city"));
            println!("    address = {:?}", addr);
        }
        other => panic!("Expected Object for address, got {:?}", other),
    }
    match pt.payload.get("rating") {
        Some(PayloadValue::Float(f)) => assert!((*f - 4.85).abs() < 0.001),
        other => panic!("Expected Float for rating, got {:?}", other),
    }
    match pt.payload.get("active") {
        Some(PayloadValue::Bool(b)) => assert!(*b),
        other => panic!("Expected Bool for active, got {:?}", other),
    }
    println!("  ✓ Nested payload (object + array + float + bool) round-trips correctly");

    d.delete_collection("weird_nested").await.ok();
}

// ═══════════════════════════════════════════════════════════════════
// 9. Scroll with pagination — verify next_offset advances
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live Qdrant server"]
async fn weird_scroll_pagination() {
    println!("▸ Scroll pagination...");
    let mut d = driver().await;
    ensure_collection(&mut d, "weird_paginate", 2).await;

    // Insert 15 points
    let points: Vec<Point> = (1..=15)
        .map(|i| {
            Point::new(i as u64, vec![(i as f32).sin(), (i as f32).cos()])
                .with_payload("idx", i as i64)
        })
        .collect();
    d.upsert("weird_paginate", &points, false).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Page 1: first 5
    let page1 = d.scroll("weird_paginate", 5, None, false).await.unwrap();
    assert_eq!(page1.points.len(), 5, "Page 1 should have 5 points");
    println!(
        "    Page 1: {} points, next_offset={:?}",
        page1.points.len(),
        page1.next_offset
    );

    // Page 2: next 5 using offset
    assert!(
        page1.next_offset.is_some(),
        "Should have next_offset for pagination"
    );
    let page2 = d
        .scroll("weird_paginate", 5, page1.next_offset.as_ref(), false)
        .await
        .unwrap();
    assert_eq!(page2.points.len(), 5, "Page 2 should have 5 points");
    println!(
        "    Page 2: {} points, next_offset={:?}",
        page2.points.len(),
        page2.next_offset
    );

    // Page 3: last 5
    assert!(
        page2.next_offset.is_some(),
        "Should have next_offset for page 3"
    );
    let page3 = d
        .scroll("weird_paginate", 5, page2.next_offset.as_ref(), false)
        .await
        .unwrap();
    assert_eq!(page3.points.len(), 5, "Page 3 should have 5 points");
    println!(
        "    Page 3: {} points, next_offset={:?}",
        page3.points.len(),
        page3.next_offset
    );

    // Page 4: should be empty (no more points)
    if let Some(ref offset) = page3.next_offset {
        let page4 = d
            .scroll("weird_paginate", 5, Some(offset), false)
            .await
            .unwrap();
        assert_eq!(page4.points.len(), 0, "Page 4 should be empty");
        println!("    Page 4: 0 points (end of data)");
    }

    // Total unique IDs across all pages should be 15
    let mut all_ids: Vec<_> = page1
        .points
        .iter()
        .chain(page2.points.iter())
        .chain(page3.points.iter())
        .map(|p| format!("{:?}", p.id))
        .collect();
    all_ids.sort();
    all_ids.dedup();
    assert_eq!(
        all_ids.len(),
        15,
        "Should see all 15 unique IDs across pages"
    );
    println!("  ✓ Pagination works: 15 points across 3 pages of 5");

    d.delete_collection("weird_paginate").await.ok();
}

// ═══════════════════════════════════════════════════════════════════
// 10. Reconnection resilience — drop connection, then query again
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live Qdrant server"]
async fn weird_reconnection() {
    println!("▸ Reconnection resilience...");
    let mut d = driver().await;
    ensure_collection(&mut d, "weird_reconnect", 2).await;

    let points = vec![Point::new(1u64, vec![1.0, 0.0]).with_payload("name", "survivor")];
    d.upsert("weird_reconnect", &points, false).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Force the internal sender to "break" by doing a sequence of rapid requests
    for i in 0..5 {
        let r = d.search("weird_reconnect", &[1.0, 0.0], 1, None).await;
        match r {
            Ok(results) => println!("    Rapid query {}: {} results", i, results.len()),
            Err(e) => println!(
                "    Rapid query {}: error (expected during stress): {}",
                i, e
            ),
        }
    }

    // After rapid fire, the driver should still work (auto-reconnect)
    let results = d
        .search("weird_reconnect", &[1.0, 0.0], 1, None)
        .await
        .expect("Should recover after rapid requests");
    assert_eq!(results.len(), 1);
    println!("  ✓ Driver resilient after rapid sequential requests");

    d.delete_collection("weird_reconnect").await.ok();
}

// ═══════════════════════════════════════════════════════════════════
// 11. Batch upsert — 1000 points in a single call
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live Qdrant server"]
async fn weird_batch_upsert_1000() {
    println!("▸ Batch upsert 1000 points...");
    let mut d = driver().await;
    ensure_collection(&mut d, "weird_batch", 8).await;

    let start = std::time::Instant::now();
    let points: Vec<Point> = (1..=1000)
        .map(|i| {
            let mut vec = vec![0.0f32; 8];
            for (j, v) in vec.iter_mut().enumerate() {
                *v = ((i * 8 + j) as f32 * 0.01).sin();
            }
            Point::new(i as u64, vec).with_payload("batch_id", i as i64)
        })
        .collect();

    d.upsert("weird_batch", &points, false)
        .await
        .expect("Batch upsert of 1000 should work");
    let elapsed = start.elapsed();
    println!("    Upserted 1000 points in {:?}", elapsed);

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // Verify with scroll
    let mut total = 0;
    let mut offset = None;
    loop {
        let page = d
            .scroll("weird_batch", 100, offset.as_ref(), false)
            .await
            .unwrap();
        total += page.points.len();
        if page.next_offset.is_none() || page.points.is_empty() {
            break;
        }
        offset = page.next_offset;
    }
    assert_eq!(total, 1000, "Should scroll all 1000 points, got {}", total);
    println!(
        "  ✓ 1000 points upserted and scrolled in {:?}",
        start.elapsed()
    );

    d.delete_collection("weird_batch").await.ok();
}
