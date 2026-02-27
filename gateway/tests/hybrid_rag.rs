#![cfg(feature = "qdrant")]
//! Hybrid PG + Qdrant integration test: simulates a RAG / semantic search workflow.
//!
//! **Scenario: Maritime Knowledge Base**
//! 1. Postgres stores route documents (id, title, content, origin, destination)
//! 2. Qdrant stores embedding vectors keyed by the same integer IDs
//! 3. A user searches by meaning → Qdrant returns ranked IDs → PG fetches full records
//!
//! Run with:
//!   podman run -d --name qdrant-test -m 256m -p 6333:6333 -p 6334:6334 qdrant/qdrant
//!   DATABASE_URL="postgresql://..." cargo test -p qail-gateway --test hybrid_rag -- --nocapture
//!
//! Requires:
//!   - Qdrant on localhost:6334 (gRPC)
//!   - Postgres accessible via DATABASE_URL env var

use qail_core::prelude::*;
use qail_pg::PgDriver;
use qail_qdrant::prelude::*;

const COLLECTION: &str = "hybrid_kb_embeddings";
const TABLE: &str = "hybrid_test_kb";
const VECTOR_DIM: u64 = 8;

/// Fake "embedding" function — deterministic based on keywords.
/// In production this would be an OpenAI/Cohere API call.
fn fake_embed(text: &str) -> Vec<f32> {
    let mut v = vec![0.0f32; VECTOR_DIM as usize];
    let lower = text.to_lowercase();
    // Weight dimensions by keyword presence (simulates semantic meaning)
    if lower.contains("bali") {
        v[0] += 1.0;
    }
    if lower.contains("lombok") {
        v[1] += 1.0;
    }
    if lower.contains("java") {
        v[2] += 1.0;
    }
    if lower.contains("fast") {
        v[3] += 1.0;
    }
    if lower.contains("ferry") {
        v[4] += 1.0;
    }
    if lower.contains("luxury") {
        v[5] += 1.0;
    }
    if lower.contains("budget") {
        v[6] += 1.0;
    }
    if lower.contains("scenic") {
        v[7] += 1.0;
    }
    // Normalize
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter_mut().for_each(|x| *x /= norm);
    }
    v
}

// ═══════════════════════════════════════════════════════════════════
// Main hybrid test
// ═══════════════════════════════════════════════════════════════════
#[tokio::test]
#[ignore = "Requires live DATABASE_URL + Qdrant server"]
async fn hybrid_rag_search_flow() {
    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║  HYBRID PG + QDRANT INTEGRATION TEST                   ║");
    println!("║  Scenario: Maritime Knowledge Base / Semantic Search    ║");
    println!("╚══════════════════════════════════════════════════════════╝\n");

    // ── 1. Connect to both backends ─────────────────────────────────
    println!("▸ Phase 1: Connecting to Postgres + Qdrant...");
    let mut pg = PgDriver::connect_env()
        .await
        .expect("Failed to connect to Postgres — set DATABASE_URL");
    let mut qd = QdrantDriver::connect("localhost", 6334)
        .await
        .expect("Failed to connect to Qdrant — is it running?");
    println!("  ✓ Postgres connected");
    println!("  ✓ Qdrant connected");

    // ── 2. Setup: Create PG table + Qdrant collection ───────────────
    println!("\n▸ Phase 2: Setting up schema...");

    // Cleanup from previous runs
    pg.execute_raw(&format!("DROP TABLE IF EXISTS {} CASCADE", TABLE))
        .await
        .ok();
    let _ = qd.delete_collection(COLLECTION).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Create PG table
    pg.execute_raw(&format!(
        "CREATE TABLE {} (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            origin TEXT,
            destination TEXT,
            price_idr BIGINT,
            created_at TIMESTAMP DEFAULT NOW()
        )",
        TABLE
    ))
    .await
    .expect("Failed to create PG table");
    println!("  ✓ PG table '{}' created", TABLE);

    // Create Qdrant collection
    qd.create_collection(COLLECTION, VECTOR_DIM, Distance::Cosine, false)
        .await
        .expect("Failed to create Qdrant collection");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    println!(
        "  ✓ Qdrant collection '{}' created (dim={})",
        COLLECTION, VECTOR_DIM
    );

    // ── 3. Insert documents into Postgres ───────────────────────────
    println!("\n▸ Phase 3: Inserting documents into Postgres...");
    let documents = vec![
        (
            1,
            "Bali Fast Ferry",
            "Express fast ferry service from Java to Bali. Quick crossing.",
            "Ketapang",
            "Gilimanuk",
            85_000i64,
        ),
        (
            2,
            "Lombok Scenic Cruise",
            "Scenic luxury cruise from Bali to Lombok. Beautiful views.",
            "Padang Bai",
            "Lembar",
            350_000,
        ),
        (
            3,
            "Java Budget Ferry",
            "Budget ferry connecting East Java ports. Affordable.",
            "Surabaya",
            "Madura",
            25_000,
        ),
        (
            4,
            "Bali Luxury Yacht",
            "Private luxury yacht charter around Bali. Premium service.",
            "Benoa",
            "Nusa Penida",
            2_500_000,
        ),
        (
            5,
            "Fast Lombok Express",
            "Fast ferry crossing from Bali to Lombok. Speed matters.",
            "Serangan",
            "Bangsal",
            150_000,
        ),
        (
            6,
            "Java Scenic Ferry",
            "Scenic ferry ride along Java northern coast. Enjoy the view.",
            "Semarang",
            "Karimunjawa",
            180_000,
        ),
        (
            7,
            "Bali Budget Boat",
            "Budget boat transfer to Bali islands. Backpacker friendly.",
            "Sanur",
            "Nusa Lembongan",
            35_000,
        ),
    ];

    for (id, title, content, origin, dest, price) in &documents {
        pg.execute_raw(&format!(
            "INSERT INTO {} (id, title, content, origin, destination, price_idr) VALUES ({}, '{}', '{}', '{}', '{}', {})",
            TABLE, id, title, content, origin, dest, price
        )).await.unwrap();
    }
    // Reset sequence
    pg.execute_raw(&format!(
        "SELECT setval(pg_get_serial_sequence('{}', 'id'), 7)",
        TABLE
    ))
    .await
    .ok();
    println!("  ✓ {} documents inserted into Postgres", documents.len());

    // ── 4. Generate embeddings and upsert to Qdrant ─────────────────
    println!("\n▸ Phase 4: Generating embeddings and upserting to Qdrant...");
    let points: Vec<Point> = documents
        .iter()
        .map(|(id, title, content, origin, dest, price)| {
            let text = format!("{} {} {} {}", title, content, origin, dest);
            let embedding = fake_embed(&text);
            Point::new(*id as u64, embedding)
                .with_payload("title", *title)
                .with_payload("price_idr", *price)
        })
        .collect();

    qd.upsert(COLLECTION, &points, false)
        .await
        .expect("Qdrant upsert failed");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    println!("  ✓ {} embeddings upserted to Qdrant", points.len());

    // ── 5. Semantic search: "fast Bali ferry" ───────────────────────
    println!("\n▸ Phase 5: Semantic search — 'fast Bali ferry'...");
    let query_embedding = fake_embed("fast Bali ferry");
    println!("  Query vector: {:?}", query_embedding);

    let search_results = qd
        .search(COLLECTION, &query_embedding, 3, None)
        .await
        .expect("Qdrant search failed");

    println!("  Qdrant returned {} results:", search_results.len());
    let matched_ids: Vec<u64> = search_results
        .iter()
        .map(|r| {
            let id = match &r.id {
                PointId::Num(n) => *n,
                PointId::Uuid(s) => s.parse().unwrap_or(0),
            };
            println!(
                "    id={}, score={:.4}, title={:?}",
                id,
                r.score,
                r.payload.get("title")
            );
            id
        })
        .collect();

    assert!(!matched_ids.is_empty(), "Should get at least 1 result");

    // ── 6. Fetch full records from Postgres by matched IDs ──────────
    println!("\n▸ Phase 6: Fetching full records from Postgres...");
    let id_list = matched_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let pg_rows = pg
        .fetch_raw(&format!(
            "SELECT id, title, content, origin, destination, price_idr FROM {} WHERE id IN ({})",
            TABLE, id_list
        ))
        .await
        .expect("Postgres query failed");

    println!("  PG returned {} full records:", pg_rows.len());
    for row in &pg_rows {
        let title = row.get_string_by_name("title").unwrap_or_default();
        let origin = row.get_string_by_name("origin").unwrap_or_default();
        let dest = row.get_string_by_name("destination").unwrap_or_default();
        let price = row.get_i64_by_name("price_idr").unwrap_or(0);
        println!("    {} — {} → {} (Rp {})", title, origin, dest, price);
    }
    assert_eq!(
        pg_rows.len(),
        matched_ids.len(),
        "PG should return same count as Qdrant matches"
    );

    // ── 7. Verify search relevance ──────────────────────────────────
    println!("\n▸ Phase 7: Verifying relevance...");
    // "fast Bali ferry" should match id=1 (Bali Fast Ferry) best
    assert_eq!(
        matched_ids[0], 1,
        "Best match should be 'Bali Fast Ferry' (id=1)"
    );
    // id=5 (Fast Lombok Express) should also rank high (has 'fast' + 'bali')
    assert!(
        matched_ids.contains(&5),
        "id=5 (Fast Lombok Express) should be in top 3"
    );
    println!("  ✓ Top result: id=1 (Bali Fast Ferry) — correct!");
    println!("  ✓ id=5 (Fast Lombok Express) in top 3 — correct!");

    // ── 8. Search: "luxury scenic" ──────────────────────────────────
    println!("\n▸ Phase 8: Semantic search — 'luxury scenic'...");
    let query2 = fake_embed("luxury scenic cruise");
    let results2 = qd.search(COLLECTION, &query2, 3, None).await.unwrap();
    let ids2: Vec<u64> = results2
        .iter()
        .map(|r| match &r.id {
            PointId::Num(n) => *n,
            PointId::Uuid(s) => s.parse().unwrap_or(0),
        })
        .collect();
    println!("  Top results: {:?}", ids2);
    // id=2 (Lombok Scenic Cruise) or id=4 (Bali Luxury Yacht) should be top
    assert!(
        ids2[0] == 2 || ids2[0] == 4,
        "Top result for 'luxury scenic' should be id=2 or id=4, got {}",
        ids2[0]
    );
    println!("  ✓ Top result: id={} — relevant!", ids2[0]);

    // ── 9. Search: "budget Java" ────────────────────────────────────
    println!("\n▸ Phase 9: Semantic search — 'budget Java'...");
    let query3 = fake_embed("budget Java");
    let results3 = qd.search(COLLECTION, &query3, 3, None).await.unwrap();
    let ids3: Vec<u64> = results3
        .iter()
        .map(|r| match &r.id {
            PointId::Num(n) => *n,
            PointId::Uuid(s) => s.parse().unwrap_or(0),
        })
        .collect();
    println!("  Top results: {:?}", ids3);
    assert_eq!(
        ids3[0], 3,
        "Top result for 'budget Java' should be id=3 (Java Budget Ferry)"
    );
    println!("  ✓ Top result: id=3 (Java Budget Ferry) — correct!");

    // ── 10. Cleanup ─────────────────────────────────────────────────
    println!("\n▸ Phase 10: Cleanup...");
    pg.execute_raw(&format!("DROP TABLE IF EXISTS {}", TABLE))
        .await
        .ok();
    qd.delete_collection(COLLECTION).await.ok();
    println!("  ✓ PG table and Qdrant collection cleaned up");

    println!("\n╔══════════════════════════════════════════════════════════╗");
    println!("║  ALL HYBRID TESTS PASSED ✓                             ║");
    println!("║  PG + Qdrant semantic search pipeline verified         ║");
    println!("╚══════════════════════════════════════════════════════════╝");
}
