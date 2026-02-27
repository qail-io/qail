#![cfg(feature = "qdrant")]
//! Speed benchmark: PG-only vs Qdrant-only vs Hybrid (Qdrant → PG)
//!
//! Measures raw driver latency for each path and prints a comparison table.
//!
//! Run with:
//!   podman run -d --name qdrant-test -m 256m -p 6333:6333 -p 6334:6334 qdrant/qdrant
//!   DATABASE_URL="postgresql://qail_user@localhost:5432/postgres" \
//!     cargo test -p qail-gateway --test bench_pg_vs_qdrant -- --nocapture

use qail_core::prelude::*;
use qail_pg::PgDriver;
use qail_qdrant::prelude::*;
use std::time::{Duration, Instant};

const COLLECTION: &str = "bench_vectors";
const TABLE: &str = "bench_routes";
const DIM: u64 = 128;
const NUM_ROUTES: usize = 500;
const ITERATIONS: usize = 200;
const WARMUP: usize = 20;

/// Deterministic fake embedding from an integer seed.
fn fake_embed(seed: usize) -> Vec<f32> {
    (0..DIM as usize)
        .map(|j| ((seed * 127 + j) as f32 * 0.017).sin())
        .collect()
}

/// Collect timing stats from a slice of durations.
struct Stats {
    min: Duration,
    max: Duration,
    avg: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
}

fn compute_stats(times: &mut Vec<Duration>) -> Stats {
    times.sort();
    let n = times.len();
    let total: Duration = times.iter().sum();
    Stats {
        min: times[0],
        max: times[n - 1],
        avg: total / n as u32,
        p50: times[n / 2],
        p95: times[(n as f64 * 0.95) as usize],
        p99: times[(n as f64 * 0.99) as usize],
    }
}

fn us(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000_000.0
}

#[tokio::test]
#[ignore = "Requires live DATABASE_URL + Qdrant server"]
async fn bench_pg_vs_qdrant() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║   SPEED BENCHMARK: PG vs QDRANT vs HYBRID                 ║");
    println!(
        "║   {} routes, {}-dim vectors, {} iterations              ║",
        NUM_ROUTES, DIM, ITERATIONS
    );
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // ── Setup ────────────────────────────────────────────────────────
    println!("▸ Setting up...");
    let mut pg = PgDriver::connect_env().await.expect("PG: set DATABASE_URL");
    let mut qd = QdrantDriver::connect("localhost", 6334)
        .await
        .expect("Qdrant: not running");

    // Cleanup
    pg.execute_raw(&format!("DROP TABLE IF EXISTS {} CASCADE", TABLE))
        .await
        .ok();
    let _ = qd.delete_collection(COLLECTION).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Create PG table with index
    pg.execute_raw(&format!(
        "CREATE TABLE {} (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            origin TEXT NOT NULL,
            destination TEXT NOT NULL,
            price_idr BIGINT NOT NULL,
            category TEXT NOT NULL
        )",
        TABLE
    ))
    .await
    .unwrap();
    pg.execute_raw(&format!(
        "CREATE INDEX idx_{}_cat ON {} (category)",
        TABLE, TABLE
    ))
    .await
    .ok();

    // Create Qdrant collection
    qd.create_collection(COLLECTION, DIM, Distance::Cosine, false)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Insert data
    let categories = ["ferry", "speedboat", "yacht", "cargo", "cruise"];
    let origins = [
        "Jakarta", "Surabaya", "Bali", "Lombok", "Makassar", "Semarang", "Medan",
    ];
    let dests = [
        "Nusa Penida",
        "Gili Trawangan",
        "Karimunjawa",
        "Flores",
        "Komodo",
        "Madura",
        "Bangka",
    ];

    // PG: batch insert
    let t0 = Instant::now();
    let mut values = Vec::with_capacity(NUM_ROUTES);
    for i in 1..=NUM_ROUTES {
        let cat = categories[i % categories.len()];
        let origin = origins[i % origins.len()];
        let dest = dests[i % dests.len()];
        let price = 25_000 + (i * 1_337) as i64;
        values.push(format!(
            "({}, 'Route {} {} to {}', '{}', '{}', {}, '{}')",
            i, i, origin, dest, origin, dest, price, cat
        ));
    }
    // Insert in chunks of 100
    for chunk in values.chunks(100) {
        pg.execute_raw(&format!(
            "INSERT INTO {} (id, title, origin, destination, price_idr, category) VALUES {}",
            TABLE,
            chunk.join(",")
        ))
        .await
        .unwrap();
    }
    let pg_insert_time = t0.elapsed();

    // Qdrant: batch upsert
    let t1 = Instant::now();
    let points: Vec<Point> = (1..=NUM_ROUTES)
        .map(|i| {
            let cat = categories[i % categories.len()];
            Point::new(i as u64, fake_embed(i))
                .with_payload("category", cat)
                .with_payload("route_id", i as i64)
        })
        .collect();
    // Upsert in chunks of 100
    for chunk in points.chunks(100) {
        qd.upsert(COLLECTION, chunk, false).await.unwrap();
    }
    let qd_insert_time = t1.elapsed();
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!(
        "  PG: {} rows inserted in {:.1}ms",
        NUM_ROUTES,
        pg_insert_time.as_secs_f64() * 1000.0
    );
    println!(
        "  QD: {} vectors upserted in {:.1}ms",
        NUM_ROUTES,
        qd_insert_time.as_secs_f64() * 1000.0
    );
    println!("  ✓ Setup complete\n");

    // ── Benchmark 1: PG SELECT by category ──────────────────────────
    println!("▸ Benchmark 1: PG SELECT (filter by category, LIMIT 10)...");
    let mut pg_times = Vec::with_capacity(ITERATIONS);
    for i in 0..WARMUP + ITERATIONS {
        let cat = categories[i % categories.len()];
        let t = Instant::now();
        let rows = pg.fetch_raw(&format!(
            "SELECT id, title, origin, destination, price_idr FROM {} WHERE category = '{}' LIMIT 10",
            TABLE, cat
        )).await.unwrap();
        let elapsed = t.elapsed();
        assert!(!rows.is_empty());
        if i >= WARMUP {
            pg_times.push(elapsed);
        }
    }
    let pg_stats = compute_stats(&mut pg_times);

    // ── Benchmark 2: Qdrant vector search ───────────────────────────
    println!("▸ Benchmark 2: Qdrant vector search (top 10)...");
    let mut qd_times = Vec::with_capacity(ITERATIONS);
    for i in 0..WARMUP + ITERATIONS {
        let query = fake_embed(i * 7 + 42);
        let t = Instant::now();
        let results = qd.search(COLLECTION, &query, 10, None).await.unwrap();
        let elapsed = t.elapsed();
        assert!(!results.is_empty());
        if i >= WARMUP {
            qd_times.push(elapsed);
        }
    }
    let qd_stats = compute_stats(&mut qd_times);

    // ── Benchmark 3: Hybrid (Qdrant search → PG fetch by IDs) ──────
    println!("▸ Benchmark 3: Hybrid Qdrant→PG (vector search + row fetch)...");
    let mut hybrid_times = Vec::with_capacity(ITERATIONS);
    for i in 0..WARMUP + ITERATIONS {
        let query = fake_embed(i * 13 + 7);
        let t = Instant::now();

        // Step A: vector search
        let results = qd.search(COLLECTION, &query, 5, None).await.unwrap();
        let ids: Vec<String> = results
            .iter()
            .map(|r| match &r.id {
                PointId::Num(n) => n.to_string(),
                PointId::Uuid(s) => s.clone(),
            })
            .collect();

        // Step B: PG fetch
        let id_list = ids.join(",");
        let _rows = pg
            .fetch_raw(&format!(
                "SELECT id, title, origin, destination, price_idr FROM {} WHERE id IN ({})",
                TABLE, id_list
            ))
            .await
            .unwrap();

        let elapsed = t.elapsed();
        if i >= WARMUP {
            hybrid_times.push(elapsed);
        }
    }
    let hy_stats = compute_stats(&mut hybrid_times);

    // ── Benchmark 4: PG-only equivalent of hybrid (LIKE search) ────
    println!("▸ Benchmark 4: PG full-text-ish search (ILIKE, LIMIT 5)...");
    let search_terms = [
        "Bali", "Lombok", "Java", "Surabaya", "Makassar", "ferry", "Jakarta",
    ];
    let mut pg_search_times = Vec::with_capacity(ITERATIONS);
    for i in 0..WARMUP + ITERATIONS {
        let term = search_terms[i % search_terms.len()];
        let t = Instant::now();
        let rows = pg.fetch_raw(&format!(
            "SELECT id, title, origin, destination, price_idr FROM {} WHERE title ILIKE '%{}%' LIMIT 5",
            TABLE, term
        )).await.unwrap();
        let _ = rows;
        let elapsed = t.elapsed();
        if i >= WARMUP {
            pg_search_times.push(elapsed);
        }
    }
    let pgs_stats = compute_stats(&mut pg_search_times);

    // ── Results ─────────────────────────────────────────────────────
    println!("\n╔══════════════════════════════════════════════════════════════════════════╗");
    println!(
        "║  RESULTS  ({} iterations, {} warmup)                                  ║",
        ITERATIONS, WARMUP
    );
    println!("╠══════════════════════════════════════════════════════════════════════════╣");
    println!("║  Path                      │  avg       │  p50       │  p95       │  p99        ║");
    println!("╠════════════════════════════╪════════════╪════════════╪════════════╪═════════════╣");
    println!(
        "║  PG SELECT (indexed)       │ {:>7.0} µs │ {:>7.0} µs │ {:>7.0} µs │ {:>7.0} µs  ║",
        us(pg_stats.avg),
        us(pg_stats.p50),
        us(pg_stats.p95),
        us(pg_stats.p99)
    );
    println!(
        "║  PG ILIKE search           │ {:>7.0} µs │ {:>7.0} µs │ {:>7.0} µs │ {:>7.0} µs  ║",
        us(pgs_stats.avg),
        us(pgs_stats.p50),
        us(pgs_stats.p95),
        us(pgs_stats.p99)
    );
    println!(
        "║  Qdrant vector search      │ {:>7.0} µs │ {:>7.0} µs │ {:>7.0} µs │ {:>7.0} µs  ║",
        us(qd_stats.avg),
        us(qd_stats.p50),
        us(qd_stats.p95),
        us(qd_stats.p99)
    );
    println!(
        "║  Hybrid (Qdrant→PG)        │ {:>7.0} µs │ {:>7.0} µs │ {:>7.0} µs │ {:>7.0} µs  ║",
        us(hy_stats.avg),
        us(hy_stats.p50),
        us(hy_stats.p95),
        us(hy_stats.p99)
    );
    println!("╠══════════════════════════════════════════════════════════════════════════╣");
    println!("║  Min/Max:                                                              ║");
    println!(
        "║    PG SELECT   : {:>7.0} µs — {:>7.0} µs                                ║",
        us(pg_stats.min),
        us(pg_stats.max)
    );
    println!(
        "║    PG ILIKE    : {:>7.0} µs — {:>7.0} µs                                ║",
        us(pgs_stats.min),
        us(pgs_stats.max)
    );
    println!(
        "║    Qdrant      : {:>7.0} µs — {:>7.0} µs                                ║",
        us(qd_stats.min),
        us(qd_stats.max)
    );
    println!(
        "║    Hybrid      : {:>7.0} µs — {:>7.0} µs                                ║",
        us(hy_stats.min),
        us(hy_stats.max)
    );
    println!("╚══════════════════════════════════════════════════════════════════════════╝");

    // Speed ratios
    let qd_vs_pg = us(qd_stats.avg) / us(pg_stats.avg);
    let hy_vs_pg = us(hy_stats.avg) / us(pg_stats.avg);
    let hy_vs_pgs = us(hy_stats.avg) / us(pgs_stats.avg);
    println!("\n  Qdrant / PG SELECT ratio: {:.2}x", qd_vs_pg);
    println!("  Hybrid / PG SELECT ratio: {:.2}x", hy_vs_pg);
    println!("  Hybrid / PG ILIKE ratio:  {:.2}x", hy_vs_pgs);

    if qd_vs_pg < 1.0 {
        println!("\n  🚀 Qdrant vector search is FASTER than PG indexed SELECT!");
    } else {
        println!(
            "\n  📊 PG indexed SELECT is {:.1}x faster than Qdrant (expected — PG is local, indexed)",
            qd_vs_pg
        );
    }

    // ── Cleanup ─────────────────────────────────────────────────────
    pg.execute_raw(&format!("DROP TABLE IF EXISTS {}", TABLE))
        .await
        .ok();
    qd.delete_collection(COLLECTION).await.ok();
    println!("\n  ✓ Cleanup done\n");
}
