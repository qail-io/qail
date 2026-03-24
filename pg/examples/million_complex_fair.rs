//! FAIR COMPLEX QUERY BENCHMARK - Using Pre-computed Prepared Statements
//!
//! This uses pipeline_execute_prepared_count for apples-to-apples comparison with Go pgx.
//! Go uses cached prepared statements - so do we!
//!
//! Run: cargo run --release --example million_complex_fair

use qail_pg::PgConnection;
use std::time::Instant;

const TOTAL_QUERIES: usize = 100_000;
const QUERIES_PER_BATCH: usize = 100;
const BATCHES: usize = TOTAL_QUERIES / QUERIES_PER_BATCH;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "example_staging").await?;

    println!("🚀 FAIR COMPLEX QUERY BENCHMARK");
    println!("================================");
    println!("Using: pipeline_execute_prepared_count (matches Go pgx)");
    println!("Total queries:    {:>12}", TOTAL_QUERIES);
    println!("Batch size:       {:>12}", QUERIES_PER_BATCH);
    println!();

    // ========================
    // Test 1: Simple SELECT
    // ========================
    println!("1️⃣  SIMPLE SELECT (baseline)");

    // Prepare ONCE
    let stmt1 = conn
        .prepare("SELECT id, name FROM harbors LIMIT $1")
        .await?;

    // Build params batch
    let params1: Vec<Vec<Option<Vec<u8>>>> = (1..=QUERIES_PER_BATCH)
        .map(|i| vec![Some(((i % 10) + 1).to_string().into_bytes())])
        .collect();

    let start = Instant::now();
    for _ in 0..BATCHES {
        conn.pipeline_execute_prepared_count(&stmt1, &params1)
            .await?;
    }
    let simple_elapsed = start.elapsed();
    let simple_qps = TOTAL_QUERIES as f64 / simple_elapsed.as_secs_f64();
    println!(
        "   ✅ {:.0} q/s ({:.2}s)",
        simple_qps,
        simple_elapsed.as_secs_f64()
    );

    // ========================
    // Test 2: SELECT with WHERE
    // ========================
    println!("\n2️⃣  SELECT with WHERE clause");

    let stmt2 = conn.prepare(
        "SELECT id, name, country, latitude, longitude FROM harbors WHERE name LIKE $1 LIMIT 10"
    ).await?;

    let params2: Vec<Vec<Option<Vec<u8>>>> = (1..=QUERIES_PER_BATCH)
        .map(|i| vec![Some(format!("%harbor{}%", i % 10).into_bytes())])
        .collect();

    let start = Instant::now();
    for _ in 0..BATCHES {
        conn.pipeline_execute_prepared_count(&stmt2, &params2)
            .await?;
    }
    let where_elapsed = start.elapsed();
    let where_qps = TOTAL_QUERIES as f64 / where_elapsed.as_secs_f64();
    println!(
        "   ✅ {:.0} q/s ({:.2}s)",
        where_qps,
        where_elapsed.as_secs_f64()
    );

    // ========================
    // Test 3: SELECT with ORDER BY
    // ========================
    println!("\n3️⃣  SELECT with ORDER BY");

    let stmt3 = conn
        .prepare(
            "SELECT id, name, country FROM harbors WHERE name LIKE $1 ORDER BY name ASC LIMIT 20",
        )
        .await?;

    let params3: Vec<Vec<Option<Vec<u8>>>> = (1..=QUERIES_PER_BATCH)
        .map(|i| vec![Some(format!("%{}%", i % 10).into_bytes())])
        .collect();

    let start = Instant::now();
    for _ in 0..BATCHES {
        conn.pipeline_execute_prepared_count(&stmt3, &params3)
            .await?;
    }
    let order_elapsed = start.elapsed();
    let order_qps = TOTAL_QUERIES as f64 / order_elapsed.as_secs_f64();
    println!(
        "   ✅ {:.0} q/s ({:.2}s)",
        order_qps,
        order_elapsed.as_secs_f64()
    );

    // ========================
    // Test 4: Many columns
    // ========================
    println!("\n4️⃣  SELECT with MANY columns");

    let stmt4 = conn.prepare(
        "SELECT id, name, country, latitude, longitude, timezone, created_at, updated_at FROM harbors WHERE name LIKE $1"
    ).await?;

    let params4: Vec<Vec<Option<Vec<u8>>>> = (1..=QUERIES_PER_BATCH)
        .map(|i| vec![Some(format!("%test{}%", i % 5).into_bytes())])
        .collect();

    let start = Instant::now();
    for _ in 0..BATCHES {
        conn.pipeline_execute_prepared_count(&stmt4, &params4)
            .await?;
    }
    let many_elapsed = start.elapsed();
    let many_qps = TOTAL_QUERIES as f64 / many_elapsed.as_secs_f64();
    println!(
        "   ✅ {:.0} q/s ({:.2}s)",
        many_qps,
        many_elapsed.as_secs_f64()
    );

    // ========================
    // Summary
    // ========================
    println!("\n📈 QAIL RESULTS (Fair - using prepared statements):");
    println!("┌──────────────────────────────────────────┐");
    println!("│ Query Type          │ Q/s      │ vs Base │");
    println!("├──────────────────────────────────────────┤");
    println!("│ Simple SELECT       │ {:>8.0} │  1.00x  │", simple_qps);
    println!(
        "│ + WHERE clause      │ {:>8.0} │  {:.2}x  │",
        where_qps,
        where_qps / simple_qps
    );
    println!(
        "│ + ORDER BY          │ {:>8.0} │  {:.2}x  │",
        order_qps,
        order_qps / simple_qps
    );
    println!(
        "│ + Many columns      │ {:>8.0} │  {:.2}x  │",
        many_qps,
        many_qps / simple_qps
    );
    println!("└──────────────────────────────────────────┘");

    println!("\n📊 vs Go pgx (276,613 q/s baseline):");
    let vs_go = simple_qps / 276613.0;
    if vs_go > 0.95 {
        println!("   🎉 QAIL matches Go! ({:.2}x)", vs_go);
    } else if vs_go > 0.85 {
        println!("   QAIL within 15% of Go ({:.2}x)", vs_go);
    } else {
        println!("   Go is {:.2}x faster", 1.0 / vs_go);
    }

    Ok(())
}
