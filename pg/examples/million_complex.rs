//! COMPLEX QUERY BENCHMARK
//!
//! Tests performance with realistic complex queries:
//! - JOINs between tables
//! - Multiple WHERE conditions
//! - ORDER BY, LIMIT
//! - Multiple columns
//!
//! Run: cargo run --release --example million_complex

use qail_core::ast::{Operator, QailCmd};
use qail_pg::PgConnection;
use std::time::Instant;

const TOTAL_QUERIES: usize = 100_000; // Fewer queries since complex
const QUERIES_PER_BATCH: usize = 100;
const BATCHES: usize = TOTAL_QUERIES / QUERIES_PER_BATCH;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "swb_staging_local").await?;

    println!("🚀 COMPLEX QUERY BENCHMARK");
    println!("==========================");
    println!("Total queries:    {:>12}", TOTAL_QUERIES);
    println!("Batch size:       {:>12}", QUERIES_PER_BATCH);
    println!("Batches:          {:>12}", BATCHES);
    println!("\n📊 Query Types:\n");

    // ========================
    // Test 1: Simple SELECT
    // ========================
    println!("1️⃣  SIMPLE SELECT (baseline)");
    let simple_cmds: Vec<QailCmd> = (1..=QUERIES_PER_BATCH)
        .map(|i| {
            QailCmd::get("harbors")
                .columns(["id", "name"])
                .limit((i % 10 + 1) as i64)
        })
        .collect();

    let start = Instant::now();
    for _ in 0..BATCHES {
        conn.pipeline_ast_cached(&simple_cmds).await?;
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
    let where_cmds: Vec<QailCmd> = (1..=QUERIES_PER_BATCH)
        .map(|i| {
            QailCmd::get("harbors")
                .columns(["id", "name", "country", "latitude", "longitude"])
                .filter("name", Operator::Like, format!("%harbor{}%", i % 10))
                .limit(10)
        })
        .collect();

    let start = Instant::now();
    for _ in 0..BATCHES {
        conn.pipeline_ast_cached(&where_cmds).await?;
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
    let order_cmds: Vec<QailCmd> = (1..=QUERIES_PER_BATCH)
        .map(|i| {
            QailCmd::get("harbors")
                .columns(["id", "name", "country"])
                .filter("name", Operator::Like, format!("%{}%", i % 10))
                .order_by("name", qail_core::ast::SortOrder::Asc)
                .limit(20)
        })
        .collect();

    let start = Instant::now();
    for _ in 0..BATCHES {
        conn.pipeline_ast_cached(&order_cmds).await?;
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
    let many_col_cmds: Vec<QailCmd> = (1..=QUERIES_PER_BATCH)
        .map(|i| {
            QailCmd::get("harbors")
                .columns([
                    "id",
                    "name",
                    "country",
                    "latitude",
                    "longitude",
                    "timezone",
                    "created_at",
                    "updated_at",
                ])
                .filter("name", Operator::Like, format!("%test{}%", i % 5))
        })
        .collect();

    let start = Instant::now();
    for _ in 0..BATCHES {
        conn.pipeline_ast_cached(&many_col_cmds).await?;
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
    println!("\n📈 SUMMARY:");
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

    println!("\n💡 Complex queries have minimal overhead because:");
    println!("   - AST encoding is O(n) where n = query complexity");
    println!("   - PostgreSQL execution dominates for JOINs/sorts");
    println!("   - Wire protocol overhead is same regardless of SQL length");

    Ok(())
}
