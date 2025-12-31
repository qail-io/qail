//! FAIR Benchmark: qail-pg vs SQLx vs SeaORM (All Cached)
//!
//! ## Methodology
//! - **Query**: `SELECT id, name, value FROM fair_bench WHERE value > 50 ORDER BY value LIMIT 25`
//! - **Table**: 100 rows of (id SERIAL, name TEXT, value INT)
//! - **Returns**: 25 rows per query
//! - **Connection**: TCP localhost (127.0.0.1:5432), NOT Unix socket
//! - **Caching**: All drivers use prepared statement caching
//!   - SQLx: statement_cache_capacity=100 (default per connection)
//!   - SeaORM: Uses SQLx under the hood (same caching)
//!   - QAIL: LRU cache with AST hash key
//!
//! ## What This Measures
//! Driver overhead + Postgres parse/plan/execute + row serialization/deserialization
//! This is NOT a "SELECT 1" microbenchmark - it tests real query execution with real data.
//!
//! ## Reproducing
//! ```bash
//! cargo run --release --example fair_benchmark
//! ```

use qail_core::prelude::*;
use qail_pg::driver::PgDriver;
use sqlx::postgres::PgPoolOptions;
use sea_orm::{Database, DatabaseConnection, Statement, ConnectionTrait};
use std::time::Instant;

const ITERATIONS: usize = 50_000;
const WARMUP: usize = 500;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🏁 FAIR Benchmark: qail-pg vs SQLx vs SeaORM");
    println!("==============================================");
    println!("Iterations: {}", ITERATIONS);
    println!("Warmup: {}", WARMUP);
    println!("Query: SELECT with WHERE + ORDER BY + LIMIT (returns 25 rows)\n");

    // Setup QAIL
    let mut qail_driver = PgDriver::connect("127.0.0.1", 5432, "orion", "postgres").await?;
    
    // Setup SQLx with statement cache ENABLED (default 100)
    let sqlx_pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .connect("postgres://orion@127.0.0.1/postgres")
        .await?;

    // Setup SeaORM (uses SQLx under the hood)
    let seaorm_db: DatabaseConnection = Database::connect("postgres://orion@127.0.0.1/postgres").await?;

    // Setup test table
    qail_driver.execute_raw("DROP TABLE IF EXISTS fair_bench").await?;
    qail_driver.execute_raw("CREATE TABLE fair_bench (id SERIAL PRIMARY KEY, name TEXT, value INT)").await?;
    qail_driver.execute_raw("INSERT INTO fair_bench (name, value) SELECT 'item' || i, i FROM generate_series(1, 100) i").await?;
    println!("✓ Test data ready (100 rows)\n");

    let sql = "SELECT id, name, value FROM fair_bench WHERE value > 50 ORDER BY value LIMIT 25";
    let qail_cmd = Qail::get("fair_bench")
        .columns(["id", "name", "value"])
        .filter("value", Operator::Gt, Value::Int(50))
        .order_by("value", SortOrder::Asc)
        .limit(25);

    // ============================================
    // Benchmark 1: SQLx with auto-caching
    // ============================================
    println!("📊 SQLx (statement_cache_capacity=100)");
    
    for _ in 0..WARMUP {
        let rows: Vec<(i32, String, i32)> = sqlx::query_as(sql)
            .fetch_all(&sqlx_pool)
            .await?;
        let _ = rows.len();
    }
    
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let rows: Vec<(i32, String, i32)> = sqlx::query_as(sql)
            .fetch_all(&sqlx_pool)
            .await?;
        let _ = rows.len();
    }
    let sqlx_time = start.elapsed();
    let sqlx_qps = ITERATIONS as f64 / sqlx_time.as_secs_f64();
    let sqlx_us = sqlx_time.as_micros() as f64 / ITERATIONS as f64;
    println!("  {:.1}μs/query | {:.0} q/s", sqlx_us, sqlx_qps);

    // ============================================
    // Benchmark 2: SeaORM (SQLx under the hood)
    // ============================================
    println!("📊 SeaORM (SQLx backend)");
    
    for _ in 0..WARMUP {
        let results = seaorm_db.query_all(Statement::from_string(
            sea_orm::DatabaseBackend::Postgres, 
            sql.to_string()
        )).await?;
        let _ = results.len();
    }
    
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let results = seaorm_db.query_all(Statement::from_string(
            sea_orm::DatabaseBackend::Postgres, 
            sql.to_string()
        )).await?;
        let _ = results.len();
    }
    let seaorm_time = start.elapsed();
    let seaorm_qps = ITERATIONS as f64 / seaorm_time.as_secs_f64();
    let seaorm_us = seaorm_time.as_micros() as f64 / ITERATIONS as f64;
    println!("  {:.1}μs/query | {:.0} q/s", seaorm_us, seaorm_qps);

    // ============================================
    // Benchmark 3: QAIL with prepared cache
    // ============================================
    println!("📊 QAIL (AST hash + LRU cache)");
    
    for _ in 0..WARMUP {
        let rows = qail_driver.fetch_all_cached(&qail_cmd).await?;
        let _ = rows.len();
    }
    
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let rows = qail_driver.fetch_all_cached(&qail_cmd).await?;
        let _ = rows.len();
    }
    let qail_time = start.elapsed();
    let qail_qps = ITERATIONS as f64 / qail_time.as_secs_f64();
    let qail_us = qail_time.as_micros() as f64 / ITERATIONS as f64;
    println!("  {:.1}μs/query | {:.0} q/s", qail_us, qail_qps);

    // ============================================
    // Results Summary
    // ============================================
    println!("\n══════════════════════════════════════════");
    println!("📈 RESULTS (All with statement caching)");
    println!("══════════════════════════════════════════");
    
    println!("SeaORM: {:>6.1}μs | {:>6.0} q/s", seaorm_us, seaorm_qps);
    println!("SQLx:   {:>6.1}μs | {:>6.0} q/s", sqlx_us, sqlx_qps);
    println!("QAIL:   {:>6.1}μs | {:>6.0} q/s ⭐", qail_us, qail_qps);
    
    println!("\n📊 Comparison (vs QAIL)");
    println!("────────────────────────");
    let sqlx_slower = ((sqlx_us / qail_us) - 1.0) * 100.0;
    let seaorm_slower = ((seaorm_us / qail_us) - 1.0) * 100.0;
    println!("SQLx:   {:.0}% slower", sqlx_slower);
    println!("SeaORM: {:.0}% slower", seaorm_slower);

    // Cleanup
    seaorm_db.close().await?;
    qail_driver.execute_raw("DROP TABLE fair_bench").await?;
    println!("\n🧹 Cleanup complete");

    Ok(())
}
