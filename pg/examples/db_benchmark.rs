//! Database Roundtrip Performance Benchmarks
//!
//! Tests qail-pg driver performance against real PostgreSQL:
//! - Single query latency
//! - 1K batch throughput  
//! - 1M insert via COPY
//!
//! Run: cargo run -p qail-pg --example db_benchmark --release

use qail_core::ast::Qail;
use qail_pg::{PgDriver, PgResult};
use std::time::Instant;

const DB_HOST: &str = "127.0.0.1";
const DB_PORT: u16 = 5432;
const DB_USER: &str = "postgres";
const DB_NAME: &str = "your_database";
// ⚠️ CHANGE THIS: Use your own password or set DB_PASSWORD env var
const DB_PASS: &str = "your_password_here";

#[tokio::main]
async fn main() -> PgResult<()> {
    // Use env var if set, otherwise use placeholder (change DB_PASS constant above)
    let db_pass = std::env::var("DB_PASSWORD").unwrap_or_else(|_| DB_PASS.to_string());

    println!("🏎️  QAIL-PG Database Roundtrip Benchmark");
    println!("==========================================");
    println!("Host: {}:{}", DB_HOST, DB_PORT);
    println!("Database: {}\n", DB_NAME);

    // Connect to database
    let mut driver =
        PgDriver::connect_with_password(DB_HOST, DB_PORT, DB_USER, DB_NAME, &db_pass).await?;

    println!("✅ Connected to PostgreSQL\n");

    // Run benchmarks
    bench_single_query(&mut driver).await?;
    bench_1k_batch(&mut driver).await?;
    // bench_1m_copy requires a test table, skip for now

    println!("✅ Benchmark complete!");
    Ok(())
}

/// Single query latency test
async fn bench_single_query(driver: &mut PgDriver) -> PgResult<()> {
    println!("📊 Single Query Latency (10,000 SELECT queries)");

    // Use an existing table
    let cmd = Qail::get("harbors").columns(["id", "name"]).limit(1);

    let iterations = 10_000;
    let mut latencies = Vec::with_capacity(iterations);

    // Warmup
    for _ in 0..100 {
        let _ = driver.fetch_all(&cmd).await?;
    }

    // Benchmark
    for _ in 0..iterations {
        let start = Instant::now();
        let _ = driver.fetch_all(&cmd).await?;
        latencies.push(start.elapsed().as_micros() as u64);
    }

    // Calculate stats
    latencies.sort();
    let total: u64 = latencies.iter().sum();
    let avg = total / iterations as u64;
    let p50 = latencies[iterations / 2];
    let p99 = latencies[iterations * 99 / 100];
    let min = latencies[0];
    let max = latencies[iterations - 1];

    println!("   Queries:    {}", iterations);
    println!("   Total:      {} ms", total / 1000);
    println!("   Avg:        {} µs", avg);
    println!("   P50:        {} µs", p50);
    println!("   P99:        {} µs", p99);
    println!("   Min:        {} µs", min);
    println!("   Max:        {} µs", max);
    println!(
        "   Throughput: {} q/s\n",
        iterations * 1_000_000 / total as usize
    );

    Ok(())
}

/// 1K batch throughput test
async fn bench_1k_batch(driver: &mut PgDriver) -> PgResult<()> {
    println!("📊 1K Batch Throughput (1,000 sequential SELECT queries)");

    let cmd = Qail::get("harbors").columns(["id", "name"]).limit(10);

    let iterations = 1_000;

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = driver.fetch_all(&cmd).await?;
    }
    let elapsed = start.elapsed();

    let qps = iterations * 1000 / elapsed.as_millis() as usize;

    println!("   Queries:    {}", iterations);
    println!("   Total:      {:?}", elapsed);
    println!("   Throughput: {} q/s\n", qps);

    Ok(())
}

/// 1M insert via COPY (requires test table)
#[allow(dead_code)]
async fn bench_1m_copy(driver: &mut PgDriver) -> PgResult<()> {
    println!("📊 1M Insert via COPY Protocol");

    // Would need to create a test table first
    // CREATE TABLE bench_test (id INT, name TEXT, value FLOAT);

    // Generate 1M rows
    let rows: Vec<Vec<qail_core::ast::Value>> = (0..1_000_000)
        .map(|i| {
            vec![
                qail_core::ast::Value::Int(i),
                qail_core::ast::Value::String(format!("row_{}", i)),
                qail_core::ast::Value::Float(i as f64 * 0.001),
            ]
        })
        .collect();

    let cmd = Qail::add("bench_test").columns(["id", "name", "value"]);

    let start = Instant::now();
    let count = driver.copy_bulk(&cmd, &rows).await?;
    let elapsed = start.elapsed();

    let rps = count * 1000 / elapsed.as_millis() as u64;

    println!("   Rows:       {}", count);
    println!("   Total:      {:?}", elapsed);
    println!("   Throughput: {} rows/s\n", rps);

    Ok(())
}
