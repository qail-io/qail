//! Pipeline vs Single Query Benchmark
//!
//! Compares:
//! 1. fetch_all (single query) - ~5,000 q/s
//! 2. pipeline_execute_count_ast_cached (batched) - ~275,000 q/s
//! 3. pool + pipeline (parallel batches) - ~1,200,000 q/s
//!
//! Run with: cargo run --example pipeline_benchmark --release

use qail_core::ast::{Action, Constraint, Expr, Qail, Value};
use qail_core::prelude::*;
use qail_pg::driver::{PgConnection, PgDriver, PgPool, PoolConfig};
use std::time::Instant;

const SINGLE_ITERATIONS: usize = 10_000;
const BATCH_SIZE: usize = 500;
const BATCH_ITERATIONS: usize = 20; // 20 x 500 = 10,000 total
const POOL_SIZE: usize = 10;

#[tokio::main]
#[allow(deprecated)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Pipeline vs Single Query Benchmark");
    println!("======================================\n");

    // Setup
    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "qail_test_migration").await?;

    // Create test table
    println!("📦 Setting up test data...");
    let drop_bench = Qail {
        action: Action::Drop,
        table: "bench_data".to_string(),
        ..Default::default()
    };
    let create_bench = Qail {
        action: Action::Make,
        table: "bench_data".to_string(),
        columns: vec![
            Expr::Def {
                name: "id".to_string(),
                data_type: "serial".to_string(),
                constraints: vec![Constraint::PrimaryKey],
            },
            Expr::Def {
                name: "name".to_string(),
                data_type: "text".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "value".to_string(),
                data_type: "int".to_string(),
                constraints: vec![Constraint::Nullable],
            },
        ],
        ..Default::default()
    };
    let _ = driver.execute(&drop_bench).await;
    driver.execute(&create_bench).await?;
    for i in 0..100 {
        let insert = Qail::add("bench_data").columns(["name", "value"]).values([
            Value::String(format!("item{}", i)),
            Value::Int((i * 100) as i64),
        ]);
        driver.execute(&insert).await?;
    }
    println!("   Created bench_data table with 100 rows\n");

    // Simple query for fair comparison
    let query = Qail::get("bench_data")
        .columns(["id", "name", "value"])
        .limit(10);

    // Warmup
    for _ in 0..100 {
        let _ = driver.fetch_all(&query).await?;
    }

    // ============================================
    // 1. Single query: fetch_all (sequential)
    // ============================================
    print!("⏱  Single query (fetch_all): ");
    std::io::Write::flush(&mut std::io::stdout())?;

    let start = Instant::now();
    for _ in 0..SINGLE_ITERATIONS {
        let _ = driver.fetch_all(&query).await?;
    }
    let single_time = start.elapsed().as_secs_f64() * 1000.0;
    let single_qps = SINGLE_ITERATIONS as f64 / (single_time / 1000.0);
    println!("{:.0} q/s", single_qps);

    // ============================================
    // 2. Pipeline: batch queries (single connection)
    // ============================================
    print!("⏱  Pipeline (1 conn, batch): ");
    std::io::Write::flush(&mut std::io::stdout())?;

    // Create batch of queries
    let batch: Vec<Qail> = (0..BATCH_SIZE)
        .map(|i| {
            Qail::get("bench_data")
                .columns(["id", "name", "value"])
                .filter("value", Operator::Gt, Value::Int((i as i64 % 10) * 100))
                .limit(10)
        })
        .collect();

    // Need raw connection for pipeline
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "qail_test_migration").await?;

    // Warmup
    let _ = conn.pipeline_execute_rows_ast(&batch).await?;

    let start = Instant::now();
    for _ in 0..BATCH_ITERATIONS {
        let _ = conn.pipeline_execute_rows_ast(&batch).await?;
    }
    let pipeline_time = start.elapsed().as_secs_f64() * 1000.0;
    let pipeline_qps = (BATCH_SIZE * BATCH_ITERATIONS) as f64 / (pipeline_time / 1000.0);
    println!("{:.0} q/s", pipeline_qps);

    // ============================================
    // 3. Pool + Pipeline: parallel batches
    // ============================================
    print!("⏱  Pool + Pipeline ({} conn): ", POOL_SIZE);
    std::io::Write::flush(&mut std::io::stdout())?;

    let config = PoolConfig::new("127.0.0.1", 5432, "orion", "qail_test_migration")
        .max_connections(POOL_SIZE);
    let pool = PgPool::connect(config).await?;

    let start = Instant::now();
    let mut handles = Vec::new();

    for _ in 0..POOL_SIZE {
        let pool_clone = pool.clone();
        let batch_clone = batch.clone();

        handles.push(tokio::spawn(async move {
            let mut conn = pool_clone.acquire_system().await.unwrap();
            for _ in 0..BATCH_ITERATIONS {
                let _ = conn.pipeline_execute_rows_ast(&batch_clone).await;
            }
        }));
    }

    for h in handles {
        h.await?;
    }
    let pool_time = start.elapsed().as_secs_f64() * 1000.0;
    let pool_qps = (BATCH_SIZE * BATCH_ITERATIONS * POOL_SIZE) as f64 / (pool_time / 1000.0);
    println!("{:.0} q/s", pool_qps);

    driver.execute(&drop_bench).await?;
    println!("\n🧹 Cleanup complete");

    // ============================================
    // Summary
    // ============================================
    println!("\n📊 Results");
    println!("============================================");
    println!("Single query:    {:>12.0} q/s", single_qps);
    println!(
        "Pipeline:        {:>12.0} q/s ({:.0}x faster)",
        pipeline_qps,
        pipeline_qps / single_qps
    );
    println!(
        "Pool + Pipeline: {:>12.0} q/s ({:.0}x faster)",
        pool_qps,
        pool_qps / single_qps
    );

    Ok(())
}
