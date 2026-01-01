//! 100 MILLION ROW COPY BENCHMARK
//!
//! Tests COPY protocol performance for bulk inserts.
//! Target: 1.36M rows/second
//!
//! Run: cargo run --example copy_benchmark --release

use qail_core::prelude::*;
use qail_pg::driver::PgDriver;
use std::time::Instant;

const TOTAL_ROWS: usize = 100_000_000;
const ROWS_PER_BATCH: usize = 10_000;
const BATCHES: usize = TOTAL_ROWS / ROWS_PER_BATCH;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 100 MILLION ROW COPY BENCHMARK");
    println!("==================================");
    println!("Total rows:       {:>15}", TOTAL_ROWS);
    println!("Batch size:       {:>15}", ROWS_PER_BATCH);
    println!("Batches:          {:>15}", BATCHES);
    println!();

    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "sailtix_test").await?;

    // Create test table
    println!("📦 Setting up test table...");
    driver.execute_raw("DROP TABLE IF EXISTS copy_bench").await.ok();
    driver.execute_raw("CREATE TABLE copy_bench (id INT, name TEXT, value FLOAT)").await?;
    println!("   Created copy_bench table\n");

    // Build COPY command
    let cmd = Qail::add("copy_bench").columns(["id", "name", "value"]);

    // Pre-generate one batch of rows (reused)
    println!("📦 Building batch ({} rows)...", ROWS_PER_BATCH);
    let batch_start = Instant::now();
    let batch: Vec<Vec<Value>> = (0..ROWS_PER_BATCH)
        .map(|i| {
            vec![
                Value::Int(i as i64),
                Value::String(format!("row_{}", i)),
                Value::Float(i as f64 * 0.001),
            ]
        })
        .collect();
    println!("   Done in {:.2}s\n", batch_start.elapsed().as_secs_f64());

    println!("📊 Executing {} COPY operations ({} rows each)...\n", BATCHES, ROWS_PER_BATCH);

    let start = Instant::now();
    let mut total_rows_inserted: u64 = 0;
    let mut last_report = Instant::now();

    for batch_num in 0..BATCHES {
        // Execute COPY batch
        let count = driver.copy_bulk(&cmd, &batch).await?;
        total_rows_inserted += count;

        // Progress report every 10M rows
        if total_rows_inserted % 10_000_000 == 0 || last_report.elapsed().as_secs() >= 5 {
            let elapsed = start.elapsed();
            let rps = total_rows_inserted as f64 / elapsed.as_secs_f64();
            let remaining = TOTAL_ROWS as u64 - total_rows_inserted;
            let eta = remaining as f64 / rps;

            println!(
                "   {:>3}M rows | {:>10.0} rows/s | ETA: {:.0}s | Batch {}/{}",
                total_rows_inserted / 1_000_000,
                rps,
                eta,
                batch_num + 1,
                BATCHES
            );
            last_report = Instant::now();
        }
    }

    let elapsed = start.elapsed();
    let rps = total_rows_inserted as f64 / elapsed.as_secs_f64();
    let per_row_ns = elapsed.as_nanos() / total_rows_inserted as u128;

    println!("\n📈 FINAL RESULTS:");
    println!("┌────────────────────────────────────────────┐");
    println!("│ 100 MILLION ROW COPY BENCHMARK             │");
    println!("├────────────────────────────────────────────┤");
    println!("│ Total Time:          {:>20.1}s │", elapsed.as_secs_f64());
    println!("│ Rows/Second:         {:>20.0} │", rps);
    println!("│ Per Row:             {:>17}ns │", per_row_ns);
    println!("│ Rows Inserted:       {:>20} │", total_rows_inserted);
    println!("│ Batches:             {:>20} │", BATCHES);
    println!("└────────────────────────────────────────────┘");

    if rps > 1_000_000.0 {
        println!("\n🏆 OVER 1 MILLION ROWS/SECOND!");
    }

    println!("\n🧹 Cleaning up...");
    driver.execute_raw("DROP TABLE copy_bench").await?;
    println!("   Done!");

    Ok(())
}
