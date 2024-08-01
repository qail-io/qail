//! Pre-encoded COPY benchmark for native Rust
//! 
//! Fair comparison with qail-py pre-encoded benchmark:
//! - Pre-encode all rows to bytes ONCE before timing
//! - Reuse same bytes for all COPY operations
//! - Uses copy_bulk_bytes (same as qail-py)

use qail_core::ast::QailCmd;
use qail_pg::{PgDriver, protocol::encode_copy_batch};
use qail_core::ast::Value;
use std::time::Instant;

const TOTAL_ROWS: usize = 100_000_000;
const ROWS_PER_COPY: usize = 10_000;
const COPIES: usize = TOTAL_ROWS / ROWS_PER_COPY;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 Connecting...");
    
    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "postgres").await?;
    
    println!("✅ Connected");
    
    // Truncate table
    driver.execute_raw("TRUNCATE TABLE _test").await?;
    println!("✅ Table truncated");
    
    println!("\n🚀 NATIVE RUST QAIL-PG PRE-ENCODED COPY BENCHMARK");
    println!("=======================================================");
    println!("Total rows:       {:>15}", TOTAL_ROWS);
    println!("Rows per COPY:    {:>15}", ROWS_PER_COPY);
    println!("COPY operations:  {:>15}", COPIES);
    println!("\n⚠️  Using PRE-ENCODED bytes (same as qail-py)\n");
    
    // Pre-build rows as Values
    println!("✅ Pre-building rows...");
    let test_row = vec![
        Value::Int(10),
        Value::Int(11),
        Value::Int(10),
        Value::String("TESTTESTTEST".to_string()),
        Value::Float(10.333),
        Value::Int(12341234),
        Value::String("123412341234".to_string()),
    ];
    
    let batch_rows: Vec<Vec<Value>> = (0..ROWS_PER_COPY)
        .map(|_| test_row.clone())
        .collect();
    
    // PRE-ENCODE to bytes ONCE (this is what qail-py does)
    println!("✅ Pre-encoding to bytes...");
    let batch_bytes = encode_copy_batch(&batch_rows);
    println!("✅ {} rows pre-encoded ({} bytes)", ROWS_PER_COPY, batch_bytes.len());
    
    // Create QailCmd for COPY
    let cmd = QailCmd::add("_test")
        .columns(&["a", "b", "c", "d", "e", "f", "g"]);
    
    println!("\n📊 Executing COPY operations...\n");
    
    let start = Instant::now();
    let mut total_inserted = 0u64;
    
    for copy_num in 0..COPIES {
        // Use pre-encoded bytes - zero encoding per COPY!
        let count = driver.copy_bulk_bytes(&cmd, &batch_bytes).await?;
        total_inserted += count;
        
        // Progress report
        if (copy_num + 1) % 10 == 0 || copy_num == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let rows_per_sec = total_inserted as f64 / elapsed;
            let copies_per_sec = (copy_num + 1) as f64 / elapsed;
            let remaining_copies = COPIES - (copy_num + 1);
            let eta = remaining_copies as f64 / copies_per_sec;
            
            println!(
                "   {:>8} rows | {:>10.0} rows/s | {:>6.1} copies/s | ETA: {:.0}s | COPY {}/{}",
                total_inserted,
                rows_per_sec,
                copies_per_sec,
                eta,
                copy_num + 1,
                COPIES
            );
        }
    }
    
    let elapsed = start.elapsed().as_secs_f64();
    let rows_per_sec = TOTAL_ROWS as f64 / elapsed;
    let copies_per_sec = COPIES as f64 / elapsed;
    let ns_per_row = (elapsed / TOTAL_ROWS as f64) * 1_000_000_000.0;
    
    println!("\n📈 FINAL RESULTS:");
    println!("┌{}┐", "─".repeat(50));
    println!("│ COPY BULK INSERT (native Rust PRE-ENCODED)       │");
    println!("├{}┤", "─".repeat(50));
    println!("│ Total Time:        {:>20.1}s │", elapsed);
    println!("│ Rows/Second:       {:>20.0} │", rows_per_sec);
    println!("│ Copies/Second:     {:>20.1} │", copies_per_sec);
    println!("│ Per Row:           {:>17.0}ns │", ns_per_row);
    println!("│ Total Inserted:    {:>20} │", total_inserted);
    println!("│ Path: Rust (pre-encoded → TCP) → Postgres        │");
    println!("└{}┘", "─".repeat(50));
    
    Ok(())
}
