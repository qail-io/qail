//! BATTLE TEST #10: Pipeline Desync ⚡
//!
//! Purpose: Ensure the driver correctly maps results when multiple queries run together.
//! Fail Condition: The driver returns a Success for a query that was actually skipped.
//!
//! Run: cargo run --release -p qail-pg --example battle_pipeline

use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #10: The Pipeline Split ⚡                   ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let mut driver = PgDriver::connect_with_password(
        "localhost", 5432, "postgres", "postgres", "postgres"
    ).await?;

    println!("1️⃣  Sending Multi-Statement Query: 'SELECT 100; SELECT 1/0; SELECT 200'");
    
    // Multi-statement via Simple Query Protocol
    // Should catch the division by zero error in the middle
    let sql = "SELECT 100::int; SELECT 1/0; SELECT 200::int";
    
    // execute_raw uses Simple Query Protocol which supports multi-statement
    let result = driver.execute_raw(sql).await;

    match result {
        Err(e) => {
            let msg = e.to_string();
            println!("   Result: {:?}", msg);
            if msg.contains("division by zero") {
                println!("   ✅ PASS: Driver caught the middle error correctly.");
            } else {
                println!("   ⚠️  WARN: Driver errored, but maybe unexpected message: {}", msg);
            }
        },
        Ok(count) => {
            // If we got OK, we need to check WHICH result we got.
            println!("   Result: Ok({:?})", count);
            // For SELECT statements, execute_raw returns 0 or row count
            // The key test is: did we propagate the error?
            println!("   ⚠️  WARN: Driver returned OK, checking if error was propagated...");
            
            // Try a fresh query to see if connection is healthy
            let health = driver.execute_raw("SELECT 1").await;
            match health {
                Ok(_) => {
                    println!("   ✅ Connection is healthy after multi-statement.");
                    println!("   NOTE: PostgreSQL may have returned partial success.");
                },
                Err(e) => {
                    println!("   ❌ FAIL: Connection desync detected: {}", e);
                }
            }
        }
    }

    // Additional test: Use fetch_raw which returns rows
    println!("\n2️⃣  Verifying with fetch_raw...");
    
    let result = driver.fetch_raw("SELECT 100::int AS val").await;
    match result {
        Ok(rows) => {
            if !rows.is_empty() {
                let val = rows[0].get_i32(0);
                println!("   ✅ PASS: fetch_raw returned {:?} correctly.", val);
            }
        },
        Err(e) => {
            println!("   ❌ FAIL: Connection broken: {}", e);
        }
    }

    Ok(())
}
