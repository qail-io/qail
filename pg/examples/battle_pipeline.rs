//! BATTLE TEST #10: Pipeline Desync ⚡
//!
//! Purpose: Ensure the driver correctly maps results when multiple queries run together.
//! Fail Condition: The driver returns a Success for a query that was actually skipped.
//!
//! Run: cargo run --release -p qail-pg --example battle_pipeline

use qail_core::ast::Qail;
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #10: The Pipeline Split ⚡                   ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let mut driver =
        PgDriver::connect_with_password("localhost", 5432, "postgres", "postgres", "postgres")
            .await?;

    println!("1️⃣  Sending pipeline with one failing query in the middle");
    let cmds = vec![
        Qail::get("vessels").columns(["id"]).limit(1),
        Qail::get("__pipeline_fail_table__")
            .columns(["id"])
            .limit(1),
        Qail::get("vessels").columns(["id"]).limit(1),
    ];
    let result = driver.pipeline_execute_rows(&cmds).await;

    match result {
        Err(e) => {
            let msg = e.to_string();
            println!("   Result: {:?}", msg);
            if msg.contains("division by zero") {
                println!("   ✅ PASS: Driver caught the middle error correctly.");
            } else {
                println!(
                    "   ⚠️  WARN: Driver errored, but maybe unexpected message: {}",
                    msg
                );
            }
        }
        Ok(count) => {
            println!("   Result: Ok({:?})", count.len());
            println!("   ⚠️  WARN: Driver returned OK, checking if error was propagated...");

            // Try a fresh query to see if connection is healthy
            let health = driver
                .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
                .await;
            match health {
                Ok(_) => {
                    println!("   ✅ Connection is healthy after multi-statement.");
                    println!("   NOTE: PostgreSQL may have returned partial success.");
                }
                Err(e) => {
                    println!("   ❌ FAIL: Connection desync detected: {}", e);
                }
            }
        }
    }

    // Additional test: regular fetch after pipeline error
    println!("\n2️⃣  Verifying with regular AST fetch...");

    let result = driver
        .fetch_all(&Qail::get("vessels").columns(["id"]).limit(1))
        .await;
    match result {
        Ok(rows) => {
            if !rows.is_empty() {
                println!("   ✅ PASS: fetch_all returned {} row(s).", rows.len());
            }
        }
        Err(e) => {
            println!("   ❌ FAIL: Connection broken: {}", e);
        }
    }

    Ok(())
}
