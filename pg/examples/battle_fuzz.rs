//! BATTLE TEST #7: Type Fuzzing (Panic Safety)
//!
//! Purpose: Ensure the driver handles invalid data conversions safely (no panics).
//! Fail Condition: Process crash (Panic).
//!
//! Run: cargo run --release -p qail-pg --example battle_fuzz

use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #7: The Type Fuzzer 🎭                       ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let mut driver =
        PgDriver::connect_with_password("localhost", 5432, "postgres", "postgres", "postgres")
            .await?;

    println!("1️⃣  Testing Integer Overflow...");
    // 9,999,999,999 fits in i64 but not i32
    let rows = driver.fetch_raw("SELECT 9999999999::bigint").await?;
    let val_opt = rows[0].get_i32(0); // Should fail to parse and return None
    match val_opt {
        None => println!("   ✅ PASS: Caught overflow safely (returned None)."),
        Some(v) => {
            println!("   ❌ FAIL: Should have failed! Got: {}", v);
            return Err("Test failed: Overflow".into());
        }
    }

    println!("2️⃣  Testing NULL into primitive...");
    // SELECT NULL
    let rows = driver.fetch_raw("SELECT NULL::int").await?;
    let val_opt = rows[0].get_i32(0); // Should see NULL and return None
    match val_opt {
        None => println!("   ✅ PASS: Caught NULL safely (returned None)."),
        Some(v) => {
            println!("   ❌ FAIL: Created value from NULL! Got: {}", v);
            return Err("Test failed: Null unsafe".into());
        }
    }

    println!("3️⃣  Testing Invalid UTF-8 / Bytea...");
    // SELECT E'\\xFF'::bytea
    // Since we use Simple Query Protocol (Text Format), Postgres sends bytea as hex string "\xff".
    // This IS valid UTF-8.
    // We verify that:
    // 1. It does NOT panic.
    // 2. It returns the hex string (safe) OR None (if we were forcing binary).
    let rows = driver.fetch_raw("SELECT E'\\\\xFF'::bytea").await?;
    let val_opt = rows[0].get_string(0);
    match val_opt {
        None => println!("   ✅ PASS: Returned None (Strict parsing)."),
        Some(v) => {
            if v == "\\xff" {
                println!(
                    "   ✅ PASS: Received valid hex string \"\\xff\" (Standard Postgres Text Format). No Panic."
                );
            } else {
                println!("   ⚠️  PASS? Received string: {:?}. Is this expected?", v);
            }
        }
    }

    println!("\n   ✅ ALL FUZZ TESTS PASSED. NO PANICS.");

    Ok(())
}
