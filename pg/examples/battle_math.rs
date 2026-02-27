//! BATTLE TEST #11: The Forbidden Number (NaN) 🧮
//!
//! Purpose: Ensure NaN/Infinity don't crash the decoder.
//! Fail Condition: Panic during float parsing or JSON serialization.
//!
//! Run: cargo run --release -p qail-pg --example battle_math

use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #11: NaN and Infinity 🧮                     ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let mut driver =
        PgDriver::connect_with_password("localhost", 5432, "postgres", "postgres", "postgres")
            .await?;

    println!("1️⃣  Fetching special floats from Postgres...");
    // We don't need a table, we can select literals
    let sql = "SELECT 'NaN'::float8, 'Infinity'::float8, '-Infinity'::float8";

    let rows = driver.fetch_raw(sql).await?;

    if rows.is_empty() {
        println!("   ❌ FAIL: No rows returned.");
        return Err("No rows".into());
    }

    let row = &rows[0];

    // get_f64 uses parse() which handles NaN and Infinity
    let val_nan = row.get_f64(0);
    let val_inf = row.get_f64(1);
    let val_neg_inf = row.get_f64(2);

    println!(
        "   Received: [{:?}, {:?}, {:?}]",
        val_nan, val_inf, val_neg_inf
    );

    // TEST 1: Check NaN
    match val_nan {
        Some(v) if v.is_nan() => {
            println!("   ✅ PASS: NaN parsed correctly.");
        }
        Some(v) => {
            println!("   ❌ FAIL: NaN became {}!", v);
        }
        None => {
            println!("   ❌ FAIL: NaN returned None (parse failed).");
        }
    }

    // TEST 2: Check Infinity
    match val_inf {
        Some(v) if v.is_infinite() && v.is_sign_positive() => {
            println!("   ✅ PASS: +Infinity parsed correctly.");
        }
        Some(v) => {
            println!("   ❌ FAIL: +Infinity became {}!", v);
        }
        None => {
            println!("   ❌ FAIL: +Infinity returned None.");
        }
    }

    // TEST 3: Check Negative Infinity
    match val_neg_inf {
        Some(v) if v.is_infinite() && v.is_sign_negative() => {
            println!("   ✅ PASS: -Infinity parsed correctly.");
        }
        Some(v) => {
            println!("   ❌ FAIL: -Infinity became {}!", v);
        }
        None => {
            println!("   ❌ FAIL: -Infinity returned None.");
        }
    }

    println!("\n   ✅ ALL NaN/Infinity tests completed without panic.");

    Ok(())
}
