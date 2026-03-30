//! BATTLE TEST #7: Type Fuzzing (Panic Safety)
//!
//! Purpose: Ensure the driver handles invalid data conversions safely (no panics).
//! Fail Condition: Process crash (Panic).
//!
//! Run: cargo run --release -p qail-pg --example battle_fuzz

use qail_core::ast::{Action, Constraint, Expr, Qail, Value};
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #7: The Type Fuzzer 🎭                       ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let mut driver =
        PgDriver::connect_with_password("localhost", 5432, "postgres", "postgres", "postgres")
            .await?;

    let drop_cmd = Qail {
        action: Action::Drop,
        table: "battle_fuzz_types".to_string(),
        ..Default::default()
    };
    let create_cmd = Qail {
        action: Action::Make,
        table: "battle_fuzz_types".to_string(),
        columns: vec![
            Expr::Def {
                name: "id".to_string(),
                data_type: "serial".to_string(),
                constraints: vec![Constraint::PrimaryKey],
            },
            Expr::Def {
                name: "big_val".to_string(),
                data_type: "bigint".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "maybe_int".to_string(),
                data_type: "int".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "bytea_text".to_string(),
                data_type: "text".to_string(),
                constraints: vec![Constraint::Nullable],
            },
        ],
        ..Default::default()
    };
    let _ = driver.execute(&drop_cmd).await;
    driver.execute(&create_cmd).await?;

    let insert = Qail::add("battle_fuzz_types")
        .columns(["big_val", "maybe_int", "bytea_text"])
        .values([
            Value::Int(9_999_999_999),
            Value::Null,
            Value::String("\\xff".to_string()),
        ]);
    driver.execute(&insert).await?;

    let row = driver
        .fetch_all(
            &Qail::get("battle_fuzz_types")
                .columns(["big_val", "maybe_int", "bytea_text"])
                .limit(1),
        )
        .await?;

    if row.is_empty() {
        return Err("No fuzz row returned".into());
    }
    let row = &row[0];

    println!("1️⃣  Testing Integer Overflow...");
    let val_opt = row.get_i32(0); // Should fail to parse and return None
    match val_opt {
        None => println!("   ✅ PASS: Caught overflow safely (returned None)."),
        Some(v) => {
            println!("   ❌ FAIL: Should have failed! Got: {}", v);
            return Err("Test failed: Overflow".into());
        }
    }

    println!("2️⃣  Testing NULL into primitive...");
    let val_opt = row.get_i32(1); // Should see NULL and return None
    match val_opt {
        None => println!("   ✅ PASS: Caught NULL safely (returned None)."),
        Some(v) => {
            println!("   ❌ FAIL: Created value from NULL! Got: {}", v);
            return Err("Test failed: Null unsafe".into());
        }
    }

    println!("3️⃣  Testing Invalid UTF-8 / Bytea...");
    let val_opt = row.get_string(2);
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

    let _ = driver.execute(&drop_cmd).await;

    println!("\n   ✅ ALL FUZZ TESTS PASSED. NO PANICS.");

    Ok(())
}
