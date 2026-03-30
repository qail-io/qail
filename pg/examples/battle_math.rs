//! BATTLE TEST #11: The Forbidden Number (NaN) 🧮
//!
//! Purpose: Ensure NaN/Infinity don't crash the decoder.
//! Fail Condition: Panic during float parsing or JSON serialization.
//!
//! Run: cargo run --release -p qail-pg --example battle_math

use qail_core::ast::{Action, Constraint, Expr, Qail, Value};
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #11: NaN and Infinity 🧮                     ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    let mut driver =
        PgDriver::connect_with_password("localhost", 5432, "postgres", "postgres", "postgres")
            .await?;

    let drop_cmd = Qail {
        action: Action::Drop,
        table: "battle_math_values".to_string(),
        ..Default::default()
    };
    let create_cmd = Qail {
        action: Action::Make,
        table: "battle_math_values".to_string(),
        columns: vec![
            Expr::Def {
                name: "id".to_string(),
                data_type: "serial".to_string(),
                constraints: vec![Constraint::PrimaryKey],
            },
            Expr::Def {
                name: "nan_val".to_string(),
                data_type: "float8".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "inf_val".to_string(),
                data_type: "float8".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "neg_inf_val".to_string(),
                data_type: "float8".to_string(),
                constraints: vec![Constraint::Nullable],
            },
        ],
        ..Default::default()
    };
    let _ = driver.execute(&drop_cmd).await;
    driver.execute(&create_cmd).await?;

    let insert = Qail::add("battle_math_values")
        .columns(["nan_val", "inf_val", "neg_inf_val"])
        .values([
            Value::Float(f64::NAN),
            Value::Float(f64::INFINITY),
            Value::Float(f64::NEG_INFINITY),
        ]);
    driver.execute(&insert).await?;

    println!("1️⃣  Fetching special floats from Postgres...");
    let rows = driver
        .fetch_all(
            &Qail::get("battle_math_values")
                .columns(["nan_val", "inf_val", "neg_inf_val"])
                .limit(1),
        )
        .await?;

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
    let _ = driver.execute(&drop_cmd).await;

    Ok(())
}
