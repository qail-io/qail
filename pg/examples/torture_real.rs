//! REAL Type Torture Test: NULLs, Empty, Ragged
//! Tests edge cases that break drivers

use qail_core::ast::{Action, Constraint, Expr, Qail, Value};
use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 REAL Type Torture Test");
    println!("{}", "━".repeat(40));

    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;

    // Setup
    let drop_cmd = Qail {
        action: Action::Drop,
        table: "torture_real".to_string(),
        ..Default::default()
    };
    let make_cmd = Qail {
        action: Action::Make,
        table: "torture_real".to_string(),
        columns: vec![
            Expr::Def {
                name: "id".to_string(),
                data_type: "serial".to_string(),
                constraints: vec![Constraint::PrimaryKey],
            },
            Expr::Def {
                name: "tags".to_string(),
                data_type: "text[]".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "matrix".to_string(),
                data_type: "int[][]".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "cube".to_string(),
                data_type: "int[][][]".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "nulls".to_string(),
                data_type: "int[]".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "empty_arr".to_string(),
                data_type: "text[]".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "payload".to_string(),
                data_type: "jsonb".to_string(),
                constraints: vec![Constraint::Nullable],
            },
        ],
        ..Default::default()
    };
    let _ = driver.execute(&drop_cmd).await;
    driver.execute(&make_cmd).await?;

    // Test 1: Array with NULL
    println!("  1. Array with NULL element...");
    let result = driver
        .execute(
            &Qail::add("torture_real")
                .columns(["nulls"])
                .values([Value::Array(vec![1.into(), Value::Null, 3.into()])]),
        )
        .await;
    match result {
        Ok(_) => println!("    ✓ NULL in array: Accepted"),
        Err(e) => println!("    ❌ NULL in array: {}", e),
    }

    // Test 2: Empty array
    println!("  2. Empty array...");
    let result = driver
        .execute(
            &Qail::add("torture_real")
                .columns(["empty_arr"])
                .values([Value::Array(vec![])]),
        )
        .await;
    match result {
        Ok(_) => println!("    ✓ Empty array: Accepted"),
        Err(e) => println!("    ❌ Empty array: {}", e),
    }

    // Test 3: Ragged array (should be REJECTED by Postgres)
    println!("  3. Ragged array (should fail)...");
    let result = driver
        .execute(
            &Qail::add("torture_real")
                .columns(["matrix"])
                .values([Value::Array(vec![
                    Value::Array(vec![1.into(), 2.into()]),
                    Value::Array(vec![3.into()]),
                ])]),
        )
        .await;
    match result {
        Ok(_) => println!("    ❌ Ragged array: ACCEPTED (driver should reject!)"),
        Err(e) => {
            if e.to_string().contains("multidimensional") || e.to_string().contains("dimension") {
                println!("    ✓ Ragged array: Correctly rejected - {}", e);
            } else {
                println!("    ⚠️ Ragged array: Failed with unexpected error - {}", e);
            }
        }
    }

    // Test 4: String with NULL bytes (should be rejected)
    println!("  4. NULL byte in text (should fail)...");
    let result = driver
        .execute(
            &Qail::add("torture_real")
                .columns(["tags"])
                .values([Value::Array(vec![Value::String(
                    "hello\0world".to_string(),
                )])]),
        )
        .await;
    match result {
        Ok(_) => println!("    ❌ NULL byte: ACCEPTED (should be rejected!)"),
        Err(e) => {
            if e.to_string().contains("0x00") || e.to_string().contains("invalid") {
                println!("    ✓ NULL byte: Correctly rejected");
            } else {
                println!("    ⚠️ NULL byte: Failed with - {}", e);
            }
        }
    }

    // Test 5: 3D array
    println!("  5. 3D array (multidimensional)...");
    let result = driver
        .execute(
            &Qail::add("torture_real")
                .columns(["cube"])
                .values([Value::Array(vec![
                    Value::Array(vec![
                        Value::Array(vec![1.into(), 2.into()]),
                        Value::Array(vec![3.into(), 4.into()]),
                    ]),
                    Value::Array(vec![
                        Value::Array(vec![5.into(), 6.into()]),
                        Value::Array(vec![7.into(), 8.into()]),
                    ]),
                ])]),
        )
        .await;
    match result {
        Ok(_) => println!("    ✓ 3D array: Works"),
        Err(e) => println!("    ❌ 3D array: {}", e),
    }

    // Test 6: JSONB with NULL
    println!("  6. JSONB with null value...");
    let result = driver
        .execute(
            &Qail::add("torture_real")
                .columns(["payload"])
                .values([Value::Json("{\"key\": null}".to_string())]),
        )
        .await;
    match result {
        Ok(_) => println!("    ✓ JSONB null: Works"),
        Err(e) => println!("    ❌ JSONB null: {}", e),
    }

    let _ = driver.execute(&drop_cmd).await;

    println!();
    println!("Type Torture Analysis Complete.");

    Ok(())
}
