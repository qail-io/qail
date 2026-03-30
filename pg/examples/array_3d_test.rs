//! 3D Array Isolation Test
//! Tests 3D array BEFORE any NULL byte operations

use qail_core::ast::{Action, Constraint, Expr, Qail, Value};
use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 3D Array Isolation Test");
    println!("{}", "━".repeat(40));

    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;

    let drop_cmd = Qail {
        action: Action::Drop,
        table: "array_shape_test".to_string(),
        ..Default::default()
    };
    let make_cmd = Qail {
        action: Action::Make,
        table: "array_shape_test".to_string(),
        columns: vec![
            Expr::Def {
                name: "id".to_string(),
                data_type: "serial".to_string(),
                constraints: vec![Constraint::PrimaryKey],
            },
            Expr::Def {
                name: "arr2".to_string(),
                data_type: "int[][]".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "arr3".to_string(),
                data_type: "int[][][]".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "arr4".to_string(),
                data_type: "int[][][][]".to_string(),
                constraints: vec![Constraint::Nullable],
            },
        ],
        ..Default::default()
    };
    let _ = driver.execute(&drop_cmd).await;
    driver.execute(&make_cmd).await?;

    let arr2 = Value::Array(vec![
        Value::Array(vec![1.into(), 2.into(), 3.into()]),
        Value::Array(vec![4.into(), 5.into(), 6.into()]),
    ]);
    let arr3 = Value::Array(vec![
        Value::Array(vec![
            Value::Array(vec![1.into(), 2.into()]),
            Value::Array(vec![3.into(), 4.into()]),
        ]),
        Value::Array(vec![
            Value::Array(vec![5.into(), 6.into()]),
            Value::Array(vec![7.into(), 8.into()]),
        ]),
    ]);
    let arr4 = Value::Array(vec![
        Value::Array(vec![
            Value::Array(vec![
                Value::Array(vec![1.into(), 2.into()]),
                Value::Array(vec![3.into(), 4.into()]),
            ]),
            Value::Array(vec![
                Value::Array(vec![5.into(), 6.into()]),
                Value::Array(vec![7.into(), 8.into()]),
            ]),
        ]),
        Value::Array(vec![
            Value::Array(vec![
                Value::Array(vec![9.into(), 10.into()]),
                Value::Array(vec![11.into(), 12.into()]),
            ]),
            Value::Array(vec![
                Value::Array(vec![13.into(), 14.into()]),
                Value::Array(vec![15.into(), 16.into()]),
            ]),
        ]),
    ]);

    println!("  1. Insert 2D/3D/4D arrays via AST payload...");
    let insert = Qail::add("array_shape_test")
        .columns(["arr2", "arr3", "arr4"])
        .values([arr2, arr3, arr4]);
    match driver.execute(&insert).await {
        Ok(_) => println!("    ✓ Insert arrays: Works"),
        Err(e) => println!("    ❌ Insert arrays: {}", e),
    }

    println!("  2. Read array columns back...");
    let select = Qail::get("array_shape_test")
        .columns(["arr2", "arr3", "arr4"])
        .limit(1);
    match driver.fetch_all(&select).await {
        Ok(rows) if !rows.is_empty() => println!("    ✓ Read arrays: Works (rows={})", rows.len()),
        Ok(_) => println!("    ⚠️  Read arrays: no rows returned"),
        Err(e) => println!("    ❌ Read arrays: {}", e),
    }

    let _ = driver.execute(&drop_cmd).await;

    println!();
    println!("3D Array Test Complete.");

    Ok(())
}
