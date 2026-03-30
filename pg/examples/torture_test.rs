//! Type Torture Test: Arrays, JSONB, Unicode
//! Tests complex Postgres types for buffer alignment and parsing

use qail_core::ast::{Action, Constraint, Expr, Qail, SortOrder, Value};
use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Test 5: Type Torture Chamber");
    println!("{}", "━".repeat(40));

    // Connect
    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;

    // Test data (no NULL bytes - Postgres doesn't allow them in UTF8)
    let jsonb_payload = r#"{"key": "value", "nested": [1, 2, 3], "unicode": "🚀"}"#;
    let weird_text = "Emoji 🚀 and ZWJ 👨‍👩‍👧‍👦 sequences, Chinese: 中文, Arabic: مرحبا, tab\ttoo";
    let drop_cmd = Qail {
        action: Action::Drop,
        table: "torture_chamber".to_string(),
        ..Default::default()
    };
    let create_cmd = Qail {
        action: Action::Make,
        table: "torture_chamber".to_string(),
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
                name: "payload".to_string(),
                data_type: "jsonb".to_string(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "weird_text".to_string(),
                data_type: "text".to_string(),
                constraints: vec![Constraint::Nullable],
            },
        ],
        ..Default::default()
    };
    let _ = driver.execute(&drop_cmd).await;
    driver.execute(&create_cmd).await?;

    let tags = Value::Array(vec![
        Value::String("rust".to_string()),
        Value::String("driver".to_string()),
        Value::String("torture".to_string()),
        Value::String("emoji: 🦀".to_string()),
    ]);
    let matrix = Value::Array(vec![
        Value::Array(vec![1.into(), 2.into(), 3.into()]),
        Value::Array(vec![4.into(), 5.into(), 6.into()]),
    ]);

    println!("  Inserting complex types...");
    let insert = Qail::add("torture_chamber")
        .columns(["tags", "matrix", "payload", "weird_text"])
        .values([
            tags,
            matrix,
            Value::Json(jsonb_payload.to_string()),
            weird_text.into(),
        ]);
    driver.execute(&insert).await?;
    println!("    ✓ Insert succeeded");

    // Fetch and verify
    println!("  Fetching and verifying...");
    let rows = driver
        .fetch_all(
            &Qail::get("torture_chamber")
                .columns(["id", "tags", "matrix", "payload", "weird_text"])
                .order_by("id", SortOrder::Desc)
                .limit(1),
        )
        .await?;
    if rows.is_empty() {
        return Err("No rows returned from torture_chamber".into());
    }
    println!("    ✓ Select succeeded");

    println!("  Validating decoded payload...");
    let row = &rows[0];
    let payload_text = row.get_json(3).unwrap_or_default();
    let weird_text_out = row.get_string(4).unwrap_or_default();
    if payload_text.is_empty() || weird_text_out.is_empty() {
        return Err("Decoded values unexpectedly empty".into());
    }
    println!("    ✓ JSON/text decode works");

    let _ = driver.execute(&drop_cmd).await;

    println!();
    println!("✓ Type Torture Test PASSED!");

    Ok(())
}
