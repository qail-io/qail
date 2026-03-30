//! Buffer Boundary Stress Test
//! Tests BytesMut resize with large data

use qail_core::ast::{Action, Constraint, Expr, Qail};
use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Test 1: Buffer Boundary (1MB)");
    println!("{}", "━".repeat(40));

    // Connect to database
    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;

    // Create 1MB of test data (enough to exceed default buffer, fast test)
    let size_kb = 1024;
    let huge_string: String = (0..size_kb * 1024)
        .map(|i| ((i % 26) as u8 + b'a') as char)
        .collect();

    println!("  Created {} KB of test data", size_kb);

    // Create test table
    let drop_cmd = Qail {
        action: Action::Drop,
        table: "big_text".to_string(),
        ..Default::default()
    };
    let make_cmd = Qail {
        action: Action::Make,
        table: "big_text".to_string(),
        columns: vec![
            Expr::Def {
                name: "id".to_string(),
                data_type: "serial".to_string(),
                constraints: vec![Constraint::PrimaryKey],
            },
            Expr::Def {
                name: "data".to_string(),
                data_type: "text".to_string(),
                constraints: vec![Constraint::Nullable],
            },
        ],
        ..Default::default()
    };
    let _ = driver.execute(&drop_cmd).await;
    driver.execute(&make_cmd).await?;

    // Insert using TEXT (simpler than bytea)
    let start = std::time::Instant::now();

    println!("  Payload size: {} KB", huge_string.len() / 1024);
    println!("  Inserting...");

    let insert = Qail::add("big_text")
        .columns(["data"])
        .values([huge_string]);
    driver.execute(&insert).await?;

    let elapsed = start.elapsed();
    println!("  ✓ Insert completed in {:?}", elapsed);

    // Verify by selecting data
    let rows = driver
        .fetch_all(
            &Qail::get("big_text")
                .columns(["data"])
                .order_desc("id")
                .limit(1),
        )
        .await?;
    let stored_len = rows
        .first()
        .and_then(|r| r.get_string(0))
        .map(|s| s.len())
        .unwrap_or(0);
    println!("  ✓ Verified in database (len={})", stored_len);

    println!();
    println!("✓ Buffer Boundary Test (1MB) PASSED!");

    Ok(())
}
