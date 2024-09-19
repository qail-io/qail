//! Type Torture Test: Arrays, JSONB, Unicode
//! Tests complex Postgres types for buffer alignment and parsing

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
    let array_literal = "ARRAY['rust', 'driver', 'torture', 'emoji: 🦀']";
    let matrix_literal = "ARRAY[[1,2,3],[4,5,6]]";
    
    // Insert using raw SQL with array literals
    let insert_sql = format!(
        "INSERT INTO torture_chamber (tags, matrix, payload, weird_text) 
         VALUES ({}, {}, '{}', '{}')",
        array_literal,
        matrix_literal,
        jsonb_payload.replace('\'', "''"),
        weird_text.replace('\'', "''")
    );
    
    println!("  Inserting complex types...");
    driver.execute_raw(&insert_sql).await?;
    println!("    ✓ Insert succeeded");
    
    // Fetch and verify
    println!("  Fetching and verifying...");
    let verify_sql = "SELECT id, tags, matrix, payload, weird_text FROM torture_chamber WHERE id = (SELECT MAX(id) FROM torture_chamber)";
    driver.execute_raw(verify_sql).await?;
    println!("    ✓ Select succeeded");
    
    // Test JSONB operators
    println!("  Testing JSONB operators...");
    let jsonb_sql = "SELECT payload->>'key', payload->'nested'->0 FROM torture_chamber";
    driver.execute_raw(jsonb_sql).await?;
    println!("    ✓ JSONB operators work");
    
    // Test array operators
    println!("  Testing array operators...");
    let array_sql = "SELECT tags[1], array_length(matrix, 1), matrix[1][2] FROM torture_chamber";
    driver.execute_raw(array_sql).await?;
    println!("    ✓ Array operators work");
    
    // Test array aggregation
    println!("  Testing array aggregation...");
    let agg_sql = "SELECT array_agg(weird_text) FROM torture_chamber";
    driver.execute_raw(agg_sql).await?;
    println!("    ✓ Array aggregation works");
    
    println!();
    println!("✓ Type Torture Test PASSED!");
    
    Ok(())
}
