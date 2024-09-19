//! REAL Type Torture Test: NULLs, Empty, Ragged
//! Tests edge cases that break drivers

use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 REAL Type Torture Test");
    println!("{}", "━".repeat(40));
    
    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;
    
    // Setup
    driver.execute_raw("DROP TABLE IF EXISTS torture_real").await?;
    driver.execute_raw("CREATE TABLE torture_real (
        id SERIAL PRIMARY KEY,
        tags TEXT[],
        matrix INT[][],
        nulls INT[],
        empty_arr TEXT[]
    )").await?;
    
    // Test 1: Array with NULL
    println!("  1. Array with NULL element...");
    let result = driver.execute_raw("INSERT INTO torture_real (nulls) VALUES (ARRAY[1, NULL, 3])").await;
    match result {
        Ok(_) => println!("    ✓ NULL in array: Accepted"),
        Err(e) => println!("    ❌ NULL in array: {}", e),
    }
    
    // Test 2: Empty array
    println!("  2. Empty array...");
    let result = driver.execute_raw("INSERT INTO torture_real (empty_arr) VALUES (ARRAY[]::TEXT[])").await;
    match result {
        Ok(_) => println!("    ✓ Empty array: Accepted"),
        Err(e) => println!("    ❌ Empty array: {}", e),
    }
    
    // Test 3: Ragged array (should be REJECTED by Postgres)
    println!("  3. Ragged array (should fail)...");
    let result = driver.execute_raw("INSERT INTO torture_real (matrix) VALUES (ARRAY[[1,2], [3]])").await;
    match result {
        Ok(_) => println!("    ❌ Ragged array: ACCEPTED (driver should reject!)"),
        Err(e) => {
            if e.to_string().contains("multidimensional") || e.to_string().contains("dimension") {
                println!("    ✓ Ragged array: Correctly rejected - {}", e);
            } else {
                println!("    ⚠️ Ragged array: Failed with unexpected error - {}", e);
            }
        },
    }
    
    // Test 4: String with NULL bytes (should be rejected)
    println!("  4. NULL byte in text (should fail)...");
    let result = driver.execute_raw("INSERT INTO torture_real (tags) VALUES (ARRAY[E'hello\\x00world'])").await;
    match result {
        Ok(_) => println!("    ❌ NULL byte: ACCEPTED (should be rejected!)"),
        Err(e) => {
            if e.to_string().contains("0x00") || e.to_string().contains("invalid") {
                println!("    ✓ NULL byte: Correctly rejected");
            } else {
                println!("    ⚠️ NULL byte: Failed with - {}", e);
            }
        },
    }
    
    // Test 5: 3D array
    println!("  5. 3D array (multidimensional)...");
    let result = driver.execute_raw("SELECT ARRAY[[[1,2],[3,4]],[[5,6],[7,8]]]::INT[][][]").await;
    match result {
        Ok(_) => println!("    ✓ 3D array: Works"),
        Err(e) => println!("    ❌ 3D array: {}", e),
    }
    
    // Test 6: JSONB with NULL
    println!("  6. JSONB with null value...");
    let result = driver.execute_raw("SELECT '{\"key\": null}'::JSONB").await;
    match result {
        Ok(_) => println!("    ✓ JSONB null: Works"),
        Err(e) => println!("    ❌ JSONB null: {}", e),
    }
    
    println!();
    println!("Type Torture Analysis Complete.");
    
    Ok(())
}
