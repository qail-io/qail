//! 3D Array Isolation Test
//! Tests 3D array BEFORE any NULL byte operations

use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 3D Array Isolation Test");
    println!("{}", "━".repeat(40));
    
    let mut driver = PgDriver::connect("localhost", 5432, "orion", "postgres").await?;
    
    // Test 3D array FIRST (clean connection)
    println!("  1. 3D array (clean connection)...");
    let result = driver.execute_raw("SELECT ARRAY[[[1,2],[3,4]],[[5,6],[7,8]]]::INT[][][]").await;
    match result {
        Ok(_) => println!("    ✓ 3D array: Works!"),
        Err(e) => println!("    ❌ 3D array: {}", e),
    }
    
    // Test 2D array for comparison
    println!("  2. 2D array...");
    let result = driver.execute_raw("SELECT ARRAY[[1,2,3],[4,5,6]]::INT[][]").await;
    match result {
        Ok(_) => println!("    ✓ 2D array: Works"),
        Err(e) => println!("    ❌ 2D array: {}", e),
    }
    
    // Test 4D array (extreme case)
    println!("  3. 4D array (extreme)...");
    let result = driver.execute_raw("SELECT ARRAY[[[[1,2],[3,4]],[[5,6],[7,8]]],[[[9,10],[11,12]],[[13,14],[15,16]]]]::INT[][][][]").await;
    match result {
        Ok(_) => println!("    ✓ 4D array: Works"),
        Err(e) => println!("    ❌ 4D array: {}", e),
    }
    
    println!();
    println!("3D Array Test Complete.");
    
    Ok(())
}
