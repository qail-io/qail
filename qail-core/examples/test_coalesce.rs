//! Test COALESCE function support

use qail_core::parse;
use qail_core::transpiler::ToSql;

fn main() {
    println!("=== Testing COALESCE Function ===\n");
    
    // Test 1: COALESCE in UPDATE SET
    let query1 = "set users values name = coalesce(:name, name) where id = :id";
    println!("Query 1: {}", query1);
    match parse(query1) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 2: COALESCE in SELECT
    let query2 = "get users fields coalesce(name, 'Unknown') as display_name";
    println!("Query 2: {}", query2);
    match parse(query2) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 3: Multiple COALESCE args
    let query3 = "get users fields coalesce(nickname, name, email) as display";
    println!("Query 3: {}", query3);
    match parse(query3) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 4: NULLIF function
    let query4 = "get users fields nullif(status, 'inactive') as active_status";
    println!("Query 4: {}", query4);
    match parse(query4) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
}
