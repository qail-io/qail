//! Test UPSERT (ON CONFLICT) feature

use qail_core::parse;
use qail_core::transpiler::ToSql;

fn main() {
    println!("=== Testing UPSERT / ON CONFLICT ===\n");
    
    // Test 1: DO NOTHING  
    let query1 = "add contacts fields phone, name values :phone, :name conflict (phone) nothing";
    println!("Query 1: {}", query1);
    match parse(query1) {
        Ok(cmd) => {
            println!("  On Conflict: {:?}", cmd.on_conflict);
            println!("  SQL: {}\n", cmd.to_sql());
        }
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 2: DO UPDATE
    let query2 = "add contacts fields phone, name values :phone, :name conflict (phone) update name = excluded.name";
    println!("Query 2: {}", query2);
    match parse(query2) {
        Ok(cmd) => {
            println!("  On Conflict: {:?}", cmd.on_conflict);
            println!("  SQL: {}\n", cmd.to_sql());
        }
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 3: Different column names
    let query3 = "add users fields email, data values :email, :data conflict (email) update data = :data";
    println!("Query 3: {}", query3);
    match parse(query3) {
        Ok(cmd) => {
            println!("  On Conflict: {:?}", cmd.on_conflict);
            println!("  SQL: {}\n", cmd.to_sql());
        }
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 4: Multiple assignments
    let query4 = "add items fields a, b, c values 1, 2, 3 conflict (a) update b = 10, c = 20";
    println!("Query 4: {}", query4);
    match parse(query4) {
        Ok(cmd) => {
            println!("  On Conflict: {:?}", cmd.on_conflict);
            println!("  SQL: {}\n", cmd.to_sql());
        }
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
}
