//! Test INSERT...SELECT and Subquery in SET

use qail_core::parse;
use qail_core::transpiler::ToSql;

fn main() {
    println!("=== Testing INSERT...SELECT ===\n");
    
    // Test 1: Basic INSERT SELECT
    let query1 = "add archive fields id, name from (get users fields id, name where active = false)";
    println!("Query 1: {}", query1);
    match parse(query1) {
        Ok(cmd) => {
            println!("  source_query: {:?}", cmd.source_query.is_some());
            println!("  SQL: {}\n", cmd.to_sql());
        }
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    println!("=== Testing Subquery in SET ===\n");
    
    // Test 2: Subquery in UPDATE
    let query2 = "set orders values total = (get items fields sum(price) where order_id = orders.id) where id = :id";
    println!("Query 2: {}", query2);
    match parse(query2) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 3: Simple subquery value
    let query3 = "set users values role_id = (get roles fields id where name = 'admin') where user_id = :id";
    println!("Query 3: {}", query3);
    match parse(query3) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
}
