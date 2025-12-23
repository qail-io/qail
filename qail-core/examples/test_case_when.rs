//! Test CASE WHEN expression support

use qail_core::parse;
use qail_core::transpiler::ToSql;

fn main() {
    println!("=== Testing CASE WHEN Expressions ===\n");
    
    // Test 1: Simple CASE WHEN
    let query1 = "get users fields case when status = 'active' then 1 else 0 end as is_active";
    println!("Query 1: {}", query1);
    match parse(query1) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 2: Multiple WHEN clauses
    let query2 = "get orders fields case when total > 1000 then 'large' when total > 100 then 'medium' else 'small' end as size";
    println!("Query 2: {}", query2);
    match parse(query2) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 3: CASE WHEN in UPDATE
    let query3 = "set products values price = case when stock > 0 then price else 0 end where id = :id";
    println!("Query 3: {}", query3);
    match parse(query3) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 4: CASE with named params
    let query4 = "get users fields case when role = :role then 1 else 0 end as matched";
    println!("Query 4: {}", query4);
    match parse(query4) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 5: IS NULL in WHERE (should work)
    let query5 = "get users where name is null";
    println!("Query 5 (WHERE IS NULL): {}", query5);
    match parse(query5) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
    
    // Test 6: IS NULL in CASE (simpler)
    let query6 = "get users fields case when name is null then email else name end";
    println!("Query 6 (CASE IS NULL): {}", query6);
    match parse(query6) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Parse error: {:?}\n", e),
    }
}
