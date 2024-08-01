//! Test Complex ORDER BY

use qail_core::parse;
use qail_core::transpiler::ToSql;

fn main() {
    println!("=== Testing Complex ORDER BY ===\n");
    
    // Test 1: Simple ORDER BY
    let query1 = "get users fields name order by created_at desc";
    println!("Query 1 (Simple ORDER BY):");
    match parse(query1) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Error: {}\n", e),
    }
    
    // Test 2: ORDER BY with expression
    let query2 = "get users fields name order by first_name || last_name";
    println!("Query 2 (ORDER BY expression):");
    match parse(query2) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Error: {}\n", e),
    }
    
    // Test 3: ORDER BY with CASE WHEN
    let query3 = "get orders fields id order by case when status = 'urgent' then 1 else 2 end";
    println!("Query 3 (ORDER BY CASE WHEN):");
    match parse(query3) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Error: {}\n", e),
    }
    
    // Test 4: Complex ORDER BY from message_repository
    let query4 = r#"get orders fields normalized_phone order by 
        case when phone like '0%' then '62' || substring(phone from 2) else phone end, 
        created_at desc"#;
    println!("Query 4 (Complex CASE + SUBSTRING in ORDER BY):");
    match parse(query4) {
        Ok(cmd) => println!("  SQL: {}\n", cmd.to_sql()),
        Err(e) => println!("  Error: {}\n", e),
    }
}
