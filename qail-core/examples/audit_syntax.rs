//! Test DISTINCT ON with Expressions

use qail_core::parse;
use qail_core::transpiler::ToSql;

fn main() {
    println!("=== Testing DISTINCT ON with Expressions ===\n");
    
    // Test 1: Simple DISTINCT ON (column)
    let q1 = "get distinct on (phone_number) messages fields phone_number, content order by phone_number, created_at desc";
    println!("Test 1 (DISTINCT ON column):");
    match parse(q1) {
        Ok(cmd) => println!("  OK: {}\n", cmd.to_sql()),
        Err(e) => println!("  ERR: {}\n", e),
    }
    
    // Test 2: DISTINCT ON with CASE WHEN expression
    let q2 = r#"get distinct on (case when phone like '0%' then '62' || substring(phone from 2) else phone end) orders 
        fields phone, name order by case when phone like '0%' then '62' || substring(phone from 2) else phone end, created_at desc"#;
    println!("Test 2 (DISTINCT ON CASE WHEN):");
    match parse(q2) {
        Ok(cmd) => println!("  OK: {}\n", cmd.to_sql()),
        Err(e) => println!("  ERR: {}\n", e),
    }
    
    // Test 3: DISTINCT ON with function
    let q3 = "get distinct on (lower(email)) users fields email, name order by lower(email), id";
    println!("Test 3 (DISTINCT ON function):");
    match parse(q3) {
        Ok(cmd) => println!("  OK: {}\n", cmd.to_sql()),
        Err(e) => println!("  ERR: {}\n", e),
    }
}
