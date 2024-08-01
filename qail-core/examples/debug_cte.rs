//! Debug JSON access in QAIL

use qail_core::parse;
use qail_core::transpiler::ToSqlParameterized;

fn main() {
    println!("=== Debug JSON Access ===\n");
    
    // Test simple JSON access
    let q1 = "get orders fields contact_info->>'phone'";
    println!("1. Simple JSON: {}", 
        parse(q1).map(|c| c.to_sql_parameterized().sql).unwrap_or_else(|e| e.to_string()));
    
    // Test JSON access with alias
    let q2 = "get orders fields contact_info->>'phone' as phone_number";
    println!("2. JSON with alias: {}", 
        parse(q2).map(|c| c.to_sql_parameterized().sql).unwrap_or_else(|e| e.to_string()));
    
    // Test JSON access in WHERE clause
    let q3 = "get orders where contact_info->>'phone' is not null";
    println!("3. JSON in WHERE: {}", 
        parse(q3).map(|c| c.to_sql_parameterized().sql).unwrap_or_else(|e| e.to_string()));
    
    // Test the exact CTE pattern  
    let q4 = r#"with
        order_counts as (
            get orders
            fields contact_info->>'phone' as phone_number, count(*) as order_count
            where contact_info->>'phone' is not null
        )
        get order_counts"#;
    println!("4. CTE with JSON: {}", 
        parse(q4).map(|c| c.to_sql_parameterized().sql).unwrap_or_else(|e| e.to_string()));
}
