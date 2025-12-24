//! Debug the CASE expression with JSON access

use qail_core::parse;
use qail_core::transpiler::ToSqlParameterized;

fn main() {
    println!("=== Debug CASE + JSON Access ===\n");
    
    // The phone_expr from message_repository.rs
    let phone_expr = "case when contact_info->>'phone' like '0%' then '62' || substring(contact_info->>'phone' from 2) else replace(contact_info->>'phone', '+', '') end";
    
    // Test order_names CTE pattern
    let q1 = format!(r#"get distinct on ({phone_expr}) orders
        fields {phone_expr} as normalized_phone, contact_info->>'name' as order_customer_name
        where contact_info->>'phone' is not null
        order by {phone_expr}, created_at desc"#,
        phone_expr = phone_expr);
    
    println!("QAIL:\n{}\n", q1);
    
    let result = parse(&q1).map(|c| c.to_sql_parameterized().sql).unwrap_or_else(|e| format!("Parse error: {}", e));
    println!("Generated SQL:\n{}\n", result);
    
    // Check for unquoted phone
    if result.contains("->>phone") || result.contains("->> phone") {
        println!("❌ Found unquoted ->>phone somewhere!");
        // Find where it is
        if let Some(pos) = result.find("->>phone") {
            println!("   Position: {} chars", pos);
            println!("   Context: ...{}...", &result[pos.saturating_sub(30)..(pos + 30).min(result.len())]);
        }
    } else if result.contains("->>'phone'") {
        println!("✅ All JSON paths correctly quoted");
    } else {
        println!("⚠️ No JSON access found");
    }
}
