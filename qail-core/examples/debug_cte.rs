//! Test GROUP BY fix for JSON access

use qail_core::parse;
use qail_core::transpiler::ToSqlParameterized;

fn main() {
    println!("=== Verify GROUP BY fix ===\n");
    
    // Original problematic pattern
    let q1 = "get orders fields contact_info->>'phone' as phone_number, count(*) as order_count where contact_info->>'phone' is not null";
    let result = parse(q1).map(|c| c.to_sql_parameterized().sql).unwrap_or_else(|e| e.to_string());
    println!("1. JSON access with count:\n   {}\n", result);
    
    if result.contains("GROUP BY") {
        println!("   ✅ GROUP BY is present!");
    } else {
        println!("   ❌ GROUP BY is MISSING!");
    }
    
    // Test simple named column
    let q2 = "get whatsapp_messages fields phone_number, count(*) as unread_count where direction = 'inbound'";
    let result2 = parse(q2).map(|c| c.to_sql_parameterized().sql).unwrap_or_else(|e| e.to_string());
    println!("\n2. Named column with count:\n   {}\n", result2);
    
    if result2.contains("GROUP BY") {
        println!("   ✅ GROUP BY is present!");
    } else {
        println!("   ❌ GROUP BY is MISSING!");
    }
}
