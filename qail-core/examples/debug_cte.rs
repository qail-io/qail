//! Test full unread_counts CTE pattern

use qail_core::parse;
use qail_core::transpiler::ToSqlParameterized;

fn main() {
    println!("=== Test unread_counts CTE Fix ===\n");
    
    // Pattern from message_repository.rs
    let q1 = r#"with
        unread_counts as (
            get whatsapp_messages
            fields phone_number, count(*) as unread_count
            where direction = 'inbound' and status = 'received' and our_phone_number_id = :phone_id
        )
        get unread_counts
        fields unread_counts.phone_number, unread_counts.unread_count"#;
    
    let result = parse(q1).map(|c| c.to_sql_parameterized().sql).unwrap_or_else(|e| format!("Parse error: {}", e));
    println!("Generated SQL:\n{}\n", result);
    
    // Check all expected parts
    let checks = [
        ("AS unread_count", "Alias in CTE"),
        ("GROUP BY phone_number", "GROUP BY present"),
        ("unread_counts.unread_count", "Column reference works"),
    ];
    
    for (pattern, desc) in checks {
        if result.to_lowercase().contains(&pattern.to_lowercase()) {
            println!("✅ {}: {}", desc, pattern);
        } else {
            println!("❌ {}: {} MISSING!", desc, pattern);
        }
    }
}
