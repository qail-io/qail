/// Test WhatsApp insights query with all SQL features
use qail_core::parser::parse;
use qail_core::transpiler::ToSql;

fn main() {
    // Test 1: Inner CTE only (no outer CASE)
    println!("=== Test 1: CTE only ===");
    let qail1 = r#"with stats as (get whatsapp_messages fields count(distinct phone_number) as total_contacts, count(*) as total_messages, count(*) filter (where direction = 'outbound' and created_at > now() - 24h) as messages_sent_24h, count(*) filter (where direction = 'inbound' and created_at > now() - 24h) as messages_received_24h, count(*) filter (where direction = 'inbound' and status = 'received') as unread_messages, count(*) filter (where direction = 'outbound' and created_at > now() - 24h and status in ('delivered', 'read')) as successful_deliveries_24h) get stats"#;
    match parse(qail1) {
        Ok(cmd) => println!("✅ Parses: {}", cmd.to_sql()),
        Err(e) => println!("❌ {}", e),
    }
    
    // Test 2: Simple outer select with CASE WHEN  
    println!("\n=== Test 2: Outer CASE WHEN ===");
    let qail2 = r#"get stats fields total_contacts, case when messages_sent_24h > 0 then 100.0 else 0.0 end as rate"#;
    match parse(qail2) {
        Ok(cmd) => println!("✅ Parses: {}", cmd.to_sql()),
        Err(e) => println!("❌ {}", e),
    }
    
    // Test 3: Combined - CTE with outer CASE WHEN
    println!("\n=== Test 3: Full Query (CTE + outer CASE) ===");
    let qail3 = r#"with stats as (get whatsapp_messages fields count(*) as total, count(*) filter (where direction = 'outbound') as sent) get stats fields total, case when sent > 0 then 100.0 else 0.0 end as rate"#;
    match parse(qail3) {
        Ok(cmd) => println!("✅ Parses: {}", cmd.to_sql()),
        Err(e) => println!("❌ {}", e),
    }
}
