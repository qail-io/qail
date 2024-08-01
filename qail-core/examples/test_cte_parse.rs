/// Test full WhatsApp insights query parsing
use qail_core::parser::parse;
use qail_core::transpiler::ToSql;

fn main() {
    // Complete WhatsApp insights query with all 6 columns
    let query = "with stats as (get whatsapp_messages fields count(distinct phone_number) as total_contacts, count(*) as total_messages, count(*) filter (where direction = 'outbound' and created_at > now() - 24h) as messages_sent_24h, count(*) filter (where direction = 'inbound' and created_at > now() - 24h) as messages_received_24h, count(*) filter (where direction = 'inbound' and status = 'received') as unread_messages, count(*) filter (where direction = 'outbound' and created_at > now() - 24h and status in ('delivered', 'read')) as successful_deliveries_24h) get stats";
    
    println!("Complete WhatsApp Insights Query (6 columns):");
    match parse(query) {
        Ok(cmd) => println!("✅\n{}", cmd.to_sql()),
        Err(e) => println!("❌ {}", e),
    }
}
