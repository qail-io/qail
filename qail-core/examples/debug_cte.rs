//! Test ToSqlParameterized with CTEs

use qail_core::parse;
use qail_core::transpiler::ToSqlParameterized;

fn main() {
    // Same query as in message_repository.rs (simplified)
    let qail = r#"with
                latest_messages as (
                    get distinct on (phone_number) whatsapp_messages
                    fields phone_number, content as last_message
                    where our_phone_number_id = :phone_id
                    order by phone_number, created_at desc
                ),
                customer_names as (
                    get distinct on (phone_number) whatsapp_messages
                    fields phone_number, sender_name
                    where direction = 'inbound' and sender_name is not null and our_phone_number_id = :phone_id
                    order by phone_number, created_at desc
                )
                get latest_messages
                left join customer_names on customer_names.phone_number = latest_messages.phone_number
                fields latest_messages.phone_number, customer_names.sender_name"#;
    
    println!("=== Test ToSqlParameterized with CTEs ===\n");
    
    match parse(qail) {
        Ok(cmd) => {
            let result = cmd.to_sql_parameterized();
            
            println!("SQL: {}\n", result.sql);
            println!("Named params: {:?}", result.named_params);
            
            // Check if WITH clause is present
            if result.sql.starts_with("WITH") {
                println!("\n✅ WITH clause present in parameterized output!");
            } else {
                println!("\n❌ WITH clause MISSING from parameterized output!");
            }
            
            // Check if :phone_id was replaced with $1
            if result.sql.contains("$1") && !result.sql.contains(":phone_id") {
                println!("✅ Named params correctly replaced with positional $N");
            } else {
                println!("❌ Named params NOT replaced");
            }
        }
        Err(e) => {
            println!("❌ Parse Error: {}", e);
        }
    }
}
