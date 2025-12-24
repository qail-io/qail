//! Test full conversation CTE pattern

use qail_core::parse;
use qail_core::transpiler::ToSql;

fn main() {
    let phone_expr = "case when contact_info->>'phone' like '0%' then '62' || substring(contact_info->>'phone' from 2) else replace(contact_info->>'phone', '+', '') end";
    
    let qail = format!(r#"with
        latest_messages as (
            get distinct on (phone_number) whatsapp_messages
            fields phone_number, content as last_message, created_at as last_message_time
            where our_phone_number_id = :phone_id
            order by phone_number, created_at desc
        ),
        customer_names as (
            get distinct on (phone_number) whatsapp_messages
            fields phone_number, sender_name as customer_sender_name
            where direction = 'inbound' and sender_name is not null and our_phone_number_id = :phone_id
            order by phone_number, created_at desc
        ),
        unread_counts as (
            get whatsapp_messages
            fields phone_number, count(*) as unread_count
            where direction = 'inbound' and status = 'received' and our_phone_number_id = :phone_id
        ),
        order_counts as (
            get orders
            fields contact_info->>'phone' as phone_number, count(*) as order_count
            where contact_info->>'phone' is not null
        ),
        order_names as (
            get distinct on ({phone_expr}) orders
            fields {phone_expr} as normalized_phone, contact_info->>'name' as order_customer_name, user_id
            where contact_info->>'phone' is not null
            order by {phone_expr}, created_at desc
        ),
        active_sessions as (
            get distinct on (phone_number) whatsapp_sessions
            fields phone_number, id as session_id, status as session_status
            order by phone_number, created_at desc
        )
        get latest_messages
        left join customer_names on customer_names.phone_number = latest_messages.phone_number
        left join unread_counts on unread_counts.phone_number = latest_messages.phone_number
        fields latest_messages.phone_number
        order by latest_messages.last_message_time desc"#,
        phone_expr = phone_expr
    );
    
    println!("=== FULL CTE Test ===\n");
    match parse(&qail) {
        Ok(cmd) => {
            println!("✅ Parse OK!");
            println!("  CTEs: {}", cmd.ctes.len());
            let sql = cmd.to_sql();
            println!("  SQL starts with: {}", &sql[..100.min(sql.len())]);
            if sql.starts_with("WITH") {
                println!("\n  ✅ WITH clause present!");
            }
        }
        Err(e) => println!("❌ {}", e),
    }
}
