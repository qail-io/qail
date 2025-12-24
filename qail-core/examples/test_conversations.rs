//! Test QAIL against real get_conversations query

use qail_core::parse;
use qail_core::transpiler::ToSql;

fn main() {
    println!("=== Testing QAIL with get_conversations Pattern ===\n");
    
    // CTE 1: latest_messages
    let cte1 = r#"get distinct on (phone_number) whatsapp_messages 
        fields phone_number, content as last_message, created_at as last_message_time
        order by phone_number, created_at desc"#;
    
    println!("CTE 1 (latest_messages):");
    match parse(cte1) {
        Ok(cmd) => {
            let sql = cmd.to_sql();
            println!("  QAIL: {}", cte1.replace('\n', " "));
            println!("  SQL:  {}\n", sql);
        }
        Err(e) => println!("  ERR: {}\n", e),
    }
    
    // CTE 2: customer_names
    let cte2 = r#"get distinct on (phone_number) whatsapp_messages 
        fields phone_number, sender_name as customer_sender_name
        where direction = 'inbound' and sender_name is not null
        order by phone_number, created_at desc"#;
    
    println!("CTE 2 (customer_names):");
    match parse(cte2) {
        Ok(cmd) => {
            let sql = cmd.to_sql();
            println!("  QAIL: {}", cte2.replace('\n', " "));
            println!("  SQL:  {}\n", sql);
        }
        Err(e) => println!("  ERR: {}\n", e),
    }
    
    // CTE 3: unread_counts
    let cte3 = r#"get whatsapp_messages 
        fields phone_number, count(*) as unread_count
        where direction = 'inbound' and status = 'received'"#;
    
    println!("CTE 3 (unread_counts):");
    match parse(cte3) {
        Ok(cmd) => {
            let sql = cmd.to_sql();
            println!("  SQL:  {}\n", sql);
        }
        Err(e) => println!("  ERR: {}\n", e),
    }
    
    // CTE 4: order_counts (JSON access)
    let cte4 = r#"get orders 
        fields contact_info->>'phone' as phone_number, count(*) as order_count
        where contact_info->>'phone' is not null"#;
    
    println!("CTE 4 (order_counts with JSON):");
    match parse(cte4) {
        Ok(cmd) => {
            let sql = cmd.to_sql();
            println!("  SQL:  {}\n", sql);
        }
        Err(e) => println!("  ERR: {}\n", e),
    }
    
    // CTE 5: order_names (Complex DISTINCT ON with CASE WHEN)
    let cte5 = r#"get distinct on (case when contact_info->>'phone' like '0%' then '62' || substring(contact_info->>'phone' from 2) else replace(contact_info->>'phone', '+', '') end) orders
        fields 
            case when contact_info->>'phone' like '0%' then '62' || substring(contact_info->>'phone' from 2) else replace(contact_info->>'phone', '+', '') end as normalized_phone,
            contact_info->>'name' as order_customer_name,
            user_id
        where contact_info->>'phone' is not null
        order by case when contact_info->>'phone' like '0%' then '62' || substring(contact_info->>'phone' from 2) else replace(contact_info->>'phone', '+', '') end, created_at desc"#;
    
    println!("CTE 5 (order_names - COMPLEX):");
    match parse(cte5) {
        Ok(cmd) => {
            let sql = cmd.to_sql();
            println!("  SQL:  {}\n", sql);
        }
        Err(e) => println!("  ERR: {}\n", e),
    }
    
    // CTE 6: active_sessions
    let cte6 = r#"get distinct on (phone_number) whatsapp_sessions 
        fields phone_number, id as session_id, status as session_status
        order by phone_number, created_at desc"#;
    
    println!("CTE 6 (active_sessions):");
    match parse(cte6) {
        Ok(cmd) => {
            let sql = cmd.to_sql();
            println!("  SQL:  {}\n", sql);
        }
        Err(e) => println!("  ERR: {}\n", e),
    }
    
    // Final query with multiple JOINs
    let final_q = r#"get latest_messages 
        left join customer_names on customer_names.phone_number = latest_messages.phone_number
        left join unread_counts on unread_counts.phone_number = latest_messages.phone_number
        left join order_counts on order_counts.phone_number = latest_messages.phone_number
        left join order_names on order_names.normalized_phone = latest_messages.phone_number
        left join users on users.id = order_names.user_id
        left join active_sessions on active_sessions.phone_number = latest_messages.phone_number
        left join whatsapp_contacts on whatsapp_contacts.phone_number = latest_messages.phone_number
        fields 
            latest_messages.phone_number,
            coalesce(whatsapp_contacts.custom_name, whatsapp_contacts.meta_profile_name, customer_names.customer_sender_name, order_names.order_customer_name, users.first_name || ' ' || users.last_name) as customer_name,
            coalesce(latest_messages.last_message, '') as last_message,
            latest_messages.last_message_time,
            coalesce(unread_counts.unread_count, 0) as unread_count,
            coalesce(order_counts.order_count, 0) as order_count,
            active_sessions.session_id,
            active_sessions.session_status
        order by latest_messages.last_message_time desc"#;
    
    println!("FINAL (multiple LEFT JOINs):");
    match parse(final_q) {
        Ok(cmd) => {
            println!("  Joins: {}", cmd.joins.len());
            let sql = cmd.to_sql();  
            println!("  SQL:  {}\n", sql);
        }
        Err(e) => println!("  ERR: {}\n", e),
    }
}
