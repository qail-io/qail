//! Battle test: get_conversations query using native QAIL
//!
//! Tests the complex multi-CTE query that powers the WhatsApp inbox.
//! Run with: cargo run --example battle_conversations

use qail_core::prelude::*;
use qail_core::ast::SortOrder;
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect via DATABASE_URL env var
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set (e.g. postgresql://user:pass@localhost:5432/dbname)");
    let mut driver = PgDriver::connect_url(&url).await?;
    
    println!("✓ Connected to database (read-only test)\n");
    
    // Test both: with filter and without filter
    let our_phone_number_id: Option<&str> = None; // Test unfiltered first
    
    // ============================================================
    // CTE 1: latest_messages (DISTINCT ON phone_number)
    // ============================================================
    let mut latest_messages = Qail::get("whatsapp_messages")
        .distinct_on(["phone_number"])
        .column("phone_number")
        .column_expr(col("content").with_alias("last_message"))
        .column_expr(col("created_at").with_alias("last_message_time"))
        .order_by("phone_number", SortOrder::Asc)
        .order_by("created_at", SortOrder::Desc);
    
    if let Some(phone_id) = our_phone_number_id {
        latest_messages = latest_messages.eq("our_phone_number_id", phone_id);
    }
    let latest_messages_cte = latest_messages.to_cte("latest_messages");
    
    // ============================================================
    // CTE 2: customer_names (DISTINCT ON phone_number, inbound only)
    // ============================================================
    let mut customer_names = Qail::get("whatsapp_messages")
        .distinct_on(["phone_number"])
        .column("phone_number")
        .column_expr(col("sender_name").with_alias("customer_sender_name"))
        .eq("direction", "inbound")
        .is_not_null("sender_name")
        .order_by("phone_number", SortOrder::Asc)
        .order_by("created_at", SortOrder::Desc);
    
    if let Some(phone_id) = our_phone_number_id {
        customer_names = customer_names.eq("our_phone_number_id", phone_id);
    }
    let customer_names_cte = customer_names.to_cte("customer_names");
    
    // ============================================================
    // CTE 3: unread_counts (COUNT + GROUP BY)
    // ============================================================
    let mut unread_counts = Qail::get("whatsapp_messages")
        .column("phone_number")
        .column_expr(count().alias("unread_count"))
        .eq("direction", "inbound")
        .eq("status", "received")
        .group_by(["phone_number"]);
    
    if let Some(phone_id) = our_phone_number_id {
        unread_counts = unread_counts.eq("our_phone_number_id", phone_id);
    }
    let unread_counts_cte = unread_counts.to_cte("unread_counts");
    
    // ============================================================
    // CTE 4: order_counts (JSON access + GROUP BY)
    // ============================================================
    let order_counts_cte = Qail::get("orders")
        .column_expr(json("contact_info", "phone").alias("phone_number"))
        .column_expr(count().alias("order_count"))
        .filter_cond(cond(json("contact_info", "phone").into(), Operator::IsNotNull, Value::Null))
        .group_by_expr([json("contact_info", "phone").into()])
        .to_cte("order_counts");
    
    // ============================================================
    // CTE 5: active_sessions (DISTINCT ON phone_number)
    // ============================================================
    let active_sessions_cte = Qail::get("whatsapp_sessions")
        .distinct_on(["phone_number"])
        .column("phone_number")
        .column_expr(col("id").with_alias("session_id"))
        .column_expr(col("status").with_alias("session_status"))
        .order_by("phone_number", SortOrder::Asc)
        .order_by("created_at", SortOrder::Desc)
        .to_cte("active_sessions");
    
    // ============================================================
    // Main Query: SELECT from CTEs with LEFT JOINs
    // Using simple approach without table aliases
    // ============================================================
    let main_query = Qail::get("latest_messages")
        .column("latest_messages.phone_number")
        .column_expr(coalesce([col("customer_names.customer_sender_name"), text("Unknown")]).alias("customer_name"))
        .column_expr(coalesce([col("latest_messages.last_message"), text("")]).alias("last_message"))
        .column("latest_messages.last_message_time")
        .column_expr(coalesce([col("unread_counts.unread_count"), int(0)]).alias("unread_count"))
        .column_expr(coalesce([col("order_counts.order_count"), int(0)]).alias("order_count"))
        .column("active_sessions.session_id")
        .column("active_sessions.session_status")
        .left_join("customer_names", "customer_names.phone_number", "latest_messages.phone_number")
        .left_join("unread_counts", "unread_counts.phone_number", "latest_messages.phone_number")
        .left_join("order_counts", "order_counts.phone_number", "latest_messages.phone_number")
        .left_join("active_sessions", "active_sessions.phone_number", "latest_messages.phone_number")
        .order_by("latest_messages.last_message_time", SortOrder::Desc)
        .with_ctes(vec![
            latest_messages_cte,
            customer_names_cte,
            unread_counts_cte,
            order_counts_cte,
            active_sessions_cte,
        ]);
    
    println!("Executing query...\n");
    
    // Debug: Print generated SQL
    use qail_core::transpiler::ToSql;
    println!("Generated SQL:\n{}\n", main_query.to_sql());
    
    let rows = driver.fetch_all(&main_query).await?;
    
    println!("✓ Query returned {} conversations\n", rows.len());
    
    // Print first 5 results
    println!("First 5 conversations:");
    println!("{:-<80}", "");
    for (i, row) in rows.iter().take(5).enumerate() {
        let phone = row.get_string(0).unwrap_or_default();
        let name = row.get_string(1).unwrap_or_else(|| "Unknown".to_string());
        let last_msg = row.get_string(2).unwrap_or_default();
        let unread = row.get_i64(4).unwrap_or(0);
        let orders = row.get_i64(5).unwrap_or(0);
        let session_id = row.get_uuid(6).unwrap_or_default();
        let session_status = row.get_string(7).unwrap_or_else(|| "NULL".to_string());
        
        println!("{}. {} ({})", i + 1, name, phone);
        println!("   Last: {} ", if last_msg.len() > 50 { &last_msg[..50] } else { &last_msg });
        println!("   Unread: {} | Orders: {}", unread, orders);
        println!("   Session: {} | Status: {}", session_id, session_status);
        println!();
    }
    
    println!("✓ Battle test complete!");
    Ok(())
}
