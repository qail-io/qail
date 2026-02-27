//! Battle Test: get_conversation (detail view)
//! Tests QAIL queries for the conversation detail endpoint against production database.
//!
//! Run: cargo run --example battle_conversation_detail --features chrono,uuid

use qail_core::ast::SortOrder;
use qail_core::prelude::*;
use qail_pg::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect via DATABASE_URL env var
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set (e.g. postgresql://user:pass@localhost:5432/dbname)");
    let mut driver = PgDriver::connect_url(&url).await?;

    println!("✓ Connected to database (read-only test)\n");

    // Test phone number with known data
    let test_phone = "628881800500";

    // ============================================================
    // Query 1: Get latest session (simple QAIL)
    // ============================================================
    println!("=== Query 1: Latest Session ===");
    let session_query = Qail::get("whatsapp_sessions")
        .column("id")
        .column("status")
        .column("created_at")
        .eq("phone_number", test_phone)
        .order_by("created_at", SortOrder::Desc)
        .limit(1);

    println!("SQL: {}\n", session_query.to_sql());

    let session = driver.fetch_all(&session_query).await?.pop();
    match &session {
        Some(row) => {
            println!("Session ID: {}", row.get_uuid(0).unwrap_or_default());
            println!("Status: {}", row.get_string(1).unwrap_or_default());
            println!("Created: {}", row.get_timestamp(2).unwrap_or_default());
        }
        None => println!("No session found"),
    }
    println!();

    // ============================================================
    // Query 2: Get messages for session (uses session_id)
    // ============================================================
    println!("=== Query 2: Messages for Session ===");
    if let Some(row) = &session {
        let session_id = row.get_uuid(0).unwrap_or_default();

        // Test the query structure - not full mapping since WhatsAppMessage has 18 fields
        let messages_query = Qail::get("whatsapp_messages")
            .column("id")
            .column("direction")
            .column("content")
            .column("status")
            .column("created_at")
            .eq("phone_number", test_phone)
            .eq("session_id", session_id.clone())
            .order_by("created_at", SortOrder::Asc);

        println!("SQL: {}\n", messages_query.to_sql());

        let messages = driver.fetch_all(&messages_query).await?;
        println!("Found {} messages in session\n", messages.len());

        for (i, msg) in messages.iter().take(3).enumerate() {
            println!(
                "{}. [{}] {} - {}",
                i + 1,
                msg.get_string(1).unwrap_or_default(), // direction
                msg.get_string(3).unwrap_or_default(), // status
                msg.get_string(2)
                    .unwrap_or_default()
                    .chars()
                    .take(50)
                    .collect::<String>()  // content
            );
        }
        if messages.len() > 3 {
            println!("... and {} more messages", messages.len() - 3);
        }
        println!();
    }

    // ============================================================
    // Query 3: Customer info (simpler approach - 3 separate queries)
    // ============================================================
    println!("=== Query 3: Customer Info (Separate Queries) ===");

    // 3a: Try contact_name from whatsapp_contacts first
    let contact_query = Qail::get("whatsapp_contacts")
        .column_expr(coalesce([col("custom_name"), col("meta_profile_name")]).alias("name"))
        .eq("phone_number", test_phone)
        .limit(1);

    let contact_name = driver
        .fetch_all(&contact_query)
        .await?
        .pop()
        .and_then(|r| r.get_string(0));
    println!("Contact name: {:?}", contact_name);

    // 3b: Try inbound sender name
    let inbound_query = Qail::get("whatsapp_messages")
        .column("sender_name")
        .eq("phone_number", test_phone)
        .eq("direction", "inbound")
        .is_not_null("sender_name")
        .order_by("created_at", SortOrder::Desc)
        .limit(1);

    let inbound_name = driver
        .fetch_all(&inbound_query)
        .await?
        .pop()
        .and_then(|r| r.get_string(0));
    println!("Inbound name: {:?}", inbound_name);

    // 3c: Try order info
    let order_query = Qail::get("orders")
        .column_expr(json("contact_info", "name").alias("name"))
        .column_expr(json("contact_info", "email").alias("email"))
        .column("user_id")
        .filter_cond(cond(
            json("contact_info", "phone").into(),
            Operator::Eq,
            Value::String(test_phone.to_string()),
        ))
        .order_by("created_at", SortOrder::Desc)
        .limit(1);

    println!("SQL: {}\n", order_query.to_sql());

    let order_info = driver.fetch_all(&order_query).await?.pop();
    let (order_name, customer_email, user_id) = match &order_info {
        Some(row) => (row.get_string(0), row.get_string(1), row.get_uuid(2)),
        None => (None, None, None),
    };
    println!(
        "Order name: {:?}, Email: {:?}, User ID: {:?}",
        order_name, customer_email, user_id
    );

    // Final customer name: COALESCE priority
    let customer_name = contact_name.or(inbound_name).or(order_name);
    println!("\nFinal customer name: {:?}", customer_name);
    println!();

    // ============================================================
    // Query 4: Orders with nested JSON paths (complex version)
    // metadata->'vessel_bookings'->0->>'depart_departure_loc'
    // ============================================================
    println!("=== Query 4: Orders (Complex Nested JSON) ===");

    // Build the route expression: departure + ' → ' + arrival
    // Using json_path for: metadata->'vessel_bookings'->0->>'field'
    let departure_loc = json_path("metadata", ["vessel_bookings", "0", "depart_departure_loc"]);
    let arrival_loc = json_path("metadata", ["vessel_bookings", "0", "depart_arrival_loc"]);
    let travel_date = json_path("metadata", ["vessel_bookings", "0", "depart_travel_date"]);

    // Build route expression: departure || ' → ' || arrival using Binary concat
    // ConcatBuilder.build() creates a chain of Expr::Binary with BinaryOp::Concat
    let route_expr = Expr::Binary {
        left: Box::new(Expr::Binary {
            left: Box::new(departure_loc.build()),
            op: BinaryOp::Concat,
            right: Box::new(text(" → ")),
            alias: None,
        }),
        op: BinaryOp::Concat,
        right: Box::new(arrival_loc.build()),
        alias: None,
    };

    let orders_query = Qail::get("orders")
        .column("id")
        .column_expr(coalesce([col("booking_number"), text("N/A")]).alias("booking_number"))
        .column("status")
        // Route: COALESCE(departure || ' → ' || arrival, 'Route')
        .column_expr(coalesce([route_expr, text("Route")]).alias("route"))
        // Travel date
        .column_expr(coalesce([travel_date.build(), text("TBD")]).alias("travel_date"))
        // Total amount: (total_fare::float / 100.0) - we need to use raw for this or expression
        .column("total_fare")
        .column_expr(coalesce([col("currency"), text("IDR")]).alias("currency"))
        .filter_cond(cond(
            json("contact_info", "phone").into(),
            Operator::Eq,
            Value::String(test_phone.to_string()),
        ))
        .order_by("created_at", SortOrder::Desc)
        .limit(10);

    println!("SQL: {}\n", orders_query.to_sql());

    let orders = driver.fetch_all(&orders_query).await?;
    println!("Found {} orders\n", orders.len());

    for (i, order) in orders.iter().take(3).enumerate() {
        println!("{}. ID: {}", i + 1, order.get_uuid(0).unwrap_or_default());
        println!("   Booking: {}", order.get_string(1).unwrap_or_default());
        println!("   Status: {}", order.get_string(2).unwrap_or_default());
        println!("   Route: {}", order.get_string(3).unwrap_or_default());
        println!("   Date: {}", order.get_string(4).unwrap_or_default());
        println!("   Fare: {}", order.get_i64(5).unwrap_or(0));
        println!("   Currency: {}", order.get_string(6).unwrap_or_default());
        println!();
    }
    println!();

    // ============================================================
    // Query 5: Mark messages as read (UPDATE)
    // ============================================================
    println!("=== Query 5: Mark Messages Read (DRY RUN) ===");
    let mark_read_query = Qail::set("whatsapp_messages")
        .set_value("status", "read")
        .eq("phone_number", test_phone)
        .eq("direction", "inbound")
        .eq("status", "received");

    println!("SQL: {}", mark_read_query.to_sql());
    println!("(Not executing - this is already in production)");
    println!();

    println!("✓ Battle test complete!");
    Ok(())
}
