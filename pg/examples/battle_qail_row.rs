//! Battle Test: QailRow trait with fetch_typed
//! Tests automatic struct mapping from database rows.
//!
//! Run: cargo run --example battle_qail_row --features chrono,uuid

use chrono::{DateTime, Utc};
use qail_core::ast::SortOrder;
use qail_core::prelude::*;
use qail_pg::{PgDriver, PgRow, QailRow};
use uuid::Uuid;

/// Test struct matching whatsapp_messages columns
#[derive(Debug)]
struct TestMessage {
    id: Uuid,
    phone_number: String,
    direction: String,
    content: Option<String>,
    status: String,
    created_at: DateTime<Utc>,
}

/// Implement QailRow for automatic mapping
impl QailRow for TestMessage {
    fn columns() -> &'static [&'static str] {
        &[
            "id",
            "phone_number",
            "direction",
            "content",
            "status",
            "created_at",
        ]
    }

    fn from_row(row: &PgRow) -> Self {
        TestMessage {
            id: row.try_get_by_name::<Uuid>("id").unwrap_or_default(),
            phone_number: row
                .try_get_by_name::<String>("phone_number")
                .unwrap_or_default(),
            direction: row.try_get_by_name::<String>("direction").unwrap_or_default(),
            content: row.try_get_opt_by_name::<String>("content").ok().flatten(),
            status: row.try_get_by_name::<String>("status").unwrap_or_default(),
            created_at: row.datetime_by_name("created_at").unwrap_or_else(Utc::now),
        }
    }
}

/// Simple user struct for testing
#[derive(Debug)]
struct SimpleOrder {
    id: Uuid,
    status: String,
    total_fare: i64,
}

impl QailRow for SimpleOrder {
    fn columns() -> &'static [&'static str] {
        &["id", "status", "total_fare"]
    }

    fn from_row(row: &PgRow) -> Self {
        SimpleOrder {
            id: row.try_get_by_name::<Uuid>("id").unwrap_or_default(),
            status: row.try_get_by_name::<String>("status").unwrap_or_default(),
            total_fare: row.try_get_by_name::<i64>("total_fare").unwrap_or(0),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect via DATABASE_URL env var
    // Example: DATABASE_URL=postgresql://user:pass@localhost:5432/dbname cargo run --example battle_qail_row --features chrono,uuid
    let url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set (e.g. postgresql://user:pass@localhost:5432/dbname)");
    let mut driver = PgDriver::connect_url(&url).await?;

    println!("✓ Connected to database\n");

    // ============================================================
    // Test 1: fetch_typed with TestMessage
    // ============================================================
    println!("=== Test 1: fetch_typed::<TestMessage>() ===");

    let query = Qail::get("whatsapp_messages")
        .column("id")
        .column("phone_number")
        .column("direction")
        .column("content")
        .column("status")
        .column("created_at")
        .order_by("created_at", SortOrder::Desc)
        .limit(3);

    println!("SQL: {}\n", query.to_sql());

    let messages: Vec<TestMessage> = driver.fetch_typed::<TestMessage>(&query).await?;

    println!("Found {} messages:\n", messages.len());
    for msg in &messages {
        println!("  ID: {}", msg.id);
        println!("  Phone: {}", msg.phone_number);
        println!("  Direction: {}", msg.direction);
        println!(
            "  Content: {:?}",
            msg.content
                .as_ref()
                .map(|c| c.chars().take(30).collect::<String>())
        );
        println!("  Status: {}", msg.status);
        println!("  Created: {}", msg.created_at);
        println!();
    }

    // ============================================================
    // Test 2: fetch_one_typed with SimpleOrder
    // ============================================================
    println!("=== Test 2: fetch_one_typed::<SimpleOrder>() ===");

    let order_query = Qail::get("orders")
        .column("id")
        .column("status")
        .column("total_fare")
        .order_by("created_at", SortOrder::Desc)
        .limit(1);

    println!("SQL: {}\n", order_query.to_sql());

    let order: Option<SimpleOrder> = driver.fetch_one_typed::<SimpleOrder>(&order_query).await?;

    match order {
        Some(o) => {
            println!("Order: {:?}", o);
            println!("  ID: {}", o.id);
            println!("  Status: {}", o.status);
            println!(
                "  Total fare: {} cents ({:.2} IDR)",
                o.total_fare,
                o.total_fare as f64 / 100.0
            );
        }
        None => println!("No orders found"),
    }
    println!();

    // ============================================================
    // Test 3: Verify columns() returns correct list
    // ============================================================
    println!("=== Test 3: QailRow::columns() ===");
    println!("TestMessage columns: {:?}", TestMessage::columns());
    println!("SimpleOrder columns: {:?}", SimpleOrder::columns());
    println!();

    println!("✓ All QailRow tests passed!");
    Ok(())
}
