//! Integration tests for qail! macros with real database
//! 
//! These tests require:
//! 1. SSH tunnel to staging DB: ssh -L 5433:localhost:5432 sailtix -N
//! 2. Schema file: cargo run --bin qail -- pull "postgresql://..."

use sqlx::postgres::PgPoolOptions;
use sqlx::FromRow;
use qail_macros::{qail, qail_one, qail_optional, qail_execute};

/// Test struct matching whatsapp_messages table
#[derive(Debug, FromRow)]
struct WhatsAppMessage {
    id: String, // UUID as string for simplicity
    phone_number: String,
    direction: String,
    message_type: String,
    content: Option<String>,
}

/// Test struct for simple queries
#[derive(Debug, FromRow)]
struct MessageCount {
    count: i64,
}

/// Get database URL from environment or use default staging URL
fn get_db_url() -> String {
    std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://sailtix:rGp5CuDhUa2tQcK4ao5uyA55@localhost:5433/swb-staging".to_string()
    })
}

/// Skip test if database is not available
async fn get_pool() -> Option<sqlx::PgPool> {
    let url = get_db_url();
    match PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
    {
        Ok(pool) => Some(pool),
        Err(e) => {
            eprintln!("⚠ Skipping integration test: DB not available ({e})");
            None
        }
    }
}

#[tokio::test]
async fn test_qail_fetch_all() {
    let Some(pool) = get_pool().await else { return };
    
    // Use the qail! macro to fetch messages
    let result = qail!(
        pool, 
        WhatsAppMessage, 
        "get whatsapp_messages fields id, phone_number, direction, message_type, content limit 5"
    ).await;
    
    match result {
        Ok(messages) => {
            println!("✓ qail! fetched {} messages", messages.len());
            assert!(messages.len() <= 5);
        }
        Err(e) => {
            panic!("qail! failed: {e}");
        }
    }
}

#[tokio::test]
async fn test_qail_with_named_params() {
    let Some(pool) = get_pool().await else { return };
    
    let direction = "inbound";
    
    // Use qail! with named parameters
    let result = qail!(
        pool,
        WhatsAppMessage,
        "get whatsapp_messages fields id, phone_number, direction, message_type, content where direction = :dir limit 3",
        dir: direction
    ).await;
    
    match result {
        Ok(messages) => {
            println!("✓ qail! with params fetched {} messages", messages.len());
            for msg in &messages {
                assert_eq!(msg.direction, "inbound");
            }
        }
        Err(e) => {
            panic!("qail! with params failed: {e}");
        }
    }
}

#[tokio::test]
async fn test_qail_optional() {
    let Some(pool) = get_pool().await else { return };
    
    // Try to fetch a message that likely doesn't exist
    let fake_id = "00000000-0000-0000-0000-000000000000";
    
    let result = qail_optional!(
        pool,
        WhatsAppMessage,
        "get whatsapp_messages fields id, phone_number, direction, message_type, content where id = :id",
        id: fake_id
    ).await;
    
    match result {
        Ok(maybe_msg) => {
            println!("✓ qail_optional! returned {:?}", maybe_msg.is_some());
            // Should be None for fake ID
            assert!(maybe_msg.is_none(), "Should not find fake ID");
        }
        Err(e) => {
            panic!("qail_optional! failed: {e}");
        }
    }
}

// Note: qail_one! and qail_execute! tests would need actual data to work with
// We skip those for safety in staging environment
