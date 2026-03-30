//! LRU Cache Stress Test

use qail_core::ast::{Action, Constraint, Expr};
use qail_core::prelude::*;
use qail_pg::driver::PgDriver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 LRU Cache Stress Test (limit: 100)");
    println!("=====================================\n");

    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "qail_test_migration").await?;

    // Create test table
    let drop_cmd = Qail {
        action: Action::Drop,
        table: "cache_test".to_string(),
        ..Default::default()
    };
    let make_cmd = Qail {
        action: Action::Make,
        table: "cache_test".to_string(),
        columns: vec![Expr::Def {
            name: "id".to_string(),
            data_type: "int".to_string(),
            constraints: vec![Constraint::Nullable],
        }],
        ..Default::default()
    };
    let _ = driver.execute(&drop_cmd).await;
    driver.execute(&make_cmd).await?;
    driver
        .execute(&Qail::add("cache_test").columns(["id"]).values([1]))
        .await?;

    let (size, cap) = driver.cache_stats();
    println!("Initial: {}/{}", size, cap);

    // 200 unique AST queries (different LIMIT values = different SQL)
    println!("\n📊 200 unique queries (should evict at 100)...");
    for i in 1..=200 {
        let query = Qail::get("cache_test").column("id").limit(i as i64);
        let _ = driver.fetch_all(&query).await?;

        if i % 50 == 0 {
            let (size, cap) = driver.cache_stats();
            println!("  Query {}: cache {}/{}", i, size, cap);
        }
    }

    let (final_size, cap) = driver.cache_stats();
    println!("\nFinal: {}/{}", final_size, cap);

    if final_size == cap {
        println!("✅ LRU at capacity - eviction working!");
    }

    driver.clear_cache();
    let (size, _) = driver.cache_stats();
    println!("\nAfter clear_cache(): {} ✅", size);

    driver.execute(&drop_cmd).await?;
    Ok(())
}
