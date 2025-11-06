//! BATTLE TEST #6: The Thundering Herd (Deadlock Test)
//! 
//! Purpose: Ensure the Pool semaphore works under extreme contention.
//! Fail Condition: The program hangs forever (Deadlock).
//!
//! Run: cargo run --release -p qail-pg --example battle_herd

use qail_pg::{PgPool, PoolConfig};
use qail_core::ast::Qail;
use std::time::Instant;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[tokio::main]
#[allow(deprecated)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║  BATTLE TEST #6: The Thundering Herd 🐃                   ║");
    println!("╚═══════════════════════════════════════════════════════════╝\n");

    // 1. Create a tiny pool (Max 5 connections)
    let config = PoolConfig::new("localhost", 5432, "postgres", "postgres")
        .max_connections(5)
        .min_connections(1);
        
    let pool = PgPool::connect(config).await?;

    println!("1️⃣  Pool created (Max 5 connections).");
    
    let task_count = 100;
    let success_count = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let mut tasks = Vec::new();

    println!("2️⃣  Spawning {} tasks simultaneously...", task_count);

    for i in 0..task_count {
        let pool = pool.clone();
        let counter = success_count.clone();
        
        let task = tokio::spawn(async move {
            // Each task fights for a connection
            // If the Semaphore is broken, this acquire() will hang eventually.
            // Using expect here might panic if pool closed or timeout, which is a fail.
            let mut conn = pool.acquire_system().await.expect("Failed to acquire");
            
            // Do a fast query: SELECT 1
            // Use function-as-table syntax for robustness
            let q = Qail::get("generate_series(1,1)"); // Returns 1 row
            conn.pipeline_ast_fast(&[q]).await.expect("Query failed");
            
            counter.fetch_add(1, Ordering::SeqCst);
            
            // Print progress every 20 tasks
            if (i + 1) % 20 == 0 {
                use std::io::Write;
                print!(".");
                std::io::stdout().flush().unwrap();
            }
        });
        tasks.push(task);
    }

    // Await all
    for t in tasks {
        let _ = t.await;
    }
    println!("\n");

    let elapsed = start.elapsed();
    let final_count = success_count.load(Ordering::SeqCst);

    if final_count == task_count {
        println!("   ✅ PASS: Processed {} tasks in {:.2}s without deadlocking!", final_count, elapsed.as_secs_f32());
    } else {
        println!("   ❌ FAIL: Only processed {}/{} tasks. (Some hung or failed)", final_count, task_count);
        std::process::exit(1);
    }

    Ok(())
}
