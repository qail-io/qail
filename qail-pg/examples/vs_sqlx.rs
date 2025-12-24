//! Head-to-head benchmark: QAIL-PG vs SQLx
//!
//! Compares real database query performance against staging DB.
//!
//! Setup:
//!   ssh -L 15432:localhost:5432 postgres -N -f
//!
//! Run:
//!   STAGING_DB_PASSWORD="password" cargo run -p qail-pg --example vs_sqlx --release

use std::time::{Duration, Instant};

const ITERATIONS: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let password = std::env::var("STAGING_DB_PASSWORD")
        .expect("Set STAGING_DB_PASSWORD");
    
    println!("🏎️  QAIL-PG vs SQLx Benchmark");
    println!("==============================");
    println!("Database: testdb (via SSH tunnel)");
    println!("Iterations: {}\n", ITERATIONS);

    // ============ QAIL-PG ============
    println!("📊 QAIL-PG (Native Driver)");
    
    let qail_connect_start = Instant::now();
    let mut qail_conn = qail_pg::PgConnection::connect_with_password(
        "127.0.0.1", 15432, "postgres", "testdb", Some(&password)
    ).await?;
    let qail_connect_time = qail_connect_start.elapsed();
    println!("   Connect time: {:?}", qail_connect_time);
    
    // Warmup
    for _ in 0..10 {
        let _ = qail_conn.query("SELECT COUNT(*) FROM vessels", &[]).await?;
    }
    
    // Benchmark: Simple count query
    let qail_count_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _ = qail_conn.query("SELECT COUNT(*) FROM vessels", &[]).await?;
    }
    let qail_count_time = qail_count_start.elapsed();
    println!("   COUNT(*) × {}: {:?} ({:?}/iter)", 
        ITERATIONS, qail_count_time, qail_count_time / ITERATIONS as u32);
    
    // Benchmark: Parameterized query
    let qail_param_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _ = qail_conn.query(
            "SELECT id, name FROM vessels WHERE is_active = $1 LIMIT 10",
            &[Some(b"t".to_vec())]
        ).await?;
    }
    let qail_param_time = qail_param_start.elapsed();
    println!("   Parameterized × {}: {:?} ({:?}/iter)",
        ITERATIONS, qail_param_time, qail_param_time / ITERATIONS as u32);
    
    // Benchmark: Fetch rows
    let qail_fetch_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _ = qail_conn.query("SELECT id, name, is_active FROM vessels LIMIT 20", &[]).await?;
    }
    let qail_fetch_time = qail_fetch_start.elapsed();
    println!("   Fetch 20 rows × {}: {:?} ({:?}/iter)",
        ITERATIONS, qail_fetch_time, qail_fetch_time / ITERATIONS as u32);

    // ============ SQLx ============
    println!("\n📊 SQLx (For comparison)");
    
    let db_url = format!(
        "postgres://postgres:{}@127.0.0.1:15432/testdb",
        password
    );
    
    let sqlx_connect_start = Instant::now();
    let sqlx_pool = sqlx::PgPool::connect(&db_url).await?;
    let sqlx_connect_time = sqlx_connect_start.elapsed();
    println!("   Connect time: {:?}", sqlx_connect_time);
    
    // Warmup
    for _ in 0..10 {
        let _: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM vessels")
            .fetch_one(&sqlx_pool).await?;
    }
    
    // Benchmark: Simple count query
    let sqlx_count_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM vessels")
            .fetch_one(&sqlx_pool).await?;
    }
    let sqlx_count_time = sqlx_count_start.elapsed();
    println!("   COUNT(*) × {}: {:?} ({:?}/iter)",
        ITERATIONS, sqlx_count_time, sqlx_count_time / ITERATIONS as u32);
    
    // Benchmark: Parameterized query
    let sqlx_param_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _: Vec<(uuid::Uuid, String)> = sqlx::query_as(
            "SELECT id, name FROM vessels WHERE is_active = $1 LIMIT 10"
        )
            .bind(true)
            .fetch_all(&sqlx_pool).await?;
    }
    let sqlx_param_time = sqlx_param_start.elapsed();
    println!("   Parameterized × {}: {:?} ({:?}/iter)",
        ITERATIONS, sqlx_param_time, sqlx_param_time / ITERATIONS as u32);
    
    // Benchmark: Fetch rows
    let sqlx_fetch_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _: Vec<(uuid::Uuid, String, bool)> = sqlx::query_as(
            "SELECT id, name, is_active FROM vessels LIMIT 20"
        )
            .fetch_all(&sqlx_pool).await?;
    }
    let sqlx_fetch_time = sqlx_fetch_start.elapsed();
    println!("   Fetch 20 rows × {}: {:?} ({:?}/iter)",
        ITERATIONS, sqlx_fetch_time, sqlx_fetch_time / ITERATIONS as u32);

    // ============ Summary ============
    println!("\n📈 Summary:");
    println!("┌─────────────────────┬──────────┬──────────┬─────────┐");
    println!("│ Test                │ QAIL-PG  │ SQLx     │ Winner  │");
    println!("├─────────────────────┼──────────┼──────────┼─────────┤");
    
    print_row("Connect", qail_connect_time, sqlx_connect_time);
    print_row("COUNT(*)", qail_count_time, sqlx_count_time);
    print_row("Parameterized", qail_param_time, sqlx_param_time);
    print_row("Fetch 20 rows", qail_fetch_time, sqlx_fetch_time);
    
    println!("└─────────────────────┴──────────┴──────────┴─────────┘");

    Ok(())
}

fn print_row(name: &str, qail: Duration, sqlx: Duration) {
    let winner = if qail < sqlx { "QAIL" } else { "SQLx" };
    let ratio = if qail < sqlx {
        format!("{:.1}x", sqlx.as_nanos() as f64 / qail.as_nanos() as f64)
    } else {
        format!("{:.1}x", qail.as_nanos() as f64 / sqlx.as_nanos() as f64)
    };
    println!("│ {:19} │ {:>8.1?} │ {:>8.1?} │ {:>7} │",
        name, 
        qail.as_millis(), 
        sqlx.as_millis(),
        format!("{} {}", winner, ratio)
    );
}
