//! Head-to-head benchmark: QAIL-PG vs SQLx vs SeaORM
//!
//! Tests raw query performance against real staging database.
//!
//! Setup:
//!   ssh -L 15432:localhost:5432 sailtix -N -f
//!
//! Run:
//!   STAGING_DB_PASSWORD="password" cargo run -p qail-pg --example vs_orms --release

use std::time::{Duration, Instant};

const ITERATIONS: usize = 100;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let password = std::env::var("STAGING_DB_PASSWORD")
        .expect("Set STAGING_DB_PASSWORD");
    
    println!("🏎️  QAIL-PG vs SQLx vs SeaORM Benchmark");
    println!("========================================");
    println!("Database: swb-staging (via SSH tunnel)");
    println!("Iterations: {}\n", ITERATIONS);

    let db_url = format!(
        "postgres://sailtix:{}@127.0.0.1:15432/swb-staging",
        password
    );

    // ============ QAIL-PG ============
    println!("📊 QAIL-PG (Native Wire Protocol)");
    
    let qail_connect_start = Instant::now();
    let mut qail_conn = qail_pg::PgConnection::connect_with_password(
        "127.0.0.1", 15432, "sailtix", "swb-staging", Some(&password)
    ).await?;
    let qail_connect_time = qail_connect_start.elapsed();
    println!("   Connect: {:?}", qail_connect_time);
    
    // Warmup
    for _ in 0..10 {
        let _ = qail_conn.query("SELECT COUNT(*) FROM vessels", &[]).await?;
    }
    
    let qail_count_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _ = qail_conn.query("SELECT COUNT(*) FROM vessels", &[]).await?;
    }
    let qail_count_time = qail_count_start.elapsed();
    println!("   COUNT(*): {:?} ({:?}/iter)", 
        qail_count_time, qail_count_time / ITERATIONS as u32);
    
    let qail_fetch_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _ = qail_conn.query("SELECT id, name, is_active FROM vessels LIMIT 20", &[]).await?;
    }
    let qail_fetch_time = qail_fetch_start.elapsed();
    println!("   Fetch 20: {:?} ({:?}/iter)",
        qail_fetch_time, qail_fetch_time / ITERATIONS as u32);

    // ============ SQLx ============
    println!("\n📊 SQLx (Raw Driver)");
    
    let sqlx_connect_start = Instant::now();
    let sqlx_pool = sqlx::PgPool::connect(&db_url).await?;
    let sqlx_connect_time = sqlx_connect_start.elapsed();
    println!("   Connect: {:?}", sqlx_connect_time);
    
    for _ in 0..10 {
        let _: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM vessels")
            .fetch_one(&sqlx_pool).await?;
    }
    
    let sqlx_count_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM vessels")
            .fetch_one(&sqlx_pool).await?;
    }
    let sqlx_count_time = sqlx_count_start.elapsed();
    println!("   COUNT(*): {:?} ({:?}/iter)",
        sqlx_count_time, sqlx_count_time / ITERATIONS as u32);
    
    let sqlx_fetch_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _: Vec<(uuid::Uuid, String, bool)> = sqlx::query_as(
            "SELECT id, name, is_active FROM vessels LIMIT 20"
        ).fetch_all(&sqlx_pool).await?;
    }
    let sqlx_fetch_time = sqlx_fetch_start.elapsed();
    println!("   Fetch 20: {:?} ({:?}/iter)",
        sqlx_fetch_time, sqlx_fetch_time / ITERATIONS as u32);

    // ============ SeaORM (uses SQLx under the hood) ============
    println!("\n📊 SeaORM (ORM layer over SQLx)");
    
    use sea_orm::{Database, DatabaseConnection, Statement, ConnectionTrait, DbBackend};
    
    let seaorm_connect_start = Instant::now();
    let seaorm_db: DatabaseConnection = Database::connect(&db_url).await?;
    let seaorm_connect_time = seaorm_connect_start.elapsed();
    println!("   Connect: {:?}", seaorm_connect_time);
    
    // Warmup
    for _ in 0..10 {
        let _ = seaorm_db.query_one(Statement::from_string(
            DbBackend::Postgres, "SELECT COUNT(*) FROM vessels"
        )).await?;
    }
    
    let seaorm_count_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _ = seaorm_db.query_one(Statement::from_string(
            DbBackend::Postgres, "SELECT COUNT(*) FROM vessels"
        )).await?;
    }
    let seaorm_count_time = seaorm_count_start.elapsed();
    println!("   COUNT(*): {:?} ({:?}/iter)",
        seaorm_count_time, seaorm_count_time / ITERATIONS as u32);
    
    let seaorm_fetch_start = Instant::now();
    for _ in 0..ITERATIONS {
        let _ = seaorm_db.query_all(Statement::from_string(
            DbBackend::Postgres, "SELECT id, name, is_active FROM vessels LIMIT 20"
        )).await?;
    }
    let seaorm_fetch_time = seaorm_fetch_start.elapsed();
    println!("   Fetch 20: {:?} ({:?}/iter)",
        seaorm_fetch_time, seaorm_fetch_time / ITERATIONS as u32);

    // ============ Summary ============
    println!("\n📈 Summary (lower is better):");
    println!("┌────────────────┬──────────┬──────────┬──────────┬───────────────┐");
    println!("│ Test           │ QAIL-PG  │ SQLx     │ SeaORM   │ Winner        │");
    println!("├────────────────┼──────────┼──────────┼──────────┼───────────────┤");
    
    print_row3("Connect", qail_connect_time, sqlx_connect_time, seaorm_connect_time);
    print_row3("COUNT(*)", qail_count_time, sqlx_count_time, seaorm_count_time);
    print_row3("Fetch 20", qail_fetch_time, sqlx_fetch_time, seaorm_fetch_time);
    
    println!("└────────────────┴──────────┴──────────┴──────────┴───────────────┘");
    
    // Calculate totals
    let qail_total = qail_count_time + qail_fetch_time;
    let sqlx_total = sqlx_count_time + sqlx_fetch_time;
    let seaorm_total = seaorm_count_time + seaorm_fetch_time;
    
    println!("\n🏆 Total Query Time:");
    println!("   QAIL-PG: {:?}", qail_total);
    println!("   SQLx:    {:?} ({:.1}x slower)", sqlx_total, sqlx_total.as_nanos() as f64 / qail_total.as_nanos() as f64);
    println!("   SeaORM:  {:?} ({:.1}x slower)", seaorm_total, seaorm_total.as_nanos() as f64 / qail_total.as_nanos() as f64);

    Ok(())
}

fn print_row3(name: &str, qail: Duration, sqlx: Duration, seaorm: Duration) {
    let times = [("QAIL", qail), ("SQLx", sqlx), ("SeaORM", seaorm)];
    let min = times.iter().min_by_key(|(_, d)| *d).unwrap();
    
    println!("│ {:14} │ {:>6}ms │ {:>6}ms │ {:>6}ms │ {:>13} │",
        name, 
        qail.as_millis(), 
        sqlx.as_millis(),
        seaorm.as_millis(),
        format!("{}", min.0)
    );
}
