//! QAIL-GO Bottleneck Analysis
//!
//! Profile individual components to find the bottleneck:
//! 1. QailCmd creation
//! 2. AST encoding
//! 3. batch_encode
//! 4. pipeline_ast_fast (I/O)
//!
//! Run:
//!   cargo run --release --bin qail_go_profile

use std::time::Instant;
use std::env;

const ITERATIONS: usize = 100_000;
const BATCH_SIZE: usize = 1000;

fn get_env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use qail_core::prelude::*;
    use qail_pg::protocol::AstEncoder;
    use qail_pg::PgConnection;

    println!("🔍 QAIL-GO BOTTLENECK ANALYSIS (Pure Rust)");
    println!("==========================================");
    println!("Iterations: {}", ITERATIONS);
    println!("Batch size: {}\n", BATCH_SIZE);

    // Test 1: QailCmd creation
    println!("📊 Test 1: QailCmd Creation");
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let mut cmd = QailCmd::get("harbors");
        cmd.columns.push(Expr::Named("id".to_string()));
        cmd.columns.push(Expr::Named("name".to_string()));
        let _ = cmd.limit(10);
    }
    let cmd_time = start.elapsed().as_nanos() as f64 / ITERATIONS as f64;
    println!("   Per command: {:.0}ns\n", cmd_time);

    // Test 2: Single command encoding
    println!("📊 Test 2: Single Command Encoding");
    let mut cmd = QailCmd::get("harbors");
    cmd.columns.push(Expr::Named("id".to_string()));
    cmd.columns.push(Expr::Named("name".to_string()));
    cmd = cmd.limit(10);
    
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let (_bytes, _params) = AstEncoder::encode_cmd(&cmd);
    }
    let encode_time = start.elapsed().as_nanos() as f64 / ITERATIONS as f64;
    println!("   Per encode: {:.0}ns\n", encode_time);

    // Test 3: Batch encoding (1000 commands)
    println!("📊 Test 3: Batch Encoding ({} commands)", BATCH_SIZE);
    let mut cmds = Vec::with_capacity(BATCH_SIZE);
    for i in 0..BATCH_SIZE {
        let mut cmd = QailCmd::get("harbors");
        cmd.columns.push(Expr::Named("id".to_string()));
        cmd.columns.push(Expr::Named("name".to_string()));
        cmd = cmd.limit((i % 10 + 1) as i64);
        cmds.push(cmd);
    }
    
    let batches = ITERATIONS / BATCH_SIZE;
    let start = Instant::now();
    for _ in 0..batches {
        let _ = AstEncoder::encode_batch(&cmds);
    }
    let batch_encode_time = start.elapsed().as_nanos() as f64 / batches as f64;
    let per_query_encode = batch_encode_time / BATCH_SIZE as f64;
    println!("   Per batch: {:.0}ns", batch_encode_time);
    println!("   Per query in batch: {:.0}ns\n", per_query_encode);

    // Test 4: Full pipeline with I/O (DB required)
    println!("📊 Test 4: Full Pipeline with I/O");
    let host = get_env_or("PG_HOST", "127.0.0.1");
    let port: u16 = get_env_or("PG_PORT", "5432").parse()?;
    let user = get_env_or("PG_USER", "postgres");
    let database = get_env_or("PG_DATABASE", "postgres");
    
    let conn_result = PgConnection::connect(&host, port, &user, &database).await;
    
    if let Ok(mut conn) = conn_result {
        let batch_count = 100;
        let start = Instant::now();
        for _ in 0..batch_count {
            let _ = conn.pipeline_ast_fast(&cmds).await?;
        }
        let io_time = start.elapsed().as_nanos() as f64 / batch_count as f64;
        let per_query_io = io_time / BATCH_SIZE as f64;
        
        println!("   Per batch (encode + I/O): {:.0}ns", io_time);
        println!("   Per query with I/O: {:.0}ns\n", per_query_io);
        
        // Calculate I/O vs encoding overhead
        let io_only = io_time - batch_encode_time;
        println!("   📌 I/O overhead only: {:.0}ns per batch", io_only);
        println!("   📌 I/O per query: {:.0}ns", io_only / BATCH_SIZE as f64);
    } else {
        println!("   ⚠️  Could not connect to DB, skipping I/O test");
    }

    // Summary
    println!("\n📈 SUMMARY:");
    println!("┌─────────────────────────────────────────────┐");
    println!("│ Component                    Time (ns)      │");
    println!("├─────────────────────────────────────────────┤");
    println!("│ QailCmd creation:           {:>10.0}      │", cmd_time);
    println!("│ Single encode:              {:>10.0}      │", encode_time);
    println!("│ Batch encode per query:     {:>10.0}      │", per_query_encode);
    println!("└─────────────────────────────────────────────┘");

    let theoretical_qps = 1e9 / per_query_encode;
    println!("\n🎯 Theoretical max (encode only): {:.0} q/s", theoretical_qps);
    println!("   pgx is ~250k q/s");

    Ok(())
}
