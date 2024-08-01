//! Profile blocking_recv overhead vs native async
//!
//! This tests the EXACT bottleneck: blocking_recv()

use std::time::Instant;
use tokio::sync::oneshot;

const ITERATIONS: usize = 100_000;

fn main() {
    println!("🔍 BLOCKING_RECV OVERHEAD ANALYSIS");
    println!("===================================\n");

    // Test 1: oneshot channel creation + blocking_recv
    println!("📊 Test 1: oneshot create + blocking_recv");
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let (tx, rx) = oneshot::channel::<i32>();
        rt.spawn(async move {
            let _ = tx.send(42);
        });
        let _ = rx.blocking_recv();
    }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    println!("   Per blocking_recv: {:.0}ns\n", per_op);

    // Test 2: Pure async (no blocking)
    println!("📊 Test 2: Pure async oneshot (no blocking)");
    let start = Instant::now();
    rt.block_on(async {
        for _ in 0..ITERATIONS {
            let (tx, rx) = oneshot::channel::<i32>();
            tokio::spawn(async move {
                let _ = tx.send(42);
            });
            let _ = rx.await;
        }
    });
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    println!("   Per async await: {:.0}ns\n", per_op);

    // Test 3: Just oneshot creation
    println!("📊 Test 3: Just oneshot creation (no send/recv)");
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let (_tx, _rx) = oneshot::channel::<i32>();
    }
    let elapsed = start.elapsed();
    let per_op = elapsed.as_nanos() as f64 / ITERATIONS as f64;
    println!("   Per oneshot create: {:.0}ns\n", per_op);

    println!("📈 CONCLUSION:");
    println!("If blocking_recv >> async await, that's the bottleneck!");
}
