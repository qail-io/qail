//! Chaos Test — Engine API Handler vs Qail
//!
//! Simulates real production load by running the SAME query used by
//! `list_harbors_public` endpoint (2×LEFT JOIN + correlated subqueries)
//! under concurrent stress.
//!
//! Tests three approaches using ONLY public PgDriver API:
//!   1. fetch_raw       — Raw SQL, Parse+Execute every call (like SQLx default)
//!   2. fetch_all_cached — Qail AST, prepared statement (Parse once, Bind+Execute after)
//!   3. fetch_all_uncached — Qail AST, full Parse+Bind+Execute every call
//!
//! The "chaos" element: N concurrent workers each hammering the DB
//! with back-to-back queries, measuring latency under contention.
//!
//! Run:
//!   DATABASE_URL=postgresql://qail_user@localhost:5432/qail_test \
//!     cargo run --example chaos_test --features chrono,uuid --release
//!
//! WARNING: This creates real DB load. Do NOT run against production.

use qail_core::prelude::*;
use qail_core::ast::{JoinKind, Operator, SortOrder};
use qail_pg::PgDriver;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use std::sync::Arc;

// ==================== CONFIG ====================

const WORKERS: usize = 10;          // Concurrent connections
const ITERATIONS: usize = 200;      // Queries per worker
const WARMUP: usize = 5;            // Warmup iterations (excluded from stats)

/// The EXACT SQL from PostgresHarborRepository::list_active()
const LIST_ACTIVE_SQL: &str = r"
SELECT 
    h.id,
    h.name,
    h.slug,
    h.is_active,
    dh.destination_id,
    d.name as destination_name,
    (h.photo_url IS NOT NULL OR EXISTS (
        SELECT 1 FROM harbor_images hi
        WHERE hi.harbor_id = h.id AND hi.deleted_at IS NULL
    )) as has_photo,
    COALESCE(
        (
            SELECT hi.image_url
            FROM harbor_images hi
            WHERE hi.harbor_id = h.id
              AND hi.deleted_at IS NULL
            ORDER BY hi.is_featured DESC, hi.updated_at DESC
            LIMIT 1
        ),
        h.photo_url
    ) as featured_image
FROM harbors h
LEFT JOIN destination_harbors dh ON h.id = dh.harbor_id
LEFT JOIN destinations d ON dh.destination_id = d.id
WHERE h.is_active = true
ORDER BY h.position ASC, h.name ASC
";

// ==================== STATS ====================

#[derive(Clone)]
struct LatencyStats {
    label: String,
    latencies: Vec<Duration>,
    row_counts: Vec<usize>,
    errors: usize,
    error_msgs: Vec<String>,
}

impl LatencyStats {
    fn new(label: &str) -> Self {
        Self {
            label: label.to_string(),
            latencies: Vec::with_capacity(ITERATIONS),
            row_counts: Vec::with_capacity(ITERATIONS),
            errors: 0,
            error_msgs: Vec::new(),
        }
    }

    fn record(&mut self, d: Duration, rows: usize) {
        self.latencies.push(d);
        self.row_counts.push(rows);
    }

    fn record_error(&mut self, msg: String) {
        self.errors += 1;
        if self.error_msgs.len() < 3 {
            self.error_msgs.push(msg);
        }
    }
}

struct AggregateStats {
    label: String,
    total_queries: usize,
    total_errors: usize,
    avg: Duration,
    median: Duration,
    p95: Duration,
    p99: Duration,
    min: Duration,
    max: Duration,
    avg_rows: f64,
    throughput_qps: f64,
    total_time: Duration,
    sample_errors: Vec<String>,
}

fn aggregate(workers: &[LatencyStats], wall_clock: Duration) -> AggregateStats {
    let label = workers[0].label.clone();
    let mut all_latencies: Vec<Duration> = workers.iter()
        .flat_map(|w| w.latencies.iter().copied())
        .collect();
    let total_rows: usize = workers.iter()
        .flat_map(|w| w.row_counts.iter())
        .sum();
    let total_errors: usize = workers.iter().map(|w| w.errors).sum();
    let sample_errors: Vec<String> = workers.iter()
        .flat_map(|w| w.error_msgs.iter().cloned())
        .take(3)
        .collect();
    
    all_latencies.sort();
    let total = all_latencies.len();
    
    if total == 0 {
        return AggregateStats {
            label, total_queries: 0, total_errors, avg: Duration::ZERO, median: Duration::ZERO,
            p95: Duration::ZERO, p99: Duration::ZERO, min: Duration::ZERO, max: Duration::ZERO,
            avg_rows: 0.0, throughput_qps: 0.0, total_time: wall_clock, sample_errors,
        };
    }

    let sum: Duration = all_latencies.iter().sum();
    let avg = sum / total as u32;
    let median = all_latencies[total / 2];
    let p95 = all_latencies[std::cmp::min((total as f64 * 0.95) as usize, total - 1)];
    let p99 = all_latencies[std::cmp::min((total as f64 * 0.99) as usize, total - 1)];
    let min = all_latencies[0];
    let max = all_latencies[total - 1];
    let avg_rows = total_rows as f64 / total as f64;
    let throughput_qps = total as f64 / wall_clock.as_secs_f64();

    AggregateStats {
        label, total_queries: total, total_errors, avg, median, p95, p99, min, max,
        avg_rows, throughput_qps, total_time: wall_clock, sample_errors,
    }
}

fn print_stats(s: &AggregateStats) {
    println!("\n━━━ {} ━━━", s.label);
    println!("  Queries: {}  │  Errors: {}  │  Avg rows: {:.0}", s.total_queries, s.total_errors, s.avg_rows);
    println!("  Avg: {:?}  │  Median: {:?}", s.avg, s.median);
    println!("  p95: {:?}  │  p99: {:?}", s.p95, s.p99);
    println!("  Min: {:?}  │  Max: {:?}", s.min, s.max);
    println!("  Throughput: {:.0} QPS  │  Wall clock: {:?}", s.throughput_qps, s.total_time);
    if !s.sample_errors.is_empty() {
        println!("  ⚠️  Sample errors:");
        for e in &s.sample_errors {
            println!("     • {}", e);
        }
    }
}

// ==================== QAIL AST QUERY ====================

/// Build a Qail AST query approximating list_active
/// (uses JOINs but cannot express correlated subqueries — tests the AST path)
fn build_harbor_list_query() -> Qail {
    Qail::get("harbors")
        .columns(vec![
            "harbors.id",
            "harbors.name",
            "harbors.slug",
            "harbors.is_active",
        ])
        .join(JoinKind::Left, "destination_harbors", "harbors.id", "destination_harbors.harbor_id")
        .join(JoinKind::Left, "destinations", "destination_harbors.destination_id", "destinations.id")
        .column("destination_harbors.destination_id")
        .column("destinations.name AS destination_name")
        .filter("harbors.is_active", Operator::Eq, Value::Bool(true))
        .order_by("harbors.name", SortOrder::Asc)
}

// ==================== WORKER FUNCTIONS ====================

async fn connect(db_url: &str) -> PgDriver {
    let mut driver = PgDriver::connect_url(db_url).await
        .expect("Failed to connect");
    // Bypass RLS for fair benchmark comparison
    driver.execute_raw("SET app.is_super_admin = 'true'").await.ok();
    driver
}

/// Worker 1: fetch_raw — Raw SQL, Simple Query Protocol (Parse every time)
/// This simulates what SQLx does by default.
async fn worker_fetch_raw(db_url: String, barrier: Arc<Barrier>) -> LatencyStats {
    let mut driver = connect(&db_url).await;
    let mut stats = LatencyStats::new("fetch_raw (Parse+Execute every call — like SQLx)");

    // Warmup
    for _ in 0..WARMUP {
        let _ = driver.fetch_raw(LIST_ACTIVE_SQL).await;
    }

    barrier.wait().await;

    for _ in 0..ITERATIONS {
        let start = Instant::now();
        match driver.fetch_raw(LIST_ACTIVE_SQL).await {
            Ok(rows) => stats.record(start.elapsed(), rows.len()),
            Err(e) => stats.record_error(format!("{e}")),
        }
    }
    stats
}

/// Worker 2: Qail AST — fetch_all_cached (prepared statement, Parse once)
async fn worker_qail_cached(db_url: String, barrier: Arc<Barrier>) -> LatencyStats {
    let mut driver = connect(&db_url).await;
    let cmd = build_harbor_list_query();
    let mut stats = LatencyStats::new("Qail AST fetch_all_cached (prepared, Bind+Execute only)");

    for _ in 0..WARMUP {
        let _ = driver.fetch_all_cached(&cmd).await;
    }

    barrier.wait().await;

    for _ in 0..ITERATIONS {
        let start = Instant::now();
        match driver.fetch_all_cached(&cmd).await {
            Ok(rows) => stats.record(start.elapsed(), rows.len()),
            Err(e) => stats.record_error(format!("{e}")),
        }
    }
    stats
}

/// Worker 3: Qail AST — fetch_all_uncached (Parse+Bind+Execute every call)
async fn worker_qail_uncached(db_url: String, barrier: Arc<Barrier>) -> LatencyStats {
    let mut driver = connect(&db_url).await;
    let cmd = build_harbor_list_query();
    let mut stats = LatencyStats::new("Qail AST fetch_all_uncached (Parse+Bind+Execute every call)");

    for _ in 0..WARMUP {
        let _ = driver.fetch_all_uncached(&cmd).await;
    }

    barrier.wait().await;

    for _ in 0..ITERATIONS {
        let start = Instant::now();
        match driver.fetch_all_uncached(&cmd).await {
            Ok(rows) => stats.record(start.elapsed(), rows.len()),
            Err(e) => stats.record_error(format!("{e}")),
        }
    }
    stats
}

// ==================== RUNNER ====================

async fn run_test<F, Fut>(
    db_url: &str,
    worker_fn: F,
) -> AggregateStats
where
    F: Fn(String, Arc<Barrier>) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = LatencyStats> + Send + 'static,
{
    let barrier = Arc::new(Barrier::new(WORKERS));
    let mut handles = Vec::new();

    let wall_start = Instant::now();
    for _ in 0..WORKERS {
        let url = db_url.to_string();
        let b = barrier.clone();
        let f = worker_fn.clone();
        handles.push(tokio::spawn(f(url, b)));
    }

    let mut results = Vec::new();
    for handle in handles {
        results.push(handle.await.unwrap());
    }
    let wall_time = wall_start.elapsed();

    let stats = aggregate(&results, wall_time);
    print_stats(&stats);
    stats
}

// ==================== MAIN ====================

#[tokio::main]
async fn main() {
    let db_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║          CHAOS TEST — Engine API Handler vs Qail               ║");
    println!("║                                                                ║");
    println!("║  Query: list_harbors_public (2×LEFT JOIN + subqueries)         ║");
    println!("║  Workers: {:>2}  │  Iters/worker: {:>4}  │  Warmup: {:>2}           ║", WORKERS, ITERATIONS, WARMUP);
    println!("║  Total queries per test: {:>5}                                 ║", WORKERS * ITERATIONS);
    println!("╚══════════════════════════════════════════════════════════════════╝");

    // Pre-flight check
    {
        let mut driver = connect(&db_url).await;
        let rows = driver.fetch_raw(LIST_ACTIVE_SQL).await.unwrap();
        println!("\n✓ Connected — list_active returns {} rows", rows.len());
    }

    println!("\n🔥 Starting chaos attack...\n");

    // ===== Test 1: Raw SQL (simple query, Parse every time) =====
    let s1 = run_test(&db_url, worker_fetch_raw).await;

    // ===== Test 2: Qail AST (fetch_all_cached, prepared) =====
    let s2 = run_test(&db_url, worker_qail_cached).await;

    // ===== Test 3: Qail AST (fetch_all_uncached, Parse every time) =====
    let s3 = run_test(&db_url, worker_qail_uncached).await;

    // ===== Summary =====
    println!("\n╔══════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║  RESULTS  (sorted by throughput, highest first)                                        ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════════════════╣");
    
    let mut all = vec![&s1, &s2, &s3];
    all.sort_by(|a, b| b.throughput_qps.partial_cmp(&a.throughput_qps).unwrap());
    
    for (i, s) in all.iter().enumerate() {
        let multiplier = if i == 0 {
            "baseline".to_string()
        } else {
            format!("{:.1}× slower", all[0].throughput_qps / s.throughput_qps)
        };
        println!("║  {}. {:55}  {:>6.0} QPS  med {:>10?}  p99 {:>10?}  {} ║",
            i + 1,
            &s.label[..s.label.len().min(55)],
            s.throughput_qps,
            s.median,
            s.p99,
            multiplier,
        );
    }
    
    println!("╠══════════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║  {} workers × {} iters × 3 tests = {} total queries                             ║", WORKERS, ITERATIONS, WORKERS * ITERATIONS * 3);
    println!("║                                                                                        ║");
    println!("║  WHAT TO LOOK FOR:                                                                     ║");
    println!("║  • p99 spikes → contention under concurrent load                                      ║");
    println!("║  • Error rate → connection limits, deadlocks, timeouts                                 ║");
    println!("║  • QPS ceiling → max throughput before degradation                                     ║");
    println!("║  • Prepared vs Uncached → how much does stmt caching help under pressure?              ║");
    println!("╚══════════════════════════════════════════════════════════════════════════════════════════╝");
}
