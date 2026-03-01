//! Qail vs GraphQL vs REST — Real Complex Query Benchmark
//!
//! Compares SIX approaches to the same complex query:
//! "Get all enabled ferry connections with origin/dest harbor names,
//!  operator name — 3×LEFT JOIN, filtered, sorted"
//!
//! Approach 1: Qail AST — Prepared (cached prepared statement, skips Parse)
//! Approach 2: Qail AST — Uncached (Parse+Bind+Execute every call)
//! Approach 3: GraphQL NAIVE (resolver pattern — N+1 queries)
//! Approach 4: GraphQL + DataLoader (batched IN queries — ~4 queries)
//! Approach 5: REST NAIVE (multiple sequential HTTP-like calls + JSON)
//! Approach 6: REST + expand (server-side JOIN — 1 query + JSON overhead)
//!
//! Run:
//!   DATABASE_URL=postgresql://qail_user@localhost:5432/qail_test \
//!     cargo run --example battle_comparison --features chrono,uuid --release
//!
//! Security note: Qail enforces RLS at the protocol level. GraphQL/REST
//! must bolt it on at the application layer. We set app.is_super_admin=true
//! for this benchmark to bypass RLS so timing is fair.

use qail_core::ast::{JoinKind, Operator, SortOrder};
use qail_core::prelude::*;
use qail_pg::PgDriver;
use std::collections::HashSet;
use std::time::{Duration, Instant};

const ITERATIONS: usize = 1000;
const WARMUP: usize = 5;

/// Build the canonical 3×JOIN query used by both Qail and REST+expand.
fn build_join_query() -> Qail {
    Qail::get("odyssey_connections")
        .columns(vec![
            "odyssey_connections.id",
            "odyssey_connections.name",
            "odyssey_connections.description",
            "odyssey_connections.is_enabled",
            "odyssey_connections.created_at",
        ])
        .join(
            JoinKind::Left,
            "harbors AS origin",
            "odyssey_connections.origin_harbor_id",
            "origin.id",
        )
        .join(
            JoinKind::Left,
            "harbors AS dest",
            "odyssey_connections.destination_harbor_id",
            "dest.id",
        )
        .join(
            JoinKind::Left,
            "operators",
            "odyssey_connections.operator_id",
            "operators.id",
        )
        .column("origin.name AS origin_harbor")
        .column("dest.name AS dest_harbor")
        .column("operators.brand_name AS operator_name")
        .filter(
            "odyssey_connections.is_enabled",
            Operator::Eq,
            Value::Bool(true),
        )
        .order_by("odyssey_connections.name", SortOrder::Asc)
        .limit(50)
}

/// Calculate median from a slice of durations.
fn median(times: &mut [Duration]) -> Duration {
    times.sort();
    let mid = times.len() / 2;
    if times.len().is_multiple_of(2) {
        (times[mid - 1] + times[mid]) / 2
    } else {
        times[mid]
    }
}

/// Calculate p99 from a sorted slice.
fn p99(times: &mut [Duration]) -> Duration {
    times.sort();
    let idx = (times.len() as f64 * 0.99) as usize;
    times[idx.min(times.len() - 1)]
}

// ============================================================================
// Approach 1: Qail AST — PREPARED (cached, skips Parse on repeat calls)
// This is how Qail runs in production: prepared statement auto-caching.
// ============================================================================

async fn run_qail_prepared(
    driver: &mut PgDriver,
) -> Result<(usize, Duration, Vec<Duration>), Box<dyn std::error::Error>> {
    let cmd = build_join_query();

    for _ in 0..WARMUP {
        let _ = driver.fetch_all_cached(&cmd).await?;
    }

    let mut per_iter: Vec<Duration> = Vec::with_capacity(ITERATIONS);
    let mut row_count = 0;

    for _ in 0..ITERATIONS {
        let t = Instant::now();
        let rows = driver.fetch_all_cached(&cmd).await?;
        per_iter.push(t.elapsed());
        row_count = rows.len();
    }

    let total: Duration = per_iter.iter().sum();
    Ok((row_count, total, per_iter))
}

// ============================================================================
// Approach 2: Qail AST — UNCACHED (Parse+Bind+Execute every call)
// ============================================================================

async fn run_qail_uncached(
    driver: &mut PgDriver,
) -> Result<(usize, Duration, Vec<Duration>), Box<dyn std::error::Error>> {
    let cmd = build_join_query();

    for _ in 0..WARMUP {
        let _ = driver.fetch_all_uncached(&cmd).await?;
    }

    let mut per_iter: Vec<Duration> = Vec::with_capacity(ITERATIONS);
    let mut row_count = 0;

    for _ in 0..ITERATIONS {
        let t = Instant::now();
        let rows = driver.fetch_all_uncached(&cmd).await?;
        per_iter.push(t.elapsed());
        row_count = rows.len();
    }

    let total: Duration = per_iter.iter().sum();
    Ok((row_count, total, per_iter))
}

// ============================================================================
// Approach 3: GraphQL NAIVE — N+1 resolver pattern (worst case)
// ============================================================================

async fn run_graphql_naive(
    driver: &mut PgDriver,
) -> Result<(usize, Duration, usize, Vec<Duration>), Box<dyn std::error::Error>> {
    let root_cmd = Qail::get("odyssey_connections")
        .filter("is_enabled", Operator::Eq, Value::Bool(true))
        .order_by("name", SortOrder::Asc)
        .limit(50);

    for _ in 0..WARMUP {
        let connections = driver.fetch_all_uncached(&root_cmd).await?;
        for conn in &connections {
            let origin_id = conn.text(2);
            let dest_id = conn.text(3);
            let op_id = conn.get_string(8);
            let _ = driver
                .fetch_all_uncached(
                    &Qail::get("harbors")
                        .filter("id", Operator::Eq, Value::String(origin_id))
                        .limit(1),
                )
                .await?;
            let _ = driver
                .fetch_all_uncached(
                    &Qail::get("harbors")
                        .filter("id", Operator::Eq, Value::String(dest_id))
                        .limit(1),
                )
                .await?;
            if let Some(oid) = op_id {
                let _ = driver
                    .fetch_all_uncached(
                        &Qail::get("operators")
                            .filter("id", Operator::Eq, Value::String(oid))
                            .limit(1),
                    )
                    .await?;
            }
        }
    }

    let mut per_iter: Vec<Duration> = Vec::with_capacity(ITERATIONS);
    let mut row_count = 0;
    let mut total_queries: usize = 0;

    for _ in 0..ITERATIONS {
        let t = Instant::now();

        let connections = driver.fetch_all_uncached(&root_cmd).await?;
        row_count = connections.len();
        total_queries += 1;

        for conn in &connections {
            let origin_id = conn.text(2);
            let dest_id = conn.text(3);
            let op_id = conn.get_string(8);

            let _ = driver
                .fetch_all_uncached(
                    &Qail::get("harbors")
                        .filter("id", Operator::Eq, Value::String(origin_id))
                        .limit(1),
                )
                .await?;
            total_queries += 1;

            let _ = driver
                .fetch_all_uncached(
                    &Qail::get("harbors")
                        .filter("id", Operator::Eq, Value::String(dest_id))
                        .limit(1),
                )
                .await?;
            total_queries += 1;

            if let Some(oid) = op_id {
                let _ = driver
                    .fetch_all_uncached(
                        &Qail::get("operators")
                            .filter("id", Operator::Eq, Value::String(oid))
                            .limit(1),
                    )
                    .await?;
                total_queries += 1;
            }
        }
        per_iter.push(t.elapsed());
    }
    let total: Duration = per_iter.iter().sum();
    Ok((row_count, total, total_queries, per_iter))
}

// ============================================================================
// Approach 4: GraphQL + DataLoader — batched IN queries (realistic)
// ============================================================================

async fn run_graphql_dataloader(
    driver: &mut PgDriver,
) -> Result<(usize, Duration, usize, Vec<Duration>), Box<dyn std::error::Error>> {
    let root_cmd = Qail::get("odyssey_connections")
        .filter("is_enabled", Operator::Eq, Value::Bool(true))
        .order_by("name", SortOrder::Asc)
        .limit(50);

    for _ in 0..WARMUP {
        let connections = driver.fetch_all_uncached(&root_cmd).await?;
        let mut harbor_ids = HashSet::new();
        let mut operator_ids = HashSet::new();
        for conn in &connections {
            harbor_ids.insert(conn.text(2));
            harbor_ids.insert(conn.text(3));
            if let Some(oid) = conn.get_string(8) {
                operator_ids.insert(oid);
            }
        }
        let harbor_ids_list: Vec<String> = harbor_ids.into_iter().collect();
        let _ = driver
            .fetch_all_uncached(
                &Qail::get("harbors").filter(
                    "id",
                    Operator::In,
                    Value::Array(
                        harbor_ids_list
                            .iter()
                            .map(|s| Value::String(s.clone()))
                            .collect(),
                    ),
                ),
            )
            .await?;
        let op_ids_list: Vec<String> = operator_ids.into_iter().collect();
        if !op_ids_list.is_empty() {
            let _ = driver
                .fetch_all_uncached(
                    &Qail::get("operators").filter(
                        "id",
                        Operator::In,
                        Value::Array(
                            op_ids_list
                                .iter()
                                .map(|s| Value::String(s.clone()))
                                .collect(),
                        ),
                    ),
                )
                .await?;
        }
    }

    let mut per_iter: Vec<Duration> = Vec::with_capacity(ITERATIONS);
    let mut row_count = 0;
    let mut total_queries: usize = 0;

    for _ in 0..ITERATIONS {
        let t = Instant::now();

        let connections = driver.fetch_all_uncached(&root_cmd).await?;
        row_count = connections.len();
        total_queries += 1;

        let mut harbor_ids = HashSet::new();
        let mut operator_ids = HashSet::new();
        for conn in &connections {
            harbor_ids.insert(conn.text(2));
            harbor_ids.insert(conn.text(3));
            if let Some(oid) = conn.get_string(8) {
                operator_ids.insert(oid);
            }
        }

        let harbor_ids_list: Vec<String> = harbor_ids.into_iter().collect();
        let _ = driver
            .fetch_all_uncached(
                &Qail::get("harbors").filter(
                    "id",
                    Operator::In,
                    Value::Array(
                        harbor_ids_list
                            .iter()
                            .map(|s| Value::String(s.clone()))
                            .collect(),
                    ),
                ),
            )
            .await?;
        total_queries += 1;

        let op_ids_list: Vec<String> = operator_ids.into_iter().collect();
        if !op_ids_list.is_empty() {
            let _ = driver
                .fetch_all_uncached(
                    &Qail::get("operators").filter(
                        "id",
                        Operator::In,
                        Value::Array(
                            op_ids_list
                                .iter()
                                .map(|s| Value::String(s.clone()))
                                .collect(),
                        ),
                    ),
                )
                .await?;
            total_queries += 1;
        }
        per_iter.push(t.elapsed());
    }

    let total: Duration = per_iter.iter().sum();
    Ok((row_count, total, total_queries, per_iter))
}

// ============================================================================
// Approach 5: REST NAIVE — sequential calls + JSON serialization (worst case)
// ============================================================================

async fn run_rest_naive(
    driver: &mut PgDriver,
) -> Result<(usize, Duration, usize, Vec<Duration>), Box<dyn std::error::Error>> {
    let root_cmd = Qail::get("odyssey_connections")
        .filter("is_enabled", Operator::Eq, Value::Bool(true))
        .order_by("name", SortOrder::Asc)
        .limit(50);

    for _ in 0..WARMUP {
        let rows = driver.fetch_all_uncached(&root_cmd).await?;
        for r in &rows {
            let _ = format!("{{\"id\":\"{}\",\"name\":\"{}\"}}", r.text(0), r.text(4));
        }
    }

    let mut per_iter: Vec<Duration> = Vec::with_capacity(ITERATIONS);
    let mut row_count = 0;
    let mut total_queries: usize = 0;

    for _ in 0..ITERATIONS {
        let t = Instant::now();

        let connections = driver.fetch_all_uncached(&root_cmd).await?;
        row_count = connections.len();
        total_queries += 1;

        let mut conn_data: Vec<String> = Vec::with_capacity(connections.len());
        for conn in &connections {
            conn_data.push(format!(
                "{{\"id\":\"{}\",\"odyssey_id\":\"{}\",\"name\":\"{}\"}}",
                conn.text(0),
                conn.text(1),
                conn.text(4)
            ));
        }

        for conn in &connections {
            let origin_id = conn.text(2);
            let dest_id = conn.text(3);
            let op_id = conn.get_string(8);

            let origin_rows = driver
                .fetch_all_uncached(
                    &Qail::get("harbors")
                        .filter("id", Operator::Eq, Value::String(origin_id))
                        .limit(1),
                )
                .await?;
            total_queries += 1;
            if let Some(h) = origin_rows.first() {
                let _ = format!("{{\"name\":\"{}\"}}", h.text(1));
            }

            let dest_rows = driver
                .fetch_all_uncached(
                    &Qail::get("harbors")
                        .filter("id", Operator::Eq, Value::String(dest_id))
                        .limit(1),
                )
                .await?;
            total_queries += 1;
            if let Some(h) = dest_rows.first() {
                let _ = format!("{{\"name\":\"{}\"}}", h.text(1));
            }

            if let Some(oid) = op_id {
                let _ = driver
                    .fetch_all_uncached(
                        &Qail::get("operators")
                            .filter("id", Operator::Eq, Value::String(oid))
                            .limit(1),
                    )
                    .await?;
                total_queries += 1;
            }
        }
        per_iter.push(t.elapsed());
    }
    let total: Duration = per_iter.iter().sum();
    Ok((row_count, total, total_queries, per_iter))
}

// ============================================================================
// Approach 6: REST + ?expand= — server-side JOIN (realistic/optimized)
// ============================================================================

async fn run_rest_expand(
    driver: &mut PgDriver,
) -> Result<(usize, Duration, Vec<Duration>), Box<dyn std::error::Error>> {
    let cmd = build_join_query();

    for _ in 0..WARMUP {
        let rows = driver.fetch_all_uncached(&cmd).await?;
        let mut json_out = String::with_capacity(4096);
        json_out.push('[');
        for (i, r) in rows.iter().enumerate() {
            if i > 0 {
                json_out.push(',');
            }
            json_out.push_str(&format!(
                "{{\"id\":\"{}\",\"name\":\"{}\",\"origin\":\"{}\",\"dest\":\"{}\",\"operator\":\"{}\"}}",
                r.text(0), r.text(1), r.text(5), r.text(6), r.text(7),
            ));
        }
        json_out.push(']');
        std::hint::black_box(&json_out);
    }

    let mut per_iter: Vec<Duration> = Vec::with_capacity(ITERATIONS);
    let mut row_count = 0;

    for _ in 0..ITERATIONS {
        let t = Instant::now();

        let rows = driver.fetch_all_uncached(&cmd).await?;
        row_count = rows.len();

        let mut json_out = String::with_capacity(4096);
        json_out.push('[');
        for (i, r) in rows.iter().enumerate() {
            if i > 0 {
                json_out.push(',');
            }
            json_out.push_str(&format!(
                "{{\"id\":\"{}\",\"name\":\"{}\",\"origin\":\"{}\",\"dest\":\"{}\",\"operator\":\"{}\"}}",
                r.text(0), r.text(1), r.text(5), r.text(6), r.text(7),
            ));
        }
        json_out.push(']');
        std::hint::black_box(&json_out);

        per_iter.push(t.elapsed());
    }

    let total: Duration = per_iter.iter().sum();
    Ok((row_count, total, per_iter))
}

// ============================================================================
// Main — run all six approaches and compare
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║       Qail vs GraphQL vs REST — Comprehensive Benchmark        ║");
    println!("║                                                                ║");
    println!("║  Query: Connections + Harbors + Operators (3×JOIN)              ║");
    println!(
        "║  {} iterations × 6 approaches (+ {} warmup each)             ║",
        ITERATIONS, WARMUP
    );
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();

    let mut driver = PgDriver::connect_url(&url).await?;
    driver
        .execute_raw("SET app.is_super_admin = 'true'")
        .await?;
    println!("✓ Connected (RLS bypassed for fair comparison)\n");

    // ── Global warmup ────────────────────────────────────────
    println!("⏳ Global warmup (loading data pages into Postgres buffer cache)...");
    {
        let warmup_join = build_join_query();
        let warmup_harbors = Qail::get("harbors");
        let warmup_operators = Qail::get("operators");

        for _ in 0..10 {
            let _ = driver.fetch_all_uncached(&warmup_join).await?;
            let _ = driver.fetch_all_uncached(&warmup_harbors).await?;
            let _ = driver.fetch_all_uncached(&warmup_operators).await?;
        }
    }
    println!("✓ Buffer cache warm — all approaches start equal\n");

    // ── Randomized execution order ───────────────────────────
    let order = [
        "qail_prepared",
        "qail_uncached",
        "graphql_naive",
        "graphql_dataloader",
        "rest_naive",
        "rest_expand",
    ];

    let mut run_order: Vec<usize> = (0..order.len()).collect();
    {
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos() as usize;
        for i in (1..run_order.len()).rev() {
            let j = (seed.wrapping_mul(i + 7)) % (i + 1);
            run_order.swap(i, j);
        }
    }

    println!(
        "🎲 Randomized execution order: {}\n",
        run_order
            .iter()
            .map(|i| order[*i])
            .collect::<Vec<_>>()
            .join(" → ")
    );

    type BenchResult = (
        String,
        usize,
        Duration,
        Duration,
        Duration,
        usize,
        Vec<Duration>,
    );
    let mut results: Vec<Option<BenchResult>> = vec![None; 6];

    for &idx in &run_order {
        match idx {
            0 => {
                println!("━━━ Qail AST — Prepared (cached, skips Parse) ━━━");
                let (rows, total, mut times) = run_qail_prepared(&mut driver).await?;
                let med = median(&mut times);
                let p = p99(&mut times);
                let avg = total / ITERATIONS as u32;
                println!(
                    "  Rows: {}  │  Avg: {:?}  │  Median: {:?}  │  p99: {:?}",
                    rows, avg, med, p
                );
                println!("  Queries/iter: 1  │  Wire: Bind+Execute (no Parse)\n");
                results[0] = Some(("Qail (prepared)".into(), rows, total, med, p, 1, times));
            }
            1 => {
                println!("━━━ Qail AST — Uncached (Parse+Bind+Execute) ━━━");
                let (rows, total, mut times) = run_qail_uncached(&mut driver).await?;
                let med = median(&mut times);
                let p = p99(&mut times);
                let avg = total / ITERATIONS as u32;
                println!(
                    "  Rows: {}  │  Avg: {:?}  │  Median: {:?}  │  p99: {:?}",
                    rows, avg, med, p
                );
                println!("  Queries/iter: 1  │  Wire: Parse+Bind+Execute\n");
                results[1] = Some(("Qail (uncached)".into(), rows, total, med, p, 1, times));
            }
            2 => {
                println!("━━━ GraphQL NAIVE (N+1 resolvers) ━━━");
                let (rows, total, q, mut times) = run_graphql_naive(&mut driver).await?;
                let med = median(&mut times);
                let p = p99(&mut times);
                let avg = total / ITERATIONS as u32;
                let qpi = q / ITERATIONS;
                println!(
                    "  Rows: {}  │  Avg: {:?}  │  Median: {:?}  │  p99: {:?}",
                    rows, avg, med, p
                );
                println!("  Queries/iter: ~{}  │  Total queries: {}\n", qpi, q);
                results[2] = Some(("GraphQL naive".into(), rows, total, med, p, qpi, times));
            }
            3 => {
                println!("━━━ GraphQL + DataLoader (batched IN queries) ━━━");
                let (rows, total, q, mut times) = run_graphql_dataloader(&mut driver).await?;
                let med = median(&mut times);
                let p = p99(&mut times);
                let avg = total / ITERATIONS as u32;
                let qpi = q / ITERATIONS;
                println!(
                    "  Rows: {}  │  Avg: {:?}  │  Median: {:?}  │  p99: {:?}",
                    rows, avg, med, p
                );
                println!("  Queries/iter: ~{}  │  Total queries: {}\n", qpi, q);
                results[3] = Some(("GraphQL+DataLoader".into(), rows, total, med, p, qpi, times));
            }
            4 => {
                println!("━━━ REST NAIVE (N+1 + JSON serialization) ━━━");
                let (rows, total, q, mut times) = run_rest_naive(&mut driver).await?;
                let med = median(&mut times);
                let p = p99(&mut times);
                let avg = total / ITERATIONS as u32;
                let qpi = q / ITERATIONS;
                println!(
                    "  Rows: {}  │  Avg: {:?}  │  Median: {:?}  │  p99: {:?}",
                    rows, avg, med, p
                );
                println!("  Queries/iter: ~{}  │  Total queries: {}\n", qpi, q);
                results[4] = Some(("REST naive".into(), rows, total, med, p, qpi, times));
            }
            5 => {
                println!("━━━ REST + ?expand= (server-side JOIN + JSON) ━━━");
                let (rows, total, mut times) = run_rest_expand(&mut driver).await?;
                let med = median(&mut times);
                let p = p99(&mut times);
                let avg = total / ITERATIONS as u32;
                println!(
                    "  Rows: {}  │  Avg: {:?}  │  Median: {:?}  │  p99: {:?}",
                    rows, avg, med, p
                );
                println!("  Queries/iter: 1  │  Wire: Parse+Bind+Execute + JSON\n");
                results[5] = Some(("REST+expand".into(), rows, total, med, p, 1, times));
            }
            _ => unreachable!(),
        }
    }

    // ── Sort results by median ───────────────────────────────
    let mut sorted: Vec<_> = results.into_iter().flatten().collect();
    sorted.sort_by_key(|r| r.3);

    let fastest_median = sorted[0].3;

    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║  RESULTS (sorted by median, fastest → slowest)                     ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║                                                                    ║");

    for (i, (name, _rows, _total, med, p, qpi, _times)) in sorted.iter().enumerate() {
        let ratio = med.as_nanos() as f64 / fastest_median.as_nanos() as f64;
        let ratio_str = if ratio < 1.05 {
            "baseline".to_string()
        } else {
            format!("{:.1}× slower", ratio)
        };
        println!(
            "║  {}. {:<20} med {:<12?} p99 {:<12?} │ {} qry │ {} ║",
            i + 1,
            name,
            med,
            p,
            qpi,
            ratio_str
        );
    }

    println!("║                                                                    ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║  WHAT THIS PROVES                                                  ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║                                                                    ║");

    let qail_prep_med = sorted
        .iter()
        .find(|r| r.0.contains("prepared"))
        .map(|r| r.3);
    let rest_exp_med = sorted
        .iter()
        .find(|r| r.0.contains("REST+expand"))
        .map(|r| r.3);
    let gql_naive_med = sorted.iter().find(|r| r.0 == "GraphQL naive").map(|r| r.3);

    if let (Some(qp), Some(re)) = (qail_prep_med, rest_exp_med) {
        let ratio = re.as_nanos() as f64 / qp.as_nanos() as f64;
        println!(
            "║  • Prepared stmt reuse: Qail {:.1}× faster than REST+expand        ║",
            ratio
        );
    }
    if let (Some(qp), Some(gn)) = (qail_prep_med, gql_naive_med) {
        let ratio = gn.as_nanos() as f64 / qp.as_nanos() as f64;
        println!(
            "║  • Vs GraphQL N+1: Qail {:.0}× faster ({} vs 151 round trips)      ║",
            ratio, 1
        );
    }
    println!("║  • Prepared statements skip Parse = Postgres skips query planning  ║");
    println!("║  • N+1 is catastrophic regardless of framework                     ║");
    println!("║  • DataLoader helps but still multiple round trips                 ║");
    println!("║                                                                    ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║  SECURITY                                                          ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║  Qail:    RLS at protocol │ No SQL injection │ Type-safe AST       ║");
    println!("║  GraphQL: Per-resolver    │ Depth attacks    │ String fields       ║");
    println!("║  REST:    Per-middleware   │ IDOR per endpt   │ No compile-time     ║");
    println!("╚══════════════════════════════════════════════════════════════════════╝");

    Ok(())
}
