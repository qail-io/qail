//! Qail vs GraphQL vs REST pattern benchmark.
//!
//! This benchmark compares six execution patterns that all target the same
//! endpoint objective and canonical payload shape:
//!   { id, name, origin_harbor, dest_harbor }
//!
//! It is an architectural benchmark (single JOIN vs batched vs N+1), not a
//! pure driver microbenchmark.
//!
//! Run:
//!   DATABASE_URL=postgresql://qail_user@localhost:5432/qail_test \
//!     cargo run --example battle_comparison --features chrono,uuid --release
//!   BATTLE_ITERATIONS=200 BATTLE_WARMUP=10 BATTLE_GLOBAL_WARMUP=10 ...
//!
//! Security note: we set app.is_super_admin=true so all approaches bypass RLS
//! equally. This isolates data-access pattern cost.

use qail_core::ast::{JoinKind, Operator, SortOrder};
use qail_core::prelude::*;
use qail_pg::{PgDriver, PreparedAstQuery};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

const DEFAULT_ITERATIONS: usize = 100;
const DEFAULT_WARMUP: usize = 10;
const DEFAULT_GLOBAL_WARMUP: usize = 10;
const DEFAULT_SIMULATED_RTT_US: u64 = 0;

#[derive(Debug, Clone, Copy)]
struct BenchConfig {
    iterations: usize,
    warmup: usize,
    global_warmup: usize,
    simulated_rtt: Duration,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct ConnectionView {
    id: String,
    name: String,
    origin_harbor: String,
    dest_harbor: String,
}

#[derive(Debug, Clone)]
struct RootConnection {
    id: String,
    name: String,
    origin_harbor_id: String,
    dest_harbor_id: String,
}

#[derive(Debug, Clone)]
struct IterationOutput {
    payload: Vec<ConnectionView>,
    queries: usize,
    json_bytes: usize,
}

#[derive(Debug, Clone)]
struct BenchResult {
    id: &'static str,
    label: &'static str,
    avg: Duration,
    median: Duration,
    p95: Duration,
    queries_per_iter: f64,
    json_bytes_per_iter: f64,
    rows: usize,
}

fn read_env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

fn read_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn duration_micros(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000_000.0
}

async fn maybe_simulate_rtt(cfg: BenchConfig) {
    let target = cfg.simulated_rtt;
    if target.is_zero() {
        return;
    }

    // tokio::sleep has coarse sub-ms resolution on some platforms, so keep a
    // short spin tail for tighter per-query RTT injection.
    let start = Instant::now();
    if target >= Duration::from_millis(2) {
        let coarse = target.saturating_sub(Duration::from_micros(200));
        tokio::time::sleep(coarse).await;
    }
    while start.elapsed() < target {
        std::hint::spin_loop();
    }
}

async fn fetch_all_uncached_with_rtt(
    driver: &mut PgDriver,
    query: &Qail,
    cfg: BenchConfig,
) -> Result<Vec<qail_pg::PgRow>, Box<dyn std::error::Error>> {
    maybe_simulate_rtt(cfg).await;
    Ok(driver.fetch_all_uncached(query).await?)
}

async fn fetch_all_prepared_with_rtt(
    driver: &mut PgDriver,
    prepared: &PreparedAstQuery,
    cfg: BenchConfig,
) -> Result<Vec<qail_pg::PgRow>, Box<dyn std::error::Error>> {
    maybe_simulate_rtt(cfg).await;
    Ok(driver.fetch_all_prepared_ast(prepared).await?)
}

fn median(times: &[Duration]) -> Duration {
    let mut sorted = times.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[mid - 1] + sorted[mid]) / 2
    } else {
        sorted[mid]
    }
}

fn percentile(times: &[Duration], p: f64) -> Duration {
    let mut sorted = times.to_vec();
    sorted.sort_unstable();
    let idx = ((sorted.len() as f64 * p).floor() as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn build_join_query() -> Qail {
    Qail::get("odyssey_connections")
        .columns(vec!["odyssey_connections.id", "odyssey_connections.name"])
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
        .column("origin.name AS origin_harbor")
        .column("dest.name AS dest_harbor")
        .filter(
            "odyssey_connections.is_enabled",
            Operator::Eq,
            Value::Bool(true),
        )
        .order_by("odyssey_connections.name", SortOrder::Asc)
        .limit(50)
}

fn build_root_query() -> Qail {
    Qail::get("odyssey_connections")
        .columns(vec![
            "odyssey_connections.id",
            "odyssey_connections.name",
            "odyssey_connections.origin_harbor_id",
            "odyssey_connections.destination_harbor_id",
        ])
        .filter(
            "odyssey_connections.is_enabled",
            Operator::Eq,
            Value::Bool(true),
        )
        .order_by("odyssey_connections.name", SortOrder::Asc)
        .limit(50)
}

fn root_rows_to_connections(rows: Vec<qail_pg::PgRow>) -> Vec<RootConnection> {
    let mut roots = Vec::with_capacity(rows.len());
    for row in rows {
        roots.push(RootConnection {
            id: row.text(0),
            name: row.text(1),
            origin_harbor_id: row.text(2),
            dest_harbor_id: row.text(3),
        });
    }
    roots
}

fn join_rows_to_payload(rows: Vec<qail_pg::PgRow>) -> Vec<ConnectionView> {
    let mut payload = Vec::with_capacity(rows.len());
    for row in rows {
        payload.push(ConnectionView {
            id: row.text(0),
            name: row.text(1),
            origin_harbor: row.text(2),
            dest_harbor: row.text(3),
        });
    }
    payload.sort();
    payload
}

fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn serialize_payload_json(payload: &[ConnectionView]) -> String {
    let mut out = String::with_capacity(payload.len().saturating_mul(128));
    out.push('[');
    for (idx, row) in payload.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push('{');
        out.push_str("\"id\":\"");
        out.push_str(&json_escape(&row.id));
        out.push_str("\",\"name\":\"");
        out.push_str(&json_escape(&row.name));
        out.push_str("\",\"origin_harbor\":\"");
        out.push_str(&json_escape(&row.origin_harbor));
        out.push_str("\",\"dest_harbor\":\"");
        out.push_str(&json_escape(&row.dest_harbor));
        out.push('"');
        out.push('}');
    }
    out.push(']');
    out
}

fn build_harbor_lookup_query(id: String) -> Qail {
    Qail::get("harbors")
        .columns(vec!["id", "name"])
        .filter("id", Operator::Eq, Value::String(id))
        .limit(1)
}

fn build_harbor_batch_query(ids: &[String]) -> Qail {
    Qail::get("harbors").columns(vec!["id", "name"]).filter(
        "id",
        Operator::In,
        Value::Array(ids.iter().cloned().map(Value::String).collect()),
    )
}

fn find_payload_mismatch(
    baseline: &[ConnectionView],
    candidate: &[ConnectionView],
    label: &str,
) -> Option<String> {
    if baseline.len() != candidate.len() {
        return Some(format!(
            "{label}: row count mismatch baseline={} candidate={}",
            baseline.len(),
            candidate.len()
        ));
    }

    for (idx, (left, right)) in baseline.iter().zip(candidate.iter()).enumerate() {
        if left != right {
            return Some(format!(
                "{label}: first mismatch at index {idx}: baseline={left:?} candidate={right:?}"
            ));
        }
    }
    None
}

fn finalize_result(
    id: &'static str,
    label: &'static str,
    times: Vec<Duration>,
    total_queries: usize,
    total_json_bytes: usize,
    rows: usize,
    iterations: usize,
) -> BenchResult {
    let total: Duration = times.iter().sum();
    let avg = total / iterations as u32;
    let median = median(&times);
    let p95 = percentile(&times, 0.95);

    BenchResult {
        id,
        label,
        avg,
        median,
        p95,
        queries_per_iter: total_queries as f64 / iterations as f64,
        json_bytes_per_iter: total_json_bytes as f64 / iterations as f64,
        rows,
    }
}

async fn run_qail_prepared_once(
    driver: &mut PgDriver,
    prepared: &PreparedAstQuery,
    cfg: BenchConfig,
) -> Result<IterationOutput, Box<dyn std::error::Error>> {
    let rows = fetch_all_prepared_with_rtt(driver, prepared, cfg).await?;
    Ok(IterationOutput {
        payload: join_rows_to_payload(rows),
        queries: 1,
        json_bytes: 0,
    })
}

async fn run_qail_uncached_once(
    driver: &mut PgDriver,
    join_query: &Qail,
    cfg: BenchConfig,
) -> Result<IterationOutput, Box<dyn std::error::Error>> {
    let rows = fetch_all_uncached_with_rtt(driver, join_query, cfg).await?;
    Ok(IterationOutput {
        payload: join_rows_to_payload(rows),
        queries: 1,
        json_bytes: 0,
    })
}

async fn run_graphql_naive_once(
    driver: &mut PgDriver,
    root_query: &Qail,
    cfg: BenchConfig,
) -> Result<IterationOutput, Box<dyn std::error::Error>> {
    let roots =
        root_rows_to_connections(fetch_all_uncached_with_rtt(driver, root_query, cfg).await?);
    let mut payload = Vec::with_capacity(roots.len());
    let mut queries = 1usize;

    for root in roots {
        let origin_rows = fetch_all_uncached_with_rtt(
            driver,
            &build_harbor_lookup_query(root.origin_harbor_id.clone()),
            cfg,
        )
        .await?;
        queries += 1;
        let origin_harbor = origin_rows
            .first()
            .map(|row| row.text(1))
            .ok_or_else(|| format!("origin harbor id {} not found", root.origin_harbor_id))?;

        let dest_rows = fetch_all_uncached_with_rtt(
            driver,
            &build_harbor_lookup_query(root.dest_harbor_id.clone()),
            cfg,
        )
        .await?;
        queries += 1;
        let dest_harbor = dest_rows
            .first()
            .map(|row| row.text(1))
            .ok_or_else(|| format!("dest harbor id {} not found", root.dest_harbor_id))?;

        payload.push(ConnectionView {
            id: root.id,
            name: root.name,
            origin_harbor,
            dest_harbor,
        });
    }

    payload.sort();
    Ok(IterationOutput {
        payload,
        queries,
        json_bytes: 0,
    })
}

async fn run_graphql_dataloader_once(
    driver: &mut PgDriver,
    root_query: &Qail,
    cfg: BenchConfig,
) -> Result<IterationOutput, Box<dyn std::error::Error>> {
    let roots =
        root_rows_to_connections(fetch_all_uncached_with_rtt(driver, root_query, cfg).await?);
    let mut queries = 1usize;

    let mut harbor_ids = HashSet::new();
    for root in &roots {
        harbor_ids.insert(root.origin_harbor_id.clone());
        harbor_ids.insert(root.dest_harbor_id.clone());
    }

    let mut harbor_map: HashMap<String, String> = HashMap::new();
    if !harbor_ids.is_empty() {
        let harbor_ids_vec: Vec<String> = harbor_ids.into_iter().collect();
        let harbor_rows =
            fetch_all_uncached_with_rtt(driver, &build_harbor_batch_query(&harbor_ids_vec), cfg)
                .await?;
        queries += 1;
        for row in harbor_rows {
            harbor_map.insert(row.text(0), row.text(1));
        }
    }

    let mut payload = Vec::with_capacity(roots.len());
    for root in roots {
        let origin_harbor = harbor_map
            .get(&root.origin_harbor_id)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "origin harbor id {} not found in batch",
                    root.origin_harbor_id
                )
            })?;

        let dest_harbor = harbor_map
            .get(&root.dest_harbor_id)
            .cloned()
            .ok_or_else(|| format!("dest harbor id {} not found in batch", root.dest_harbor_id))?;

        payload.push(ConnectionView {
            id: root.id,
            name: root.name,
            origin_harbor,
            dest_harbor,
        });
    }

    payload.sort();
    Ok(IterationOutput {
        payload,
        queries,
        json_bytes: 0,
    })
}

async fn run_rest_naive_once(
    driver: &mut PgDriver,
    root_query: &Qail,
    cfg: BenchConfig,
) -> Result<IterationOutput, Box<dyn std::error::Error>> {
    let roots =
        root_rows_to_connections(fetch_all_uncached_with_rtt(driver, root_query, cfg).await?);
    let mut payload = Vec::with_capacity(roots.len());
    let mut queries = 1usize;

    for root in roots {
        let origin_rows = fetch_all_uncached_with_rtt(
            driver,
            &build_harbor_lookup_query(root.origin_harbor_id.clone()),
            cfg,
        )
        .await?;
        queries += 1;
        let origin_harbor = origin_rows
            .first()
            .map(|row| row.text(1))
            .ok_or_else(|| format!("origin harbor id {} not found", root.origin_harbor_id))?;

        let dest_rows = fetch_all_uncached_with_rtt(
            driver,
            &build_harbor_lookup_query(root.dest_harbor_id.clone()),
            cfg,
        )
        .await?;
        queries += 1;
        let dest_harbor = dest_rows
            .first()
            .map(|row| row.text(1))
            .ok_or_else(|| format!("dest harbor id {} not found", root.dest_harbor_id))?;

        payload.push(ConnectionView {
            id: root.id,
            name: root.name,
            origin_harbor,
            dest_harbor,
        });
    }

    payload.sort();
    let json_out = serialize_payload_json(&payload);
    std::hint::black_box(&json_out);

    Ok(IterationOutput {
        payload,
        queries,
        json_bytes: json_out.len(),
    })
}

async fn run_rest_expand_once(
    driver: &mut PgDriver,
    join_query: &Qail,
    cfg: BenchConfig,
) -> Result<IterationOutput, Box<dyn std::error::Error>> {
    let rows = fetch_all_uncached_with_rtt(driver, join_query, cfg).await?;
    let mut payload = join_rows_to_payload(rows);
    payload.sort();

    let json_out = serialize_payload_json(&payload);
    std::hint::black_box(&json_out);

    Ok(IterationOutput {
        payload,
        queries: 1,
        json_bytes: json_out.len(),
    })
}

async fn bench_qail_prepared(
    driver: &mut PgDriver,
    cfg: BenchConfig,
    prepared: &PreparedAstQuery,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    for _ in 0..cfg.warmup {
        let warm = run_qail_prepared_once(driver, prepared, cfg).await?;
        std::hint::black_box(warm.json_bytes);
    }

    let mut times = Vec::with_capacity(cfg.iterations);
    let mut total_queries = 0usize;
    let mut total_json_bytes = 0usize;
    let mut rows = 0usize;

    for _ in 0..cfg.iterations {
        let start = Instant::now();
        let out = run_qail_prepared_once(driver, prepared, cfg).await?;
        times.push(start.elapsed());
        total_queries += out.queries;
        total_json_bytes += out.json_bytes;
        rows = out.payload.len();
    }

    Ok(finalize_result(
        "qail_prepared",
        "Qail AST (prepared)",
        times,
        total_queries,
        total_json_bytes,
        rows,
        cfg.iterations,
    ))
}

async fn bench_qail_uncached(
    driver: &mut PgDriver,
    cfg: BenchConfig,
    join_query: &Qail,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    for _ in 0..cfg.warmup {
        let warm = run_qail_uncached_once(driver, join_query, cfg).await?;
        std::hint::black_box(warm.json_bytes);
    }

    let mut times = Vec::with_capacity(cfg.iterations);
    let mut total_queries = 0usize;
    let mut total_json_bytes = 0usize;
    let mut rows = 0usize;

    for _ in 0..cfg.iterations {
        let start = Instant::now();
        let out = run_qail_uncached_once(driver, join_query, cfg).await?;
        times.push(start.elapsed());
        total_queries += out.queries;
        total_json_bytes += out.json_bytes;
        rows = out.payload.len();
    }

    Ok(finalize_result(
        "qail_uncached",
        "Qail AST (uncached)",
        times,
        total_queries,
        total_json_bytes,
        rows,
        cfg.iterations,
    ))
}

async fn bench_graphql_naive(
    driver: &mut PgDriver,
    cfg: BenchConfig,
    root_query: &Qail,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    for _ in 0..cfg.warmup {
        let warm = run_graphql_naive_once(driver, root_query, cfg).await?;
        std::hint::black_box(warm.json_bytes);
    }

    let mut times = Vec::with_capacity(cfg.iterations);
    let mut total_queries = 0usize;
    let mut total_json_bytes = 0usize;
    let mut rows = 0usize;

    for _ in 0..cfg.iterations {
        let start = Instant::now();
        let out = run_graphql_naive_once(driver, root_query, cfg).await?;
        times.push(start.elapsed());
        total_queries += out.queries;
        total_json_bytes += out.json_bytes;
        rows = out.payload.len();
    }

    Ok(finalize_result(
        "graphql_naive",
        "GraphQL naive (N+1)",
        times,
        total_queries,
        total_json_bytes,
        rows,
        cfg.iterations,
    ))
}

async fn bench_graphql_dataloader(
    driver: &mut PgDriver,
    cfg: BenchConfig,
    root_query: &Qail,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    for _ in 0..cfg.warmup {
        let warm = run_graphql_dataloader_once(driver, root_query, cfg).await?;
        std::hint::black_box(warm.json_bytes);
    }

    let mut times = Vec::with_capacity(cfg.iterations);
    let mut total_queries = 0usize;
    let mut total_json_bytes = 0usize;
    let mut rows = 0usize;

    for _ in 0..cfg.iterations {
        let start = Instant::now();
        let out = run_graphql_dataloader_once(driver, root_query, cfg).await?;
        times.push(start.elapsed());
        total_queries += out.queries;
        total_json_bytes += out.json_bytes;
        rows = out.payload.len();
    }

    Ok(finalize_result(
        "graphql_dataloader",
        "GraphQL + DataLoader",
        times,
        total_queries,
        total_json_bytes,
        rows,
        cfg.iterations,
    ))
}

async fn bench_rest_naive(
    driver: &mut PgDriver,
    cfg: BenchConfig,
    root_query: &Qail,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    for _ in 0..cfg.warmup {
        let warm = run_rest_naive_once(driver, root_query, cfg).await?;
        std::hint::black_box(warm.json_bytes);
    }

    let mut times = Vec::with_capacity(cfg.iterations);
    let mut total_queries = 0usize;
    let mut total_json_bytes = 0usize;
    let mut rows = 0usize;

    for _ in 0..cfg.iterations {
        let start = Instant::now();
        let out = run_rest_naive_once(driver, root_query, cfg).await?;
        times.push(start.elapsed());
        total_queries += out.queries;
        total_json_bytes += out.json_bytes;
        rows = out.payload.len();
    }

    Ok(finalize_result(
        "rest_naive",
        "REST naive (N+1 + JSON)",
        times,
        total_queries,
        total_json_bytes,
        rows,
        cfg.iterations,
    ))
}

async fn bench_rest_expand(
    driver: &mut PgDriver,
    cfg: BenchConfig,
    join_query: &Qail,
) -> Result<BenchResult, Box<dyn std::error::Error>> {
    for _ in 0..cfg.warmup {
        let warm = run_rest_expand_once(driver, join_query, cfg).await?;
        std::hint::black_box(warm.json_bytes);
    }

    let mut times = Vec::with_capacity(cfg.iterations);
    let mut total_queries = 0usize;
    let mut total_json_bytes = 0usize;
    let mut rows = 0usize;

    for _ in 0..cfg.iterations {
        let start = Instant::now();
        let out = run_rest_expand_once(driver, join_query, cfg).await?;
        times.push(start.elapsed());
        total_queries += out.queries;
        total_json_bytes += out.json_bytes;
        rows = out.payload.len();
    }

    Ok(finalize_result(
        "rest_expand",
        "REST + expand (JOIN + JSON)",
        times,
        total_queries,
        total_json_bytes,
        rows,
        cfg.iterations,
    ))
}

async fn validate_payload_equivalence(
    driver: &mut PgDriver,
    prepared: &PreparedAstQuery,
    join_query: &Qail,
    root_query: &Qail,
    cfg: BenchConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let baseline = run_qail_uncached_once(driver, join_query, cfg)
        .await?
        .payload;

    let prepared_rows = run_qail_prepared_once(driver, prepared, cfg).await?.payload;
    if let Some(msg) = find_payload_mismatch(&baseline, &prepared_rows, "qail_prepared") {
        return Err(msg.into());
    }

    let gql_naive = run_graphql_naive_once(driver, root_query, cfg)
        .await?
        .payload;
    if let Some(msg) = find_payload_mismatch(&baseline, &gql_naive, "graphql_naive") {
        return Err(msg.into());
    }

    let gql_dataloader = run_graphql_dataloader_once(driver, root_query, cfg)
        .await?
        .payload;
    if let Some(msg) = find_payload_mismatch(&baseline, &gql_dataloader, "graphql_dataloader") {
        return Err(msg.into());
    }

    let rest_naive = run_rest_naive_once(driver, root_query, cfg).await?.payload;
    if let Some(msg) = find_payload_mismatch(&baseline, &rest_naive, "rest_naive") {
        return Err(msg.into());
    }

    let rest_expand = run_rest_expand_once(driver, join_query, cfg).await?.payload;
    if let Some(msg) = find_payload_mismatch(&baseline, &rest_expand, "rest_expand") {
        return Err(msg.into());
    }

    println!(
        "PAYLOAD_EQUIVALENCE ok rows={} shape={{id,name,origin_harbor,dest_harbor}}",
        baseline.len()
    );

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = BenchConfig {
        iterations: read_env_usize("BATTLE_ITERATIONS", DEFAULT_ITERATIONS),
        warmup: read_env_usize("BATTLE_WARMUP", DEFAULT_WARMUP),
        global_warmup: read_env_usize("BATTLE_GLOBAL_WARMUP", DEFAULT_GLOBAL_WARMUP),
        simulated_rtt: Duration::from_micros(read_env_u64(
            "BATTLE_SIMULATED_RTT_US",
            DEFAULT_SIMULATED_RTT_US,
        )),
    };

    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    println!("N+1 pattern benchmark (same payload, different execution patterns)");
    println!(
        "BENCH_CONFIG iterations={} warmup={} global_warmup={} simulated_rtt_us={}",
        cfg.iterations,
        cfg.warmup,
        cfg.global_warmup,
        cfg.simulated_rtt.as_micros()
    );

    let mut driver = PgDriver::connect_url(&url).await?;
    driver
        .execute_raw("SET app.is_super_admin = 'true'")
        .await?;

    let join_query = build_join_query();
    let root_query = build_root_query();
    let prepared_join = driver.prepare_ast_query(&join_query).await?;

    println!("GLOBAL_WARMUP start");
    for _ in 0..cfg.global_warmup {
        let _ = fetch_all_uncached_with_rtt(&mut driver, &join_query, cfg).await?;
        let _ = fetch_all_uncached_with_rtt(&mut driver, &root_query, cfg).await?;
    }
    println!("GLOBAL_WARMUP done");

    validate_payload_equivalence(&mut driver, &prepared_join, &join_query, &root_query, cfg)
        .await?;

    let labels = [
        "qail_prepared",
        "qail_uncached",
        "graphql_naive",
        "graphql_dataloader",
        "rest_naive",
        "rest_expand",
    ];

    let mut run_order: Vec<usize> = (0..labels.len()).collect();
    let mut seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as usize;
    for i in (1..run_order.len()).rev() {
        seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let j = seed % (i + 1);
        run_order.swap(i, j);
    }

    let order_text = run_order
        .iter()
        .map(|idx| labels[*idx])
        .collect::<Vec<_>>()
        .join(" -> ");
    println!("RUN_ORDER {}", order_text);

    let mut results = Vec::with_capacity(labels.len());

    for idx in run_order {
        let result = match idx {
            0 => {
                println!("RUN qail_prepared start");
                bench_qail_prepared(&mut driver, cfg, &prepared_join).await?
            }
            1 => {
                println!("RUN qail_uncached start");
                bench_qail_uncached(&mut driver, cfg, &join_query).await?
            }
            2 => {
                println!("RUN graphql_naive start");
                bench_graphql_naive(&mut driver, cfg, &root_query).await?
            }
            3 => {
                println!("RUN graphql_dataloader start");
                bench_graphql_dataloader(&mut driver, cfg, &root_query).await?
            }
            4 => {
                println!("RUN rest_naive start");
                bench_rest_naive(&mut driver, cfg, &root_query).await?
            }
            5 => {
                println!("RUN rest_expand start");
                bench_rest_expand(&mut driver, cfg, &join_query).await?
            }
            _ => unreachable!(),
        };

        println!(
            "RESULT|{}|label={} median_us={:.1} p95_us={:.1} avg_us={:.1} queries_per_iter={:.1} rows={} json_bytes_per_iter={:.1}",
            result.id,
            result.label,
            duration_micros(result.median),
            duration_micros(result.p95),
            duration_micros(result.avg),
            result.queries_per_iter,
            result.rows,
            result.json_bytes_per_iter,
        );

        results.push(result);
    }

    results.sort_by_key(|r| r.median);
    let fastest = results
        .first()
        .map(|r| r.median)
        .unwrap_or(Duration::from_nanos(1));

    println!("\nSUMMARY sorted_by=median");
    for (idx, result) in results.iter().enumerate() {
        let ratio = result.median.as_secs_f64() / fastest.as_secs_f64();
        println!(
            "{}. {:<28} median={:>8.1}us p95={:>8.1}us q/iter={:>5.1} rows={} ratio={:.1}x",
            idx + 1,
            result.label,
            duration_micros(result.median),
            duration_micros(result.p95),
            result.queries_per_iter,
            result.rows,
            ratio
        );
    }

    println!(
        "\nMETHODOLOGY same endpoint objective + canonical payload equivalence; architecture patterns differ (single JOIN vs batched vs N+1)."
    );

    Ok(())
}
