//! 2-minute soak for auto count mode on live PostgreSQL.
//!
//! Usage:
//!   cargo run --release -p qail-pg --example auto_mode_soak
//!   SOAK_SECS=120 cargo run --release -p qail-pg --example auto_mode_soak

use qail_core::ast::Qail;
use qail_pg::{AutoCountPath, PgDriver, PgPool, PoolConfig, TlsMode};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

const DEFAULT_SOAK_SECS: u64 = 120;

fn parse_soak_secs() -> u64 {
    std::env::var("SOAK_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_SOAK_SECS)
}

fn build_batch(size: usize, seed: usize) -> Vec<Qail> {
    (0..size)
        .map(|i| {
            let id = (((seed + i) % 10_000) + 1) as i64;
            Qail::get("harbors").columns(["id", "name"]).eq("id", id)
        })
        .collect()
}

fn bump_path_counts(path_counts: &mut BTreeMap<&'static str, usize>, path: AutoCountPath) {
    let key = match path {
        AutoCountPath::SingleCached => "single_cached",
        AutoCountPath::PipelineOneShot => "pipeline_oneshot",
        AutoCountPath::PipelineCached => "pipeline_cached",
        AutoCountPath::PoolParallel => "pool_parallel",
    };
    *path_counts.entry(key).or_insert(0) += 1;
}

#[tokio::main]
#[allow(deprecated)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let soak_secs = parse_soak_secs();
    let soak_duration = Duration::from_secs(soak_secs);

    println!("🔥 QAIL auto-mode soak test");
    println!("===========================");
    println!(
        "Duration: {}s, DB: 127.0.0.1:5432/example_staging, user: orion",
        soak_secs
    );

    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "example_staging").await?;

    let pool_cfg = PoolConfig::new("127.0.0.1", 5432, "orion", "example_staging")
        .max_connections(10)
        .min_connections(10)
        .tls_mode(TlsMode::Disable);
    let pool = PgPool::connect(pool_cfg).await?;

    let driver_single = build_batch(1, 11);
    let driver_oneshot = build_batch(4, 101);
    let driver_cached = build_batch(64, 1201);

    let pool_oneshot = build_batch(4, 2101);
    let pool_cached = build_batch(64, 3101);
    let pool_parallel = build_batch(4096, 4101);

    // Warmup (untimed) to avoid startup skew.
    for batch in [&driver_single, &driver_oneshot, &driver_cached] {
        let (completed, _plan) = driver.execute_count_auto_with_plan(batch).await?;
        if completed != batch.len() {
            return Err(format!(
                "driver warmup mismatch: got {}, want {}",
                completed,
                batch.len()
            )
            .into());
        }
    }
    for batch in [&pool_oneshot, &pool_cached, &pool_parallel] {
        let (completed, _plan) = pool.execute_count_auto_with_plan(batch).await?;
        if completed != batch.len() {
            return Err(format!(
                "pool warmup mismatch: got {}, want {}",
                completed,
                batch.len()
            )
            .into());
        }
    }

    let mut total_calls: usize = 0;
    let mut total_queries: usize = 0;
    let mut path_counts: BTreeMap<&'static str, usize> = BTreeMap::new();

    let start = Instant::now();
    let mut next_report = start + Duration::from_secs(10);

    while start.elapsed() < soak_duration {
        for batch in [&driver_single, &driver_oneshot, &driver_cached] {
            let (completed, plan) = driver.execute_count_auto_with_plan(batch).await?;
            if completed != batch.len() {
                return Err(format!(
                    "driver mismatch: got {}, want {} (path={:?})",
                    completed,
                    batch.len(),
                    plan.path
                )
                .into());
            }
            total_calls += 1;
            total_queries += completed;
            bump_path_counts(&mut path_counts, plan.path);
        }

        for batch in [&pool_oneshot, &pool_cached, &pool_parallel] {
            let (completed, plan) = pool.execute_count_auto_with_plan(batch).await?;
            if completed != batch.len() {
                return Err(format!(
                    "pool mismatch: got {}, want {} (path={:?})",
                    completed,
                    batch.len(),
                    plan.path
                )
                .into());
            }
            total_calls += 1;
            total_queries += completed;
            bump_path_counts(&mut path_counts, plan.path);
        }

        let now = Instant::now();
        if now >= next_report {
            let elapsed = start.elapsed().as_secs_f64();
            let qps = total_queries as f64 / elapsed;
            println!(
                "  {:.0}s: calls={}, queries={}, {:.0} q/s",
                elapsed, total_calls, total_queries, qps
            );
            next_report += Duration::from_secs(10);
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    let qps = total_queries as f64 / elapsed;

    println!("\n=== AUTO SOAK SUMMARY ===");
    println!("  elapsed:       {:.2}s", elapsed);
    println!("  total calls:   {}", total_calls);
    println!("  total queries: {}", total_queries);
    println!("  avg qps:       {:.0}", qps);
    println!("  path counts:");
    for (k, v) in &path_counts {
        println!("    {}: {}", k, v);
    }

    if path_counts.get("pool_parallel").copied().unwrap_or(0) == 0 {
        return Err("pool_parallel path was not selected during soak".into());
    }
    if path_counts.get("pipeline_cached").copied().unwrap_or(0) == 0 {
        return Err("pipeline_cached path was not selected during soak".into());
    }
    if path_counts.get("pipeline_oneshot").copied().unwrap_or(0) == 0 {
        return Err("pipeline_oneshot path was not selected during soak".into());
    }
    if path_counts.get("single_cached").copied().unwrap_or(0) == 0 {
        return Err("single_cached path was not selected during soak".into());
    }

    Ok(())
}
