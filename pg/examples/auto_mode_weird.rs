//! Weird/edge-case live-DB validation for auto count mode.
//!
//! Runs:
//! - boundary-size checks around auto thresholds
//! - pool slot-pressure fallback checks
//! - concurrent mixed-size workload checks
//!
//! Usage:
//!   cargo run --release -p qail-pg --example auto_mode_weird
//!   WEIRD_WORKERS=10 WEIRD_ITERS=40 cargo run --release -p qail-pg --example auto_mode_weird

use qail_core::ast::Qail;
use qail_pg::{AutoCountPath, PgDriver, PgPool, PoolConfig, TlsMode};
use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

type AnyErr = Box<dyn Error + Send + Sync>;
type AnyRes<T> = Result<T, AnyErr>;

fn build_batch(size: usize, seed: usize) -> Vec<Qail> {
    (0..size)
        .map(|i| {
            let id = (((seed + i) % 10_000) + 1) as i64;
            Qail::get("harbors").columns(["id", "name"]).eq("id", id)
        })
        .collect()
}

fn path_label(path: AutoCountPath) -> &'static str {
    match path {
        AutoCountPath::SingleCached => "single_cached",
        AutoCountPath::PipelineOneShot => "pipeline_oneshot",
        AutoCountPath::PipelineCached => "pipeline_cached",
        AutoCountPath::PoolParallel => "pool_parallel",
    }
}

fn parse_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
}

async fn assert_driver_case(
    driver: &mut PgDriver,
    size: usize,
    expected: AutoCountPath,
    seed: usize,
) -> AnyRes<()> {
    let batch = build_batch(size, seed);
    let (completed, plan) = driver.execute_count_auto_with_plan(&batch).await?;
    if completed != batch.len() {
        return Err(format!(
            "driver completed mismatch: got {}, want {}",
            completed,
            batch.len()
        )
        .into());
    }
    if plan.path != expected {
        return Err(format!(
            "driver plan mismatch for size {}: got {:?}, expected {:?}",
            size, plan.path, expected
        )
        .into());
    }
    Ok(())
}

async fn assert_pool_case_min(
    pool: &PgPool,
    size: usize,
    min_expected: &[AutoCountPath],
    seed: usize,
) -> AnyRes<AutoCountPath> {
    let batch = build_batch(size, seed);
    let (completed, plan) = pool.execute_count_auto_with_plan(&batch).await?;
    if completed != batch.len() {
        return Err(format!(
            "pool completed mismatch: got {}, want {}",
            completed,
            batch.len()
        )
        .into());
    }
    if !min_expected.contains(&plan.path) {
        return Err(format!(
            "pool plan mismatch for size {}: got {:?}, expected one of {:?}",
            size, plan.path, min_expected
        )
        .into());
    }
    Ok(plan.path)
}

#[tokio::main]
#[allow(deprecated)]
async fn main() -> AnyRes<()> {
    let workers = parse_env_usize("WEIRD_WORKERS", 8);
    let iters = parse_env_usize("WEIRD_ITERS", 30);

    println!("🧪 QAIL auto-mode weird test");
    println!("============================");
    println!(
        "DB: 127.0.0.1:5432/example_staging user=orion workers={} iters={}",
        workers, iters
    );

    let mut driver = PgDriver::connect("127.0.0.1", 5432, "orion", "example_staging").await?;
    let pool_cfg = PoolConfig::new("127.0.0.1", 5432, "orion", "example_staging")
        .max_connections(6)
        .min_connections(6)
        .tls_mode(TlsMode::Disable);
    let pool = PgPool::connect(pool_cfg).await?;

    println!("1) Boundary checks");
    assert_driver_case(&mut driver, 0, AutoCountPath::SingleCached, 11).await?;
    assert_driver_case(&mut driver, 1, AutoCountPath::SingleCached, 12).await?;
    assert_driver_case(&mut driver, 2, AutoCountPath::PipelineOneShot, 13).await?;
    assert_driver_case(&mut driver, 7, AutoCountPath::PipelineOneShot, 14).await?;
    assert_driver_case(&mut driver, 8, AutoCountPath::PipelineCached, 15).await?;
    assert_driver_case(&mut driver, 256, AutoCountPath::PipelineCached, 16).await?;

    let _ = assert_pool_case_min(&pool, 0, &[AutoCountPath::SingleCached], 21).await?;
    let _ = assert_pool_case_min(&pool, 1, &[AutoCountPath::SingleCached], 22).await?;
    let _ = assert_pool_case_min(&pool, 2, &[AutoCountPath::PipelineOneShot], 23).await?;
    let _ = assert_pool_case_min(&pool, 7, &[AutoCountPath::PipelineOneShot], 24).await?;
    let _ = assert_pool_case_min(&pool, 8, &[AutoCountPath::PipelineCached], 25).await?;
    let _ = assert_pool_case_min(&pool, 4095, &[AutoCountPath::PipelineCached], 26).await?;
    let p4096 = assert_pool_case_min(
        &pool,
        4096,
        &[AutoCountPath::PipelineCached, AutoCountPath::PoolParallel],
        27,
    )
    .await?;
    println!("   - size 4096 chose {:?}", p4096);

    println!("2) Slot-pressure fallback checks");
    let mut held = Vec::new();
    for _ in 0..5 {
        held.push(pool.acquire_system().await?);
    }
    let pressured_plan = pool.plan_auto_count(4096);
    println!("   - pressured plan: {:?}", pressured_plan.path);
    if pressured_plan.path == AutoCountPath::PoolParallel {
        return Err("expected non-parallel plan under slot pressure".into());
    }
    let _ = assert_pool_case_min(
        &pool,
        4096,
        &[
            AutoCountPath::PipelineCached,
            AutoCountPath::PipelineOneShot,
        ],
        31,
    )
    .await?;
    while let Some(conn) = held.pop() {
        conn.release().await;
    }
    let released_plan = pool.plan_auto_count(4096);
    println!("   - released plan: {:?}", released_plan.path);
    if released_plan.path != AutoCountPath::PoolParallel {
        return Err("expected pool_parallel plan after releasing pressure".into());
    }

    println!("3) Concurrent mixed weird workload");
    let sizes: Arc<Vec<usize>> = Arc::new(vec![0, 1, 2, 3, 7, 8, 15, 63, 127, 1023, 4095, 4096]);
    let path_counts: Arc<Mutex<BTreeMap<&'static str, usize>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    let mut tasks = JoinSet::new();

    for worker in 0..workers {
        let pool = pool.clone();
        let sizes = Arc::clone(&sizes);
        let path_counts = Arc::clone(&path_counts);
        tasks.spawn(async move {
            for i in 0..iters {
                let idx = (worker * 17 + i * 7) % sizes.len();
                let size = sizes[idx];
                let batch = build_batch(size, worker * 10000 + i * 100);
                let (completed, plan) = pool.execute_count_auto_with_plan(&batch).await?;
                if completed != batch.len() {
                    return Err::<(), AnyErr>(
                        format!(
                            "worker {} iter {} mismatch: got {}, want {}",
                            worker,
                            i,
                            completed,
                            batch.len()
                        )
                        .into(),
                    );
                }
                let key = path_label(plan.path);
                let mut guard = path_counts.lock().await;
                *guard.entry(key).or_insert(0) += 1;
            }
            Ok::<(), AnyErr>(())
        });
    }

    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(format!("worker join error: {e}").into()),
        }
    }

    let counts = path_counts.lock().await;
    println!("   - path counts:");
    for (k, v) in counts.iter() {
        println!("     {}: {}", k, v);
    }

    for required in ["single_cached", "pipeline_oneshot", "pipeline_cached"] {
        if counts.get(required).copied().unwrap_or(0) == 0 {
            return Err(format!("missing path '{}' in weird workload", required).into());
        }
    }
    if counts.get("pool_parallel").copied().unwrap_or(0) == 0 {
        println!(
            "   - note: pool_parallel not observed in mixed workload (expected under slot pressure)"
        );
    }

    println!("\n✅ Weird test passed");
    Ok(())
}
