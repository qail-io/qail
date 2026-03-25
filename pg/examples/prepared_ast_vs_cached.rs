//! Compare classic fetch_all_cached vs PreparedAstQuery fast path.
//!
//! Run:
//! DATABASE_URL="postgresql://orion@localhost:5432/swb_staging_local?sslmode=disable" \
//! cargo run -p qail-pg --example prepared_ast_vs_cached \
//!   --features chrono,uuid,legacy-raw-examples --release

use qail_core::ast::{JoinKind, Operator, SortOrder};
use qail_core::prelude::*;
use qail_pg::PgDriver;
use std::time::Instant;

const ITER: usize = 400;
const WARMUP: usize = 20;

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

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    values[values.len() / 2]
}

async fn run_cached(driver: &mut PgDriver, cmd: &Qail) -> Result<f64, Box<dyn std::error::Error>> {
    for _ in 0..WARMUP {
        let rows = driver.fetch_all_cached(cmd).await?;
        std::hint::black_box(rows.len());
    }

    let start = Instant::now();
    for _ in 0..ITER {
        let rows = driver.fetch_all_cached(cmd).await?;
        std::hint::black_box(rows.len());
    }
    let elapsed = start.elapsed();

    Ok(ITER as f64 / elapsed.as_secs_f64())
}

async fn run_prepared_ast(
    driver: &mut PgDriver,
    cmd: &Qail,
) -> Result<f64, Box<dyn std::error::Error>> {
    let prepared = driver.prepare_ast_query(cmd).await?;

    for _ in 0..WARMUP {
        let rows = driver.fetch_all_prepared_ast(&prepared).await?;
        std::hint::black_box(rows.len());
    }

    let start = Instant::now();
    for _ in 0..ITER {
        let rows = driver.fetch_all_prepared_ast(&prepared).await?;
        std::hint::black_box(rows.len());
    }
    let elapsed = start.elapsed();

    Ok(ITER as f64 / elapsed.as_secs_f64())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let mut driver = PgDriver::connect_url(&db_url).await?;
    driver
        .execute_raw("SET app.is_super_admin = 'true'")
        .await?;

    let cmd = build_join_query();

    for _ in 0..20 {
        let _ = driver.fetch_all_uncached(&cmd).await?;
    }

    let mut cached_runs = Vec::new();
    let mut prepared_runs = Vec::new();

    let order = [true, false, false, true]; // ABBA
    for cached_first in order {
        if cached_first {
            cached_runs.push(run_cached(&mut driver, &cmd).await?);
            prepared_runs.push(run_prepared_ast(&mut driver, &cmd).await?);
        } else {
            prepared_runs.push(run_prepared_ast(&mut driver, &cmd).await?);
            cached_runs.push(run_cached(&mut driver, &cmd).await?);
        }
    }

    let mut cached_med = cached_runs.clone();
    let mut prepared_med = prepared_runs.clone();
    let cached_median = median(&mut cached_med);
    let prepared_median = median(&mut prepared_med);
    let delta_pct = ((prepared_median / cached_median) - 1.0) * 100.0;

    println!("cached runs q/s      : {:?}", cached_runs);
    println!("prepared_ast runs q/s: {:?}", prepared_runs);
    println!("cached median q/s      : {:.0}", cached_median);
    println!("prepared_ast median q/s: {:.0}", prepared_median);
    println!("delta prepared_ast vs cached: {:+.2}%", delta_pct);

    Ok(())
}
