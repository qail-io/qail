//! Single-run QAIL mode benchmark for ABBA orchestration.
//!
//! Usage:
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- single --plain
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- pipeline --plain
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- pool10 --plain

use qail_core::ast::Qail;
use qail_pg::driver::PreparedStatement;
use qail_pg::{PgConnection, PgPool, PoolConfig, TlsMode};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

const SQL_BY_ID: &str = "SELECT id, name FROM harbors WHERE id = $1";
const TOTAL_QUERIES: usize = 10_000;
const ITERATIONS: usize = 5;
const POOL_SIZE: usize = 10;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Single,
    Pipeline,
    Pool10,
}

impl Mode {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "single" => Ok(Self::Single),
            "pipeline" => Ok(Self::Pipeline),
            "pool10" | "pool" => Ok(Self::Pool10),
            other => Err(format!(
                "unknown mode '{}' (expected single | pipeline | pool10)",
                other
            )),
        }
    }
}

fn build_param_batch(total: usize) -> Vec<Vec<Option<Vec<u8>>>> {
    (1..=total)
        .map(|i| {
            let id = ((i % 10_000) + 1).to_string();
            vec![Some(id.into_bytes())]
        })
        .collect()
}

fn build_ast_batch(total: usize) -> Vec<Qail> {
    (1..=total)
        .map(|i| {
            let id = ((i % 10_000) + 1) as i64;
            Qail::get("harbors").columns(["id", "name"]).eq("id", id)
        })
        .collect()
}

async fn run_single_iteration(
    conn: &mut PgConnection,
    stmt: &PreparedStatement,
    params: &[Vec<Option<Vec<u8>>>],
) -> Result<(), Box<dyn std::error::Error>> {
    for p in params {
        let _ = conn.query_prepared_single(stmt, p).await?;
    }
    Ok(())
}

async fn run_single_mode(
    params: &[Vec<Option<Vec<u8>>>],
) -> Result<f64, Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "example_staging").await?;
    let stmt = conn.prepare(SQL_BY_ID).await?;

    run_single_iteration(&mut conn, &stmt, params).await?;

    let mut total = Duration::ZERO;
    for _ in 0..ITERATIONS {
        let start = Instant::now();
        run_single_iteration(&mut conn, &stmt, params).await?;
        total += start.elapsed();
    }

    Ok((params.len() * ITERATIONS) as f64 / total.as_secs_f64())
}

async fn run_pipeline_mode(cmds: &[Qail]) -> Result<f64, Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "example_staging").await?;

    let warm = conn.pipeline_execute_count_ast_cached(cmds).await?;
    if warm != cmds.len() {
        return Err(format!("warmup completed {} queries, expected {}", warm, cmds.len()).into());
    }

    let mut total = Duration::ZERO;
    for _ in 0..ITERATIONS {
        let start = Instant::now();
        let completed = conn.pipeline_execute_count_ast_cached(cmds).await?;
        total += start.elapsed();
        if completed != cmds.len() {
            return Err(format!(
                "run completed {} queries, expected {}",
                completed,
                cmds.len()
            )
            .into());
        }
    }

    Ok((cmds.len() * ITERATIONS) as f64 / total.as_secs_f64())
}

async fn run_pool10_mode(
    worker_params: Vec<Vec<Vec<Option<Vec<u8>>>>>,
) -> Result<f64, Box<dyn std::error::Error>> {
    let config = PoolConfig::new("127.0.0.1", 5432, "orion", "example_staging")
        .max_connections(POOL_SIZE)
        .min_connections(POOL_SIZE)
        .tls_mode(TlsMode::Disable);
    let pool = PgPool::connect(config).await?;

    let start_barrier = Arc::new(Barrier::new(POOL_SIZE + 1));
    let end_barrier = Arc::new(Barrier::new(POOL_SIZE + 1));
    let mut tasks = JoinSet::new();

    for params in worker_params {
        let pool = pool.clone();
        let start_barrier = Arc::clone(&start_barrier);
        let end_barrier = Arc::clone(&end_barrier);

        tasks.spawn(async move {
            let mut pooled = pool.acquire_system().await.map_err(|e| e.to_string())?;
            {
                let conn = pooled.get_mut().map_err(|e| e.to_string())?;
                let stmt = conn.prepare(SQL_BY_ID).await.map_err(|e| e.to_string())?;

                run_single_iteration(conn, &stmt, &params)
                    .await
                    .map_err(|e| e.to_string())?;

                start_barrier.wait().await;
                for _ in 0..ITERATIONS {
                    run_single_iteration(conn, &stmt, &params)
                        .await
                        .map_err(|e| e.to_string())?;
                }
                end_barrier.wait().await;
            }
            pooled.release().await;
            Ok::<(), String>(())
        });
    }

    start_barrier.wait().await;
    let start = Instant::now();
    end_barrier.wait().await;
    let elapsed = start.elapsed();

    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e.into()),
            Err(e) => return Err(e.to_string().into()),
        }
    }

    let total_timed_queries = TOTAL_QUERIES * ITERATIONS;
    Ok(total_timed_queries as f64 / elapsed.as_secs_f64())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut mode: Option<Mode> = None;
    let mut plain = false;

    for arg in std::env::args().skip(1) {
        if arg == "--plain" {
            plain = true;
            continue;
        }
        if mode.is_none() {
            mode = Some(Mode::parse(&arg)?);
            continue;
        }
        return Err(format!("unexpected argument '{}'", arg).into());
    }

    let mode =
        mode.ok_or_else(|| "missing mode argument: single | pipeline | pool10".to_string())?;
    let qps = match mode {
        Mode::Single => {
            let params = build_param_batch(TOTAL_QUERIES);
            run_single_mode(&params).await?
        }
        Mode::Pipeline => {
            let cmds = build_ast_batch(TOTAL_QUERIES);
            run_pipeline_mode(&cmds).await?
        }
        Mode::Pool10 => {
            let per_worker = TOTAL_QUERIES / POOL_SIZE;
            let mut worker_params = Vec::with_capacity(POOL_SIZE);
            for worker in 0..POOL_SIZE {
                let offset = worker * per_worker;
                let params = (0..per_worker)
                    .map(|i| {
                        let id = (((offset + i) % 10_000) + 1).to_string();
                        vec![Some(id.into_bytes())]
                    })
                    .collect();
                worker_params.push(params);
            }
            run_pool10_mode(worker_params).await?
        }
    };

    if plain {
        println!("{:.3}", qps);
    } else {
        println!(
            "qail {}: {:.0} q/s",
            format!("{mode:?}").to_lowercase(),
            qps
        );
    }

    Ok(())
}
