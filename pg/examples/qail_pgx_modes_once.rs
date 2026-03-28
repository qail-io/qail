//! Single-run QAIL mode benchmark for ABBA orchestration.
//!
//! Usage:
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- single --plain
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- pipeline --plain
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- pool10 --plain
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- single --workload wide_rows --plain
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- pipeline --workload many_params --plain

use qail_pg::driver::PreparedStatement;
use qail_pg::{PgConnection, PgEncoder, PgPool, PoolConfig, TlsMode};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

const SQL_BY_ID: &str = "SELECT id, name FROM harbors WHERE id = $1";
const WIDE_ROWS_SQL: &str = concat!(
    "SELECT gs AS id, ",
    "('harbor-' || gs)::text AS name, ",
    "repeat(md5(gs::text), 4) AS bio, ",
    "repeat(md5((gs * 17)::text), 3) AS region, ",
    "(gs * 11) AS visits, ",
    "(gs % 2 = 0) AS active, ",
    "round((gs::numeric / 7.0), 3) AS ratio, ",
    "CASE WHEN gs % 5 = 0 THEN NULL ELSE repeat(md5((gs * 3)::text), 2) END AS optional_note ",
    "FROM generate_series(1, $1::int) AS gs"
);
const MANY_PARAMS_PARAM_COUNT: usize = 32;
const MANY_PARAMS_SQL: &str = concat!(
    "SELECT ",
    "$1::int + $2::int + $3::int + $4::int + $5::int + $6::int + $7::int + $8::int + ",
    "$9::int + $10::int + $11::int + $12::int + $13::int + $14::int + $15::int + $16::int + ",
    "$17::int + $18::int + $19::int + $20::int + $21::int + $22::int + $23::int + $24::int + ",
    "$25::int + $26::int + $27::int + $28::int + $29::int + $30::int + $31::int + $32::int ",
    "AS total"
);
const POINT_TOTAL_QUERIES: usize = 10_000;
const POINT_ITERATIONS: usize = 5;
const WIDE_ROWS_TOTAL_QUERIES: usize = 100;
const WIDE_ROWS_ITERATIONS: usize = 3;
const MANY_PARAMS_TOTAL_QUERIES: usize = 5_000;
const MANY_PARAMS_ITERATIONS: usize = 5;
const POOL_SIZE: usize = 10;
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 1099511628211;

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

    fn name(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Pipeline => "pipeline",
            Self::Pool10 => "pool10",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Workload {
    Point,
    WideRows,
    ManyParams,
}

impl Workload {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "point" | "lookup" => Ok(Self::Point),
            "wide_rows" | "wide" => Ok(Self::WideRows),
            "many_params" | "params" => Ok(Self::ManyParams),
            other => Err(format!(
                "unknown workload '{}' (expected point | wide_rows | many_params)",
                other
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResultMode {
    CompleteOnly,
    ScalarInt,
    WideRows,
}

#[derive(Clone, Copy, Debug)]
struct WorkloadSpec {
    workload: Workload,
    name: &'static str,
    sql: &'static str,
    total_queries: usize,
    iterations: usize,
    result_mode: ResultMode,
}

impl WorkloadSpec {
    fn new(workload: Workload) -> Self {
        match workload {
            Workload::Point => Self {
                workload,
                name: "point",
                sql: SQL_BY_ID,
                total_queries: POINT_TOTAL_QUERIES,
                iterations: POINT_ITERATIONS,
                result_mode: ResultMode::CompleteOnly,
            },
            Workload::WideRows => Self {
                workload,
                name: "wide_rows",
                sql: WIDE_ROWS_SQL,
                total_queries: WIDE_ROWS_TOTAL_QUERIES,
                iterations: WIDE_ROWS_ITERATIONS,
                result_mode: ResultMode::WideRows,
            },
            Workload::ManyParams => Self {
                workload,
                name: "many_params",
                sql: MANY_PARAMS_SQL,
                total_queries: MANY_PARAMS_TOTAL_QUERIES,
                iterations: MANY_PARAMS_ITERATIONS,
                result_mode: ResultMode::ScalarInt,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct BatchStats {
    completed: usize,
    rows: usize,
    bytes: usize,
    checksum: u64,
}

impl BatchStats {
    fn add(&mut self, other: Self) {
        self.completed += other.completed;
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.checksum = self.checksum.wrapping_add(other.checksum);
    }
}

#[derive(Clone, Copy, Debug)]
struct BenchmarkResult {
    qps: f64,
    rows_per_sec: Option<f64>,
    mib_per_sec: Option<f64>,
    checksum: u64,
}

fn build_param_batch(spec: WorkloadSpec) -> Vec<Vec<Option<Vec<u8>>>> {
    match spec.workload {
        Workload::Point => build_point_params(spec.total_queries),
        Workload::WideRows => build_wide_rows_params(spec.total_queries),
        Workload::ManyParams => build_many_params_batch(spec.total_queries),
    }
}

fn build_point_params(total: usize) -> Vec<Vec<Option<Vec<u8>>>> {
    (1..=total)
        .map(|i| {
            let id = ((i % 10_000) + 1).to_string();
            vec![Some(id.into_bytes())]
        })
        .collect()
}

fn build_wide_rows_params(total: usize) -> Vec<Vec<Option<Vec<u8>>>> {
    const ROW_COUNTS: [&str; 4] = ["128", "256", "384", "512"];

    (0..total)
        .map(|i| vec![Some(ROW_COUNTS[i % ROW_COUNTS.len()].as_bytes().to_vec())])
        .collect()
}

fn build_many_params_batch(total: usize) -> Vec<Vec<Option<Vec<u8>>>> {
    let cache: Vec<Vec<u8>> = (1..=256).map(|i| i.to_string().into_bytes()).collect();

    (0..total)
        .map(|query_idx| {
            (0..MANY_PARAMS_PARAM_COUNT)
                .map(|param_idx| {
                    let value_idx = (query_idx + param_idx * 7) % cache.len();
                    Some(cache[value_idx].clone())
                })
                .collect()
        })
        .collect()
}

async fn run_single_iteration(
    conn: &mut PgConnection,
    stmt: &PreparedStatement,
    params: &[Vec<Option<Vec<u8>>>],
    result_mode: ResultMode,
) -> Result<BatchStats, Box<dyn std::error::Error>> {
    let mut stats = BatchStats::default();

    for p in params {
        match result_mode {
            ResultMode::CompleteOnly => {
                conn.query_prepared_single_count(stmt, p).await?;
                stats.completed += 1;
            }
            ResultMode::ScalarInt => {
                let rows = conn
                    .query_prepared_single_reuse_with_result_format(stmt, p, PgEncoder::FORMAT_TEXT)
                    .await?;
                stats.completed += 1;
                consume_scalar_rows(&rows, &mut stats);
            }
            ResultMode::WideRows => {
                conn.query_prepared_single_reuse_visit_rows_with_result_format(
                    stmt,
                    p,
                    PgEncoder::FORMAT_TEXT,
                    |row| {
                        consume_wide_row(row, &mut stats);
                        Ok(())
                    },
                )
                .await?;
                stats.completed += 1;
            }
        }
    }

    Ok(stats)
}

async fn run_pipeline_iteration(
    conn: &mut PgConnection,
    stmt: &PreparedStatement,
    params: &[Vec<Option<Vec<u8>>>],
    result_mode: ResultMode,
) -> Result<BatchStats, Box<dyn std::error::Error>> {
    match result_mode {
        ResultMode::CompleteOnly => Ok(BatchStats {
            completed: conn.pipeline_execute_prepared_count(stmt, params).await?,
            ..BatchStats::default()
        }),
        ResultMode::ScalarInt => {
            let results = conn.pipeline_execute_prepared_rows(stmt, params).await?;
            if results.len() != params.len() {
                return Err(format!(
                    "pipeline returned {} results, expected {}",
                    results.len(),
                    params.len()
                )
                .into());
            }

            let mut stats = BatchStats::default();
            stats.completed = results.len();
            for rows in &results {
                consume_scalar_rows(rows, &mut stats);
            }
            Ok(stats)
        }
        ResultMode::WideRows => {
            let mut stats = BatchStats::default();
            stats.completed = conn
                .pipeline_execute_prepared_visit_rows(stmt, params, |row| {
                    consume_wide_row(row, &mut stats);
                    Ok(())
                })
                .await?;
            Ok(stats)
        }
    }
}

async fn run_single_mode(
    spec: WorkloadSpec,
    params: &[Vec<Option<Vec<u8>>>],
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "example_staging").await?;
    let stmt = conn.prepare(spec.sql).await?;

    let warmup = run_single_iteration(&mut conn, &stmt, params, spec.result_mode).await?;
    if warmup.completed != params.len() {
        return Err(format!(
            "warmup completed {} queries, expected {}",
            warmup.completed,
            params.len()
        )
        .into());
    }

    let mut total = Duration::ZERO;
    let mut aggregate = BatchStats::default();
    for _ in 0..spec.iterations {
        let start = Instant::now();
        let stats = run_single_iteration(&mut conn, &stmt, params, spec.result_mode).await?;
        total += start.elapsed();
        if stats.completed != params.len() {
            return Err(format!(
                "run completed {} queries, expected {}",
                stats.completed,
                params.len()
            )
            .into());
        }
        aggregate.add(stats);
    }

    Ok(make_benchmark_result(aggregate, total))
}

async fn run_pipeline_mode(
    spec: WorkloadSpec,
    params: &[Vec<Option<Vec<u8>>>],
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "example_staging").await?;
    let stmt = conn.prepare(spec.sql).await?;

    let warmup = run_pipeline_iteration(&mut conn, &stmt, params, spec.result_mode).await?;
    if warmup.completed != params.len() {
        return Err(format!(
            "warmup completed {} queries, expected {}",
            warmup.completed,
            params.len()
        )
        .into());
    }

    let mut total = Duration::ZERO;
    let mut aggregate = BatchStats::default();
    for _ in 0..spec.iterations {
        let start = Instant::now();
        let stats = run_pipeline_iteration(&mut conn, &stmt, params, spec.result_mode).await?;
        total += start.elapsed();
        if stats.completed != params.len() {
            return Err(format!(
                "run completed {} queries, expected {}",
                stats.completed,
                params.len()
            )
            .into());
        }
        aggregate.add(stats);
    }

    Ok(make_benchmark_result(aggregate, total))
}

async fn run_pool10_mode(
    spec: WorkloadSpec,
    worker_params: Vec<Vec<Vec<Option<Vec<u8>>>>>,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
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
            let mut local_err: Option<String> = None;
            let mut measured = BatchStats::default();
            let mut pooled = match pool.acquire_system().await {
                Ok(pooled) => Some(pooled),
                Err(e) => {
                    local_err = Some(e.to_string());
                    None
                }
            };

            if let Some(pooled) = pooled.as_mut() {
                let warmup_result = async {
                    let conn = pooled.get_mut().map_err(|e| e.to_string())?;
                    let stmt = conn.prepare(spec.sql).await.map_err(|e| e.to_string())?;
                    let warmup = run_single_iteration(conn, &stmt, &params, spec.result_mode)
                        .await
                        .map_err(|e| e.to_string())?;
                    if warmup.completed != params.len() {
                        return Err(format!(
                            "warmup completed {} queries, expected {}",
                            warmup.completed,
                            params.len()
                        ));
                    }
                    Ok::<(), String>(())
                }
                .await;

                if let Err(err) = warmup_result {
                    local_err = Some(err);
                }
            }

            start_barrier.wait().await;

            if local_err.is_none()
                && let Some(pooled) = pooled.as_mut()
            {
                let measured_result = async {
                    let conn = pooled.get_mut().map_err(|e| e.to_string())?;
                    let stmt = conn.prepare(spec.sql).await.map_err(|e| e.to_string())?;

                    for _ in 0..spec.iterations {
                        let stats = run_single_iteration(conn, &stmt, &params, spec.result_mode)
                            .await
                            .map_err(|e| e.to_string())?;
                        if stats.completed != params.len() {
                            return Err(format!(
                                "run completed {} queries, expected {}",
                                stats.completed,
                                params.len()
                            ));
                        }
                        measured.add(stats);
                    }

                    Ok::<(), String>(())
                }
                .await;

                if let Err(err) = measured_result {
                    local_err = Some(err);
                }
            }

            end_barrier.wait().await;

            if let Some(pooled) = pooled {
                pooled.release().await;
            }

            match local_err {
                Some(err) => Err(err),
                None => Ok::<BatchStats, String>(measured),
            }
        });
    }

    start_barrier.wait().await;
    let start = Instant::now();
    end_barrier.wait().await;
    let elapsed = start.elapsed();

    let mut aggregate = BatchStats::default();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(stats)) => aggregate.add(stats),
            Ok(Err(e)) => return Err(e.into()),
            Err(e) => return Err(e.to_string().into()),
        }
    }

    Ok(make_benchmark_result(aggregate, elapsed))
}

fn consume_scalar_rows(rows: &[Vec<Option<Vec<u8>>>], stats: &mut BatchStats) {
    for row in rows {
        consume_scalar_row(row, stats);
    }
}

fn consume_scalar_row(row: &[Option<Vec<u8>>], stats: &mut BatchStats) {
    stats.rows += 1;
    if let Some(value) = row.first().and_then(|col| col.as_deref()) {
        stats.bytes += value.len();
        let parsed = std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(value.len() as i64);
        stats.checksum = stats.checksum.wrapping_add(parsed as u64);
    } else {
        stats.checksum = stats.checksum.wrapping_add(1);
    }
}

fn consume_wide_row(row: &[Option<Vec<u8>>], stats: &mut BatchStats) {
    let mut row_hash = FNV_OFFSET;
    for (idx, value) in row.iter().enumerate() {
        match value.as_deref() {
            Some(bytes) => {
                stats.bytes += bytes.len();
                match idx {
                    0 | 4 => {
                        let parsed = std::str::from_utf8(bytes)
                            .ok()
                            .and_then(|s| s.parse::<i64>().ok())
                            .unwrap_or(bytes.len() as i64);
                        row_hash = row_hash.wrapping_add(parsed as u64);
                    }
                    5 => {
                        row_hash = row_hash.wrapping_add(usize::from(
                            bytes.first().is_some_and(|b| matches!(*b, b't' | b'T')),
                        ) as u64);
                    }
                    6 => {
                        let parsed = std::str::from_utf8(bytes)
                            .ok()
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        row_hash = row_hash.wrapping_add((parsed * 1000.0) as u64);
                    }
                    _ => row_hash = mix_hash(row_hash, bytes),
                }
            }
            None => {
                row_hash = mix_hash(row_hash, b"NULL");
                row_hash = row_hash.wrapping_add(idx as u64);
            }
        }
    }
    stats.rows += 1;
    stats.checksum = stats.checksum.wrapping_add(row_hash);
}

fn mix_hash(seed: u64, bytes: &[u8]) -> u64 {
    let mut hash = seed;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn make_benchmark_result(stats: BatchStats, elapsed: Duration) -> BenchmarkResult {
    let seconds = elapsed.as_secs_f64();
    BenchmarkResult {
        qps: stats.completed as f64 / seconds,
        rows_per_sec: (stats.rows > 0).then(|| stats.rows as f64 / seconds),
        mib_per_sec: (stats.bytes > 0).then(|| (stats.bytes as f64 / (1024.0 * 1024.0)) / seconds),
        checksum: stats.checksum,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut mode: Option<Mode> = None;
    let mut workload = Workload::Point;
    let mut plain = false;
    let mut expect_workload = false;

    for arg in std::env::args().skip(1) {
        if expect_workload {
            workload = Workload::parse(&arg)?;
            expect_workload = false;
            continue;
        }
        if arg == "--plain" {
            plain = true;
            continue;
        }
        if arg == "--workload" || arg == "--scenario" {
            expect_workload = true;
            continue;
        }
        if mode.is_none() {
            mode = Some(Mode::parse(&arg)?);
            continue;
        }
        return Err(format!("unexpected argument '{}'", arg).into());
    }

    if expect_workload {
        return Err("missing workload value after --workload".into());
    }

    let mode =
        mode.ok_or_else(|| "missing mode argument: single | pipeline | pool10".to_string())?;
    let spec = WorkloadSpec::new(workload);
    let params = build_param_batch(spec);

    let result = match mode {
        Mode::Single => run_single_mode(spec, &params).await?,
        Mode::Pipeline => run_pipeline_mode(spec, &params).await?,
        Mode::Pool10 => {
            if params.len() % POOL_SIZE != 0 {
                return Err(format!(
                    "workload '{}' produced {} params, not divisible by pool size {}",
                    spec.name,
                    params.len(),
                    POOL_SIZE
                )
                .into());
            }
            let per_worker = params.len() / POOL_SIZE;
            let worker_params = params
                .chunks(per_worker)
                .map(|chunk| chunk.to_vec())
                .collect();
            run_pool10_mode(spec, worker_params).await?
        }
    };

    if plain {
        println!("{:.3}", result.qps);
    } else {
        print!("qail {}/{}: {:.0} q/s", mode.name(), spec.name, result.qps);
        if let Some(rows_per_sec) = result.rows_per_sec {
            print!(" | {:.0} rows/s", rows_per_sec);
        }
        if let Some(mib_per_sec) = result.mib_per_sec {
            print!(" | {:.2} MiB/s", mib_per_sec);
        }
        if spec.result_mode != ResultMode::CompleteOnly {
            print!(" | checksum=0x{:x}", result.checksum);
        }
        println!();
    }

    Ok(())
}
