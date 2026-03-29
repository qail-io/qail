//! Single-run QAIL mode benchmark for ABBA orchestration.
//!
//! Usage:
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- single --plain
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- pipeline --plain
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- pool10 --plain
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- single --workload wide_rows --plain
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- pipeline --workload many_params --plain
//!   cargo run --release -p qail-pg --example qail_pgx_modes_once -- latency --workload monster_cte --plain

use qail_pg::driver::PreparedStatement;
use qail_pg::{
    ConnectOptions, PgBytesRow, PgConnection, PgEncoder, PgPool, PgRow, PoolConfig, TlsMode,
};
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
const LARGE_ROWS_SQL: &str = concat!(
    "SELECT id, name, bio, region, visits, active, ratio, optional_note ",
    "FROM qail_bench_payload ",
    "WHERE id <= $1::int ",
    "ORDER BY id"
);
const MONSTER_CTE_SQL: &str = concat!(
    "WITH base AS (",
    "  SELECT id, visits, active, COALESCE(octet_length(optional_note), 0) AS note_len ",
    "  FROM qail_bench_payload ",
    "  WHERE id <= $1::int",
    "), ranked AS (",
    "  SELECT id, visits, note_len, ",
    "         row_number() OVER (ORDER BY visits DESC) AS rn, ",
    "         lag(visits, 1, 0) OVER (ORDER BY visits DESC) AS prev_visits ",
    "  FROM base",
    "), bucketed AS (",
    "  SELECT (id % 32) AS bucket, ",
    "         sum(visits) AS total_visits, ",
    "         max(note_len) AS max_note_len, ",
    "         sum(CASE WHEN active THEN 1 ELSE 0 END) AS active_count ",
    "  FROM base ",
    "  GROUP BY 1",
    "), joined AS (",
    "  SELECT r.id, r.visits, r.prev_visits, r.note_len, ",
    "         b.total_visits, b.max_note_len, b.active_count ",
    "  FROM ranked r ",
    "  JOIN bucketed b ON (r.id % 32) = b.bucket ",
    "  WHERE r.rn <= 256",
    ") ",
    "SELECT (",
    "  COALESCE(sum(visits + prev_visits + note_len), 0)::bigint + ",
    "  COALESCE(max(total_visits), 0)::bigint + ",
    "  COALESCE(max(max_note_len), 0)::bigint + ",
    "  COALESCE(sum(active_count), 0)::bigint",
    ") AS total ",
    "FROM joined"
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
const LARGE_ROWS_TOTAL_QUERIES: usize = 20;
const LARGE_ROWS_ITERATIONS: usize = 2;
const MANY_PARAMS_TOTAL_QUERIES: usize = 5_000;
const MANY_PARAMS_ITERATIONS: usize = 5;
const MONSTER_CTE_TOTAL_QUERIES: usize = 20;
const MONSTER_CTE_ITERATIONS: usize = 2;
const POOL_SIZE: usize = 10;
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 1099511628211;
const BENCH_PAYLOAD_TARGET_ROWS: usize = 20_000;
const BENCH_SETUP_LOCK_SQL: &str = "SELECT pg_advisory_lock(60119029)";
const BENCH_SETUP_UNLOCK_SQL: &str = "SELECT pg_advisory_unlock(60119029)";
const CREATE_BENCH_PAYLOAD_SQL: &str = concat!(
    "CREATE TABLE IF NOT EXISTS qail_bench_payload (",
    "id INTEGER PRIMARY KEY, ",
    "name TEXT NOT NULL, ",
    "bio TEXT NOT NULL, ",
    "region TEXT NOT NULL, ",
    "visits INTEGER NOT NULL, ",
    "active BOOLEAN NOT NULL, ",
    "ratio NUMERIC(12, 3) NOT NULL, ",
    "optional_note TEXT NULL",
    ")"
);

#[derive(Clone, Copy, Debug)]
enum Mode {
    Single,
    Pipeline,
    Pool10,
    Latency,
}

impl Mode {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "single" => Ok(Self::Single),
            "pipeline" => Ok(Self::Pipeline),
            "pool10" | "pool" => Ok(Self::Pool10),
            "latency" | "lat" => Ok(Self::Latency),
            other => Err(format!(
                "unknown mode '{}' (expected single | pipeline | pool10 | latency)",
                other
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Pipeline => "pipeline",
            Self::Pool10 => "pool10",
            Self::Latency => "latency",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StatementMode {
    Prepared,
    Unprepared,
}

impl StatementMode {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "prepared" | "prep" => Ok(Self::Prepared),
            "unprepared" | "uncached" | "raw" => Ok(Self::Unprepared),
            other => Err(format!(
                "unknown statement mode '{}' (expected prepared | unprepared)",
                other
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Unprepared => "unprepared",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Workload {
    Point,
    WideRows,
    LargeRows,
    ManyParams,
    MonsterCte,
}

impl Workload {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "point" | "lookup" => Ok(Self::Point),
            "wide_rows" | "wide" => Ok(Self::WideRows),
            "large_rows" | "large" => Ok(Self::LargeRows),
            "many_params" | "params" => Ok(Self::ManyParams),
            "monster_cte" | "cte" | "server_heavy" => Ok(Self::MonsterCte),
            other => Err(format!(
                "unknown workload '{}' (expected point | wide_rows | large_rows | many_params | monster_cte)",
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
    latency_samples: usize,
    result_mode: ResultMode,
    requires_bench_payload: bool,
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
                latency_samples: 2_000,
                result_mode: ResultMode::CompleteOnly,
                requires_bench_payload: false,
            },
            Workload::WideRows => Self {
                workload,
                name: "wide_rows",
                sql: WIDE_ROWS_SQL,
                total_queries: WIDE_ROWS_TOTAL_QUERIES,
                iterations: WIDE_ROWS_ITERATIONS,
                latency_samples: 120,
                result_mode: ResultMode::WideRows,
                requires_bench_payload: false,
            },
            Workload::LargeRows => Self {
                workload,
                name: "large_rows",
                sql: LARGE_ROWS_SQL,
                total_queries: LARGE_ROWS_TOTAL_QUERIES,
                iterations: LARGE_ROWS_ITERATIONS,
                latency_samples: 40,
                result_mode: ResultMode::WideRows,
                requires_bench_payload: true,
            },
            Workload::ManyParams => Self {
                workload,
                name: "many_params",
                sql: MANY_PARAMS_SQL,
                total_queries: MANY_PARAMS_TOTAL_QUERIES,
                iterations: MANY_PARAMS_ITERATIONS,
                latency_samples: 2_000,
                result_mode: ResultMode::ScalarInt,
                requires_bench_payload: false,
            },
            Workload::MonsterCte => Self {
                workload,
                name: "monster_cte",
                sql: MONSTER_CTE_SQL,
                total_queries: MONSTER_CTE_TOTAL_QUERIES,
                iterations: MONSTER_CTE_ITERATIONS,
                latency_samples: 40,
                result_mode: ResultMode::ScalarInt,
                requires_bench_payload: true,
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

#[derive(Clone, Copy, Debug)]
struct LatencyResult {
    avg_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

#[derive(Clone, Debug)]
struct BenchDbConfig {
    host: String,
    port: u16,
    user: String,
    database: String,
    password: Option<String>,
    tls_mode: TlsMode,
}

impl BenchDbConfig {
    fn from_env() -> Result<Self, String> {
        if let Some(url) = std::env::var("QAIL_BENCH_DATABASE_URL")
            .ok()
            .or_else(|| std::env::var("DATABASE_URL").ok())
        {
            return Self::from_database_url(&url);
        }

        let host = env_override("QAIL_BENCH_HOST", "PGHOST").unwrap_or_else(|| "127.0.0.1".into());
        let port = env_override("QAIL_BENCH_PORT", "PGPORT")
            .unwrap_or_else(|| "5432".into())
            .parse::<u16>()
            .map_err(|e| format!("invalid benchmark port: {}", e))?;
        let user = env_override("QAIL_BENCH_USER", "PGUSER").unwrap_or_else(|| "orion".into());
        let database =
            env_override("QAIL_BENCH_DB", "PGDATABASE").unwrap_or_else(|| "example_staging".into());
        let password = env_override("QAIL_BENCH_PASSWORD", "PGPASSWORD");
        let sslmode =
            env_override("QAIL_BENCH_SSLMODE", "PGSSLMODE").unwrap_or_else(|| "disable".into());
        let tls_mode = TlsMode::parse_sslmode(&sslmode)
            .ok_or_else(|| format!("invalid sslmode '{}'", sslmode))?;

        Ok(Self {
            host,
            port,
            user,
            database,
            password,
            tls_mode,
        })
    }

    fn from_database_url(url: &str) -> Result<Self, String> {
        let after_scheme = url
            .split("://")
            .nth(1)
            .ok_or_else(|| "invalid DATABASE_URL: missing scheme".to_string())?;

        let (auth_part, host_db_part) = if let Some(at_pos) = after_scheme.rfind('@') {
            (Some(&after_scheme[..at_pos]), &after_scheme[at_pos + 1..])
        } else {
            (None, after_scheme)
        };

        let (user, password) = if let Some(auth) = auth_part {
            let parts: Vec<&str> = auth.splitn(2, ':').collect();
            if parts.len() == 2 {
                (percent_decode(parts[0]), Some(percent_decode(parts[1])))
            } else {
                (percent_decode(parts[0]), None)
            }
        } else {
            return Err("invalid DATABASE_URL: missing user".to_string());
        };

        let (host_port, database) = if let Some(slash_pos) = host_db_part.find('/') {
            let raw_db = &host_db_part[slash_pos + 1..];
            let db = raw_db.split('?').next().unwrap_or(raw_db).to_string();
            (&host_db_part[..slash_pos], db)
        } else {
            return Err("invalid DATABASE_URL: missing database name".to_string());
        };

        let (host, port) = if let Some(colon_pos) = host_port.rfind(':') {
            let port_str = &host_port[colon_pos + 1..];
            let port = port_str
                .parse::<u16>()
                .map_err(|_| format!("invalid DATABASE_URL port '{}'", port_str))?;
            (host_port[..colon_pos].to_string(), port)
        } else {
            (host_port.to_string(), 5432)
        };

        let tls_mode = url
            .split('?')
            .nth(1)
            .and_then(|query| {
                query.split('&').find_map(|pair| {
                    let mut kv = pair.splitn(2, '=');
                    let key = kv.next()?.trim();
                    let value = kv.next().unwrap_or_default().trim();
                    key.eq_ignore_ascii_case("sslmode")
                        .then(|| TlsMode::parse_sslmode(value))
                        .flatten()
                })
            })
            .unwrap_or(TlsMode::Disable);

        Ok(Self {
            host,
            port,
            user,
            database,
            password,
            tls_mode,
        })
    }
}

fn env_override(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

fn percent_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2
                && let Ok(byte) = u8::from_str_radix(&hex, 16)
            {
                result.push(byte as char);
                continue;
            }
            result.push('%');
            result.push_str(&hex);
        } else {
            result.push(c);
        }
    }

    result
}

async fn connect_bench_connection(
    cfg: &BenchDbConfig,
) -> Result<PgConnection, Box<dyn std::error::Error>> {
    let mut options = ConnectOptions::default();
    options.tls_mode = cfg.tls_mode;
    Ok(PgConnection::connect_with_options(
        &cfg.host,
        cfg.port,
        &cfg.user,
        &cfg.database,
        cfg.password.as_deref(),
        options,
    )
    .await?)
}

async fn connect_bench_pool(cfg: &BenchDbConfig) -> Result<PgPool, Box<dyn std::error::Error>> {
    let mut config = PoolConfig::new_dev(&cfg.host, cfg.port, &cfg.user, &cfg.database)
        .max_connections(POOL_SIZE)
        .min_connections(POOL_SIZE)
        .tls_mode(cfg.tls_mode);
    if let Some(password) = &cfg.password {
        config = config.password(password);
    }
    Ok(PgPool::connect(config).await?)
}

fn build_param_batch(spec: WorkloadSpec) -> Vec<Vec<Option<Vec<u8>>>> {
    match spec.workload {
        Workload::Point => build_point_params(spec.total_queries),
        Workload::WideRows => build_wide_rows_params(spec.total_queries),
        Workload::LargeRows => build_large_rows_params(spec.total_queries),
        Workload::ManyParams => build_many_params_batch(spec.total_queries),
        Workload::MonsterCte => build_monster_cte_params(spec.total_queries),
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

fn build_large_rows_params(total: usize) -> Vec<Vec<Option<Vec<u8>>>> {
    const ROW_COUNTS: [&str; 4] = ["10000", "12000", "14000", "16000"];

    (0..total)
        .map(|i| vec![Some(ROW_COUNTS[i % ROW_COUNTS.len()].as_bytes().to_vec())])
        .collect()
}

fn build_monster_cte_params(total: usize) -> Vec<Vec<Option<Vec<u8>>>> {
    const ROW_COUNTS: [&str; 4] = ["8000", "12000", "16000", "20000"];

    (0..total)
        .map(|i| vec![Some(ROW_COUNTS[i % ROW_COUNTS.len()].as_bytes().to_vec())])
        .collect()
}

fn parse_first_i64(rows: &[PgRow]) -> Result<i64, Box<dyn std::error::Error>> {
    let value = rows
        .first()
        .and_then(|row| row.get_bytes(0))
        .ok_or_else(|| "expected one scalar row".to_string())?;
    let parsed = std::str::from_utf8(value)
        .map_err(|e| format!("benchmark setup value is not utf8: {}", e))?
        .parse::<i64>()
        .map_err(|e| format!("benchmark setup value is not integer: {}", e))?;
    Ok(parsed)
}

async fn ensure_bench_payload(conn: &mut PgConnection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_simple(BENCH_SETUP_LOCK_SQL).await?;
    let setup_result = async {
        conn.execute_simple(CREATE_BENCH_PAYLOAD_SQL).await?;

        let current_rows = parse_first_i64(
            &conn
                .query_rows_with_result_format(
                    "SELECT COALESCE(MAX(id), 0) FROM qail_bench_payload",
                    &[],
                    PgEncoder::FORMAT_TEXT,
                )
                .await?,
        )?;

        if current_rows < BENCH_PAYLOAD_TARGET_ROWS as i64 {
            let start_id = current_rows + 1;
            let insert_sql = format!(
                concat!(
                    "INSERT INTO qail_bench_payload ",
                    "(id, name, bio, region, visits, active, ratio, optional_note) ",
                    "SELECT gs, ",
                    "       ('harbor-' || gs)::text, ",
                    "       repeat(md5(gs::text), 4), ",
                    "       repeat(md5((gs * 17)::text), 3), ",
                    "       (gs * 11), ",
                    "       (gs % 2 = 0), ",
                    "       round((gs::numeric / 7.0), 3), ",
                    "       CASE WHEN gs % 5 = 0 THEN NULL ELSE repeat(md5((gs * 3)::text), 2) END ",
                    "FROM generate_series({}, {}) AS gs ",
                    "ON CONFLICT (id) DO NOTHING"
                ),
                start_id, BENCH_PAYLOAD_TARGET_ROWS
            );
            conn.execute_simple(&insert_sql).await?;
            let _ = conn.execute_simple("ANALYZE qail_bench_payload").await;
        }

        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let unlock_result = conn.execute_simple(BENCH_SETUP_UNLOCK_SQL).await;

    if let Err(err) = setup_result {
        return Err(err);
    }
    unlock_result?;
    Ok(())
}

async fn ensure_workload_ready(
    cfg: &BenchDbConfig,
    spec: WorkloadSpec,
) -> Result<(), Box<dyn std::error::Error>> {
    if !spec.requires_bench_payload {
        return Ok(());
    }

    let mut conn = connect_bench_connection(cfg).await?;
    ensure_bench_payload(&mut conn).await
}

async fn run_single_iteration_prepared(
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
                conn.query_prepared_single_reuse_visit_first_column_bytes_with_result_format(
                    stmt,
                    p,
                    PgEncoder::FORMAT_TEXT,
                    |value| {
                        consume_scalar_value(value, &mut stats);
                        Ok(())
                    },
                )
                .await?;
                stats.completed += 1;
            }
            ResultMode::WideRows => {
                conn.query_prepared_single_reuse_visit_bytes_rows_with_result_format(
                    stmt,
                    p,
                    PgEncoder::FORMAT_TEXT,
                    |row| {
                        consume_wide_bytes_row(row, &mut stats);
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

async fn run_single_iteration_unprepared(
    conn: &mut PgConnection,
    sql: &str,
    params: &[Vec<Option<Vec<u8>>>],
    result_mode: ResultMode,
) -> Result<BatchStats, Box<dyn std::error::Error>> {
    let mut stats = BatchStats::default();

    for p in params {
        match result_mode {
            ResultMode::CompleteOnly => {
                conn.query_count(sql, p).await?;
                stats.completed += 1;
            }
            ResultMode::ScalarInt => {
                conn.query_visit_first_column_bytes_with_result_format(
                    sql,
                    p,
                    PgEncoder::FORMAT_TEXT,
                    |value| {
                        consume_scalar_value(value, &mut stats);
                        Ok(())
                    },
                )
                .await?;
                stats.completed += 1;
            }
            ResultMode::WideRows => {
                conn.query_visit_bytes_rows_with_result_format(
                    sql,
                    p,
                    PgEncoder::FORMAT_TEXT,
                    |row| {
                        consume_wide_bytes_row(row, &mut stats);
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

async fn run_single_iteration(
    conn: &mut PgConnection,
    sql: &str,
    stmt: Option<&PreparedStatement>,
    params: &[Vec<Option<Vec<u8>>>],
    result_mode: ResultMode,
    statement_mode: StatementMode,
) -> Result<BatchStats, Box<dyn std::error::Error>> {
    match statement_mode {
        StatementMode::Prepared => {
            run_single_iteration_prepared(
                conn,
                stmt.ok_or_else(|| "missing prepared statement for prepared mode".to_string())?,
                params,
                result_mode,
            )
            .await
        }
        StatementMode::Unprepared => {
            run_single_iteration_unprepared(conn, sql, params, result_mode).await
        }
    }
}

async fn run_latency_mode(
    cfg: &BenchDbConfig,
    spec: WorkloadSpec,
    statement_mode: StatementMode,
    params: &[Vec<Option<Vec<u8>>>],
) -> Result<LatencyResult, Box<dyn std::error::Error>> {
    let mut conn = connect_bench_connection(cfg).await?;
    let stmt = match statement_mode {
        StatementMode::Prepared => Some(conn.prepare(spec.sql).await?),
        StatementMode::Unprepared => None,
    };

    let warmup_count = spec.latency_samples.min(20);
    for param in params.iter().cycle().take(warmup_count) {
        let stats = run_single_iteration(
            &mut conn,
            spec.sql,
            stmt.as_ref(),
            std::slice::from_ref(param),
            spec.result_mode,
            statement_mode,
        )
        .await?;
        if stats.completed != 1 {
            return Err(format!(
                "latency warmup completed {} queries, expected 1",
                stats.completed
            )
            .into());
        }
    }

    let mut samples = Vec::with_capacity(spec.latency_samples);
    let mut total = Duration::ZERO;
    for param in params.iter().cycle().take(spec.latency_samples) {
        let start = Instant::now();
        let stats = run_single_iteration(
            &mut conn,
            spec.sql,
            stmt.as_ref(),
            std::slice::from_ref(param),
            spec.result_mode,
            statement_mode,
        )
        .await?;
        let elapsed = start.elapsed();
        if stats.completed != 1 {
            return Err(format!(
                "latency sample completed {} queries, expected 1",
                stats.completed
            )
            .into());
        }
        total += elapsed;
        samples.push(elapsed);
    }

    samples.sort_unstable();
    let len = samples.len();
    let p50 = samples[len / 2];
    let p95 = samples[((len as f64 * 0.95).ceil() as usize)
        .saturating_sub(1)
        .min(len - 1)];
    let p99 = samples[((len as f64 * 0.99).ceil() as usize)
        .saturating_sub(1)
        .min(len - 1)];

    Ok(LatencyResult {
        avg_ms: total.as_secs_f64() * 1000.0 / len as f64,
        p50_ms: p50.as_secs_f64() * 1000.0,
        p95_ms: p95.as_secs_f64() * 1000.0,
        p99_ms: p99.as_secs_f64() * 1000.0,
    })
}

async fn run_pipeline_iteration_prepared(
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
            let mut stats = BatchStats::default();
            stats.completed = conn
                .pipeline_execute_prepared_visit_first_column_bytes(stmt, params, |value| {
                    consume_scalar_value(value, &mut stats);
                    Ok(())
                })
                .await?;
            if stats.completed != params.len() {
                return Err(format!(
                    "pipeline completed {} queries, expected {}",
                    stats.completed,
                    params.len()
                )
                .into());
            }
            Ok(stats)
        }
        ResultMode::WideRows => {
            let mut stats = BatchStats::default();
            stats.completed = conn
                .pipeline_execute_prepared_visit_bytes_rows(stmt, params, |row| {
                    consume_wide_bytes_row(row, &mut stats);
                    Ok(())
                })
                .await?;
            Ok(stats)
        }
    }
}

async fn run_pipeline_iteration_unprepared(
    conn: &mut PgConnection,
    sql: &str,
    params: &[Vec<Option<Vec<u8>>>],
    result_mode: ResultMode,
) -> Result<BatchStats, Box<dyn std::error::Error>> {
    let queries: Vec<(&str, &[Option<Vec<u8>>])> = params
        .iter()
        .map(|param_set| (sql, param_set.as_slice()))
        .collect();

    match result_mode {
        ResultMode::CompleteOnly => {
            let completed = conn.query_pipeline_count(&queries).await?;
            if completed != params.len() {
                return Err(format!(
                    "pipeline completed {} queries, expected {}",
                    completed,
                    params.len()
                )
                .into());
            }
            Ok(BatchStats {
                completed,
                ..BatchStats::default()
            })
        }
        ResultMode::ScalarInt => {
            let mut stats = BatchStats::default();
            stats.completed = conn
                .query_pipeline_visit_first_column_bytes(&queries, |value| {
                    consume_scalar_value(value, &mut stats);
                    Ok(())
                })
                .await?;
            if stats.completed != params.len() {
                return Err(format!(
                    "pipeline completed {} queries, expected {}",
                    stats.completed,
                    params.len()
                )
                .into());
            }
            Ok(stats)
        }
        ResultMode::WideRows => {
            let mut stats = BatchStats::default();
            stats.completed = conn
                .query_pipeline_visit_bytes_rows(&queries, |row| {
                    consume_wide_bytes_row(row, &mut stats);
                    Ok(())
                })
                .await?;
            Ok(stats)
        }
    }
}

async fn run_pipeline_iteration(
    conn: &mut PgConnection,
    sql: &str,
    stmt: Option<&PreparedStatement>,
    params: &[Vec<Option<Vec<u8>>>],
    result_mode: ResultMode,
    statement_mode: StatementMode,
) -> Result<BatchStats, Box<dyn std::error::Error>> {
    match statement_mode {
        StatementMode::Prepared => {
            run_pipeline_iteration_prepared(
                conn,
                stmt.ok_or_else(|| "missing prepared statement for prepared mode".to_string())?,
                params,
                result_mode,
            )
            .await
        }
        StatementMode::Unprepared => {
            run_pipeline_iteration_unprepared(conn, sql, params, result_mode).await
        }
    }
}

async fn run_single_mode(
    cfg: &BenchDbConfig,
    spec: WorkloadSpec,
    statement_mode: StatementMode,
    params: &[Vec<Option<Vec<u8>>>],
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    ensure_workload_ready(cfg, spec).await?;
    let mut conn = connect_bench_connection(cfg).await?;
    let stmt = match statement_mode {
        StatementMode::Prepared => Some(conn.prepare(spec.sql).await?),
        StatementMode::Unprepared => None,
    };

    let warmup = run_single_iteration(
        &mut conn,
        spec.sql,
        stmt.as_ref(),
        params,
        spec.result_mode,
        statement_mode,
    )
    .await?;
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
        let stats = run_single_iteration(
            &mut conn,
            spec.sql,
            stmt.as_ref(),
            params,
            spec.result_mode,
            statement_mode,
        )
        .await?;
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
    cfg: &BenchDbConfig,
    spec: WorkloadSpec,
    statement_mode: StatementMode,
    params: &[Vec<Option<Vec<u8>>>],
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    ensure_workload_ready(cfg, spec).await?;
    let mut conn = connect_bench_connection(cfg).await?;
    let stmt = match statement_mode {
        StatementMode::Prepared => Some(conn.prepare(spec.sql).await?),
        StatementMode::Unprepared => None,
    };

    let warmup = run_pipeline_iteration(
        &mut conn,
        spec.sql,
        stmt.as_ref(),
        params,
        spec.result_mode,
        statement_mode,
    )
    .await?;
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
        let stats = run_pipeline_iteration(
            &mut conn,
            spec.sql,
            stmt.as_ref(),
            params,
            spec.result_mode,
            statement_mode,
        )
        .await?;
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
    cfg: &BenchDbConfig,
    spec: WorkloadSpec,
    statement_mode: StatementMode,
    worker_params: Vec<Vec<Vec<Option<Vec<u8>>>>>,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    ensure_workload_ready(cfg, spec).await?;
    let pool = connect_bench_pool(cfg).await?;

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
            let mut stmt: Option<PreparedStatement> = None;
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
                    if statement_mode == StatementMode::Prepared {
                        stmt = Some(conn.prepare(spec.sql).await.map_err(|e| e.to_string())?);
                    }
                    let warmup = run_single_iteration(
                        conn,
                        spec.sql,
                        stmt.as_ref(),
                        &params,
                        spec.result_mode,
                        statement_mode,
                    )
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

                    for _ in 0..spec.iterations {
                        let stats = run_single_iteration(
                            conn,
                            spec.sql,
                            stmt.as_ref(),
                            &params,
                            spec.result_mode,
                            statement_mode,
                        )
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

fn consume_scalar_value(value: Option<&[u8]>, stats: &mut BatchStats) {
    stats.rows += 1;
    if let Some(value) = value {
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

fn consume_wide_cell(idx: usize, value: Option<&[u8]>, row_hash: &mut u64, stats: &mut BatchStats) {
    match value {
        Some(bytes) => {
            stats.bytes += bytes.len();
            match idx {
                0 | 4 => {
                    let parsed = std::str::from_utf8(bytes)
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or(bytes.len() as i64);
                    *row_hash = row_hash.wrapping_add(parsed as u64);
                }
                5 => {
                    *row_hash = row_hash.wrapping_add(usize::from(
                        bytes.first().is_some_and(|b| matches!(*b, b't' | b'T')),
                    ) as u64);
                }
                6 => {
                    let parsed = std::str::from_utf8(bytes)
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    *row_hash = row_hash.wrapping_add((parsed * 1000.0) as u64);
                }
                _ => *row_hash = mix_hash(*row_hash, bytes),
            }
        }
        None => {
            *row_hash = mix_hash(*row_hash, b"NULL");
            *row_hash = row_hash.wrapping_add(idx as u64);
        }
    }
}

fn finish_wide_row(row_hash: u64, stats: &mut BatchStats) {
    stats.rows += 1;
    stats.checksum = stats.checksum.wrapping_add(row_hash);
}

fn consume_wide_bytes_row(row: &PgBytesRow, stats: &mut BatchStats) {
    let mut row_hash = FNV_OFFSET;
    row.for_each_column(|idx, value| consume_wide_cell(idx, value, &mut row_hash, stats));
    finish_wide_row(row_hash, stats);
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
    let mut statement_mode = StatementMode::Prepared;
    let mut plain = false;
    let mut expect_workload = false;
    let mut expect_statement_mode = false;

    for arg in std::env::args().skip(1) {
        if expect_workload {
            workload = Workload::parse(&arg)?;
            expect_workload = false;
            continue;
        }
        if expect_statement_mode {
            statement_mode = StatementMode::parse(&arg)?;
            expect_statement_mode = false;
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
        if arg == "--statement-mode" || arg == "--stmt-mode" {
            expect_statement_mode = true;
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
    if expect_statement_mode {
        return Err("missing statement mode after --statement-mode".into());
    }

    let mode = mode
        .ok_or_else(|| "missing mode argument: single | pipeline | pool10 | latency".to_string())?;
    let cfg = BenchDbConfig::from_env()?;
    let spec = WorkloadSpec::new(workload);
    let params = build_param_batch(spec);

    match mode {
        Mode::Single => {
            let result = run_single_mode(&cfg, spec, statement_mode, &params).await?;
            if plain {
                println!("{:.3}", result.qps);
            } else {
                print!(
                    "qail {}/{}/{}: {:.0} q/s",
                    mode.name(),
                    statement_mode.name(),
                    spec.name,
                    result.qps
                );
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
        }
        Mode::Pipeline => {
            let result = run_pipeline_mode(&cfg, spec, statement_mode, &params).await?;
            if plain {
                println!("{:.3}", result.qps);
            } else {
                print!(
                    "qail {}/{}/{}: {:.0} q/s",
                    mode.name(),
                    statement_mode.name(),
                    spec.name,
                    result.qps
                );
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
        }
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
            let result = run_pool10_mode(&cfg, spec, statement_mode, worker_params).await?;
            if plain {
                println!("{:.3}", result.qps);
            } else {
                print!(
                    "qail {}/{}/{}: {:.0} q/s",
                    mode.name(),
                    statement_mode.name(),
                    spec.name,
                    result.qps
                );
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
        }
        Mode::Latency => {
            ensure_workload_ready(&cfg, spec).await?;
            let result = run_latency_mode(&cfg, spec, statement_mode, &params).await?;
            if plain {
                println!(
                    "{:.6},{:.6},{:.6},{:.6}",
                    result.p50_ms, result.p95_ms, result.p99_ms, result.avg_ms
                );
            } else {
                println!(
                    "qail {}/{}/{}: p50={:.3} ms | p95={:.3} ms | p99={:.3} ms | avg={:.3} ms",
                    mode.name(),
                    statement_mode.name(),
                    spec.name,
                    result.p50_ms,
                    result.p95_ms,
                    result.p99_ms,
                    result.avg_ms
                );
            }
        }
    }

    Ok(())
}
