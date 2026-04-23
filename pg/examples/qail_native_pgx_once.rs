//! Single-run native-QAIL benchmark for ABBA orchestration.
//!
//! QAIL uses native AST/DSL APIs here. This is intentionally separate from
//! `qail_pgx_modes_once.rs`, which benchmarks raw-SQL driver paths.

use qail_core::ast::Qail;
use qail_core::ast::builders::{count, count_filter, eq as cond_eq, max, sum};
use bytes::{Bytes, BytesMut};
use qail_pg::protocol::AstEncoder;
use qail_pg::{ConnectOptions, PgBytesRow, PgConnection, PgEncoder, PgPool, PgRow, PoolConfig, TlsMode};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

const POINT_TOTAL_QUERIES: usize = 10_000;
const POINT_ITERATIONS: usize = 5;
const WIDE_ROWS_TOTAL_QUERIES: usize = 100;
const WIDE_ROWS_ITERATIONS: usize = 3;
const LARGE_ROWS_TOTAL_QUERIES: usize = 20;
const LARGE_ROWS_ITERATIONS: usize = 2;
const MANY_PARAMS_TOTAL_QUERIES: usize = 5_000;
const MANY_PARAMS_ITERATIONS: usize = 5;
const AGGREGATE_TOTAL_QUERIES: usize = 2000;
const AGGREGATE_ITERATIONS: usize = 3;
const MANY_PARAMS_PARAM_COUNT: usize = 32;
const POOL_SIZE: usize = 10;
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 1099511628211;
const BENCH_PAYLOAD_TARGET_ROWS: usize = 20_000;
const BENCH_MANY_PARAMS_TARGET_ROWS: usize = 512;
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
}

#[derive(Clone, Copy, Debug)]
enum Workload {
    Point,
    WideRows,
    LargeRows,
    ManyParams,
    Aggregate,
}

impl Workload {
    fn parse(s: &str) -> Result<Self, String> {
        match s {
            "point" | "lookup" => Ok(Self::Point),
            "wide_rows" | "wide" => Ok(Self::WideRows),
            "large_rows" | "large" => Ok(Self::LargeRows),
            "many_params" | "params" => Ok(Self::ManyParams),
            "aggregate" | "server_heavy" | "agg" => Ok(Self::Aggregate),
            other => Err(format!(
                "unknown workload '{}' (expected point | wide_rows | large_rows | many_params | aggregate)",
                other
            )),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum ResultMode {
    PointRows,
    WideRows,
    ScalarInt,
    AggregateScalars,
}

#[derive(Clone, Copy, Debug)]
struct WorkloadSpec {
    workload: Workload,
    name: &'static str,
    total_queries: usize,
    iterations: usize,
    latency_samples: usize,
    result_mode: ResultMode,
    requires_payload: bool,
    requires_many_params: bool,
}

impl WorkloadSpec {
    fn new(workload: Workload) -> Self {
        let spec = match workload {
            Workload::Point => Self {
                workload,
                name: "point",
                total_queries: POINT_TOTAL_QUERIES,
                iterations: POINT_ITERATIONS,
                latency_samples: 2_000,
                result_mode: ResultMode::PointRows,
                requires_payload: false,
                requires_many_params: false,
            },
            Workload::WideRows => Self {
                workload,
                name: "wide_rows",
                total_queries: WIDE_ROWS_TOTAL_QUERIES,
                iterations: WIDE_ROWS_ITERATIONS,
                latency_samples: 120,
                result_mode: ResultMode::WideRows,
                requires_payload: true,
                requires_many_params: false,
            },
            Workload::LargeRows => Self {
                workload,
                name: "large_rows",
                total_queries: LARGE_ROWS_TOTAL_QUERIES,
                iterations: LARGE_ROWS_ITERATIONS,
                latency_samples: 40,
                result_mode: ResultMode::WideRows,
                requires_payload: true,
                requires_many_params: false,
            },
            Workload::ManyParams => Self {
                workload,
                name: "many_params",
                total_queries: MANY_PARAMS_TOTAL_QUERIES,
                iterations: MANY_PARAMS_ITERATIONS,
                latency_samples: 2_000,
                result_mode: ResultMode::ScalarInt,
                requires_payload: false,
                requires_many_params: true,
            },
            Workload::Aggregate => Self {
                workload,
                name: "aggregate",
                total_queries: AGGREGATE_TOTAL_QUERIES,
                iterations: AGGREGATE_ITERATIONS,
                latency_samples: 120,
                result_mode: ResultMode::AggregateScalars,
                requires_payload: true,
                requires_many_params: false,
            },
        };

        spec.with_env_scale()
    }

    fn with_env_scale(mut self) -> Self {
        let scale = env_usize("QAIL_BENCH_SCALE").unwrap_or(1).max(1);
        self.total_queries = self.total_queries.saturating_mul(scale);
        self.latency_samples = self.latency_samples.saturating_mul(scale);
        self
    }
}

#[derive(Clone, Debug)]
enum BenchInput {
    Id(i64),
    Limit(i64),
    ManyParams(Vec<i64>),
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
struct PreparedBatch {
    sql: String,
    params_batch: Vec<Vec<Option<Vec<u8>>>>,
}

#[derive(Clone, Debug)]
struct PreparedSinglesWireBatch {
    wires: Vec<Bytes>,
}

#[derive(Clone, Debug, Default)]
struct SingleProfile {
    send: Duration,
    consume: Duration,
    calls: usize,
}

impl SingleProfile {
    fn record(&mut self, send: Duration, consume: Duration) {
        self.send += send;
        self.consume += consume;
        self.calls += 1;
    }
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

fn want_single_profile() -> bool {
    std::env::var("QAIL_PROFILE_SINGLE")
        .ok()
        .map(|v| {
            let normalized = v.trim().to_ascii_lowercase();
            !(normalized.is_empty()
                || normalized == "0"
                || normalized == "false"
                || normalized == "no"
                || normalized == "off")
        })
        .unwrap_or(false)
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
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

fn many_param_column_name(idx: usize) -> String {
    format!("p{:02}", idx + 1)
}

fn build_many_params_create_sql() -> String {
    let mut sql = String::from(
        "CREATE TABLE IF NOT EXISTS qail_bench_many_params (slot INTEGER PRIMARY KEY, total BIGINT NOT NULL",
    );
    for idx in 0..MANY_PARAMS_PARAM_COUNT {
        sql.push_str(", ");
        sql.push_str(&many_param_column_name(idx));
        sql.push_str(" INTEGER NOT NULL");
    }
    sql.push(')');
    sql
}

fn build_many_params_index_sql() -> String {
    let cols = (0..MANY_PARAMS_PARAM_COUNT)
        .map(many_param_column_name)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS qail_bench_many_params_lookup_idx ON qail_bench_many_params ({})",
        cols
    )
}

fn build_many_params_insert_sql(start_slot: usize, end_slot: usize) -> String {
    let sum_coeff: i64 = (1..=MANY_PARAMS_PARAM_COUNT as i64).sum();
    let mut sql = String::from("INSERT INTO qail_bench_many_params (slot, total");
    for idx in 0..MANY_PARAMS_PARAM_COUNT {
        sql.push_str(", ");
        sql.push_str(&many_param_column_name(idx));
    }
    sql.push_str(") SELECT gs, ");
    sql.push_str(&format!("(gs * {})::bigint", sum_coeff));
    for idx in 0..MANY_PARAMS_PARAM_COUNT {
        sql.push_str(", ");
        sql.push_str(&format!("gs * {}", idx + 1));
    }
    sql.push_str(&format!(
        " FROM generate_series({}, {}) AS gs ON CONFLICT (slot) DO NOTHING",
        start_slot, end_slot
    ));
    sql
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
                    0,
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

async fn ensure_bench_many_params(
    conn: &mut PgConnection,
) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute_simple(BENCH_SETUP_LOCK_SQL).await?;
    let setup_result = async {
        conn.execute_simple(&build_many_params_create_sql()).await?;
        conn.execute_simple(&build_many_params_index_sql()).await?;

        let current_rows = parse_first_i64(
            &conn
                .query_rows_with_result_format(
                    "SELECT COALESCE(MAX(slot), 0) FROM qail_bench_many_params",
                    &[],
                    0,
                )
                .await?,
        )?;

        if current_rows < BENCH_MANY_PARAMS_TARGET_ROWS as i64 {
            conn.execute_simple(&build_many_params_insert_sql(
                current_rows as usize + 1,
                BENCH_MANY_PARAMS_TARGET_ROWS,
            ))
            .await?;
            let _ = conn.execute_simple("ANALYZE qail_bench_many_params").await;
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
    if !spec.requires_payload && !spec.requires_many_params {
        return Ok(());
    }

    let mut conn = connect_bench_connection(cfg).await?;
    if spec.requires_payload {
        ensure_bench_payload(&mut conn).await?;
    }
    if spec.requires_many_params {
        ensure_bench_many_params(&mut conn).await?;
    }
    Ok(())
}

fn build_inputs(spec: WorkloadSpec) -> Vec<BenchInput> {
    match spec.workload {
        Workload::Point => (1..=spec.total_queries)
            .map(|i| BenchInput::Id(((i % 10_000) + 1) as i64))
            .collect(),
        Workload::WideRows => {
            const ROW_COUNTS: [i64; 4] = [128, 256, 384, 512];
            (0..spec.total_queries)
                .map(|i| BenchInput::Limit(ROW_COUNTS[i % ROW_COUNTS.len()]))
                .collect()
        }
        Workload::LargeRows => {
            const ROW_COUNTS: [i64; 4] = [10_000, 12_000, 14_000, 16_000];
            (0..spec.total_queries)
                .map(|i| BenchInput::Limit(ROW_COUNTS[i % ROW_COUNTS.len()]))
                .collect()
        }
        Workload::ManyParams => (0..spec.total_queries)
            .map(|i| {
                let slot = ((i % BENCH_MANY_PARAMS_TARGET_ROWS) + 1) as i64;
                BenchInput::ManyParams(
                    (0..MANY_PARAMS_PARAM_COUNT)
                        .map(|idx| slot * (idx as i64 + 1))
                        .collect(),
                )
            })
            .collect(),
        Workload::Aggregate => {
            const ROW_COUNTS: [i64; 4] = [8_000, 12_000, 16_000, 20_000];
            (0..spec.total_queries)
                .map(|i| BenchInput::Limit(ROW_COUNTS[i % ROW_COUNTS.len()]))
                .collect()
        }
    }
}

fn build_qail_command(spec: WorkloadSpec, input: &BenchInput) -> Qail {
    match (spec.workload, input) {
        (Workload::Point, BenchInput::Id(id)) => {
            Qail::get("harbors").columns(["id", "name"]).eq("id", *id)
        }
        (Workload::WideRows | Workload::LargeRows, BenchInput::Limit(limit)) => {
            Qail::get("qail_bench_payload")
                .columns([
                    "id",
                    "name",
                    "bio",
                    "region",
                    "visits",
                    "active",
                    "ratio",
                    "optional_note",
                ])
                .lte("id", *limit)
                .order_asc("id")
        }
        (Workload::ManyParams, BenchInput::ManyParams(values)) => {
            let mut cmd = Qail::get("qail_bench_many_params")
                .columns(["total"])
                .limit(1);
            for (idx, value) in values.iter().enumerate() {
                cmd = cmd.eq(many_param_column_name(idx), *value);
            }
            cmd
        }
        (Workload::Aggregate, BenchInput::Limit(limit)) => Qail::get("qail_bench_payload")
            .select_exprs([
                sum("visits").alias("sum_visits"),
                max("visits").alias("max_visits"),
                count().alias("row_count"),
                count_filter(vec![cond_eq("active", true)]).alias("active_count"),
            ])
            .lte("id", *limit),
        _ => unreachable!("invalid input for workload {:?}", spec.workload),
    }
}

fn build_command_batch(spec: WorkloadSpec) -> Vec<Qail> {
    build_inputs(spec)
        .iter()
        .map(|input| build_qail_command(spec, input))
        .collect()
}

fn encode_prepared_batch(cmds: &[Qail]) -> Result<PreparedBatch, Box<dyn std::error::Error>> {
    let mut sql_template: Option<String> = None;
    let mut params_batch = Vec::with_capacity(cmds.len());

    for cmd in cmds {
        let (sql, params) =
            AstEncoder::encode_cmd_sql(cmd).map_err(|e| format!("ast encode failed: {}", e))?;
        if let Some(existing) = &sql_template {
            if existing != &sql {
                return Err(format!(
                    "native benchmark expected one SQL template per batch; saw both '{}' and '{}'",
                    existing, sql
                )
                .into());
            }
        } else {
            sql_template = Some(sql);
        }
        params_batch.push(params);
    }

    Ok(PreparedBatch {
        sql: sql_template.unwrap_or_default(),
        params_batch,
    })
}

fn encode_prepared_singles_wire_batch(
    stmt: &qail_pg::driver::PreparedStatement,
    params_batch: &[Vec<Option<Vec<u8>>>],
    result_format: i16,
) -> Result<PreparedSinglesWireBatch, Box<dyn std::error::Error>> {
    let mut wires = Vec::with_capacity(params_batch.len());

    for params in params_batch {
        let needed = PgEncoder::bind_execute_sync_wire_len_with_formats(
            stmt.name(),
            params,
            PgEncoder::FORMAT_TEXT,
            result_format,
        )
        .map_err(|e| format!("failed to size prepared single wire: {}", e))?;

        let mut wire = BytesMut::with_capacity(needed);
        PgEncoder::encode_bind_to_with_result_format(&mut wire, stmt.name(), params, result_format)
            .map_err(|e| format!("failed to encode prepared single wire: {}", e))?;
        PgEncoder::encode_execute_to(&mut wire);
        PgEncoder::encode_sync_to(&mut wire);
        debug_assert_eq!(wire.len(), needed);
        wires.push(wire.freeze());
    }

    Ok(PreparedSinglesWireBatch { wires })
}

fn split_commands_for_pool(cmds: &[Qail]) -> Result<Vec<Vec<Qail>>, Box<dyn std::error::Error>> {
    if cmds.len() % POOL_SIZE != 0 {
        return Err(format!(
            "workload produced {} commands, not divisible by pool size {}",
            cmds.len(),
            POOL_SIZE
        )
        .into());
    }

    let per_worker = cmds.len() / POOL_SIZE;
    Ok((0..POOL_SIZE)
        .map(|idx| {
            let start = idx * per_worker;
            cmds[start..start + per_worker].to_vec()
        })
        .collect())
}

fn consume_point_row(row: &PgBytesRow, stats: &mut BatchStats) {
    stats.rows += 1;
    let mut row_hash = FNV_OFFSET;
    if let Some(id) = row.get_bytes(0) {
        stats.bytes += id.len();
        let parsed = std::str::from_utf8(id)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(id.len() as i64);
        row_hash = row_hash.wrapping_add(parsed as u64);
    }
    if let Some(name) = row.get_bytes(1) {
        stats.bytes += name.len();
        row_hash = mix_hash(row_hash, name);
    }
    stats.checksum = stats.checksum.wrapping_add(row_hash);
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

fn consume_aggregate_columns(columns: [Option<&[u8]>; 4], stats: &mut BatchStats) {
    stats.rows += 1;
    let mut row_hash = FNV_OFFSET;
    for value in columns {
        if let Some(value) = value {
            stats.bytes += value.len();
            let parsed = std::str::from_utf8(value)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(value.len() as i64);
            row_hash = row_hash.wrapping_add(parsed as u64);
        }
    }
    stats.checksum = stats.checksum.wrapping_add(row_hash);
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

fn consume_wide_row(row: &PgBytesRow, stats: &mut BatchStats) {
    stats.rows += 1;
    let mut row_hash = FNV_OFFSET;
    for idx in 0..row.len() {
        consume_wide_cell(idx, row.get_bytes(idx), &mut row_hash, stats);
    }
    stats.checksum = stats.checksum.wrapping_add(row_hash);
}

fn consume_bytes_row(row: &PgBytesRow, result_mode: ResultMode, stats: &mut BatchStats) {
    match result_mode {
        ResultMode::PointRows => consume_point_row(row, stats),
        ResultMode::WideRows => consume_wide_row(row, stats),
        ResultMode::ScalarInt => unreachable!("scalar mode uses first-column visitor"),
        ResultMode::AggregateScalars => unreachable!("aggregate mode uses four-column visitor"),
    }
}

async fn run_single_iteration(
    conn: &mut PgConnection,
    stmt: &qail_pg::driver::PreparedStatement,
    params_batch: &[Vec<Option<Vec<u8>>>],
    result_mode: ResultMode,
) -> Result<BatchStats, Box<dyn std::error::Error>> {
    let mut stats = BatchStats::default();
    for params in params_batch {
        match result_mode {
            ResultMode::ScalarInt => {
                conn.query_prepared_single_reuse_visit_first_column_bytes_with_result_format(
                    stmt,
                    params,
                    0,
                    |value| {
                        consume_scalar_value(value, &mut stats);
                        Ok(())
                    },
                )
                .await?;
            }
            ResultMode::AggregateScalars => {
                conn.query_prepared_single_reuse_visit_first_four_columns_bytes_with_result_format(
                    stmt,
                    params,
                    0,
                    |columns| {
                        consume_aggregate_columns(columns, &mut stats);
                        Ok(())
                    },
                )
                .await?;
            }
            _ => {
                conn.query_prepared_single_reuse_visit_bytes_rows_with_result_format(
                    stmt,
                    params,
                    0,
                    |row| {
                        consume_bytes_row(row, result_mode, &mut stats);
                        Ok(())
                    },
                )
                .await?;
            }
        }
        stats.completed += 1;
    }
    Ok(stats)
}

async fn run_single_iteration_encoded(
    conn: &mut PgConnection,
    wire_batch: &PreparedSinglesWireBatch,
    result_mode: ResultMode,
    mut profile: Option<&mut SingleProfile>,
) -> Result<BatchStats, Box<dyn std::error::Error>> {
    let mut stats = BatchStats::default();
    for wire in &wire_batch.wires {
        match result_mode {
            ResultMode::ScalarInt => {
                if let Some(single_profile) = profile.as_deref_mut() {
                    let (_, send, consume) = conn
                        .query_prepared_single_encoded_visit_first_column_bytes_profiled(
                            wire.as_ref(),
                            |value| {
                                consume_scalar_value(value, &mut stats);
                                Ok(())
                            },
                        )
                        .await?;
                    single_profile.record(send, consume);
                } else {
                    conn.query_prepared_single_encoded_visit_first_column_bytes(
                        wire.as_ref(),
                        |value| {
                            consume_scalar_value(value, &mut stats);
                            Ok(())
                        },
                    )
                    .await?;
                }
            }
            ResultMode::AggregateScalars => {
                if let Some(single_profile) = profile.as_deref_mut() {
                    let (_, send, consume) = conn
                        .query_prepared_single_encoded_visit_first_four_columns_bytes_profiled(
                            wire.as_ref(),
                            |columns| {
                                consume_aggregate_columns(columns, &mut stats);
                                Ok(())
                            },
                        )
                        .await?;
                    single_profile.record(send, consume);
                } else {
                    conn.query_prepared_single_encoded_visit_first_four_columns_bytes(
                        wire.as_ref(),
                        |columns| {
                            consume_aggregate_columns(columns, &mut stats);
                            Ok(())
                        },
                    )
                    .await?;
                }
            }
            _ => {
                if let Some(single_profile) = profile.as_deref_mut() {
                    let (_, send, consume) = conn
                        .query_prepared_single_encoded_visit_bytes_rows_profiled(
                            wire.as_ref(),
                            |row| {
                                consume_bytes_row(row, result_mode, &mut stats);
                                Ok(())
                            },
                        )
                        .await?;
                    single_profile.record(send, consume);
                } else {
                    conn.query_prepared_single_encoded_visit_bytes_rows(wire.as_ref(), |row| {
                        consume_bytes_row(row, result_mode, &mut stats);
                        Ok(())
                    })
                    .await?;
                }
            }
        }
        stats.completed += 1;
    }
    Ok(stats)
}

async fn run_pipeline_iteration(
    conn: &mut PgConnection,
    stmt: &qail_pg::driver::PreparedStatement,
    params_batch: &[Vec<Option<Vec<u8>>>],
    result_mode: ResultMode,
) -> Result<BatchStats, Box<dyn std::error::Error>> {
    let mut stats = BatchStats {
        completed: params_batch.len(),
        ..BatchStats::default()
    };

    let completed = match result_mode {
        ResultMode::ScalarInt => {
            conn.pipeline_execute_prepared_visit_first_column_bytes(stmt, params_batch, |value| {
                consume_scalar_value(value, &mut stats);
                Ok(())
            })
            .await?
        }
        ResultMode::AggregateScalars => {
            conn.pipeline_execute_prepared_visit_first_four_columns_bytes(
                stmt,
                params_batch,
                |columns| {
                    consume_aggregate_columns(columns, &mut stats);
                    Ok(())
                },
            )
            .await?
        }
        _ => {
            conn.pipeline_execute_prepared_visit_bytes_rows(stmt, params_batch, |row| {
                consume_bytes_row(row, result_mode, &mut stats);
                Ok(())
            })
            .await?
        }
    };

    if completed != params_batch.len() {
        return Err(format!(
            "pipeline completed {} queries, expected {}",
            completed,
            params_batch.len()
        )
        .into());
    }
    Ok(stats)
}

async fn run_single_mode(
    cfg: &BenchDbConfig,
    spec: WorkloadSpec,
    cmds: &[Qail],
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    ensure_workload_ready(cfg, spec).await?;
    let mut conn = connect_bench_connection(cfg).await?;
    let prepared = encode_prepared_batch(cmds)?;
    let stmt = conn.prepare(&prepared.sql).await?;
    let wire_batch = encode_prepared_singles_wire_batch(&stmt, &prepared.params_batch, 0)?;
    let profile_single = want_single_profile();

    let warmup = run_single_iteration_encoded(&mut conn, &wire_batch, spec.result_mode, None).await?;
    if warmup.completed != wire_batch.wires.len() {
        return Err(format!(
            "warmup completed {} queries, expected {}",
            warmup.completed,
            wire_batch.wires.len()
        )
        .into());
    }

    let mut total = Duration::ZERO;
    let mut aggregate = BatchStats::default();
    let mut single_profile = SingleProfile::default();
    for _ in 0..spec.iterations {
        let start = Instant::now();
        let stats = if profile_single {
            run_single_iteration_encoded(
                &mut conn,
                &wire_batch,
                spec.result_mode,
                Some(&mut single_profile),
            )
            .await?
        } else {
            run_single_iteration_encoded(&mut conn, &wire_batch, spec.result_mode, None).await?
        };
        total += start.elapsed();
        if stats.completed != wire_batch.wires.len() {
            return Err(format!(
                "run completed {} queries, expected {}",
                stats.completed,
                wire_batch.wires.len()
            )
            .into());
        }
        aggregate.add(stats);
    }

    if profile_single && single_profile.calls > 0 {
        let calls = single_profile.calls as f64;
        eprintln!(
            "single split avg/call: send={:.3}ms consume={:.3}ms calls={}",
            single_profile.send.as_secs_f64() * 1000.0 / calls,
            single_profile.consume.as_secs_f64() * 1000.0 / calls,
            single_profile.calls
        );
    }

    Ok(make_benchmark_result(aggregate, total))
}

async fn run_pipeline_mode(
    cfg: &BenchDbConfig,
    spec: WorkloadSpec,
    cmds: &[Qail],
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    ensure_workload_ready(cfg, spec).await?;
    let mut conn = connect_bench_connection(cfg).await?;
    let prepared = encode_prepared_batch(cmds)?;
    let stmt = conn.prepare(&prepared.sql).await?;

    let warmup =
        run_pipeline_iteration(&mut conn, &stmt, &prepared.params_batch, spec.result_mode).await?;
    if warmup.completed != prepared.params_batch.len() {
        return Err(format!(
            "warmup completed {} queries, expected {}",
            warmup.completed,
            prepared.params_batch.len()
        )
        .into());
    }

    let mut total = Duration::ZERO;
    let mut aggregate = BatchStats::default();
    for _ in 0..spec.iterations {
        let start = Instant::now();
        let stats =
            run_pipeline_iteration(&mut conn, &stmt, &prepared.params_batch, spec.result_mode)
                .await?;
        total += start.elapsed();
        if stats.completed != prepared.params_batch.len() {
            return Err(format!(
                "run completed {} queries, expected {}",
                stats.completed,
                prepared.params_batch.len()
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
    worker_cmds: Vec<Vec<Qail>>,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    ensure_workload_ready(cfg, spec).await?;
    let pool = connect_bench_pool(cfg).await?;

    let start_barrier = Arc::new(Barrier::new(POOL_SIZE + 1));
    let end_barrier = Arc::new(Barrier::new(POOL_SIZE + 1));
    let mut tasks = JoinSet::new();

    for cmds in worker_cmds {
        let pool = pool.clone();
        let start_barrier = Arc::clone(&start_barrier);
        let end_barrier = Arc::clone(&end_barrier);

        tasks.spawn(async move {
            let mut local_err: Option<String> = None;
            let mut measured = BatchStats::default();
            let prepared = match encode_prepared_batch(&cmds) {
                Ok(prepared) => Some(prepared),
                Err(err) => {
                    local_err = Some(err.to_string());
                    None
                }
            };
            let mut pooled = match pool.acquire_system().await {
                Ok(pooled) => Some(pooled),
                Err(e) => {
                    local_err = Some(e.to_string());
                    None
                }
            };
            let mut stmt: Option<qail_pg::driver::PreparedStatement> = None;

            if let Some(pooled) = pooled.as_mut() {
                let warmup_result = async {
                    let prepared = prepared
                        .as_ref()
                        .ok_or_else(|| "missing prepared batch".to_string())?;
                    let conn = pooled.get_mut().map_err(|e| e.to_string())?;
                    let prepared_stmt = conn
                        .prepare(&prepared.sql)
                        .await
                        .map_err(|e| e.to_string())?;
                    let warmup = run_single_iteration(
                        conn,
                        &prepared_stmt,
                        &prepared.params_batch,
                        spec.result_mode,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                    if warmup.completed != prepared.params_batch.len() {
                        return Err(format!(
                            "warmup completed {} queries, expected {}",
                            warmup.completed,
                            prepared.params_batch.len()
                        ));
                    }
                    stmt = Some(prepared_stmt);
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
                    let prepared = prepared
                        .as_ref()
                        .ok_or_else(|| "missing prepared batch".to_string())?;
                    let conn = pooled.get_mut().map_err(|e| e.to_string())?;
                    let stmt = stmt
                        .as_ref()
                        .ok_or_else(|| "missing prepared statement".to_string())?;
                    for _ in 0..spec.iterations {
                        let stats = run_single_iteration(
                            conn,
                            stmt,
                            &prepared.params_batch,
                            spec.result_mode,
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                        if stats.completed != prepared.params_batch.len() {
                            return Err(format!(
                                "run completed {} queries, expected {}",
                                stats.completed,
                                prepared.params_batch.len()
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

async fn run_latency_mode(
    cfg: &BenchDbConfig,
    spec: WorkloadSpec,
    cmds: &[Qail],
) -> Result<LatencyResult, Box<dyn std::error::Error>> {
    ensure_workload_ready(cfg, spec).await?;
    let mut conn = connect_bench_connection(cfg).await?;
    let prepared = encode_prepared_batch(cmds)?;
    let stmt = conn.prepare(&prepared.sql).await?;

    let warmup_count = spec.latency_samples.min(20);
    for params in prepared.params_batch.iter().cycle().take(warmup_count) {
        let stats = run_single_iteration(
            &mut conn,
            &stmt,
            std::slice::from_ref(params),
            spec.result_mode,
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
    for params in prepared
        .params_batch
        .iter()
        .cycle()
        .take(spec.latency_samples)
    {
        let start = Instant::now();
        let stats = run_single_iteration(
            &mut conn,
            &stmt,
            std::slice::from_ref(params),
            spec.result_mode,
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
        return Err("missing value for --workload".into());
    }

    let mode = mode.ok_or("missing benchmark mode (single | pipeline | pool10 | latency)")?;
    let cfg = BenchDbConfig::from_env()?;
    let spec = WorkloadSpec::new(workload);
    let cmds = build_command_batch(spec);

    match mode {
        Mode::Single => {
            let result = run_single_mode(&cfg, spec, &cmds).await?;
            if plain {
                println!("{:.3}", result.qps);
            } else {
                print!("{} single: {:.0} q/s", spec.name, result.qps);
                if let Some(rows_per_sec) = result.rows_per_sec {
                    print!(" | {:.0} rows/s", rows_per_sec);
                }
                if let Some(mib_per_sec) = result.mib_per_sec {
                    print!(" | {:.2} MiB/s", mib_per_sec);
                }
                print!(" | checksum=0x{:x}", result.checksum);
                println!();
            }
        }
        Mode::Pipeline => {
            let result = run_pipeline_mode(&cfg, spec, &cmds).await?;
            if plain {
                println!("{:.3}", result.qps);
            } else {
                print!("{} pipeline: {:.0} q/s", spec.name, result.qps);
                if let Some(rows_per_sec) = result.rows_per_sec {
                    print!(" | {:.0} rows/s", rows_per_sec);
                }
                if let Some(mib_per_sec) = result.mib_per_sec {
                    print!(" | {:.2} MiB/s", mib_per_sec);
                }
                print!(" | checksum=0x{:x}", result.checksum);
                println!();
            }
        }
        Mode::Pool10 => {
            let worker_cmds = split_commands_for_pool(&cmds)?;
            let result = run_pool10_mode(&cfg, spec, worker_cmds).await?;
            if plain {
                println!("{:.3}", result.qps);
            } else {
                print!("{} pool10: {:.0} q/s", spec.name, result.qps);
                if let Some(rows_per_sec) = result.rows_per_sec {
                    print!(" | {:.0} rows/s", rows_per_sec);
                }
                if let Some(mib_per_sec) = result.mib_per_sec {
                    print!(" | {:.2} MiB/s", mib_per_sec);
                }
                print!(" | checksum=0x{:x}", result.checksum);
                println!();
            }
        }
        Mode::Latency => {
            let result = run_latency_mode(&cfg, spec, &cmds).await?;
            if plain {
                println!(
                    "{:.6},{:.6},{:.6},{:.6}",
                    result.p50_ms, result.p95_ms, result.p99_ms, result.avg_ms
                );
            } else {
                println!(
                    "{} latency: avg={:.3} ms | p50={:.3} ms | p95={:.3} ms | p99={:.3} ms",
                    spec.name, result.avg_ms, result.p50_ms, result.p95_ms, result.p99_ms
                );
            }
        }
    }

    Ok(())
}
