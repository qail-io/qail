//! FAIR Benchmark: QAIL-pg vs libpq Pipeline Mode
//!
//! Uses SAME query as fifty_million.rs for true comparison.
//! Both drivers use wire-level pipelining.

use qail_core::ast::Qail;
use qail_pg::{PgConnection, protocol::AstEncoder, protocol::PgEncoder};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::time::{Duration, Instant};

// libpq FFI bindings including Pipeline Mode (PostgreSQL 14+)
#[link(name = "pq")]
unsafe extern "C" {
    fn PQconnectdb(conninfo: *const c_char) -> *mut c_void;
    fn PQstatus(conn: *const c_void) -> c_int;
    fn PQfinish(conn: *mut c_void);
    #[allow(dead_code)]
    fn PQexec(conn: *mut c_void, query: *const c_char) -> *mut c_void;
    fn PQclear(res: *mut c_void);
    fn PQresultStatus(res: *const c_void) -> c_int;
    fn PQerrorMessage(conn: *const c_void) -> *const c_char;
    fn PQprepare(
        conn: *mut c_void,
        stmt_name: *const c_char,
        query: *const c_char,
        n_params: c_int,
        param_types: *const u32,
    ) -> *mut c_void;

    // Pipeline Mode API (PostgreSQL 14+)
    fn PQenterPipelineMode(conn: *mut c_void) -> c_int;
    fn PQexitPipelineMode(conn: *mut c_void) -> c_int;
    fn PQpipelineSync(conn: *mut c_void) -> c_int;
    fn PQsendQueryPrepared(
        conn: *mut c_void,
        stmt_name: *const c_char,
        n_params: c_int,
        param_values: *const *const c_char,
        param_lengths: *const c_int,
        param_formats: *const c_int,
        result_format: c_int,
    ) -> c_int;
    fn PQgetResult(conn: *mut c_void) -> *mut c_void;
    fn PQsetnonblocking(conn: *mut c_void, arg: c_int) -> c_int;
    fn PQflush(conn: *mut c_void) -> c_int;
    fn PQconsumeInput(conn: *mut c_void) -> c_int;
    fn PQisBusy(conn: *mut c_void) -> c_int;
}

const CONNECTION_OK: c_int = 0;
#[allow(dead_code)]
const PGRES_TUPLES_OK: c_int = 2;
const PGRES_COMMAND_OK: c_int = 1;
const PGRES_PIPELINE_SYNC: c_int = 10;

// SAME batch size as fifty_million.rs
const BATCH_SIZE: usize = 10_000;
const ITERATIONS: usize = 5;
const TRACE_CLIENT_ITERS: usize = 100;

#[derive(Debug, Clone, Copy)]
struct ClientHotProfile {
    encode_sql_ns_per_q: f64,
    lookup_ns_per_q: f64,
    bind_exec_ns_per_q: f64,
    total_ns_per_q: f64,
}

struct LibpqPreparedCall {
    stmt_idx: usize,
    _param_storage: Vec<Option<CString>>,
    param_values: Vec<*const c_char>,
    param_lengths: Vec<c_int>,
    param_formats: Vec<c_int>,
}

fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    }
}

fn percentile(values: &[f64], p: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let rank = ((p * sorted.len() as f64).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1);
    sorted[rank]
}

fn stmt_name_from_sql(sql: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    sql.hash(&mut hasher);
    format!("s{:016x}", hasher.finish())
}

fn profile_qail_ast_cached_client_hot(
    cmds: &[Qail],
    iterations: usize,
) -> Result<ClientHotProfile, String> {
    if cmds.is_empty() || iterations == 0 {
        return Err("profile requires non-empty cmds and iterations > 0".to_string());
    }

    let mut prepared_names: HashMap<String, String> = HashMap::new();
    for cmd in cmds {
        let (sql, _params) =
            AstEncoder::encode_cmd_sql(cmd).map_err(|e| format!("encode_cmd_sql warmup: {e}"))?;
        let stmt_name = stmt_name_from_sql(&sql);
        prepared_names.entry(stmt_name).or_insert(sql);
    }

    let mut encode_total = Duration::ZERO;
    let mut lookup_total = Duration::ZERO;
    let mut bind_exec_total = Duration::ZERO;

    for _ in 0..iterations {
        let mut encoded: Vec<(String, Vec<Option<Vec<u8>>>)> = Vec::with_capacity(cmds.len());

        let t = Instant::now();
        for cmd in cmds {
            let v = AstEncoder::encode_cmd_sql(cmd)
                .map_err(|e| format!("encode_cmd_sql trace loop: {e}"))?;
            encoded.push(v);
        }
        encode_total += t.elapsed();

        let mut stmt_params: Vec<(String, Vec<Option<Vec<u8>>>)> = Vec::with_capacity(cmds.len());
        let t = Instant::now();
        for (sql, params) in encoded {
            let stmt_name = stmt_name_from_sql(&sql);
            if !prepared_names.contains_key(&stmt_name) {
                prepared_names.insert(stmt_name.clone(), sql);
            }
            stmt_params.push((stmt_name, params));
        }
        lookup_total += t.elapsed();

        let t = Instant::now();
        let mut buf = Vec::<u8>::with_capacity(cmds.len() * 64);
        for (stmt_name, params) in &stmt_params {
            let bind = PgEncoder::encode_bind("", stmt_name, params).map_err(|e| e.to_string())?;
            let exec = PgEncoder::try_encode_execute("", 0).map_err(|e| e.to_string())?;
            buf.extend_from_slice(&bind);
            buf.extend(exec);
        }
        buf.extend(PgEncoder::encode_sync());
        std::hint::black_box(buf.len());
        bind_exec_total += t.elapsed();
    }

    let q_count = (cmds.len() * iterations) as f64;
    let encode_ns = encode_total.as_nanos() as f64 / q_count;
    let lookup_ns = lookup_total.as_nanos() as f64 / q_count;
    let bind_ns = bind_exec_total.as_nanos() as f64 / q_count;

    Ok(ClientHotProfile {
        encode_sql_ns_per_q: encode_ns,
        lookup_ns_per_q: lookup_ns,
        bind_exec_ns_per_q: bind_ns,
        total_ns_per_q: encode_ns + lookup_ns + bind_ns,
    })
}

fn run_libpq_pipeline_benchmark() -> Result<f64, String> {
    unsafe {
        let conninfo =
            CString::new("host=localhost port=5432 user=orion dbname=example_staging").unwrap();
        let conn = PQconnectdb(conninfo.as_ptr());

        if PQstatus(conn) != CONNECTION_OK {
            let err = CStr::from_ptr(PQerrorMessage(conn)).to_str().unwrap();
            return Err(format!("libpq connection failed: {}", err));
        }

        if PQsetnonblocking(conn, 1) != 0 {
            return Err("Failed to set non-blocking mode".to_string());
        }

        // SAME query as fifty_million.rs: SELECT id, name FROM harbors LIMIT $1
        let stmt_name = CString::new("bench_stmt").unwrap();
        let query = CString::new("SELECT id, name FROM harbors LIMIT $1").unwrap();
        let res = PQprepare(conn, stmt_name.as_ptr(), query.as_ptr(), 1, ptr::null());
        PQclear(res);

        // SAME params as fifty_million.rs: limit values 1-10 cycling
        let param_strings: Vec<CString> = (1..=BATCH_SIZE)
            .map(|i| CString::new(((i % 10) + 1).to_string()).unwrap())
            .collect();

        let mut total_time = std::time::Duration::ZERO;

        for _ in 0..ITERATIONS {
            if PQenterPipelineMode(conn) != 1 {
                return Err("Failed to enter pipeline mode".to_string());
            }

            let start = Instant::now();

            // Send all queries
            for param_cstr in &param_strings {
                let param_values = [param_cstr.as_ptr()];
                let param_lengths = [0i32];
                let param_formats = [0i32];

                PQsendQueryPrepared(
                    conn,
                    stmt_name.as_ptr(),
                    1,
                    param_values.as_ptr(),
                    param_lengths.as_ptr(),
                    param_formats.as_ptr(),
                    0,
                );
            }

            PQpipelineSync(conn);
            while PQflush(conn) > 0 {}

            // Collect results
            loop {
                while PQisBusy(conn) != 0 {
                    PQconsumeInput(conn);
                }

                let res = PQgetResult(conn);
                if res.is_null() {
                    continue;
                }

                let status = PQresultStatus(res);
                PQclear(res);

                if status == PGRES_PIPELINE_SYNC {
                    break;
                }
            }

            total_time += start.elapsed();
            PQexitPipelineMode(conn);
        }

        PQfinish(conn);

        let qps = (BATCH_SIZE * ITERATIONS) as f64 / total_time.as_secs_f64();
        Ok(qps)
    }
}

fn run_libpq_dsl_template_cached_benchmark(cmds: &[Qail]) -> Result<f64, String> {
    unsafe {
        let conninfo =
            CString::new("host=localhost port=5432 user=orion dbname=example_staging").unwrap();
        let conn = PQconnectdb(conninfo.as_ptr());

        if PQstatus(conn) != CONNECTION_OK {
            let err = CStr::from_ptr(PQerrorMessage(conn)).to_str().unwrap();
            return Err(format!("libpq connection failed: {}", err));
        }

        if PQsetnonblocking(conn, 1) != 0 {
            return Err("Failed to set non-blocking mode".to_string());
        }

        // Build exact SQL template + param sequence from QAIL DSL encoding.
        let mut ordered_sql: Vec<String> = Vec::with_capacity(cmds.len());
        let mut ordered_params: Vec<Vec<Option<Vec<u8>>>> = Vec::with_capacity(cmds.len());
        for cmd in cmds {
            let (sql, params) =
                AstEncoder::encode_cmd_sql(cmd).map_err(|e| format!("encode_cmd_sql: {e}"))?;
            ordered_sql.push(sql);
            ordered_params.push(params);
        }

        // Prepare each unique SQL template once (outside timed loops).
        let mut sql_to_idx: HashMap<String, usize> = HashMap::new();
        let mut unique_sql: Vec<String> = Vec::new();
        let mut unique_param_counts: Vec<usize> = Vec::new();
        for (sql, params) in ordered_sql.iter().zip(ordered_params.iter()) {
            if let Some(idx) = sql_to_idx.get(sql) {
                let expected = unique_param_counts[*idx];
                if expected != params.len() {
                    return Err(format!(
                        "template param-count drift for SQL `{}`: expected {}, got {}",
                        sql,
                        expected,
                        params.len()
                    ));
                }
            } else {
                let idx = unique_sql.len();
                sql_to_idx.insert(sql.clone(), idx);
                unique_sql.push(sql.clone());
                unique_param_counts.push(params.len());
            }
        }

        let mut stmt_names: Vec<CString> = Vec::with_capacity(unique_sql.len());
        for (i, sql) in unique_sql.iter().enumerate() {
            let stmt = CString::new(format!("dsl_stmt_{i:04}"))
                .map_err(|_| "invalid stmt name (contains NUL)".to_string())?;
            let sql_c = CString::new(sql.as_str())
                .map_err(|_| "invalid SQL template (contains NUL)".to_string())?;
            let n_params = unique_param_counts[i] as c_int;

            let res = PQprepare(conn, stmt.as_ptr(), sql_c.as_ptr(), n_params, ptr::null());
            let status = PQresultStatus(res);
            if status != PGRES_COMMAND_OK {
                let err = CStr::from_ptr(PQerrorMessage(conn))
                    .to_string_lossy()
                    .into_owned();
                PQclear(res);
                PQfinish(conn);
                return Err(format!(
                    "PQprepare failed for template {i} with status {status}: {err}"
                ));
            }
            PQclear(res);
            stmt_names.push(stmt);
        }

        let mut calls: Vec<LibpqPreparedCall> = Vec::with_capacity(ordered_sql.len());
        for (sql, params) in ordered_sql.iter().zip(ordered_params.iter()) {
            let idx = *sql_to_idx
                .get(sql)
                .ok_or_else(|| "internal SQL index mismatch".to_string())?;

            let mut param_storage: Vec<Option<CString>> = Vec::with_capacity(params.len());
            for param in params {
                match param {
                    Some(bytes) => {
                        let c = CString::new(bytes.as_slice()).map_err(|_| {
                            "parameter contains NUL byte; unsupported in libpq text mode"
                                .to_string()
                        })?;
                        param_storage.push(Some(c));
                    }
                    None => param_storage.push(None),
                }
            }

            let mut param_values: Vec<*const c_char> = Vec::with_capacity(param_storage.len());
            for p in &param_storage {
                param_values.push(match p {
                    Some(c) => c.as_ptr(),
                    None => ptr::null(),
                });
            }
            let param_lengths = vec![0i32; param_storage.len()];
            let param_formats = vec![0i32; param_storage.len()];

            calls.push(LibpqPreparedCall {
                stmt_idx: idx,
                _param_storage: param_storage,
                param_values,
                param_lengths,
                param_formats,
            });
        }

        let send_pipeline = |conn: *mut c_void| -> Result<(), String> {
            if PQenterPipelineMode(conn) != 1 {
                return Err("Failed to enter pipeline mode".to_string());
            }

            for call in &calls {
                let stmt = &stmt_names[call.stmt_idx];
                let n_params = call.param_values.len() as c_int;
                let (values_ptr, lengths_ptr, formats_ptr) = if call.param_values.is_empty() {
                    (ptr::null(), ptr::null(), ptr::null())
                } else {
                    (
                        call.param_values.as_ptr(),
                        call.param_lengths.as_ptr(),
                        call.param_formats.as_ptr(),
                    )
                };

                if PQsendQueryPrepared(
                    conn,
                    stmt.as_ptr(),
                    n_params,
                    values_ptr,
                    lengths_ptr,
                    formats_ptr,
                    0,
                ) != 1
                {
                    let err = CStr::from_ptr(PQerrorMessage(conn))
                        .to_string_lossy()
                        .into_owned();
                    PQexitPipelineMode(conn);
                    return Err(format!("PQsendQueryPrepared failed: {err}"));
                }
            }

            PQpipelineSync(conn);
            while PQflush(conn) > 0 {}

            loop {
                while PQisBusy(conn) != 0 {
                    PQconsumeInput(conn);
                }
                let res = PQgetResult(conn);
                if res.is_null() {
                    continue;
                }
                let status = PQresultStatus(res);
                PQclear(res);
                if status == PGRES_PIPELINE_SYNC {
                    break;
                }
            }

            PQexitPipelineMode(conn);
            Ok(())
        };

        // Untimed warmup to mirror QAIL strict benchmark behavior.
        send_pipeline(conn)?;

        let expected_completed = cmds.len();
        if expected_completed != calls.len() {
            PQfinish(conn);
            return Err(format!(
                "libpq strict expected {} calls but built {}",
                expected_completed,
                calls.len()
            ));
        }

        let mut total_time = std::time::Duration::ZERO;
        for _ in 0..ITERATIONS {
            let start = Instant::now();
            if let Err(e) = send_pipeline(conn) {
                PQfinish(conn);
                return Err(e);
            }
            total_time += start.elapsed();
        }

        PQfinish(conn);

        let qps = (cmds.len() * ITERATIONS) as f64 / total_time.as_secs_f64();
        Ok(qps)
    }
}

async fn run_qail_dsl_template_cached_benchmark(cmds: &[Qail]) -> Result<f64, String> {
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "example_staging")
        .await
        .map_err(|e| format!("qail connect failed: {e}"))?;

    let warm = conn
        .pipeline_execute_count_ast_cached(cmds)
        .await
        .map_err(|e| format!("qail strict warmup failed: {e}"))?;
    if warm != cmds.len() {
        return Err(format!(
            "QAIL strict warmup completed {} queries, expected {}",
            warm,
            cmds.len()
        ));
    }

    let mut total_time = std::time::Duration::ZERO;
    for _ in 0..ITERATIONS {
        let start = Instant::now();
        let completed = conn
            .pipeline_execute_count_ast_cached(cmds)
            .await
            .map_err(|e| format!("qail strict cached failed: {e}"))?;
        let elapsed = start.elapsed();
        total_time += elapsed;
        if completed != cmds.len() {
            return Err(format!(
                "QAIL strict cached completed {} queries, expected {}",
                completed,
                cmds.len()
            ));
        }
    }

    Ok((cmds.len() * ITERATIONS) as f64 / total_time.as_secs_f64())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🏁 FAIR BENCHMARK: QAIL-pg vs libpq Pipeline");
    println!("=============================================");
    println!("Query: SELECT id, name FROM harbors LIMIT $1");
    println!("Same query as fifty_million.rs test\n");

    // ============================================
    // libpq: Pipeline Mode
    // ============================================
    println!("=== libpq (C driver, PIPELINE MODE) ===");

    match run_libpq_pipeline_benchmark() {
        Ok(qps) => println!("  libpq Pipeline: {:>8.0} q/s\n", qps),
        Err(e) => println!("  libpq error: {}\n", e),
    }

    // ============================================
    // QAIL-pg: prepared SQL pipeline (baseline)
    // ============================================
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "example_staging").await?;

    let stmt = conn
        .prepare("SELECT id, name FROM harbors LIMIT $1")
        .await?;
    let params_batch: Vec<Vec<Option<Vec<u8>>>> = (1..=BATCH_SIZE)
        .map(|i| {
            let limit = ((i % 10) + 1).to_string();
            vec![Some(limit.into_bytes())]
        })
        .collect();

    println!("=== QAIL-pg (pipeline_execute_prepared_count, SQL prepare) ===");

    let mut total_time_prepared = std::time::Duration::ZERO;

    for iter in 0..ITERATIONS {
        let start = Instant::now();
        let completed = conn
            .pipeline_execute_prepared_count(&stmt, &params_batch)
            .await?;
        let elapsed = start.elapsed();
        total_time_prepared += elapsed;
        if completed != BATCH_SIZE {
            return Err(format!(
                "QAIL prepared pipeline completed {} queries, expected {}",
                completed, BATCH_SIZE
            )
            .into());
        }

        let qps = BATCH_SIZE as f64 / elapsed.as_secs_f64();
        println!(
            "  Iteration {}: {:>8.0} q/s | {:>6.2}ms",
            iter + 1,
            qps,
            elapsed.as_secs_f64() * 1000.0
        );
    }

    let qail_prepared_qps = (BATCH_SIZE * ITERATIONS) as f64 / total_time_prepared.as_secs_f64();
    println!("\n  📈 QAIL-pg Prepared: {:>8.0} q/s\n", qail_prepared_qps);

    // ============================================
    // QAIL-pg: Native DSL (AST) pipeline
    // ============================================
    // Equivalent logical query pattern via native QAIL DSL.
    let cmds_batch: Vec<Qail> = (1..=BATCH_SIZE)
        .map(|i| {
            let limit = ((i % 10) + 1) as i64;
            Qail::get("harbors").columns(["id", "name"]).limit(limit)
        })
        .collect();

    println!("=== QAIL-pg (pipeline_execute_count_ast_oneshot, native DSL; encodes each run) ===");

    let mut total_time_dsl = std::time::Duration::ZERO;

    for iter in 0..ITERATIONS {
        let start = Instant::now();
        let completed = conn.pipeline_execute_count_ast_oneshot(&cmds_batch).await?;
        let elapsed = start.elapsed();
        total_time_dsl += elapsed;
        if completed != BATCH_SIZE {
            return Err(format!(
                "QAIL DSL pipeline completed {} queries, expected {}",
                completed, BATCH_SIZE
            )
            .into());
        }

        let qps = BATCH_SIZE as f64 / elapsed.as_secs_f64();
        println!(
            "  Iteration {}: {:>8.0} q/s | {:>6.2}ms",
            iter + 1,
            qps,
            elapsed.as_secs_f64() * 1000.0
        );
    }

    let qail_dsl_qps = (BATCH_SIZE * ITERATIONS) as f64 / total_time_dsl.as_secs_f64();
    println!("\n  📈 QAIL-pg DSL: {:>8.0} q/s\n", qail_dsl_qps);

    // ============================================
    // QAIL-pg: Native DSL cached prepared path
    // ============================================
    println!("=== QAIL-pg (pipeline_execute_count_ast_cached, native DSL cached) ===");

    let mut total_time_dsl_cached = std::time::Duration::ZERO;

    for iter in 0..ITERATIONS {
        let start = Instant::now();
        let completed = conn.pipeline_execute_count_ast_cached(&cmds_batch).await?;
        let elapsed = start.elapsed();
        total_time_dsl_cached += elapsed;
        if completed != BATCH_SIZE {
            return Err(format!(
                "QAIL DSL cached pipeline completed {} queries, expected {}",
                completed, BATCH_SIZE
            )
            .into());
        }

        let qps = BATCH_SIZE as f64 / elapsed.as_secs_f64();
        println!(
            "  Iteration {}: {:>8.0} q/s | {:>6.2}ms",
            iter + 1,
            qps,
            elapsed.as_secs_f64() * 1000.0
        );
    }

    let qail_dsl_cached_qps =
        (BATCH_SIZE * ITERATIONS) as f64 / total_time_dsl_cached.as_secs_f64();
    println!(
        "\n  📈 QAIL-pg DSL cached: {:>8.0} q/s\n",
        qail_dsl_cached_qps
    );

    // ============================================
    // QAIL-pg: Native DSL pre-encoded bytes
    // ============================================
    let dsl_wire = AstEncoder::encode_batch(&cmds_batch)
        .map_err(|e| std::io::Error::other(format!("encode dsl batch failed: {e}")))?;

    println!("=== QAIL-pg (pipeline_execute_count_wire, DSL pre-encoded) ===");

    let mut total_time_dsl_pre = std::time::Duration::ZERO;

    for iter in 0..ITERATIONS {
        let start = Instant::now();
        let completed = conn
            .pipeline_execute_count_wire(&dsl_wire, BATCH_SIZE)
            .await?;
        let elapsed = start.elapsed();
        total_time_dsl_pre += elapsed;
        if completed != BATCH_SIZE {
            return Err(format!(
                "QAIL DSL pre-encoded pipeline completed {} queries, expected {}",
                completed, BATCH_SIZE
            )
            .into());
        }

        let qps = BATCH_SIZE as f64 / elapsed.as_secs_f64();
        println!(
            "  Iteration {}: {:>8.0} q/s | {:>6.2}ms",
            iter + 1,
            qps,
            elapsed.as_secs_f64() * 1000.0
        );
    }

    let qail_dsl_pre_qps = (BATCH_SIZE * ITERATIONS) as f64 / total_time_dsl_pre.as_secs_f64();
    println!(
        "\n  📈 QAIL-pg DSL pre-encoded: {:>8.0} q/s\n",
        qail_dsl_pre_qps
    );

    // ============================================
    // STRICT FAIR: ABBA order + warmup parity (literal + parameterized)
    // ============================================
    println!("=== STRICT FAIR (ABBA order, warmup parity) ===");

    let cmds_batch_param: Vec<Qail> = (1..=BATCH_SIZE)
        .map(|i| {
            let id = ((i % 10_000) + 1) as i64;
            Qail::get("harbors").columns(["id", "name"]).eq("id", id)
        })
        .collect();
    let strict_orders = [true, false, false, true]; // ABBA: libpq->qail, qail->libpq, ...

    println!("  Workload A: template-cached literal LIMIT (0 bind params)");
    let mut literal_libpq_runs = Vec::with_capacity(strict_orders.len());
    let mut literal_qail_runs = Vec::with_capacity(strict_orders.len());
    for (round, libpq_first) in strict_orders.iter().enumerate() {
        let order = if *libpq_first {
            "libpq -> QAIL"
        } else {
            "QAIL -> libpq"
        };
        println!("    Round {} ({})", round + 1, order);
        if *libpq_first {
            let libpq_qps = run_libpq_dsl_template_cached_benchmark(&cmds_batch)?;
            let qail_qps = run_qail_dsl_template_cached_benchmark(&cmds_batch).await?;
            println!("      libpq: {:>8.0} q/s", libpq_qps);
            println!("      QAIL : {:>8.0} q/s", qail_qps);
            literal_libpq_runs.push(libpq_qps);
            literal_qail_runs.push(qail_qps);
        } else {
            let qail_qps = run_qail_dsl_template_cached_benchmark(&cmds_batch).await?;
            let libpq_qps = run_libpq_dsl_template_cached_benchmark(&cmds_batch)?;
            println!("      QAIL : {:>8.0} q/s", qail_qps);
            println!("      libpq: {:>8.0} q/s", libpq_qps);
            literal_qail_runs.push(qail_qps);
            literal_libpq_runs.push(libpq_qps);
        }
    }
    let literal_libpq_median = median(&literal_libpq_runs);
    let literal_qail_median = median(&literal_qail_runs);
    let literal_libpq_p95 = percentile(&literal_libpq_runs, 0.95);
    let literal_qail_p95 = percentile(&literal_qail_runs, 0.95);
    let strict_literal_delta = ((literal_qail_median / literal_libpq_median) - 1.0) * 100.0;
    println!(
        "    libpq median/p95: {:>8.0} / {:>8.0} q/s",
        literal_libpq_median, literal_libpq_p95
    );
    println!(
        "    QAIL  median/p95: {:>8.0} / {:>8.0} q/s",
        literal_qail_median, literal_qail_p95
    );
    println!(
        "    delta (QAIL vs libpq, median): {:+.1}%\n",
        strict_literal_delta
    );

    println!("  Workload B: template-cached parameterized filter (1 bind param)");
    let mut param_libpq_runs = Vec::with_capacity(strict_orders.len());
    let mut param_qail_runs = Vec::with_capacity(strict_orders.len());
    for (round, libpq_first) in strict_orders.iter().enumerate() {
        let order = if *libpq_first {
            "libpq -> QAIL"
        } else {
            "QAIL -> libpq"
        };
        println!("    Round {} ({})", round + 1, order);
        if *libpq_first {
            let libpq_qps = run_libpq_dsl_template_cached_benchmark(&cmds_batch_param)?;
            let qail_qps = run_qail_dsl_template_cached_benchmark(&cmds_batch_param).await?;
            println!("      libpq: {:>8.0} q/s", libpq_qps);
            println!("      QAIL : {:>8.0} q/s", qail_qps);
            param_libpq_runs.push(libpq_qps);
            param_qail_runs.push(qail_qps);
        } else {
            let qail_qps = run_qail_dsl_template_cached_benchmark(&cmds_batch_param).await?;
            let libpq_qps = run_libpq_dsl_template_cached_benchmark(&cmds_batch_param)?;
            println!("      QAIL : {:>8.0} q/s", qail_qps);
            println!("      libpq: {:>8.0} q/s", libpq_qps);
            param_qail_runs.push(qail_qps);
            param_libpq_runs.push(libpq_qps);
        }
    }
    let param_libpq_median = median(&param_libpq_runs);
    let param_qail_median = median(&param_qail_runs);
    let param_libpq_p95 = percentile(&param_libpq_runs, 0.95);
    let param_qail_p95 = percentile(&param_qail_runs, 0.95);
    let strict_param_delta = ((param_qail_median / param_libpq_median) - 1.0) * 100.0;
    println!(
        "    libpq median/p95: {:>8.0} / {:>8.0} q/s",
        param_libpq_median, param_libpq_p95
    );
    println!(
        "    QAIL  median/p95: {:>8.0} / {:>8.0} q/s",
        param_qail_median, param_qail_p95
    );
    println!(
        "    delta (QAIL vs libpq, median): {:+.1}%\n",
        strict_param_delta
    );

    let hot = profile_qail_ast_cached_client_hot(&cmds_batch, TRACE_CLIENT_ITERS)?;
    println!("=== TRACE (QAIL AST cached client hot path) ===");
    println!(
        "  AST->SQL encode:       {:>8.0} ns/query",
        hot.encode_sql_ns_per_q
    );
    println!(
        "  stmt hash+lookup:      {:>8.0} ns/query",
        hot.lookup_ns_per_q
    );
    println!(
        "  bind+execute build:    {:>8.0} ns/query",
        hot.bind_exec_ns_per_q
    );
    println!(
        "  client total hot path: {:>8.0} ns/query (~{:>8.0} q/s max)\n",
        hot.total_ns_per_q,
        1_000_000_000.0 / hot.total_ns_per_q
    );

    println!("=== SUMMARY ===");
    println!("Both drivers: wire-level pipelining, equivalent query pattern");
    println!(
        "QAIL DSL (encode-each-run) vs QAIL Prepared: {:+.1}%",
        ((qail_dsl_qps / qail_prepared_qps) - 1.0) * 100.0
    );
    println!(
        "QAIL DSL cached vs QAIL Prepared: {:+.1}%",
        ((qail_dsl_cached_qps / qail_prepared_qps) - 1.0) * 100.0
    );
    println!(
        "QAIL DSL pre-encoded vs QAIL Prepared: {:+.1}%",
        ((qail_dsl_pre_qps / qail_prepared_qps) - 1.0) * 100.0
    );
    println!(
        "STRICT literal (ABBA median) QAIL vs libpq: {:+.1}%",
        strict_literal_delta
    );
    println!(
        "STRICT parameterized (ABBA median) QAIL vs libpq: {:+.1}%",
        strict_param_delta
    );

    Ok(())
}
