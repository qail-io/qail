//! FAIR Benchmark: QAIL-pg vs libpq Pipeline Mode
//!
//! Uses SAME query as fifty_million.rs for true comparison.
//! Both drivers use wire-level pipelining.

use qail_pg::PgConnection;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::time::Instant;

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
const PGRES_PIPELINE_SYNC: c_int = 10;

// SAME batch size as fifty_million.rs
const BATCH_SIZE: usize = 10_000;
const ITERATIONS: usize = 5;

fn run_libpq_pipeline_benchmark() -> Result<f64, String> {
    unsafe {
        let conninfo = CString::new("host=localhost port=5432 user=orion dbname=swb_staging_local").unwrap();
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
    // QAIL-pg: Same as fifty_million.rs
    // ============================================
    let mut conn = PgConnection::connect("127.0.0.1", 5432, "orion", "swb_staging_local").await?;
    
    // SAME query as fifty_million.rs
    let stmt = conn.prepare("SELECT id, name FROM harbors LIMIT $1").await?;
    
    // SAME params as fifty_million.rs
    let params_batch: Vec<Vec<Option<Vec<u8>>>> = (1..=BATCH_SIZE)
        .map(|i| {
            let limit = ((i % 10) + 1).to_string();
            vec![Some(limit.into_bytes())]
        })
        .collect();
    
    println!("=== QAIL-pg (pipeline_prepared_fast) ===");
    
    let mut total_time = std::time::Duration::ZERO;
    
    for iter in 0..ITERATIONS {
        let start = Instant::now();
        conn.pipeline_prepared_fast(&stmt, &params_batch).await?;
        let elapsed = start.elapsed();
        total_time += elapsed;
        
        let qps = BATCH_SIZE as f64 / elapsed.as_secs_f64();
        println!("  Iteration {}: {:>8.0} q/s | {:>6.2}ms", iter + 1, qps, elapsed.as_secs_f64() * 1000.0);
    }
    
    let qail_qps = (BATCH_SIZE * ITERATIONS) as f64 / total_time.as_secs_f64();
    println!("\n  📈 QAIL-pg Pipeline: {:>8.0} q/s\n", qail_qps);

    println!("=== SUMMARY ===");
    println!("Both drivers: wire-level pipelining, same query, same params");

    Ok(())
}
