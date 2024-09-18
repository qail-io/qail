//! libpq Pool + Pipeline 60-second benchmark
//! 10 connections running pipelined queries in parallel

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[link(name = "pq")]
unsafe extern "C" {
    fn PQconnectdb(conninfo: *const c_char) -> *mut c_void;
    fn PQstatus(conn: *const c_void) -> c_int;
    fn PQfinish(conn: *mut c_void);
    fn PQclear(res: *mut c_void);
    fn PQresultStatus(res: *const c_void) -> c_int;
    fn PQerrorMessage(conn: *const c_void) -> *const c_char;
    fn PQprepare(conn: *mut c_void, stmt_name: *const c_char, query: *const c_char, n_params: c_int, param_types: *const u32) -> *mut c_void;
    fn PQenterPipelineMode(conn: *mut c_void) -> c_int;
    fn PQexitPipelineMode(conn: *mut c_void) -> c_int;
    fn PQpipelineSync(conn: *mut c_void) -> c_int;
    fn PQsendQueryPrepared(conn: *mut c_void, stmt_name: *const c_char, n_params: c_int, param_values: *const *const c_char, param_lengths: *const c_int, param_formats: *const c_int, result_format: c_int) -> c_int;
    fn PQgetResult(conn: *mut c_void) -> *mut c_void;
    fn PQsetnonblocking(conn: *mut c_void, arg: c_int) -> c_int;
    fn PQflush(conn: *mut c_void) -> c_int;
    fn PQconsumeInput(conn: *mut c_void) -> c_int;
    fn PQisBusy(conn: *mut c_void) -> c_int;
    fn PQntuples(res: *const c_void) -> c_int;
}

const CONNECTION_OK: c_int = 0;
const PGRES_TUPLES_OK: c_int = 2;
const PGRES_PIPELINE_SYNC: c_int = 10;
const POOL_SIZE: usize = 10;
const BATCH_SIZE: usize = 10_000;
const TARGET_SECS: u64 = 60;

fn main() {
    println!("🏁 libpq POOL + PIPELINE 60-Second Benchmark");
    println!("=============================================\n");
    
    println!("Pool: {} connections", POOL_SIZE);
    println!("Query: SELECT id, name FROM harbors LIMIT $1");
    println!("Target: 60 seconds, batch size: {}\n", BATCH_SIZE);
    
    let total_queries = Arc::new(AtomicUsize::new(0));
    let total_rows = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let start = Instant::now();
    
    // Pre-generate params
    let param_strings: Vec<CString> = (1..=BATCH_SIZE)
        .map(|i| CString::new(((i % 10) + 1).to_string()).unwrap())
        .collect();
    let param_strings = Arc::new(param_strings);
    
    let mut handles = Vec::new();
    
    // Spawn 10 parallel worker threads
    for worker_id in 0..POOL_SIZE {
        let total_queries = Arc::clone(&total_queries);
        let total_rows = Arc::clone(&total_rows);
        let running = Arc::clone(&running);
        let param_strings = Arc::clone(&param_strings);
        
        handles.push(thread::spawn(move || {
            unsafe {
                let conninfo = CString::new("host=localhost port=5432 user=orion dbname=postgres").unwrap();
                let conn = PQconnectdb(conninfo.as_ptr());
                
                if PQstatus(conn) != CONNECTION_OK {
                    let err = CStr::from_ptr(PQerrorMessage(conn)).to_str().unwrap();
                    println!("Worker {} error: {}", worker_id, err);
                    return;
                }
                
                PQsetnonblocking(conn, 1);
                
                let stmt_name = CString::new(format!("stmt_{}", worker_id)).unwrap();
                let query = CString::new("SELECT id, name FROM harbors LIMIT $1").unwrap();
                let res = PQprepare(conn, stmt_name.as_ptr(), query.as_ptr(), 1, ptr::null());
                PQclear(res);
                
                while running.load(Ordering::Relaxed) {
                    PQenterPipelineMode(conn);
                    
                    for param_cstr in param_strings.as_ref() {
                        let param_values = [param_cstr.as_ptr()];
                        let param_lengths = [0i32];
                        let param_formats = [0i32];
                        PQsendQueryPrepared(conn, stmt_name.as_ptr(), 1, param_values.as_ptr(), param_lengths.as_ptr(), param_formats.as_ptr(), 0);
                    }
                    
                    PQpipelineSync(conn);
                    while PQflush(conn) > 0 {}
                    
                    let mut batch_queries = 0;
                    let mut batch_rows = 0;
                    loop {
                        while PQisBusy(conn) != 0 { PQconsumeInput(conn); }
                        let res = PQgetResult(conn);
                        if res.is_null() { continue; }
                        
                        let status = PQresultStatus(res);
                        if status == PGRES_TUPLES_OK {
                            batch_rows += PQntuples(res) as usize;
                            batch_queries += 1;
                        }
                        PQclear(res);
                        
                        if status == PGRES_PIPELINE_SYNC { break; }
                    }
                    
                    PQexitPipelineMode(conn);
                    total_queries.fetch_add(batch_queries, Ordering::Relaxed);
                    total_rows.fetch_add(batch_rows, Ordering::Relaxed);
                }
                
                PQfinish(conn);
            }
        }));
    }
    
    // Monitor and report progress
    let target = Duration::from_secs(TARGET_SECS);
    while start.elapsed() < target {
        thread::sleep(Duration::from_secs(5));
        let elapsed = start.elapsed().as_secs_f64();
        let queries = total_queries.load(Ordering::Relaxed);
        let rows = total_rows.load(Ordering::Relaxed);
        let qps = queries as f64 / elapsed;
        println!("  {:.0}s: {} queries, {} rows, {:.0} q/s", elapsed, queries, rows, qps);
    }
    
    running.store(false, Ordering::SeqCst);
    
    for handle in handles {
        handle.join().ok();
    }
    
    let elapsed = start.elapsed();
    let queries = total_queries.load(Ordering::Relaxed);
    let rows = total_rows.load(Ordering::Relaxed);
    let qps = queries as f64 / elapsed.as_secs_f64();
    
    println!("\n=== FINAL RESULTS ===");
    println!("  Pool Size: {} connections", POOL_SIZE);
    println!("  Duration:  {:.2}s", elapsed.as_secs_f64());
    println!("  Queries:   {}", queries);
    println!("  Rows:      {} (consumed)", rows);
    println!("  📈 Average: {:.0} q/s", qps);
}
