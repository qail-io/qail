//! libpq 1-MINUTE benchmark with result consumption verification

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
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
const BATCH_SIZE: usize = 10_000;
const TARGET_DURATION: Duration = Duration::from_secs(60);

fn main() {
    unsafe {
        println!("🏁 libpq 1-MINUTE Stability Benchmark");
        println!("======================================\n");
        
        let conninfo = CString::new("host=localhost port=5432 user=orion dbname=postgres").unwrap();
        let conn = PQconnectdb(conninfo.as_ptr());
        
        if PQstatus(conn) != CONNECTION_OK {
            let err = CStr::from_ptr(PQerrorMessage(conn)).to_str().unwrap();
            println!("Error: {}", err);
            return;
        }
        
        PQsetnonblocking(conn, 1);
        
        let stmt_name = CString::new("bench_stmt").unwrap();
        let query = CString::new("SELECT id, name FROM harbors LIMIT $1").unwrap();
        let res = PQprepare(conn, stmt_name.as_ptr(), query.as_ptr(), 1, ptr::null());
        PQclear(res);
        
        let param_strings: Vec<CString> = (1..=BATCH_SIZE)
            .map(|i| CString::new(((i % 10) + 1).to_string()).unwrap())
            .collect();
        
        println!("Query: SELECT id, name FROM harbors LIMIT $1");
        println!("Target: 60 seconds, batch size: {}\n", BATCH_SIZE);
        
        let start = Instant::now();
        let mut total_queries: usize = 0;
        let mut total_rows: usize = 0;
        let mut batch_count = 0;
        
        while start.elapsed() < TARGET_DURATION {
            PQenterPipelineMode(conn);
            
            // Send all queries in batch
            for param_cstr in &param_strings {
                let param_values = [param_cstr.as_ptr()];
                let param_lengths = [0i32];
                let param_formats = [0i32];
                PQsendQueryPrepared(conn, stmt_name.as_ptr(), 1, param_values.as_ptr(), param_lengths.as_ptr(), param_formats.as_ptr(), 0);
            }
            
            PQpipelineSync(conn);
            while PQflush(conn) > 0 {}
            
            // Consume ALL results and count rows
            let mut results_in_batch = 0;
            loop {
                while PQisBusy(conn) != 0 { PQconsumeInput(conn); }
                let res = PQgetResult(conn);
                if res.is_null() { continue; }
                
                let status = PQresultStatus(res);
                if status == PGRES_TUPLES_OK {
                    total_rows += PQntuples(res) as usize;
                    results_in_batch += 1;
                }
                PQclear(res);
                
                if status == PGRES_PIPELINE_SYNC { break; }
            }
            
            PQexitPipelineMode(conn);
            total_queries += results_in_batch;
            batch_count += 1;
            
            // Progress every 10 seconds
            if batch_count % 100 == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                let qps = total_queries as f64 / elapsed;
                println!("  {:.0}s: {} queries, {} rows, {:.0} q/s", elapsed, total_queries, total_rows, qps);
            }
        }
        
        let elapsed = start.elapsed();
        let qps = total_queries as f64 / elapsed.as_secs_f64();
        
        println!("\n=== FINAL RESULTS ===");
        println!("  Duration: {:.2}s", elapsed.as_secs_f64());
        println!("  Queries:  {}", total_queries);
        println!("  Rows:     {} (consumed)", total_rows);
        println!("  📈 Average: {:.0} q/s", qps);
        
        PQfinish(conn);
    }
}
