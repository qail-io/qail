//! CPU-only benchmark for flamegraph profiling
//!
//! This tests encoding/decoding without network to isolate CPU bottlenecks.
//!
//! Run: cargo flamegraph -p qail-pg --example profile_cpu

use qail_core::ast::QailCmd;
use qail_core::transpiler::ToSqlParameterized;

const ITERATIONS: usize = 100_000;

fn main() {
    println!("🔬 CPU Profiling Benchmark (no network)");
    println!("=========================================");
    println!("Iterations: {}\n", ITERATIONS);
    
    // ========== 1. AST Building ==========
    println!("📊 Test 1: AST Building");
    let start = std::time::Instant::now();
    for _ in 0..ITERATIONS {
        let _cmd = QailCmd::get("users")
            .columns(["id", "name", "email"])
            .filter("active", qail_core::ast::Operator::Eq, true)
            .limit(20);
    }
    println!("   AST build: {:?} ({:?}/iter)", 
        start.elapsed(), start.elapsed() / ITERATIONS as u32);

    // ========== 2. SQL Generation ==========
    println!("\n📊 Test 2: to_sql_parameterized()");
    let cmd = QailCmd::get("users")
        .columns(["id", "name", "email"])
        .filter("active", qail_core::ast::Operator::Eq, true)
        .limit(20);
    
    let start = std::time::Instant::now();
    for _ in 0..ITERATIONS {
        let _result = cmd.to_sql_parameterized();
    }
    println!("   SQL gen: {:?} ({:?}/iter)", 
        start.elapsed(), start.elapsed() / ITERATIONS as u32);
    
    // ========== 3. Value to Bytes ==========
    println!("\n📊 Test 3: Value to Bytes Conversion");
    use qail_core::ast::Value;
    let values = vec![
        Value::String("Alice".to_string()),
        Value::Int(42),
        Value::Bool(true),
        Value::String("Hello, World!".to_string()),
    ];
    
    let start = std::time::Instant::now();
    for _ in 0..ITERATIONS {
        for v in &values {
            let _bytes = value_to_bytes(v);
        }
    }
    println!("   Value→bytes: {:?} ({:?}/iter)", 
        start.elapsed(), start.elapsed() / ITERATIONS as u32);

    // ========== 4. Wire Protocol Encoding ==========
    println!("\n📊 Test 4: Extended Query Encoding");
    use qail_pg::protocol::PgEncoder;
    let sql = "SELECT id, name, email FROM users WHERE active = $1 LIMIT $2";
    let params: Vec<Option<Vec<u8>>> = vec![
        Some(b"t".to_vec()),
        Some(b"20".to_vec()),
    ];
    
    let start = std::time::Instant::now();
    for _ in 0..ITERATIONS {
        let _bytes = PgEncoder::encode_extended_query(sql, &params);
    }
    println!("   Wire encode: {:?} ({:?}/iter)", 
        start.elapsed(), start.elapsed() / ITERATIONS as u32);

    // ========== 5. Wire Protocol Decoding (simulated) ==========
    println!("\n📊 Test 5: DataRow Decoding");
    // Simulate a DataRow with 3 columns: UUID, String, Bool
    let mut fake_data_row = vec![b'D']; // DataRow
    let payload = build_fake_data_row();
    let len = (payload.len() + 4) as i32;
    fake_data_row.extend_from_slice(&len.to_be_bytes());
    fake_data_row.extend_from_slice(&payload);
    
    use qail_pg::protocol::BackendMessage;
    
    let start = std::time::Instant::now();
    for _ in 0..ITERATIONS {
        let _msg = BackendMessage::decode(&fake_data_row);
    }
    println!("   Wire decode: {:?} ({:?}/iter)", 
        start.elapsed(), start.elapsed() / ITERATIONS as u32);

    println!("\n✅ Profiling complete");
}

fn value_to_bytes(value: &qail_core::ast::Value) -> Option<Vec<u8>> {
    use qail_core::ast::Value;
    match value {
        Value::Null => None,
        Value::Bool(b) => Some(if *b { b"t".to_vec() } else { b"f".to_vec() }),
        Value::Int(i) => Some(i.to_string().into_bytes()),
        Value::Float(f) => Some(f.to_string().into_bytes()),
        Value::String(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

fn build_fake_data_row() -> Vec<u8> {
    let mut payload = Vec::new();
    // 3 columns
    payload.extend_from_slice(&3i16.to_be_bytes());
    
    // Column 1: UUID (36 bytes)
    let uuid = b"550e8400-e29b-41d4-a716-446655440000";
    payload.extend_from_slice(&(uuid.len() as i32).to_be_bytes());
    payload.extend_from_slice(uuid);
    
    // Column 2: String (5 bytes)
    let name = b"Alice";
    payload.extend_from_slice(&(name.len() as i32).to_be_bytes());
    payload.extend_from_slice(name);
    
    // Column 3: Bool (1 byte)
    let active = b"t";
    payload.extend_from_slice(&(active.len() as i32).to_be_bytes());
    payload.extend_from_slice(active);
    
    payload
}
