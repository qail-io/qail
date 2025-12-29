use qail_core::analyzer::detect_raw_sql;

fn main() {
    let code = std::fs::read_to_string("/Users/orion/engine-sailtix-com/src/repository/payment/postgres_impl/payment_events.rs")
        .expect("Failed to read file");
    
    let matches = detect_raw_sql(&code);
    
    println!("Found {} SQL matches:\n", matches.len());
    
    for (i, m) in matches.iter().take(5).enumerate() {
        println!("=== Match #{} ===", i + 1);
        println!("Type: {}", m.sql_type);
        println!("Lines: {} to {}", m.line, m.end_line);
        println!("SQL: {}", &m.raw_sql.chars().take(150).collect::<String>().replace('\n', " "));
        println!("Suggested QAIL:\n{}", m.suggested_qail);
        println!();
    }
}
