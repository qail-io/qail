use qail_core::analyzer::detect_raw_sql;

fn main() {
    let code = std::fs::read_to_string("/Users/orion/engine-sailtix-com/src/repository/whatsapp/message_repository copy.rs").unwrap();
    let matches = detect_raw_sql(&code);
    if let Some(m) = matches.first() {
        println!("SQL Type: {}", m.sql_type);
        println!("Line: {} to {}", m.line, m.end_line);
        println!("=== Suggested QAIL ===");
        println!("{}", m.suggested_qail);
    } else {
        println!("No SQL matches found");
    }
}
