use qail_core::analyzer::detect_raw_sql;

fn main() {
    let code = std::fs::read_to_string("/Users/orion/engine-sailtix-com/src/repository/whatsapp/message_repository.rs")
        .expect("Failed to read file");
    
    let matches = detect_raw_sql(&code);
    
    // Show the suggested QAIL for get_insights (match #2)
    if let Some(m) = matches.get(1) {
        println!("=== get_insights SQL ===");
        println!("Suggested QAIL replacement:");
        println!("{}", m.suggested_qail);
        println!("\n=== What should be in source ===");
        let lines: Vec<&str> = code.lines().collect();
        for i in (m.line - 1)..m.end_line {
            if i < lines.len() {
                println!("{}: {}", i + 1, lines[i]);
            }
        }
    }
}
