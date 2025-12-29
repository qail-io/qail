use qail_core::analyzer::detect_raw_sql;

fn main() {
    // Use the exact code from the user's file
    let code = std::fs::read_to_string("/Users/orion/engine-sailtix-com/src/repository/whatsapp/message_repository.rs")
        .expect("Failed to read file");
    
    let matches = detect_raw_sql(&code);
    
    println!("Found {} SQL matches in message_repository.rs:\n", matches.len());
    
    for (i, m) in matches.iter().enumerate() {
        println!("=== Match #{} ===", i + 1);
        println!("Type: {}", m.sql_type);
        println!("Line (1-indexed): {}:{} to {}:{}", m.line, m.column, m.end_line, m.end_column);
        println!("LSP Range (0-indexed): {}:{} to {}:{}", m.line - 1, m.column, m.end_line - 1, m.end_column);
        println!("SQL length: {} chars", m.raw_sql.len());
        println!("First 80 chars: {}", &m.raw_sql.chars().take(80).collect::<String>().replace('\n', " "));
        println!();
    }
    
    // Also check the exact lines in the source
    let lines: Vec<&str> = code.lines().collect();
    if let Some(m) = matches.get(1) { // Second match (get_insights)
        println!("=== Verifying get_insights SQL bounds ===");
        if m.line > 0 && m.line <= lines.len() {
            println!("Start line {}: {}", m.line, lines[m.line - 1]);
        }
        if m.end_line > 0 && m.end_line <= lines.len() {
            println!("End line {}: {}", m.end_line, lines[m.end_line - 1]);
        }
    }
}
