use qail_core::analyzer::detect_raw_sql;

fn main() {
    // Test: Multi-line raw string like in the user's code
    let code = r##"
fn get_insights() {
    let insights = sqlx::query_as::<_, Insights>(
        r#"
            WITH stats AS (
                SELECT
                    COUNT(DISTINCT phone_number) AS total_contacts,
                    COUNT(*) AS total_messages
                FROM whatsapp_messages
            )
            SELECT
                total_contacts,
                total_messages
            FROM stats
            "#
    )
    .fetch_one(&self.pool);
}
"##;

    let matches = detect_raw_sql(code);
    
    println!("Found {} SQL matches:", matches.len());
    for m in &matches {
        println!("---");
        println!("Type: {}", m.sql_type);
        println!("Line range: {}:{} to {}:{}", m.line, m.column, m.end_line, m.end_column);
        println!("Raw SQL ({} chars):", m.raw_sql.len());
        println!("{}", m.raw_sql);
        println!("\nSuggested QAIL:");
        println!("{}", m.suggested_qail);
    }
}
