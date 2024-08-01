//! Tests for qail-macros parsing and validation

#[test]
fn test_schema_loaded() {
    // Verify schema file exists and can be loaded
    let content = std::fs::read_to_string("qail.schema.json")
        .expect("qail.schema.json should exist - run 'cargo run --bin qail -- pull <url>'");
    
    let schema: serde_json::Value = serde_json::from_str(&content)
        .expect("Schema should be valid JSON");
    
    let tables = schema["tables"].as_array().expect("Should have tables");
    assert!(tables.len() > 0, "Should have at least one table");
    
    // Check for whatsapp_messages table
    let has_whatsapp = tables.iter().any(|t| {
        t["name"].as_str() == Some("whatsapp_messages")
    });
    assert!(has_whatsapp, "Should have whatsapp_messages table");
    
    println!("✓ Schema loaded with {} tables", tables.len());
}

#[test]
fn test_levenshtein_distance() {
    // Simple Levenshtein distance implementation for testing
    fn levenshtein(a: &str, b: &str) -> usize {
        let a_len = a.chars().count();
        let b_len = b.chars().count();
        
        if a_len == 0 { return b_len; }
        if b_len == 0 { return a_len; }
        
        let mut matrix = vec![vec![0usize; b_len + 1]; a_len + 1];
        
        for i in 0..=a_len { matrix[i][0] = i; }
        for j in 0..=b_len { matrix[0][j] = j; }
        
        for (i, ca) in a.chars().enumerate() {
            for (j, cb) in b.chars().enumerate() {
                let cost = if ca == cb { 0 } else { 1 };
                matrix[i + 1][j + 1] = std::cmp::min(
                    std::cmp::min(matrix[i][j + 1] + 1, matrix[i + 1][j] + 1),
                    matrix[i][j] + cost,
                );
            }
        }
        
        matrix[a_len][b_len]
    }

    assert_eq!(levenshtein("users", "users"), 0);
    assert_eq!(levenshtein("users", "usrs"), 1);
    assert_eq!(levenshtein("name", "naem"), 2);
    assert_eq!(levenshtein("phone_number", "phone_numbe"), 1);
    assert_eq!(levenshtein("whatsapp_messages", "whatsap_messages"), 1);
}

#[test]
fn test_parse_qail_table() {
    fn parse_qail_table(query: &str) -> Option<String> {
        let query = query.trim().to_lowercase();
        let words: Vec<&str> = query.split_whitespace().collect();
        
        if words.len() >= 2 && matches!(words[0], "get" | "add" | "set" | "del") {
            return Some(words[1].to_string());
        }
        None
    }

    assert_eq!(parse_qail_table("get users"), Some("users".to_string()));
    assert_eq!(parse_qail_table("add orders columns x values y"), Some("orders".to_string()));
    assert_eq!(parse_qail_table("set products where id = :id"), Some("products".to_string()));
    assert_eq!(parse_qail_table("del items where id = :id"), Some("items".to_string()));
    assert_eq!(parse_qail_table("get whatsapp_messages where id = :id"), Some("whatsapp_messages".to_string()));
}

#[test]
fn test_parse_qail_columns() {
    fn parse_qail_columns(query: &str) -> Vec<String> {
        let mut columns = Vec::new();
        let query_lower = query.to_lowercase();
        
        if let Some(where_pos) = query_lower.find("where") {
            let after_where = &query[where_pos + 5..];
            for word in after_where.split_whitespace() {
                let word_lower = word.to_lowercase();
                if !matches!(word_lower.as_str(), "and" | "or" | "=" | "!=" | "<" | ">" | 
                             "like" | "ilike" | "in" | "is" | "null" | "not" | "order" | "by" | 
                             "limit" | "offset" | "asc" | "desc" | "set" | "fields" | "true" | "false") 
                   && !word.starts_with(':') 
                   && !word.starts_with('$')
                   && !word.chars().next().map(|c| c.is_numeric()).unwrap_or(false)
                   && !word.starts_with('\'')
                   && !word.starts_with('"') {
                    let clean = word.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
                    if !clean.is_empty() && clean.len() > 1 {
                        columns.push(clean.to_string());
                    }
                }
            }
        }
        
        columns
    }

    let cols = parse_qail_columns("get users where name = :name and age > 18");
    assert!(cols.contains(&"name".to_string()));
    assert!(cols.contains(&"age".to_string()));
    
    let cols2 = parse_qail_columns("get whatsapp_messages where phone_number = :phone");
    assert!(cols2.contains(&"phone_number".to_string()));
}
