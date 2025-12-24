//! Query validation with "Did you mean?" suggestions.

use proc_macro::TokenStream;
use crate::schema;

/// Parse table name from QAIL query
pub fn parse_qail_table(query: &str) -> Option<String> {
    let query = query.trim().to_lowercase();
    let words: Vec<&str> = query.split_whitespace().collect();
    
    if words.len() >= 2 && matches!(words[0], "get" | "add" | "set" | "del") {
        return Some(words[1].to_string());
    }
    None
}

/// Parse column names from QAIL query
pub fn parse_qail_columns(query: &str) -> Vec<String> {
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

/// Validate QAIL query at compile time
pub fn validate_query(query_str: &str, query_span: proc_macro2::Span) -> Result<(), TokenStream> {
    // Phase 1: Parse validation using qail-core
    let cmd = match qail_core::parse(query_str) {
        Ok(cmd) => cmd,
        Err(e) => {
            let error = format!("QAIL parse error: {}", e);
            return Err(syn::Error::new(query_span, error).to_compile_error().into());
        }
    };
    
    // Phase 2: Transpile validation - generate SQL and check for issues
    use qail_core::transpiler::ToSqlParameterized;
    let result = cmd.to_sql_parameterized();
    
    // Check for common SQL generation issues
    if result.sql.is_empty() {
        return Err(syn::Error::new(query_span, "QAIL generated empty SQL").to_compile_error().into());
    }
    
    // Check for untranspiled QAIL keywords (should have been converted to SQL)
    let sql_lower = result.sql.to_lowercase();
    if sql_lower.contains("get ") && !sql_lower.contains("select") {
        return Err(syn::Error::new(
            query_span, 
            "QAIL transpiler error: 'get' keyword not converted to SELECT"
        ).to_compile_error().into());
    }
    
    // Check for CTEs missing WITH clause
    if !cmd.ctes.is_empty() && !result.sql.to_uppercase().starts_with("WITH") {
        return Err(syn::Error::new(
            query_span,
            "QAIL transpiler error: CTEs defined but WITH clause missing from generated SQL"
        ).to_compile_error().into());
    }
    
    // Check for unquoted JSON access (common bug: contact_info->>phone instead of contact_info->>'phone')
    if regex_simple_check(&result.sql) {
        return Err(syn::Error::new(
            query_span,
            "QAIL transpiler error: JSON access path missing quotes (e.g., ->>col instead of ->>'col')"
        ).to_compile_error().into());
    }
    
    // Phase 3: Schema validation (original logic)
    let schema = match schema::Schema::load() {
        Some(s) => s,
        None => return Ok(()), // No schema = skip schema validation
    };

    if let Some(table_name) = parse_qail_table(query_str) {
        if schema.find_table(&table_name).is_none() {
            let similar = schema.similar_tables(&table_name);
            let suggestion = if !similar.is_empty() {
                format!("\n\nDid you mean: {:?}?", similar)
            } else {
                String::new()
            };
            
            let error = format!(
                "table '{}' not found in schema.{}",
                table_name, suggestion
            );
            return Err(syn::Error::new(query_span, error).to_compile_error().into());
        }
        
        if let Some(table) = schema.find_table(&table_name) {
            for col_name in parse_qail_columns(query_str) {
                if table.find_column(&col_name).is_none() {
                    let similar = table.similar_columns(&col_name);
                    let suggestion = if !similar.is_empty() {
                        format!("\n\nDid you mean: {:?}?", similar)
                    } else {
                        String::new()
                    };
                    
                    let error = format!(
                        "column '{}' not found in table '{}'.{}",
                        col_name, table_name, suggestion
                    );
                    return Err(syn::Error::new(query_span, error).to_compile_error().into());
                }
            }
        }
    }
    
    Ok(())
}

/// Simple check for unquoted JSON access pattern
fn regex_simple_check(sql: &str) -> bool {
    // Look for ->>identifier (not ->>') which indicates missing quotes
    let bytes = sql.as_bytes();
    let len = bytes.len();
    
    for i in 0..len.saturating_sub(3) {
        if bytes[i] == b'-' && bytes[i+1] == b'>' && bytes[i+2] == b'>' {
            // Check next char after ->>
            if i + 3 < len {
                let next = bytes[i + 3];
                // If next char is alphanumeric (not ' or space), it's unquoted
                if next.is_ascii_alphanumeric() || next == b'_' {
                    return true;
                }
            }
        }
    }
    false
}
