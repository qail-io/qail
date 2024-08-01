//! Transpiler traits and utilities.

/// SQL reserved words that must be quoted when used as identifiers.
pub const RESERVED_WORDS: &[&str] = &[
    "order", "group", "user", "table", "select", "from", "where", "join",
    "left", "right", "inner", "outer", "on", "and", "or", "not", "null",
    "true", "false", "limit", "offset", "as", "in", "is", "like", "between",
    "having", "union", "all", "distinct", "case", "when", "then", "else", "end",
    "create", "alter", "drop", "insert", "update", "delete", "index", "key",
    "primary", "foreign", "references", "default", "constraint", "check",
];

/// Escape an identifier if it's a reserved word or contains special chars.
/// Returns the identifier quoted with double quotes if needed.
/// Handles dotted identifiers (e.g., `table.column`) by quoting each part.
pub fn escape_identifier(name: &str) -> String {
    // Handle dotted identifiers (e.g., lm.phone_number -> "lm"."phone_number")
    if name.contains('.') {
        return name
            .split('.')
            .map(|part| escape_single_identifier(part))
            .collect::<Vec<_>>()
            .join(".");
    }
    escape_single_identifier(name)
}

/// Escape a single identifier part (no dots).
fn escape_single_identifier(name: &str) -> String {
    let lower = name.to_lowercase();
    let needs_escaping = RESERVED_WORDS.contains(&lower.as_str())
        || name.chars().any(|c| !c.is_alphanumeric() && c != '_')
        || name.chars().next().map(|c| c.is_numeric()).unwrap_or(false);
    
    if needs_escaping {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

/// Trait for dialect-specific SQL generation.
pub trait SqlGenerator {
    /// Quote an identifier (table or column name).
    fn quote_identifier(&self, name: &str) -> String;
    /// Generate the parameter placeholder (e.g., $1, ?, @p1) for a given index.
    fn placeholder(&self, index: usize) -> String;
    /// Get the fuzzy matching operator (ILIKE vs LIKE).
    fn fuzzy_operator(&self) -> &str;
    /// Get the boolean literal (true/false vs 1/0).
    fn bool_literal(&self, val: bool) -> String;
    /// Generate string concatenation expression (e.g. 'a' || 'b' vs CONCAT('a', 'b')).
    fn string_concat(&self, parts: &[&str]) -> String;
    /// Generate LIMIT/OFFSET clause.
    fn limit_offset(&self, limit: Option<usize>, offset: Option<usize>) -> String;
    /// Generate JSON access syntax.
    /// path components are the keys to traverse.
    /// Default implementation returns "col"."key1"."key2" (Standard SQL composite).
    fn json_access(&self, col: &str, path: &[&str]) -> String {
        let mut parts = vec![self.quote_identifier(col)];
        for key in path {
            parts.push(self.quote_identifier(key));
        }
        parts.join(".")
    }
    /// Generate JSON/Array contains expression.
    /// Default implementation returns Postgres-compatible `col @> value`.
    fn json_contains(&self, col: &str, value: &str) -> String {
        format!("{} @> {}", col, value)
    }
    /// Generate JSON key exists expression.
    /// Default implementation returns Postgres-compatible `col ? 'key'`.
    fn json_key_exists(&self, col: &str, key: &str) -> String {
        format!("{} ? {}", col, key)
    }
    
    /// JSON_EXISTS - check if path exists in JSON (Postgres 17+, SQL/JSON standard)
    fn json_exists(&self, col: &str, path: &str) -> String {
        format!("JSON_EXISTS({}, '{}')", col, path)
    }
    
    /// JSON_QUERY - extract JSON object/array at path (Postgres 17+, SQL/JSON standard)
    fn json_query(&self, col: &str, path: &str) -> String {
        format!("JSON_QUERY({}, '{}')", col, path)
    }
    
    /// JSON_VALUE - extract scalar value at path (Postgres 17+, SQL/JSON standard)
    fn json_value(&self, col: &str, path: &str) -> String {
        format!("JSON_VALUE({}, '{}')", col, path)
    }
}
