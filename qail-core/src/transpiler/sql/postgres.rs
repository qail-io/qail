use crate::transpiler::traits::SqlGenerator;
use crate::transpiler::escape_identifier;

/// PostgreSQL Generator (Default).
pub struct PostgresGenerator;

impl PostgresGenerator {
    pub fn new() -> Self {
        Self
    }
}

impl SqlGenerator for PostgresGenerator {
    fn quote_identifier(&self, name: &str) -> String {
        escape_identifier(name)
    }

    fn placeholder(&self, index: usize) -> String {
        format!("${}", index)
    }

    fn fuzzy_operator(&self) -> &str {
        "ILIKE"
    }

    fn bool_literal(&self, val: bool) -> String {
        if val { "true".to_string() } else { "false".to_string() }
    }

    fn string_concat(&self, parts: &[&str]) -> String {
        parts.join(" || ")
    }

    fn limit_offset(&self, limit: Option<usize>, offset: Option<usize>) -> String {
        let mut sql = String::new();
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {}", n));
        }
        if let Some(n) = offset {
            sql.push_str(&format!(" OFFSET {}", n));
        }
        sql
    }

    fn json_access(&self, col: &str, path: &[&str]) -> String {
        let mut sql = self.quote_identifier(col);
        
        for (i, key) in path.iter().enumerate() {
            let is_last = i == path.len() - 1;
            // Use -> (json) for intermediates, ->> (text) for last
            // Note: If the column is not text, an explicit cast may be required.
            // Postgres ->> returns text, suitable for comparisons.
            let op = if is_last { "->>" } else { "->" };
            sql.push_str(&format!("{}'{}'", op, key));
        }
        sql
    }
}
