use super::super::traits::SqlGenerator;

pub struct BigQueryGenerator;

impl SqlGenerator for BigQueryGenerator {
    fn quote_identifier(&self, id: &str) -> String {
        format!("`{}`", id.replace('`', "\\`"))
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn fuzzy_operator(&self) -> &str {
        "LIKE"
    }

    fn bool_literal(&self, val: bool) -> String {
        if val { "true".to_string() } else { "false".to_string() }
    }

    fn string_concat(&self, parts: &[&str]) -> String {
        // BigQuery uses CONCAT(s1, s2, ...)
        format!("CONCAT({})", parts.join(", "))
    }

    fn limit_offset(&self, limit: Option<usize>, offset: Option<usize>) -> String {
        // BigQuery standard SQL supports LIMIT x OFFSET y
        let mut sql = String::new();
        if let Some(n) = limit {
            sql.push_str(&format!(" LIMIT {}", n));
        }
        if let Some(n) = offset {
            sql.push_str(&format!(" OFFSET {}", n));
        }
        sql
    }
}
