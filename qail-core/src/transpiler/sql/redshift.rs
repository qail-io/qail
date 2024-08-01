use super::super::traits::SqlGenerator;

pub struct RedshiftGenerator;

impl SqlGenerator for RedshiftGenerator {
    fn quote_identifier(&self, id: &str) -> String {
        format!("\"{}\"", id.replace('"', "\"\""))
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
}
