use super::super::traits::SqlGenerator;

pub struct MariaDbGenerator;

impl SqlGenerator for MariaDbGenerator {
    fn quote_identifier(&self, id: &str) -> String {
        format!("`{}`", id.replace('`', "``"))
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn fuzzy_operator(&self) -> &str {
        "LIKE"
    }

    fn bool_literal(&self, val: bool) -> String {
        if val { "1".to_string() } else { "0".to_string() }
    }

    fn string_concat(&self, parts: &[&str]) -> String {
        format!("CONCAT({})", parts.join(", "))
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
