use crate::transpiler::traits::SqlGenerator;

/// MySQL Generator.
pub struct MysqlGenerator;

impl MysqlGenerator {
    pub fn new() -> Self {
        Self
    }
}

impl SqlGenerator for MysqlGenerator {
    fn quote_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
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
        // MySQL uses LIMIT [offset,] row_count OR LIMIT row_count OFFSET offset
        // Standard LIMIT/OFFSET works in modern MySQL
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
        // syntax: col->"$.key"
        let p = path.join(".");
        format!("{}->\"$.{}\"", self.quote_identifier(col), p)
    }

    fn json_contains(&self, col: &str, value: &str) -> String {
        // MySQL uses JSON_CONTAINS(target, candidate)
        format!("JSON_CONTAINS({}, {})", col, value)
    }

    fn json_key_exists(&self, col: &str, key: &str) -> String {
        // MySQL uses JSON_CONTAINS_PATH(col, 'one', '$.key')
        // key comes as 'keyname', we need to convert to $.keyname
        let clean_key = key.trim_matches('\'');
        format!("JSON_CONTAINS_PATH({}, 'one', '$.{}')", col, clean_key)
    }
    
    // MySQL 8.0+ has native JSON_VALUE support
    fn json_value(&self, col: &str, path: &str) -> String {
        format!("JSON_VALUE({}, '{}')", self.quote_identifier(col), path)
    }
}
