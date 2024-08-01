use super::super::traits::SqlGenerator;

pub struct OracleGenerator;

impl SqlGenerator for OracleGenerator {
    fn quote_identifier(&self, id: &str) -> String {
        // Oracle standardly uses double quotes for case-sensitive identifiers
        format!("\"{}\"", id.replace('"', "\"\""))
    }

    fn placeholder(&self, index: usize) -> String {
        // Oracle uses :1, :2, etc. (1-based index)
        format!(":{}", index)
    }

    fn fuzzy_operator(&self) -> &str {
        "LIKE"
    }

    fn bool_literal(&self, val: bool) -> String {
        // Oracle has no BOOLEAN type in SQL (only PL/SQL). commonly use 1/0 or 'Y'/'N'.
        // We'll stick to 1/0 for consistency with our other "fake boolean" dialects.
        if val { "1".to_string() } else { "0".to_string() }
    }

    fn string_concat(&self, parts: &[&str]) -> String {
        parts.join(" || ")
    }

    fn limit_offset(&self, limit: Option<usize>, offset: Option<usize>) -> String {
        // Oracle 12c+ syntax
        // OFFSET n ROWS FETCH NEXT m ROWS ONLY
        
        let mut sql = String::new();
        let off = offset.unwrap_or(0);
        
        if limit.is_some() || offset.is_some() {
            sql.push_str(&format!(" OFFSET {} ROWS", off));
            
            if let Some(lim) = limit {
                sql.push_str(&format!(" FETCH NEXT {} ROWS ONLY", lim));
            }
        }
        
        sql
    }
}
