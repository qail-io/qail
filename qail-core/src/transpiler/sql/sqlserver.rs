use super::super::traits::SqlGenerator;

pub struct SqlServerGenerator;

impl SqlGenerator for SqlServerGenerator {
    fn quote_identifier(&self, id: &str) -> String {
        format!("[{}]", id)
    }

    fn placeholder(&self, index: usize) -> String {
        format!("@p{}", index)
    }

    fn fuzzy_operator(&self) -> &str {
        "LIKE"
    }

    fn bool_literal(&self, val: bool) -> String {
        if val { "1".to_string() } else { "0".to_string() }
    }

    fn string_concat(&self, parts: &[&str]) -> String {
        parts.join(" + ")
    }

    fn limit_offset(&self, limit: Option<usize>, offset: Option<usize>) -> String {
        // T-SQL requires ORDER BY for OFFSET/FETCH.
        // Syntax: OFFSET n ROWS FETCH NEXT m ROWS ONLY
        // Note: If no offset, we use TOP (in SELECT clause) or default offset 0 if strict?
        // Actually, T-SQL supports `OFFSET 0 ROWS` if you have ORDER BY.
        // If no limit, just OFFSET works.
        
        let mut sql = String::new();
        let off = offset.unwrap_or(0);
        
        // Only generate if we have at least one of limit or offset
        if limit.is_some() || offset.is_some() {
            sql.push_str(&format!(" OFFSET {} ROWS", off));
            
            if let Some(lim) = limit {
                sql.push_str(&format!(" FETCH NEXT {} ROWS ONLY", lim));
            }
        }
        
        sql
    }
}
