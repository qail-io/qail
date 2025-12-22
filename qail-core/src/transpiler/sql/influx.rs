use crate::transpiler::traits::SqlGenerator;

/// InfluxDB (InfluxQL) Generator.
/// InfluxQL is essentially SQL-like.
pub struct InfluxGenerator;

impl SqlGenerator for InfluxGenerator {
    fn quote_identifier(&self, name: &str) -> String {
        // Influx uses double quotes for identifiers (measurements, tags) if they have special chars
        // But often unquoted is fine. We force quotes for safety.
        format!("\"{}\"", name)
    }

    fn placeholder(&self, _index: usize) -> String {
         // InfluxQL HTTP API usually handles literals.
         "?".to_string() 
    }

    fn fuzzy_operator(&self) -> &'static str {
        "=~" // Regex match for tags
    }

    fn bool_literal(&self, val: bool) -> String {
        if val { "true".to_string() } else { "false".to_string() }
    }
    
    fn string_concat(&self, _parts: &[&str]) -> String {
        // Influx doesn't really do string concat in SELECT
        "CONCAT_UNSUPPORTED".to_string()
    }
    
    fn limit_offset(&self, limit: Option<usize>, offset: Option<usize>) -> String {
        let mut s = String::new();
        if let Some(l) = limit {
            s.push_str(&format!("LIMIT {}", l));
        }
        if let Some(o) = offset {
            if !s.is_empty() { s.push(' '); }
             // InfluxQL supports OFFSET
            s.push_str(&format!("OFFSET {}", o));
        }
        s
    }
}
