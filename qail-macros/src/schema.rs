//! Schema types for compile-time validation.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub tables: Vec<TableDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type", alias = "typ")]
    pub typ: String,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub primary_key: bool,
}

impl Schema {
    pub fn load() -> Option<Self> {
        let paths = [
            "qail.schema.json",
            ".qail/schema.json",
            "../qail.schema.json",
        ];

        for path in paths {
            if let Ok(content) = std::fs::read_to_string(path) {
                if let Ok(schema) = serde_json::from_str(&content) {
                    return Some(schema);
                }
            }
        }
        None
    }

    pub fn find_table(&self, name: &str) -> Option<&TableDef> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Find similar table names for "did you mean" suggestions
    pub fn similar_tables(&self, name: &str) -> Vec<&str> {
        self.tables
            .iter()
            .filter(|t| {
                levenshtein(&t.name, name) <= 3 || t.name.contains(name) || name.contains(&t.name)
            })
            .map(|t| t.name.as_str())
            .take(5)
            .collect()
    }
}

impl TableDef {
    pub fn find_column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Find similar column names for "did you mean" suggestions
    pub fn similar_columns(&self, name: &str) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| {
                levenshtein(&c.name, name) <= 3 || c.name.contains(name) || name.contains(&c.name)
            })
            .map(|c| c.name.as_str())
            .take(5)
            .collect()
    }
}

/// Simple Levenshtein distance for "did you mean" suggestions
pub fn levenshtein(a: &str, b: &str) -> usize {
    let a_len = a.chars().count();
    let b_len = b.chars().count();
    
    if a_len == 0 { return b_len; }
    if b_len == 0 { return a_len; }
    
    let mut matrix = vec![vec![0usize; b_len + 1]; a_len + 1];
    
    for i in 0..=a_len { matrix[i][0] = i; }
    for j in 0..=b_len { matrix[0][j] = j; }
    
    for (i, ca) in a.chars().enumerate() {
        for (j, cb) in b.chars().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            matrix[i + 1][j + 1] = std::cmp::min(
                std::cmp::min(matrix[i][j + 1] + 1, matrix[i + 1][j] + 1),
                matrix[i][j] + cost,
            );
        }
    }
    
    matrix[a_len][b_len]
}
