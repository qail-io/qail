//! Schema validator and fuzzy matching suggestions.

use std::collections::HashMap;
use strsim::levenshtein;

/// Validates query elements against known schema and provides suggestions.
#[derive(Debug, Clone)]
pub struct Validator {
    tables: Vec<String>,
    columns: HashMap<String, Vec<String>>,
}

impl Validator {
    /// Create a new Validator with known tables and columns.
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            columns: HashMap::new(),
        }
    }

    /// Register a table and its columns.
    pub fn add_table(&mut self, table: &str, cols: &[&str]) {
        self.tables.push(table.to_string());
        self.columns.insert(
            table.to_string(),
            cols.iter().map(|s| s.to_string()).collect(),
        );
    }

    /// Check if a table exists. If not, returns suggested names.
    pub fn validate_table(&self, table: &str) -> Result<(), String> {
        if self.tables.contains(&table.to_string()) {
            Ok(())
        } else {
            let suggestions = self.did_you_mean(table, &self.tables);
            if let Some(sugg) = suggestions {
                Err(format!("Table '{}' not found. Did you mean '{}'?", table, sugg))
            } else {
                Err(format!("Table '{}' not found.", table))
            }
        }
    }

    /// Check if a column exists in a table. If not, returns suggested names.
    pub fn validate_column(&self, table: &str, column: &str) -> Result<(), String> {
        // If table doesn't exist, we can't validate column
        if !self.tables.contains(&table.to_string()) {
            return Ok(());
        }

        if let Some(cols) = self.columns.get(table) {
            // Check literal match
            if cols.contains(&column.to_string()) || column == "*" {
                return Ok(());
            }

            // Fuzzy match
            let suggestions = self.did_you_mean(column, cols);
            if let Some(sugg) = suggestions {
                Err(format!(
                    "Column '{}' not found in table '{}'. Did you mean '{}'?",
                    column, table, sugg
                ))
            } else {
                Err(format!("Column '{}' not found in table '{}'.", column, table))
            }
        } else {
            Ok(())
        }
    }

    /// Find the best match with Levenshtein distance <= 3.
    fn did_you_mean(&self, input: &str, candidates: &[impl AsRef<str>]) -> Option<String> {
        let mut best_match = None;
        let mut min_dist = usize::MAX;

        for cand in candidates {
            let cand_str = cand.as_ref();
            let dist = levenshtein(input, cand_str);

            // Dynamic threshold based on length
            let threshold = match input.len() {
                0..=2 => 0,      // Precise match only
                3..=5 => 2,      // Allow 2 char diff (e.g. usr -> users)
                _ => 3,          // Allow 3 char diff
            };

            if dist <= threshold && dist < min_dist {
                min_dist = dist;
                best_match = Some(cand_str.to_string());
            }
        }

        best_match
    }

    /// Validate an entire QailCmd against the schema.
    /// Returns Ok(()) if valid, or a list of validation errors.
    pub fn validate_command(&self, cmd: &crate::ast::QailCmd) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Check table exists
        if let Err(e) = self.validate_table(&cmd.table) {
            errors.push(e);
        }

        // Check columns exist
        for col in &cmd.columns {
            if let crate::ast::Column::Named(name) = col {
                if let Err(e) = self.validate_column(&cmd.table, name) {
                    errors.push(e);
                }
            }
        }

        // Check filter/payload conditions
        for cage in &cmd.cages {
            for cond in &cage.conditions {
                if let Err(e) = self.validate_column(&cmd.table, &cond.column) {
                    errors.push(e);
                }
            }
        }

        // Check join tables
        for join in &cmd.joins {
            if let Err(e) = self.validate_table(&join.table) {
                errors.push(e);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_did_you_mean_table() {
        let mut v = Validator::new();
        v.add_table("users", &["id", "name"]);
        v.add_table("orders", &["id", "total"]);

        assert!(v.validate_table("users").is_ok());
        
        let err = v.validate_table("usr").unwrap_err();
        assert!(err.contains("Did you mean 'users'?")); // distance 2

        let err = v.validate_table("usrs").unwrap_err();
        assert!(err.contains("Did you mean 'users'?")); // distance 1
    }

    #[test]
    fn test_did_you_mean_column() {
        let mut v = Validator::new();
        v.add_table("users", &["email", "password"]);

        assert!(v.validate_column("users", "email").is_ok());

        let err = v.validate_column("users", "emial").unwrap_err();
        assert!(err.contains("Did you mean 'email'?"));
    }
}
