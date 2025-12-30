//! Schema validator and fuzzy matching suggestions.
//!
//! Provides compile-time-like validation for Qail against a known schema.
//! Used by CLI, LSP, and the encoder to catch errors before they hit the wire.

use crate::ast::{Expr, Qail};
use std::collections::HashMap;
use strsim::levenshtein;

/// Validation error with structured information.
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    TableNotFound {
        table: String,
        suggestion: Option<String>,
    },
    ColumnNotFound {
        table: String,
        column: String,
        suggestion: Option<String>,
    },
    /// Type mismatch (future: when schema includes types)
    TypeMismatch {
        table: String,
        column: String,
        expected: String,
        got: String,
    },
    /// Invalid operator for column type (future)
    InvalidOperator {
        column: String,
        operator: String,
        reason: String,
    },
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::TableNotFound { table, suggestion } => {
                if let Some(s) = suggestion {
                    write!(f, "Table '{}' not found. Did you mean '{}'?", table, s)
                } else {
                    write!(f, "Table '{}' not found.", table)
                }
            }
            ValidationError::ColumnNotFound {
                table,
                column,
                suggestion,
            } => {
                if let Some(s) = suggestion {
                    write!(
                        f,
                        "Column '{}' not found in table '{}'. Did you mean '{}'?",
                        column, table, s
                    )
                } else {
                    write!(f, "Column '{}' not found in table '{}'.", column, table)
                }
            }
            ValidationError::TypeMismatch {
                table,
                column,
                expected,
                got,
            } => {
                write!(
                    f,
                    "Type mismatch for '{}.{}': expected {}, got {}",
                    table, column, expected, got
                )
            }
            ValidationError::InvalidOperator {
                column,
                operator,
                reason,
            } => {
                write!(
                    f,
                    "Invalid operator '{}' for column '{}': {}",
                    operator, column, reason
                )
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Result of validation
pub type ValidationResult = Result<(), Vec<ValidationError>>;

/// Validates query elements against known schema and provides suggestions.
#[derive(Debug, Clone)]
pub struct Validator {
    tables: Vec<String>,
    columns: HashMap<String, Vec<String>>,
    #[allow(dead_code)]
    column_types: HashMap<String, HashMap<String, String>>,
}

impl Default for Validator {
    fn default() -> Self {
        Self::new()
    }
}

impl Validator {
    /// Create a new Validator with known tables and columns.
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            columns: HashMap::new(),
            column_types: HashMap::new(),
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

    /// Register a table with column types (for future type validation).
    pub fn add_table_with_types(&mut self, table: &str, cols: &[(&str, &str)]) {
        self.tables.push(table.to_string());
        let col_names: Vec<String> = cols.iter().map(|(name, _)| name.to_string()).collect();
        self.columns.insert(table.to_string(), col_names);

        let type_map: HashMap<String, String> = cols
            .iter()
            .map(|(name, typ)| (name.to_string(), typ.to_string()))
            .collect();
        self.column_types.insert(table.to_string(), type_map);
    }

    /// Get list of all table names (for autocomplete).
    pub fn table_names(&self) -> &[String] {
        &self.tables
    }

    /// Get column names for a table (for autocomplete).
    pub fn column_names(&self, table: &str) -> Option<&Vec<String>> {
        self.columns.get(table)
    }

    /// Check if a table exists.
    pub fn table_exists(&self, table: &str) -> bool {
        self.tables.contains(&table.to_string())
    }

    /// Check if a table exists. If not, returns structured error with suggestion.
    pub fn validate_table(&self, table: &str) -> Result<(), ValidationError> {
        if self.tables.contains(&table.to_string()) {
            Ok(())
        } else {
            let suggestion = self.did_you_mean(table, &self.tables);
            Err(ValidationError::TableNotFound {
                table: table.to_string(),
                suggestion,
            })
        }
    }

    /// Check if a column exists in a table. If not, returns structured error.
    pub fn validate_column(&self, table: &str, column: &str) -> Result<(), ValidationError> {
        // If table doesn't exist, skip column validation (table error takes precedence)
        if !self.tables.contains(&table.to_string()) {
            return Ok(());
        }

        // Always allow * and qualified names like "table.column"
        if column == "*" || column.contains('.') {
            return Ok(());
        }

        if let Some(cols) = self.columns.get(table) {
            if cols.contains(&column.to_string()) {
                Ok(())
            } else {
                let suggestion = self.did_you_mean(column, cols);
                Err(ValidationError::ColumnNotFound {
                    table: table.to_string(),
                    column: column.to_string(),
                    suggestion,
                })
            }
        } else {
            Ok(())
        }
    }

    /// Extract column name from an Expr for validation.
    fn extract_column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Named(name) => Some(name.clone()),
            Expr::Aliased { name, .. } => Some(name.clone()),
            Expr::Aggregate { col, .. } => Some(col.clone()),
            Expr::Cast { expr, .. } => Self::extract_column_name(expr),
            Expr::JsonAccess { column, .. } => Some(column.clone()),
            _ => None,
        }
    }

    /// Validate an entire Qail against the schema.
    pub fn validate_command(&self, cmd: &Qail) -> ValidationResult {
        let mut errors = Vec::new();

        if let Err(e) = self.validate_table(&cmd.table) {
            errors.push(e);
        }

        for col in &cmd.columns {
            if let Some(name) = Self::extract_column_name(col)
                && let Err(e) = self.validate_column(&cmd.table, &name)
            {
                errors.push(e);
            }
        }

        for cage in &cmd.cages {
            for cond in &cage.conditions {
                if let Some(name) = Self::extract_column_name(&cond.left) {
                    // For join conditions, column might be qualified (table.column)
                    if name.contains('.') {
                        let parts: Vec<&str> = name.split('.').collect();
                        if parts.len() == 2
                            && let Err(e) = self.validate_column(parts[0], parts[1])
                        {
                            errors.push(e);
                        }
                    } else if let Err(e) = self.validate_column(&cmd.table, &name) {
                        errors.push(e);
                    }
                }
            }
        }

        for join in &cmd.joins {
            // Validate join table exists
            if let Err(e) = self.validate_table(&join.table) {
                errors.push(e);
            }

            // Validate columns in ON conditions
            if let Some(conditions) = &join.on {
                for cond in conditions {
                    if let Some(name) = Self::extract_column_name(&cond.left)
                        && name.contains('.')
                    {
                        let parts: Vec<&str> = name.split('.').collect();
                        if parts.len() == 2
                            && let Err(e) = self.validate_column(parts[0], parts[1])
                        {
                            errors.push(e);
                        }
                    }
                    // Also check right side if it's a column reference
                    if let crate::ast::Value::Column(col_name) = &cond.value
                        && col_name.contains('.')
                    {
                        let parts: Vec<&str> = col_name.split('.').collect();
                        if parts.len() == 2
                            && let Err(e) = self.validate_column(parts[0], parts[1])
                        {
                            errors.push(e);
                        }
                    }
                }
            }
        }

        if let Some(returning) = &cmd.returning {
            for col in returning {
                if let Some(name) = Self::extract_column_name(col)
                    && let Err(e) = self.validate_column(&cmd.table, &name)
                {
                    errors.push(e);
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Find the best match with Levenshtein distance within threshold.
    fn did_you_mean(&self, input: &str, candidates: &[impl AsRef<str>]) -> Option<String> {
        let mut best_match = None;
        let mut min_dist = usize::MAX;

        for cand in candidates {
            let cand_str = cand.as_ref();
            let dist = levenshtein(input, cand_str);

            // Dynamic threshold based on length
            let threshold = match input.len() {
                0..=2 => 0, // Precise match only for very short strings
                3..=5 => 2, // Allow 2 char diff (e.g. usr -> users)
                _ => 3,     // Allow 3 char diff for longer strings
            };

            if dist <= threshold && dist < min_dist {
                min_dist = dist;
                best_match = Some(cand_str.to_string());
            }
        }

        best_match
    }

    // =========================================================================
    // Legacy API (for backward compatibility)
    // =========================================================================

    /// Legacy: validate_table that returns String error
    #[deprecated(note = "Use validate_table() which returns ValidationError")]
    pub fn validate_table_legacy(&self, table: &str) -> Result<(), String> {
        self.validate_table(table).map_err(|e| e.to_string())
    }

    /// Legacy: validate_column that returns String error
    #[deprecated(note = "Use validate_column() which returns ValidationError")]
    pub fn validate_column_legacy(&self, table: &str, column: &str) -> Result<(), String> {
        self.validate_column(table, column)
            .map_err(|e| e.to_string())
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
        assert!(
            matches!(err, ValidationError::TableNotFound { suggestion: Some(ref s), .. } if s == "users")
        );

        let err = v.validate_table("usrs").unwrap_err();
        assert!(
            matches!(err, ValidationError::TableNotFound { suggestion: Some(ref s), .. } if s == "users")
        );
    }

    #[test]
    fn test_did_you_mean_column() {
        let mut v = Validator::new();
        v.add_table("users", &["email", "password"]);

        assert!(v.validate_column("users", "email").is_ok());
        assert!(v.validate_column("users", "*").is_ok());

        let err = v.validate_column("users", "emial").unwrap_err();
        assert!(
            matches!(err, ValidationError::ColumnNotFound { suggestion: Some(ref s), .. } if s == "email")
        );
    }

    #[test]
    fn test_qualified_column_name() {
        let mut v = Validator::new();
        v.add_table("users", &["id", "name"]);
        v.add_table("profiles", &["user_id", "avatar"]);

        // Qualified names should pass through
        assert!(v.validate_column("users", "users.id").is_ok());
        assert!(v.validate_column("users", "profiles.user_id").is_ok());
    }

    #[test]
    fn test_validate_command() {
        let mut v = Validator::new();
        v.add_table("users", &["id", "email", "name"]);

        let cmd = Qail::get("users").columns(["id", "email"]);
        assert!(v.validate_command(&cmd).is_ok());

        let cmd = Qail::get("users").columns(["id", "emial"]); // typo
        let errors = v.validate_command(&cmd).unwrap_err();
        assert_eq!(errors.len(), 1);
        assert!(
            matches!(&errors[0], ValidationError::ColumnNotFound { column, .. } if column == "emial")
        );
    }

    #[test]
    fn test_error_display() {
        let err = ValidationError::TableNotFound {
            table: "usrs".to_string(),
            suggestion: Some("users".to_string()),
        };
        assert_eq!(
            err.to_string(),
            "Table 'usrs' not found. Did you mean 'users'?"
        );

        let err = ValidationError::ColumnNotFound {
            table: "users".to_string(),
            column: "emial".to_string(),
            suggestion: Some("email".to_string()),
        };
        assert_eq!(
            err.to_string(),
            "Column 'emial' not found in table 'users'. Did you mean 'email'?"
        );
    }
}
