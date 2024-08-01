//! Schema definitions for QAIL validation.
//!
//! Provides types for representing database schemas and loading them from JSON/TOML.
//!
//! # Example
//! ```
//! use qail_core::schema::Schema;
//! 
//! let json = r#"{
//!     "tables": [{
//!         "name": "users",
//!         "columns": [
//!             { "name": "id", "typ": "uuid", "nullable": false },
//!             { "name": "email", "typ": "varchar", "nullable": false }
//!         ]
//!     }]
//! }"#;
//! 
//! let schema: Schema = serde_json::from_str(json).unwrap();
//! let validator = schema.to_validator();
//! ```

use serde::{Deserialize, Serialize};
use crate::validator::Validator;

/// Database schema definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub tables: Vec<TableDef>,
}

/// Table definition with columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

/// Column definition with type information.
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
    /// Create an empty schema.
    pub fn new() -> Self {
        Self { tables: Vec::new() }
    }

    /// Add a table to the schema.
    pub fn add_table(&mut self, table: TableDef) {
        self.tables.push(table);
    }

    /// Convert schema to a Validator for query validation.
    pub fn to_validator(&self) -> Validator {
        let mut v = Validator::new();
        for table in &self.tables {
            let cols: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
            v.add_table(&table.name, &cols);
        }
        v
    }

    /// Load schema from JSON string.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Load schema from QAIL schema format (schema.qail).
    /// 
    /// Parses text like:
    /// ```text
    /// table users (
    ///     id string not null,
    ///     email string not null,
    ///     created_at date
    /// )
    /// ```
    pub fn from_qail_schema(input: &str) -> Result<Self, String> {
        let mut schema = Schema::new();
        let mut current_table: Option<TableDef> = None;
        
        for line in input.lines() {
            let line = line.trim();
            
            // Skip empty lines and comments
            if line.is_empty() || line.starts_with("--") {
                continue;
            }
            
            // Match "table tablename ("
            if line.starts_with("table ") {
                // Save previous table if any
                if let Some(t) = current_table.take() {
                    schema.tables.push(t);
                }
                
                // Parse table name: "table users (" -> "users"
                let rest = &line[6..]; // Skip "table "
                let name = rest.split('(').next()
                    .map(|s| s.trim())
                    .ok_or_else(|| format!("Invalid table line: {}", line))?;
                
                current_table = Some(TableDef::new(name));
            }
            // Match closing paren
            else if line == ")" {
                if let Some(t) = current_table.take() {
                    schema.tables.push(t);
                }
            }
            // Match column definition: "name type [not null],"
            else if let Some(ref mut table) = current_table {
                // Remove trailing comma
                let line = line.trim_end_matches(',');
                
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    let col_name = parts[0];
                    let col_type = parts[1];
                    let not_null = parts.len() > 2 && 
                        parts.iter().any(|&p| p.eq_ignore_ascii_case("not")) &&
                        parts.iter().any(|&p| p.eq_ignore_ascii_case("null"));
                    
                    table.columns.push(ColumnDef {
                        name: col_name.to_string(),
                        typ: col_type.to_string(),
                        nullable: !not_null,
                        primary_key: false,
                    });
                }
            }
        }
        
        // Don't forget the last table
        if let Some(t) = current_table {
            schema.tables.push(t);
        }
        
        Ok(schema)
    }

    /// Load schema from file path (auto-detects format).
    pub fn from_file(path: &std::path::Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
        
        // Detect format: .json -> JSON, else -> QAIL schema
        if path.extension().map(|e| e == "json").unwrap_or(false) {
            Self::from_json(&content).map_err(|e| e.to_string())
        } else {
            Self::from_qail_schema(&content)
        }
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

impl TableDef {
    /// Create a new table definition.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            columns: Vec::new(),
        }
    }

    /// Add a column to the table.
    pub fn add_column(&mut self, col: ColumnDef) {
        self.columns.push(col);
    }

    /// Builder: add a simple column.
    pub fn column(mut self, name: &str, typ: &str) -> Self {
        self.columns.push(ColumnDef {
            name: name.to_string(),
            typ: typ.to_string(),
            nullable: true,
            primary_key: false,
        });
        self
    }

    /// Builder: add a primary key column.
    pub fn pk(mut self, name: &str, typ: &str) -> Self {
        self.columns.push(ColumnDef {
            name: name.to_string(),
            typ: typ.to_string(),
            nullable: false,
            primary_key: true,
        });
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_from_json() {
        let json = r#"{
            "tables": [{
                "name": "users",
                "columns": [
                    { "name": "id", "type": "uuid", "nullable": false, "primary_key": true },
                    { "name": "email", "type": "varchar", "nullable": false }
                ]
            }]
        }"#;

        let schema = Schema::from_json(json).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "users");
        assert_eq!(schema.tables[0].columns.len(), 2);
    }

    #[test]
    fn test_schema_to_validator() {
        let schema = Schema {
            tables: vec![
                TableDef::new("users").pk("id", "uuid").column("email", "varchar"),
            ],
        };

        let validator = schema.to_validator();
        assert!(validator.validate_table("users").is_ok());
        assert!(validator.validate_column("users", "id").is_ok());
        assert!(validator.validate_column("users", "email").is_ok());
    }

    #[test]
    fn test_table_builder() {
        let table = TableDef::new("orders")
            .pk("id", "uuid")
            .column("total", "decimal")
            .column("status", "varchar");

        assert_eq!(table.columns.len(), 3);
        assert!(table.columns[0].primary_key);
    }
}
