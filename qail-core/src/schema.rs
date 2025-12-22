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
