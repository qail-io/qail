//! Schema validation module
//!
//! Loads table schemas and validates queries against them.

use crate::error::GatewayError;
use qail_core::ast::{Action, Qail};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;

/// Column definition in schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub primary_key: bool,
}

/// Table schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnDef>,
}

/// Schema configuration loaded from YAML
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaConfig {
    pub tables: Vec<TableSchema>,
}

/// Schema validator
#[derive(Debug, Default)]
pub struct SchemaValidator {
    tables: HashMap<String, TableSchema>,
}

impl SchemaValidator {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Load schema from YAML file
    pub fn load_from_file(&mut self, path: &str) -> Result<(), GatewayError> {
        let content = fs::read_to_string(path)
            .map_err(|e| GatewayError::Schema(format!("Failed to read schema: {}", e)))?;
        
        let config: SchemaConfig = serde_yaml::from_str(&content)
            .map_err(|e| GatewayError::Schema(format!("Failed to parse schema: {}", e)))?;
        
        for table in config.tables {
            tracing::debug!("Loaded schema for table: {}", table.name);
            self.tables.insert(table.name.clone(), table);
        }
        
        tracing::info!("Loaded {} table schemas from {}", self.tables.len(), path);
        Ok(())
    }
    
    pub fn add_table(&mut self, schema: TableSchema) {
        self.tables.insert(schema.name.clone(), schema);
    }
    
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
    
    pub fn get_columns(&self, table: &str) -> Option<Vec<&str>> {
        self.tables.get(table).map(|t| {
            t.columns.iter().map(|c| c.name.as_str()).collect()
        })
    }
    
    pub fn validate(&self, cmd: &Qail) -> Result<(), GatewayError> {
        if self.tables.is_empty() {
            return Ok(());
        }
        
        match cmd.action {
            Action::Make | Action::Drop | Action::Alter | 
            Action::TxnStart | Action::TxnCommit | Action::TxnRollback |
            Action::Listen | Action::Unlisten | Action::Notify => {
                return Ok(());
            }
            _ => {}
        }
        
        if !self.table_exists(&cmd.table) {
            return Err(GatewayError::InvalidQuery(format!(
                "Table '{}' not found in schema", cmd.table
            )));
        }
        
        if let Some(table_schema) = self.tables.get(&cmd.table) {
            let valid_columns: HashSet<&str> = table_schema.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            
            for col_expr in &cmd.columns {
                if let qail_core::ast::Expr::Named(col_name) = col_expr {
                    if col_name != "*" && !valid_columns.contains(col_name.as_str()) {
                        return Err(GatewayError::InvalidQuery(format!(
                            "Column '{}' not found in table '{}'", col_name, cmd.table
                        )));
                    }
                }
            }
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_schema_validation() {
        let mut validator = SchemaValidator::new();
        validator.add_table(TableSchema {
            name: "users".to_string(),
            columns: vec![
                ColumnDef { name: "id".to_string(), col_type: "int".to_string(), nullable: false, primary_key: true },
                ColumnDef { name: "name".to_string(), col_type: "text".to_string(), nullable: false, primary_key: false },
            ],
        });
        
        let cmd = Qail::get("users").columns(["id", "name"]);
        assert!(validator.validate(&cmd).is_ok());
        
        let cmd = Qail::get("users").columns(["id", "invalid_col"]);
        assert!(validator.validate(&cmd).is_err());
        
        let cmd = Qail::get("nonexistent");
        assert!(validator.validate(&cmd).is_err());
    }
}
