//! Schema module for QAIL Gateway
//!
//! Loads table schemas from `.qail` files (native) or YAML (backward compat).
//! Provides schema metadata to the router for auto-REST route generation.

use crate::error::GatewayError;
use qail_core::ast::{Action, Qail};
use qail_core::migrate::{self, Column as QailColumn, Table as QailTable};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;

// ============================================================================
// Table metadata for gateway (derived from .qail schema)
// ============================================================================

/// Column metadata exposed to the gateway router
#[derive(Debug, Clone, Serialize)]
pub struct GatewayColumn {
    /// Column name.
    pub name: String,
    /// Qail-level type (e.g. `"string"`, `"int"`).
    pub col_type: String,
    /// PostgreSQL native type (e.g. `"text"`, `"int4"`).
    pub pg_type: String,
    /// Whether the column accepts NULL values.
    pub nullable: bool,
    /// Whether this column is (part of) the primary key.
    pub primary_key: bool,
    /// Whether the column has a UNIQUE constraint.
    pub unique: bool,
    /// Whether the column has a server-side DEFAULT.
    pub has_default: bool,
    /// Foreign key reference, if any.
    pub foreign_key: Option<GatewayForeignKey>,
}

/// Foreign key reference
#[derive(Debug, Clone, Serialize)]
pub struct GatewayForeignKey {
    /// Referenced table name.
    pub ref_table: String,
    /// Referenced column name.
    pub ref_column: String,
}

/// Table metadata for auto-REST generation
#[derive(Debug, Clone, Serialize)]
pub struct GatewayTable {
    /// Table name.
    pub name: String,
    /// Ordered list of column definitions.
    pub columns: Vec<GatewayColumn>,
    /// Name of the primary key column (if single-column PK)
    pub primary_key: Option<String>,
}

impl GatewayTable {
    /// Get column names suitable for SELECT
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Get columns that can be set by the user (no auto-generated PKs, no defaults-only)
    pub fn insertable_columns(&self) -> Vec<&GatewayColumn> {
        self.columns
            .iter()
            .filter(|c| {
                // Skip serial/bigserial PKs — auto-generated
                if c.primary_key {
                    let t = c.pg_type.to_uppercase();
                    if t == "SERIAL" || t == "BIGSERIAL" {
                        return false;
                    }
                }
                true
            })
            .collect()
    }

    /// Get columns that are required (NOT NULL, no default)
    pub fn required_columns(&self) -> Vec<&GatewayColumn> {
        self.columns
            .iter()
            .filter(|c| !c.nullable && !c.has_default && !c.primary_key)
            .collect()
    }

    /// Get all foreign keys from this table → other tables
    pub fn foreign_keys(&self) -> Vec<(&str, &GatewayForeignKey)> {
        self.columns
            .iter()
            .filter_map(|c| c.foreign_key.as_ref().map(|fk| (c.name.as_str(), fk)))
            .collect()
    }
}

// ============================================================================
// Schema registry
// ============================================================================

/// Schema registry — the gateway's knowledge of the database schema
#[derive(Debug, Default)]
pub struct SchemaRegistry {
    tables: HashMap<String, GatewayTable>,
}

impl SchemaRegistry {
    /// Create an empty schema registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Load schema from a `.qail` file (native format)
    pub fn load_from_qail_file(&mut self, path: &str) -> Result<(), GatewayError> {
        let content = fs::read_to_string(path)
            .map_err(|e| GatewayError::Schema(format!("Failed to read schema: {}", e)))?;

        self.load_from_qail_str(&content)
    }

    /// Load schema from a `.qail` string
    pub fn load_from_qail_str(&mut self, content: &str) -> Result<(), GatewayError> {
        let qail_schema = migrate::parse_qail(content)
            .map_err(|e| GatewayError::Schema(format!("Failed to parse .qail schema: {}", e)))?;

        for (name, table) in &qail_schema.tables {
            let gateway_table = convert_table(table);
            tracing::debug!(
                "Loaded table: {} ({} columns, pk={:?})",
                name,
                gateway_table.columns.len(),
                gateway_table.primary_key
            );
            self.tables.insert(name.clone(), gateway_table);
        }

        tracing::info!(
            "Loaded {} tables from .qail schema",
            qail_schema.tables.len()
        );
        Ok(())
    }

    /// Load schema from YAML file (backward compatibility)
    pub fn load_from_yaml_file(&mut self, path: &str) -> Result<(), GatewayError> {
        let content = fs::read_to_string(path)
            .map_err(|e| GatewayError::Schema(format!("Failed to read schema: {}", e)))?;

        let config: YamlSchemaConfig = serde_yaml::from_str(&content)
            .map_err(|e| GatewayError::Schema(format!("Failed to parse schema: {}", e)))?;

        for table in config.tables {
            let pk = table
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone());

            let gateway_table = GatewayTable {
                name: table.name.clone(),
                columns: table
                    .columns
                    .into_iter()
                    .map(|c| GatewayColumn {
                        pg_type: c.col_type.to_uppercase(),
                        col_type: c.col_type.clone(),
                        name: c.name,
                        nullable: c.nullable,
                        primary_key: c.primary_key,
                        unique: false,
                        has_default: false,
                        foreign_key: None,
                    })
                    .collect(),
                primary_key: pk,
            };

            tracing::debug!("Loaded table: {}", table.name);
            self.tables.insert(table.name, gateway_table);
        }

        tracing::info!("Loaded {} table schemas from YAML", self.tables.len());
        Ok(())
    }

    /// Load from a file, auto-detecting format by extension
    pub fn load_from_file(&mut self, path: &str) -> Result<(), GatewayError> {
        if path.ends_with(".qail") {
            self.load_from_qail_file(path)
        } else if path.ends_with(".yaml") || path.ends_with(".yml") {
            self.load_from_yaml_file(path)
        } else {
            Err(GatewayError::Schema(format!(
                "Unknown schema format: {}. Use .qail or .yaml",
                path
            )))
        }
    }

    // -- Accessors --

    /// Look up a table by name.
    pub fn table(&self, name: &str) -> Option<&GatewayTable> {
        self.tables.get(name)
    }

    /// Return all registered table names.
    pub fn table_names(&self) -> Vec<&str> {
        self.tables.keys().map(|s| s.as_str()).collect()
    }

    /// Return a reference to all tables.
    pub fn tables(&self) -> &HashMap<String, GatewayTable> {
        &self.tables
    }

    /// Returns `true` if a table with the given name is registered.
    pub fn table_exists(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    // -- Relations --

    /// Discover one-to-many: which tables have FKs pointing to `parent_table`?
    ///
    /// Returns `Vec<(child_table, child_fk_column, parent_pk_column)>`.
    ///
    /// # Arguments
    ///
    /// * `parent_table` — Name of the parent table to find children of.
    pub fn children_of(&self, parent_table: &str) -> Vec<(&str, &str, &str)> {
        let mut children = Vec::new();
        for (table_name, table) in &self.tables {
            for col in &table.columns {
                if let Some(ref fk) = col.foreign_key {
                    if fk.ref_table == parent_table {
                        children.push((
                            table_name.as_str(),
                            col.name.as_str(),
                            fk.ref_column.as_str(),
                        ));
                    }
                }
            }
        }
        children
    }

    /// Get the FK relation from `child_table` → `parent_table`.
    ///
    /// Returns `Some((child_fk_col, parent_pk_col))`.
    ///
    /// # Arguments
    ///
    /// * `child_table` — Table holding the foreign key.
    /// * `parent_table` — Table being referenced.
    pub fn relation_for(&self, child_table: &str, parent_table: &str) -> Option<(&str, &str)> {
        let table = self.tables.get(child_table)?;
        for col in &table.columns {
            if let Some(ref fk) = col.foreign_key {
                if fk.ref_table == parent_table {
                    return Some((col.name.as_str(), fk.ref_column.as_str()));
                }
            }
        }
        None
    }

    /// Discover parents: which tables does `child_table` reference via FK?
    ///
    /// Returns `Vec<(parent_table, child_fk_col, parent_pk_col)>`.
    ///
    /// # Arguments
    ///
    /// * `child_table` — Name of the child table to find parents of.
    pub fn parents_of(&self, child_table: &str) -> Vec<(&str, &str, &str)> {
        let table = match self.tables.get(child_table) {
            Some(t) => t,
            None => return Vec::new(),
        };
        table
            .columns
            .iter()
            .filter_map(|col| {
                col.foreign_key.as_ref().map(|fk| {
                    (
                        fk.ref_table.as_str(),
                        col.name.as_str(),
                        fk.ref_column.as_str(),
                    )
                })
            })
            .collect()
    }

    /// Validate a Qail AST command against the schema
    pub fn validate(&self, cmd: &Qail) -> Result<(), GatewayError> {
        if self.tables.is_empty() {
            return Ok(());
        }

        match cmd.action {
            Action::Make
            | Action::Drop
            | Action::Alter
            | Action::TxnStart
            | Action::TxnCommit
            | Action::TxnRollback
            | Action::Listen
            | Action::Unlisten
            | Action::Notify => {
                return Ok(());
            }
            _ => {}
        }

        if !self.table_exists(&cmd.table) {
            return Err(GatewayError::InvalidQuery(format!(
                "Table '{}' not found in schema",
                cmd.table
            )));
        }

        if let Some(table) = self.table(&cmd.table) {
            let valid_columns: HashSet<&str> =
                table.columns.iter().map(|c| c.name.as_str()).collect();

            for col_expr in &cmd.columns {
                if let qail_core::ast::Expr::Named(col_name) = col_expr {
                    if col_name != "*" && !valid_columns.contains(col_name.as_str()) {
                        return Err(GatewayError::InvalidQuery(format!(
                            "Column '{}' not found in table '{}'",
                            col_name, cmd.table
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}

// ============================================================================
// Conversion from qail-core types
// ============================================================================

fn convert_table(table: &QailTable) -> GatewayTable {
    let columns: Vec<GatewayColumn> = table.columns.iter().map(convert_column).collect();
    let primary_key = columns
        .iter()
        .find(|c| c.primary_key)
        .map(|c| c.name.clone());

    GatewayTable {
        name: table.name.clone(),
        columns,
        primary_key,
    }
}

fn convert_column(col: &QailColumn) -> GatewayColumn {
    GatewayColumn {
        name: col.name.clone(),
        col_type: col.data_type.name().to_string(),
        pg_type: col.data_type.to_pg_type(),
        nullable: col.nullable,
        primary_key: col.primary_key,
        unique: col.unique,
        has_default: col.default.is_some() || col.generated.is_some(),
        foreign_key: col.foreign_key.as_ref().map(|fk| GatewayForeignKey {
            ref_table: fk.table.clone(),
            ref_column: fk.column.clone(),
        }),
    }
}

// ============================================================================
// YAML backward compatibility types
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct YamlColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub col_type: String,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub primary_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct YamlTableSchema {
    pub name: String,
    pub columns: Vec<YamlColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct YamlSchemaConfig {
    pub tables: Vec<YamlTableSchema>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qail_schema_loading() {
        let schema_str = r#"
table users {
    id uuid primary_key default gen_random_uuid()
    email text not_null unique
    name text nullable
    created_at timestamptz default now()
}

table orders {
    id uuid primary_key default gen_random_uuid()
    user_id uuid not_null references users(id)
    total decimal not_null
    status text not_null default 'pending'
    created_at timestamptz default now()
}
        "#;

        let mut registry = SchemaRegistry::new();
        registry.load_from_qail_str(schema_str).unwrap();

        // Check tables loaded
        assert!(registry.table_exists("users"));
        assert!(registry.table_exists("orders"));
        assert!(!registry.table_exists("nonexistent"));

        // Check users table
        let users = registry.table("users").unwrap();
        assert_eq!(users.primary_key, Some("id".to_string()));
        assert_eq!(users.columns.len(), 4);

        let email = users.columns.iter().find(|c| c.name == "email").unwrap();
        assert!(!email.nullable);
        assert!(email.unique);
        assert_eq!(email.pg_type, "TEXT");

        // Check orders table FK
        let orders = registry.table("orders").unwrap();
        let user_id = orders
            .columns
            .iter()
            .find(|c| c.name == "user_id")
            .unwrap();
        assert!(user_id.foreign_key.is_some());
        let fk = user_id.foreign_key.as_ref().unwrap();
        assert_eq!(fk.ref_table, "users");
        assert_eq!(fk.ref_column, "id");

        // Check insertable columns (should skip auto-generated PKs)
        let insertable = users.insertable_columns();
        assert!(insertable.iter().all(|c| c.name != "id" || c.pg_type != "SERIAL"));

        // Check FKs
        let fks = orders.foreign_keys();
        assert_eq!(fks.len(), 1);
        assert_eq!(fks[0].0, "user_id");
    }

    #[test]
    fn test_schema_validation() {
        let mut registry = SchemaRegistry::new();
        registry
            .load_from_qail_str(
                r#"
table users {
    id uuid primary_key
    name text
}
        "#,
            )
            .unwrap();

        let cmd = Qail::get("users").columns(["id", "name"]);
        assert!(registry.validate(&cmd).is_ok());

        let cmd = Qail::get("users").columns(["id", "invalid_col"]);
        assert!(registry.validate(&cmd).is_err());

        let cmd = Qail::get("nonexistent");
        assert!(registry.validate(&cmd).is_err());
    }
}
