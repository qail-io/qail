use std::collections::HashMap;
use std::fs;
use std::path::Path;

use qail_core::ast::Qail;
use qail_core::migrate;

use crate::error::GatewayError;

use super::convert::convert_table;
use super::validate::validate_cmd;
use super::yaml::YamlSchemaConfig;
use super::{GatewayColumn, GatewayTable};

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
        let content = qail_core::schema_source::read_qail_schema_source(path)
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
        if Path::new(path).is_dir() || path.ends_with(".qail") {
            self.load_from_qail_file(path)
        } else if path.ends_with(".yaml") || path.ends_with(".yml") {
            self.load_from_yaml_file(path)
        } else {
            Err(GatewayError::Schema(format!(
                "Unknown schema format: {}. Use .qail, schema directory, or .yaml",
                path
            )))
        }
    }

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
                if let Some(ref fk) = col.foreign_key
                    && fk.ref_table == parent_table
                {
                    children.push((
                        table_name.as_str(),
                        col.name.as_str(),
                        fk.ref_column.as_str(),
                    ));
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
            if let Some(ref fk) = col.foreign_key
                && fk.ref_table == parent_table
            {
                return Some((col.name.as_str(), fk.ref_column.as_str()));
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
        validate_cmd(self, cmd)
    }
}
