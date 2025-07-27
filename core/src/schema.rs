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

use crate::validator::Validator;
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
    /// Parses text like:
    /// ```text
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
            if let Some(rest) = line.strip_prefix("table ") {
                // Save previous table if any
                if let Some(t) = current_table.take() {
                    schema.tables.push(t);
                }

                // Skip "table "
                let name = rest
                    .split('(')
                    .next()
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
                    let not_null = parts.len() > 2
                        && parts.iter().any(|&p| p.eq_ignore_ascii_case("not"))
                        && parts.iter().any(|&p| p.eq_ignore_ascii_case("null"));

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
                TableDef::new("users")
                    .pk("id", "uuid")
                    .column("email", "varchar"),
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
    
    // =========================================================================
    // First-Class Relations Tests
    // =========================================================================
    
    #[test]
    fn test_build_schema_parses_ref_syntax() {
        let schema_content = r#"
table users {
    id UUID primary_key
    email TEXT
}

table posts {
    id UUID primary_key
    user_id UUID ref:users.id
    title TEXT
}
"#;
        
        let schema = crate::build::Schema::parse(schema_content).unwrap();
        
        // Check tables exist
        assert!(schema.has_table("users"));
        assert!(schema.has_table("posts"));
        
        // Check foreign key was parsed
        let posts = schema.table("posts").unwrap();
        assert_eq!(posts.foreign_keys.len(), 1);
        
        let fk = &posts.foreign_keys[0];
        assert_eq!(fk.column, "user_id");
        assert_eq!(fk.ref_table, "users");
        assert_eq!(fk.ref_column, "id");
    }
    
    #[test]
    fn test_relation_registry_forward_lookup() {
        let mut registry = RelationRegistry::new();
        registry.register("posts", "user_id", "users", "id");
        
        // Forward lookup: posts -> users
        let result = registry.get("posts", "users");
        assert!(result.is_some());
        let (from_col, to_col) = result.unwrap();
        assert_eq!(from_col, "user_id");
        assert_eq!(to_col, "id");
    }
    
    #[test]
    fn test_relation_registry_from_build_schema() {
        let schema_content = r#"
table users {
    id UUID
}

table posts {
    user_id UUID ref:users.id
}

table comments {
    post_id UUID ref:posts.id
    user_id UUID ref:users.id
}
"#;
        
        let schema = crate::build::Schema::parse(schema_content).unwrap();
        let registry = RelationRegistry::from_build_schema(&schema);
        
        // Check posts -> users
        assert!(registry.get("posts", "users").is_some());
        
        // Check comments -> posts
        assert!(registry.get("comments", "posts").is_some());
        
        // Check comments -> users
        assert!(registry.get("comments", "users").is_some());
        
        // Check reverse lookups
        let referencing = registry.referencing("users");
        assert!(referencing.contains(&"posts"));
        assert!(referencing.contains(&"comments"));
    }
    
    #[test]
    fn test_join_on_produces_correct_ast() {
        use crate::Qail;
        
        // Setup: Register a relation manually
        {
            let mut reg = super::RUNTIME_RELATIONS.write().unwrap();
            reg.register("posts", "user_id", "users", "id");
        }
        
        // Test forward join: from users, join posts
        // This should find reverse: posts.user_id -> users.id
        let query = Qail::get("users").join_on("posts");
        
        assert_eq!(query.joins.len(), 1);
        let join = &query.joins[0];
        assert_eq!(join.table, "posts");
        
        // Verify join conditions
        let on = join.on.as_ref().expect("Should have ON conditions");
        assert_eq!(on.len(), 1);
        
        // Clean up
        {
            let mut reg = super::RUNTIME_RELATIONS.write().unwrap();
            *reg = RelationRegistry::new();
        }
    }
    
    #[test]
    fn test_join_on_optional_returns_self_when_no_relation() {
        use crate::Qail;
        
        // Clear registry
        {
            let mut reg = super::RUNTIME_RELATIONS.write().unwrap();
            *reg = RelationRegistry::new();
        }
        
        // join_on_optional should not panic, just return self unchanged
        let query = Qail::get("users").join_on_optional("nonexistent");
        assert!(query.joins.is_empty());
    }
}

use std::collections::HashMap;
use std::sync::RwLock;
use once_cell::sync::Lazy;

#[derive(Debug, Default)]
pub struct RelationRegistry {
    /// Forward lookups: (from_table, to_table) -> (from_col, to_col)
    forward: HashMap<(String, String), (String, String)>,
    /// Reverse lookups: to_table -> list of tables that reference it
    reverse: HashMap<String, Vec<String>>,
}

impl RelationRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Register a relation from schema.
    pub fn register(&mut self, from_table: &str, from_col: &str, to_table: &str, to_col: &str) {
        self.forward.insert(
            (from_table.to_string(), to_table.to_string()),
            (from_col.to_string(), to_col.to_string()),
        );
        
        self.reverse
            .entry(to_table.to_string())
            .or_default()
            .push(from_table.to_string());
    }
    
    /// Lookup join columns for a relation.
    /// Returns (from_col, to_col) if relation exists.
    pub fn get(&self, from_table: &str, to_table: &str) -> Option<(&str, &str)> {
        self.forward
            .get(&(from_table.to_string(), to_table.to_string()))
            .map(|(a, b)| (a.as_str(), b.as_str()))
    }
    
    /// Get all tables that reference this table (for reverse joins).
    pub fn referencing(&self, table: &str) -> Vec<&str> {
        self.reverse
            .get(table)
            .map(|v| v.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }
    
    /// Load relations from a parsed build::Schema.
    pub fn from_build_schema(schema: &crate::build::Schema) -> Self {
        let mut registry = Self::new();
        
        for table in schema.tables.values() {
            for fk in &table.foreign_keys {
                registry.register(
                    &table.name,
                    &fk.column,
                    &fk.ref_table,
                    &fk.ref_column,
                );
            }
        }
        
        registry
    }
}

/// Global mutable registry for runtime schema loading.
pub static RUNTIME_RELATIONS: Lazy<RwLock<RelationRegistry>> = 
    Lazy::new(|| RwLock::new(RelationRegistry::new()));

/// Load relations from a schema.qail file into the runtime registry.
/// Returns the number of relations loaded.
pub fn load_schema_relations(path: &str) -> Result<usize, String> {
    let schema = crate::build::Schema::parse_file(path)?;
    let mut registry = RUNTIME_RELATIONS.write().map_err(|e| format!("Lock error: {}", e))?;
    
    let mut count = 0;
    for table in schema.tables.values() {
        for fk in &table.foreign_keys {
            registry.register(&table.name, &fk.column, &fk.ref_table, &fk.ref_column);
            count += 1;
        }
    }
    
    Ok(count)
}

/// Lookup join info for implicit join.
/// Returns (from_col, to_col) if relation exists.
pub fn lookup_relation(from_table: &str, to_table: &str) -> Option<(String, String)> {
    let registry = RUNTIME_RELATIONS.read().ok()?;
    let (fc, tc) = registry.get(from_table, to_table)?;
    Some((fc.to_string(), tc.to_string()))
}
