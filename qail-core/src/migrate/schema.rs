//! QAIL Schema Format (Native AST)
//!
//! Replaces JSON with a human-readable, intent-aware schema format.
//!
//! ```qail
//! table users {
//!   id serial primary_key
//!   name text not_null
//!   email text nullable unique
//! }
//!
//! unique index idx_users_email on users (email)
//!
//! rename users.username -> users.name
//! ```

use std::collections::HashMap;
use super::types::ColumnType;

/// A complete database schema.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub tables: HashMap<String, Table>,
    pub indexes: Vec<Index>,
    pub migrations: Vec<MigrationHint>,
}

/// A table definition.
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

/// A column definition with compile-time type safety.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<String>,
    pub foreign_key: Option<ForeignKey>,
}

/// Foreign key reference definition.
#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
    pub on_delete: FkAction,
    pub on_update: FkAction,
}

/// Foreign key action on DELETE/UPDATE.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum FkAction {
    #[default]
    NoAction,
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
}

/// An index definition.
#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

/// Migration hints (intent-aware).
#[derive(Debug, Clone)]
pub enum MigrationHint {
    /// Rename a column (not delete + add)
    Rename { from: String, to: String },
    /// Transform data with expression
    Transform { expression: String, target: String },
    /// Drop with confirmation
    Drop { target: String, confirmed: bool },
}

impl Schema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), table);
    }

    pub fn add_index(&mut self, index: Index) {
        self.indexes.push(index);
    }

    pub fn add_hint(&mut self, hint: MigrationHint) {
        self.migrations.push(hint);
    }
    
    /// Validate all foreign key references in the schema.
    /// 
    /// Returns a list of validation errors if any references are invalid.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        
        for table in self.tables.values() {
            for col in &table.columns {
                if let Some(ref fk) = col.foreign_key {
                    // Check referenced table exists
                    if !self.tables.contains_key(&fk.table) {
                        errors.push(format!(
                            "FK error: {}.{} references non-existent table '{}'",
                            table.name, col.name, fk.table
                        ));
                    } else {
                        // Check referenced column exists
                        let ref_table = &self.tables[&fk.table];
                        if !ref_table.columns.iter().any(|c| c.name == fk.column) {
                            errors.push(format!(
                                "FK error: {}.{} references non-existent column '{}.{}'",
                                table.name, col.name, fk.table, fk.column
                            ));
                        }
                    }
                }
            }
        }
        
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
        }
    }

    pub fn column(mut self, col: Column) -> Self {
        self.columns.push(col);
        self
    }
}

impl Column {
    /// Create a new column with compile-time type validation.
    pub fn new(name: impl Into<String>, data_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            primary_key: false,
            unique: false,
            default: None,
            foreign_key: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Set as primary key with compile-time validation.
    /// 
    /// Validates that the column type can be a primary key.
    /// Panics at runtime if type doesn't support PK (caught in tests).
    pub fn primary_key(mut self) -> Self {
        if !self.data_type.can_be_primary_key() {
            panic!(
                "Column '{}' of type {} cannot be a primary key. \
                 Valid PK types: UUID, SERIAL, BIGSERIAL, INT, BIGINT",
                self.name,
                self.data_type.name()
            );
        }
        self.primary_key = true;
        self.nullable = false;
        self
    }

    /// Set as unique with compile-time validation.
    /// 
    /// Validates that the column type supports indexing.
    pub fn unique(mut self) -> Self {
        if !self.data_type.supports_indexing() {
            panic!(
                "Column '{}' of type {} cannot have UNIQUE constraint. \
                 JSONB and BYTEA types do not support standard indexing.",
                self.name,
                self.data_type.name()
            );
        }
        self.unique = true;
        self
    }

    pub fn default(mut self, val: impl Into<String>) -> Self {
        self.default = Some(val.into());
        self
    }
    
    /// Add a foreign key reference to another table.
    /// 
    /// # Example
    /// ```
    /// Column::new("user_id", ColumnType::Uuid)
    ///     .references("users", "id")
    ///     .on_delete(FkAction::Cascade)
    /// ```
    pub fn references(mut self, table: &str, column: &str) -> Self {
        self.foreign_key = Some(ForeignKey {
            table: table.to_string(),
            column: column.to_string(),
            on_delete: FkAction::default(),
            on_update: FkAction::default(),
        });
        self
    }
    
    /// Set the ON DELETE action for the foreign key.
    pub fn on_delete(mut self, action: FkAction) -> Self {
        if let Some(ref mut fk) = self.foreign_key {
            fk.on_delete = action;
        }
        self
    }
    
    /// Set the ON UPDATE action for the foreign key.
    pub fn on_update(mut self, action: FkAction) -> Self {
        if let Some(ref mut fk) = self.foreign_key {
            fk.on_update = action;
        }
        self
    }
}

impl Index {
    pub fn new(name: impl Into<String>, table: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            columns,
            unique: false,
        }
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// Format a Schema to .qail format string.
pub fn to_qail_string(schema: &Schema) -> String {
    let mut output = String::new();
    output.push_str("# QAIL Schema\n\n");

    for table in schema.tables.values() {
        output.push_str(&format!("table {} {{\n", table.name));
        for col in &table.columns {
            let mut constraints: Vec<String> = Vec::new();
            if col.primary_key {
                constraints.push("primary_key".to_string());
            }
            if !col.nullable && !col.primary_key {
                constraints.push("not_null".to_string());
            }
            if col.unique {
                constraints.push("unique".to_string());
            }
            if let Some(def) = &col.default {
                constraints.push(format!("default {}", def));
            }
            
            let constraint_str = if constraints.is_empty() {
                String::new()
            } else {
                format!(" {}", constraints.join(" "))
            };
            
            output.push_str(&format!("  {} {}{}\n", col.name, col.data_type.to_pg_type(), constraint_str));
        }
        output.push_str("}\n\n");
    }

    for idx in &schema.indexes {
        let unique = if idx.unique { "unique " } else { "" };
        output.push_str(&format!(
            "{}index {} on {} ({})\n",
            unique,
            idx.name,
            idx.table,
            idx.columns.join(", ")
        ));
    }

    for hint in &schema.migrations {
        match hint {
            MigrationHint::Rename { from, to } => {
                output.push_str(&format!("rename {} -> {}\n", from, to));
            }
            MigrationHint::Transform { expression, target } => {
                output.push_str(&format!("transform {} -> {}\n", expression, target));
            }
            MigrationHint::Drop { target, confirmed } => {
                let confirm = if *confirmed { " confirm" } else { "" };
                output.push_str(&format!("drop {}{}\n", target, confirm));
            }
        }
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_builder() {
        let mut schema = Schema::new();
        
        let users = Table::new("users")
            .column(Column::new("id", ColumnType::Serial).primary_key())
            .column(Column::new("name", ColumnType::Text).not_null())
            .column(Column::new("email", ColumnType::Text).unique());
        
        schema.add_table(users);
        schema.add_index(Index::new("idx_users_email", "users", vec!["email".into()]).unique());
        
        let output = to_qail_string(&schema);
        assert!(output.contains("table users"));
        assert!(output.contains("id SERIAL primary_key"));
        assert!(output.contains("unique index idx_users_email"));
    }

    #[test]
    fn test_migration_hints() {
        let mut schema = Schema::new();
        schema.add_hint(MigrationHint::Rename {
            from: "users.username".into(),
            to: "users.name".into(),
        });
        
        let output = to_qail_string(&schema);
        assert!(output.contains("rename users.username -> users.name"));
    }
    
    #[test]
    #[should_panic(expected = "cannot be a primary key")]
    fn test_invalid_primary_key_type() {
        // TEXT cannot be a primary key
        Column::new("data", ColumnType::Text).primary_key();
    }
    
    #[test]
    #[should_panic(expected = "cannot have UNIQUE")]
    fn test_invalid_unique_type() {
        // JSONB cannot have standard unique index
        Column::new("data", ColumnType::Jsonb).unique();
    }
    
    #[test]
    fn test_foreign_key_valid() {
        let mut schema = Schema::new();
        
        // Add users table first
        schema.add_table(Table::new("users")
            .column(Column::new("id", ColumnType::Uuid).primary_key()));
        
        // Add posts with valid FK to users
        schema.add_table(Table::new("posts")
            .column(Column::new("id", ColumnType::Uuid).primary_key())
            .column(Column::new("user_id", ColumnType::Uuid)
                .references("users", "id")
                .on_delete(FkAction::Cascade)));
        
        // Should pass validation
        assert!(schema.validate().is_ok());
    }
    
    #[test]
    fn test_foreign_key_invalid_table() {
        let mut schema = Schema::new();
        
        // Add posts with FK to non-existent table
        schema.add_table(Table::new("posts")
            .column(Column::new("id", ColumnType::Uuid).primary_key())
            .column(Column::new("user_id", ColumnType::Uuid)
                .references("nonexistent", "id")));
        
        // Should fail validation
        let result = schema.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err()[0].contains("non-existent table"));
    }
    
    #[test]
    fn test_foreign_key_invalid_column() {
        let mut schema = Schema::new();
        
        // Add users table
        schema.add_table(Table::new("users")
            .column(Column::new("id", ColumnType::Uuid).primary_key()));
        
        // Add posts with FK to non-existent column
        schema.add_table(Table::new("posts")
            .column(Column::new("id", ColumnType::Uuid).primary_key())
            .column(Column::new("user_id", ColumnType::Uuid)
                .references("users", "wrong_column")));
        
        // Should fail validation
        let result = schema.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err()[0].contains("non-existent column"));
    }
}
