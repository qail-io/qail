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

/// A column definition.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default: Option<String>,
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
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
            nullable: true,
            primary_key: false,
            unique: false,
            default: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn default(mut self, val: impl Into<String>) -> Self {
        self.default = Some(val.into());
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
            
            output.push_str(&format!("  {} {}{}\n", col.name, col.data_type, constraint_str));
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
            .column(Column::new("id", "serial").primary_key())
            .column(Column::new("name", "text").not_null())
            .column(Column::new("email", "text").unique());
        
        schema.add_table(users);
        schema.add_index(Index::new("idx_users_email", "users", vec!["email".into()]).unique());
        
        let output = to_qail_string(&schema);
        assert!(output.contains("table users"));
        assert!(output.contains("id serial primary_key"));
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
}
