//! Schema definitions for QAIL validation.
//!
//! Provides types for representing database schemas and loading them from QAIL schema sources.
//!
//! # Example
//! ```
//! use qail_core::schema::Schema;
//!
//! let qail = r#"
//! table users (
//!     id uuid not null,
//!     email varchar not null
//! )
//! "#;
//!
//! let schema = Schema::from_qail_schema(qail).unwrap();
//! let validator = schema.to_validator();
//! ```

use crate::validator::Validator;

fn strip_schema_comments(line: &str) -> &str {
    let line = line.split_once("--").map_or(line, |(left, _)| left);
    line.split_once('#').map_or(line, |(left, _)| left).trim()
}

/// A database schema comprising one or more table definitions.
#[derive(Debug, Clone)]
pub struct Schema {
    /// Table definitions.
    pub tables: Vec<TableDef>,
}

/// Definition of a single table.
#[derive(Debug, Clone)]
pub struct TableDef {
    /// Table name.
    pub name: String,
    /// Column definitions.
    pub columns: Vec<ColumnDef>,
}

/// Definition of a single column.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// SQL data type.
    pub typ: String,
    /// Whether the column accepts NULL.
    pub nullable: bool,
    /// Whether the column is a primary key.
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

        for raw_line in input.lines() {
            let line = strip_schema_comments(raw_line);

            // Skip empty lines and comments
            if line.is_empty() {
                continue;
            }

            // Match "table tablename ("
            if let Some(rest) = line.strip_prefix("table ") {
                // Save previous table if any
                if let Some(t) = current_table.take() {
                    schema.tables.push(t);
                }

                let name = rest
                    .trim()
                    .trim_end_matches('{')
                    .trim_end_matches('(')
                    .trim();
                if name.is_empty() {
                    return Err(format!("Invalid table line: {}", line));
                }

                current_table = Some(TableDef::new(name));
            }
            // Match closing paren
            else if matches!(line.trim_end_matches(';'), ")" | "}") {
                if let Some(t) = current_table.take() {
                    schema.tables.push(t);
                }
            }
            // Match column definition: "name type [not null],"
            else if let Some(ref mut table) = current_table {
                // Remove trailing comma
                let line = line.trim_end_matches(',');

                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() < 2 {
                    return Err(format!(
                        "Invalid column line in table '{}': {}",
                        table.name, line
                    ));
                }
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

        // Don't forget the last table
        if let Some(t) = current_table {
            schema.tables.push(t);
        }

        Ok(schema)
    }

    /// Load schema from QAIL schema source path (file or modular directory).
    pub fn from_file(path: &std::path::Path) -> Result<Self, String> {
        let content = crate::schema_source::read_qail_schema_source(path)?;
        Self::from_qail_schema(&content)
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
    use std::sync::Mutex;

    static RELATION_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn test_schema_from_qail_schema() {
        let qail = r#"
table users (
    id uuid not null,
    email varchar not null
)
"#;

        let schema = Schema::from_qail_schema(qail).unwrap();
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
        let _guard = RELATION_TEST_LOCK.lock().expect("relation test lock");

        // Setup: Register a relation manually
        {
            let mut reg = super::RUNTIME_RELATIONS.write().unwrap();
            reg.register("posts", "user_id", "users", "id");
        }

        // Test forward join: from users, join posts
        // This should find reverse: posts.user_id -> users.id
        let query = Qail::get("users")
            .join_on("posts")
            .expect("relation should join");

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
        let _guard = RELATION_TEST_LOCK.lock().expect("relation test lock");

        // Clear registry
        {
            let mut reg = super::RUNTIME_RELATIONS.write().unwrap();
            *reg = RelationRegistry::new();
        }

        // join_on_optional should not panic, just return self unchanged
        let query = Qail::get("users").join_on_optional("nonexistent");
        assert!(query.joins.is_empty());
    }

    #[test]
    fn test_join_on_returns_error_on_ambiguous_relation() {
        use crate::Qail;
        let _guard = RELATION_TEST_LOCK.lock().expect("relation test lock");

        {
            let mut reg = super::RUNTIME_RELATIONS.write().unwrap();
            *reg = RelationRegistry::new();
            reg.register("invoices", "buyer_id", "users", "id");
            reg.register("invoices", "seller_id", "users", "id");
        }

        let err = Qail::get("invoices")
            .join_on("users")
            .expect_err("ambiguous relation should error");
        assert!(err.to_string().contains("Ambiguous relation"));

        {
            let mut reg = super::RUNTIME_RELATIONS.write().unwrap();
            *reg = RelationRegistry::new();
        }
    }

    #[test]
    fn test_join_on_optional_returns_self_on_ambiguous_relation() {
        use crate::Qail;
        let _guard = RELATION_TEST_LOCK.lock().expect("relation test lock");

        {
            let mut reg = super::RUNTIME_RELATIONS.write().unwrap();
            *reg = RelationRegistry::new();
            reg.register("invoices", "buyer_id", "users", "id");
            reg.register("invoices", "seller_id", "users", "id");
        }

        let query = Qail::get("invoices").join_on_optional("users");
        assert!(query.joins.is_empty());

        {
            let mut reg = super::RUNTIME_RELATIONS.write().unwrap();
            *reg = RelationRegistry::new();
        }
    }

    #[test]
    fn test_join_on_returns_error_when_no_relation() {
        use crate::Qail;
        let _guard = RELATION_TEST_LOCK.lock().expect("relation test lock");

        {
            let mut reg = super::RUNTIME_RELATIONS.write().unwrap();
            *reg = RelationRegistry::new();
        }

        let err = Qail::get("users")
            .join_on("nonexistent")
            .expect_err("missing relation should error");
        assert!(err.to_string().contains("No relation found"));
    }

    #[test]
    fn test_from_qail_schema_supports_brace_table_blocks() {
        let qail = r#"
table users {
    id uuid not null
    email varchar
}
"#;
        let schema = Schema::from_qail_schema(qail).expect("brace-style schema should parse");
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "users");
        assert_eq!(schema.tables[0].columns.len(), 2);
    }

    #[test]
    fn test_from_qail_schema_errors_on_malformed_column_line() {
        let qail = r#"
table users (
    id uuid not null,
    email,
)
"#;
        let err = Schema::from_qail_schema(qail).expect_err("malformed column should error");
        assert!(err.contains("Invalid column line"));
        assert!(err.contains("users"));
    }

    #[test]
    fn test_from_qail_schema_ignores_hash_and_inline_comments() {
        let qail = r#"
# top-level comment
table users { -- inline table comment
    id uuid not null, # id comment
    # line comment inside table
    email varchar -- email comment
}
"#;
        let schema = Schema::from_qail_schema(qail).expect("schema with comments should parse");
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "users");
        assert_eq!(schema.tables[0].columns.len(), 2);
        assert_eq!(schema.tables[0].columns[0].name, "id");
        assert_eq!(schema.tables[0].columns[1].name, "email");
    }

    #[test]
    fn test_replace_schema_relations_replaces_registry_state() {
        use std::fs;
        let _guard = RELATION_TEST_LOCK.lock().expect("relation test lock");

        // Ensure clean global state for this test.
        {
            let mut reg = super::RUNTIME_RELATIONS.write().expect("registry lock");
            *reg = RelationRegistry::new();
        }

        let base = std::env::temp_dir().join(format!(
            "qail_schema_relations_reload_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&base).expect("mkdir temp");

        let schema_with_fk = base.join("schema_with_fk.qail");
        fs::write(
            &schema_with_fk,
            r#"
table users {
    id UUID primary_key
}
table posts {
    id UUID primary_key
    user_id UUID ref:users.id
}
"#,
        )
        .expect("write schema 1");

        let schema_without_fk = base.join("schema_without_fk.qail");
        fs::write(
            &schema_without_fk,
            r#"
table users {
    id UUID primary_key
}
table posts {
    id UUID primary_key
}
"#,
        )
        .expect("write schema 2");

        let count1 = replace_schema_relations(schema_with_fk.to_str().expect("path utf8")).unwrap();
        assert_eq!(count1, 1);
        assert!(lookup_relation("posts", "users").is_some());

        let count2 =
            replace_schema_relations(schema_without_fk.to_str().expect("path utf8")).unwrap();
        assert_eq!(count2, 0);
        assert!(lookup_relation("posts", "users").is_none());

        {
            let mut reg = super::RUNTIME_RELATIONS.write().expect("registry lock");
            *reg = RelationRegistry::new();
        }
        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn test_load_schema_relations_merges_registry_state() {
        use std::fs;
        let _guard = RELATION_TEST_LOCK.lock().expect("relation test lock");

        {
            let mut reg = super::RUNTIME_RELATIONS.write().expect("registry lock");
            *reg = RelationRegistry::new();
        }

        let base = std::env::temp_dir().join(format!(
            "qail_schema_relations_merge_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&base).expect("mkdir temp");

        let schema_with_fk = base.join("schema_with_fk.qail");
        fs::write(
            &schema_with_fk,
            r#"
table users {
    id UUID primary_key
}
table posts {
    id UUID primary_key
    user_id UUID ref:users.id
}
"#,
        )
        .expect("write schema 1");

        let schema_without_fk = base.join("schema_without_fk.qail");
        fs::write(
            &schema_without_fk,
            r#"
table invoices {
    id UUID primary_key
    user_id UUID ref:users.id
}
"#,
        )
        .expect("write schema 2");

        let count1 = load_schema_relations(schema_with_fk.to_str().expect("path utf8")).unwrap();
        assert_eq!(count1, 1);
        assert!(lookup_relation("posts", "users").is_some());

        let count2 = load_schema_relations(schema_without_fk.to_str().expect("path utf8")).unwrap();
        assert_eq!(count2, 1);
        assert!(lookup_relation("posts", "users").is_some());
        assert!(lookup_relation("invoices", "users").is_some());

        {
            let mut reg = super::RUNTIME_RELATIONS.write().expect("registry lock");
            *reg = RelationRegistry::new();
        }
        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn test_merge_schema_relations_merges_registry_state() {
        use std::fs;
        let _guard = RELATION_TEST_LOCK.lock().expect("relation test lock");

        {
            let mut reg = super::RUNTIME_RELATIONS.write().expect("registry lock");
            *reg = RelationRegistry::new();
        }

        let base = std::env::temp_dir().join(format!(
            "qail_schema_relations_merge_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::create_dir_all(&base).expect("mkdir temp");

        let schema_with_fk = base.join("schema_with_fk.qail");
        fs::write(
            &schema_with_fk,
            r#"
table users {
    id UUID primary_key
}
table posts {
    id UUID primary_key
    user_id UUID ref:users.id
}
"#,
        )
        .expect("write schema 1");

        let schema_without_fk = base.join("schema_without_fk.qail");
        fs::write(
            &schema_without_fk,
            r#"
table invoices {
    id UUID primary_key
}
"#,
        )
        .expect("write schema 2");

        let count1 = merge_schema_relations(schema_with_fk.to_str().expect("path utf8")).unwrap();
        assert_eq!(count1, 1);
        assert!(lookup_relation("posts", "users").is_some());

        let count2 =
            merge_schema_relations(schema_without_fk.to_str().expect("path utf8")).unwrap();
        assert_eq!(count2, 0);
        assert!(lookup_relation("posts", "users").is_some());

        {
            let mut reg = super::RUNTIME_RELATIONS.write().expect("registry lock");
            *reg = RelationRegistry::new();
        }
        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn test_lookup_relation_state_errors_on_ambiguous_multi_fk_pair() {
        let _guard = RELATION_TEST_LOCK.lock().expect("relation test lock");
        let schema_content = r#"
table users {
    id UUID primary_key
}

table invoices {
    id UUID primary_key
    buyer_id UUID ref:users.id
    seller_id UUID ref:users.id
}
"#;

        let schema = crate::build::Schema::parse(schema_content).expect("schema parse");
        let registry = RelationRegistry::from_build_schema(&schema);
        {
            let mut reg = super::RUNTIME_RELATIONS.write().expect("registry lock");
            *reg = registry;
        }

        let err = lookup_relation_state("invoices", "users").expect_err("ambiguous relation");
        assert!(err.to_string().contains("Ambiguous relation"));

        {
            let mut reg = super::RUNTIME_RELATIONS.write().expect("registry lock");
            *reg = RelationRegistry::new();
        }
    }
}

use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::RwLock;

/// Registry of table foreign-key relationships for auto-join inference.
#[derive(Debug, Default)]
pub struct RelationRegistry {
    /// Forward lookups: (from_table, to_table) -> [(from_col, to_col), ...]
    forward: HashMap<(String, String), Vec<(String, String)>>,
    /// Reverse lookups: to_table -> list of tables that reference it
    reverse: HashMap<String, Vec<String>>,
}

impl RelationRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a foreign-key relation from schema.
    ///
    /// # Arguments
    ///
    /// * `from_table` — Source (referencing) table.
    /// * `from_col` — Foreign-key column in the source table.
    /// * `to_table` — Target (referenced) table.
    /// * `to_col` — Primary-key column in the target table.
    pub fn register(&mut self, from_table: &str, from_col: &str, to_table: &str, to_col: &str) {
        let entry = self
            .forward
            .entry((from_table.to_string(), to_table.to_string()))
            .or_default();
        let pair = (from_col.to_string(), to_col.to_string());
        if !entry.iter().any(|existing| existing == &pair) {
            entry.push(pair);
        }

        let entry = self.reverse.entry(to_table.to_string()).or_default();
        if !entry.iter().any(|existing| existing == from_table) {
            entry.push(from_table.to_string());
        }
    }

    /// Lookup join columns for a relation.
    ///
    /// Returns `(from_col, to_col)` if the relation exists.
    ///
    /// # Arguments
    ///
    /// * `from_table` — Source table name.
    /// * `to_table` — Target table name.
    pub fn get(&self, from_table: &str, to_table: &str) -> Option<(&str, &str)> {
        let options = self.get_all(from_table, to_table)?;
        if options.len() != 1 {
            return None;
        }
        let (a, b) = &options[0];
        Some((a.as_str(), b.as_str()))
    }

    /// Lookup all join-column candidates for a relation.
    pub fn get_all(&self, from_table: &str, to_table: &str) -> Option<&[(String, String)]> {
        self.forward
            .get(&(from_table.to_string(), to_table.to_string()))
            .map(|pairs| pairs.as_slice())
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
                registry.register(&table.name, &fk.column, &fk.ref_table, &fk.ref_column);
            }
        }

        registry
    }
}

/// Global mutable registry for runtime schema loading.
pub static RUNTIME_RELATIONS: LazyLock<RwLock<RelationRegistry>> =
    LazyLock::new(|| RwLock::new(RelationRegistry::new()));

/// Load relations from a schema.qail file into the runtime registry.
///
/// This function merges relations into the existing runtime relation state.
/// Returns the number of relations parsed from `path`.
pub fn load_schema_relations(path: &str) -> Result<usize, String> {
    merge_schema_relations(path)
}

/// Merge relations from a schema.qail file into the runtime registry.
///
/// Use this when multiple schema fragments are loaded incrementally and previously
/// registered relations should be retained.
pub fn merge_schema_relations(path: &str) -> Result<usize, String> {
    let schema = crate::build::Schema::parse_file(path)?;
    let count: usize = schema
        .tables
        .values()
        .map(|table| table.foreign_keys.len())
        .sum();
    let mut registry = RUNTIME_RELATIONS
        .write()
        .map_err(|e| format!("Lock error: {}", e))?;
    for table in schema.tables.values() {
        for fk in &table.foreign_keys {
            registry.register(&table.name, &fk.column, &fk.ref_table, &fk.ref_column);
        }
    }

    Ok(count)
}

/// Replace all runtime relations with relations loaded from a schema.qail file.
///
/// Use this for hot-reload workflows where runtime registry state should exactly
/// match a schema snapshot.
pub fn replace_schema_relations(path: &str) -> Result<usize, String> {
    let schema = crate::build::Schema::parse_file(path)?;
    let replacement = RelationRegistry::from_build_schema(&schema);
    let count: usize = schema
        .tables
        .values()
        .map(|table| table.foreign_keys.len())
        .sum();
    let mut registry = RUNTIME_RELATIONS
        .write()
        .map_err(|e| format!("Lock error: {}", e))?;
    *registry = replacement;

    Ok(count)
}

/// Lookup join info for implicit join.
/// Returns (from_col, to_col) if relation exists.
pub fn lookup_relation(from_table: &str, to_table: &str) -> Option<(String, String)> {
    lookup_relation_state(from_table, to_table).ok().flatten()
}

/// Lookup join info and return an explicit error when relation metadata is ambiguous.
pub fn lookup_relation_state(
    from_table: &str,
    to_table: &str,
) -> crate::error::QailBuildResult<Option<(String, String)>> {
    let registry = RUNTIME_RELATIONS
        .read()
        .map_err(|e| crate::error::QailBuildError::RelationRegistryLock(e.to_string()))?;
    let Some(options) = registry.get_all(from_table, to_table) else {
        return Ok(None);
    };

    if options.len() > 1 {
        return Err(crate::error::QailBuildError::AmbiguousRelation {
            from_table: from_table.to_string(),
            to_table: to_table.to_string(),
            foreign_key_count: options.len(),
        });
    }

    let (fc, tc) = options[0].clone();
    Ok(Some((fc, tc)))
}
