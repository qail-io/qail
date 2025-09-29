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

use super::types::ColumnType;
use std::collections::HashMap;

/// A complete database schema.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub tables: HashMap<String, Table>,
    pub indexes: Vec<Index>,
    pub migrations: Vec<MigrationHint>,
    /// PostgreSQL extensions (e.g. uuid-ossp, pgcrypto, PostGIS)
    pub extensions: Vec<Extension>,
    /// Schema-level comments on tables/columns
    pub comments: Vec<Comment>,
    /// Standalone sequences
    pub sequences: Vec<Sequence>,
    /// Standalone ENUM types
    pub enums: Vec<EnumType>,
    /// Views
    pub views: Vec<ViewDef>,
    /// PL/pgSQL functions
    pub functions: Vec<SchemaFunctionDef>,
    /// Triggers
    pub triggers: Vec<SchemaTriggerDef>,
    /// GRANT/REVOKE permissions
    pub grants: Vec<Grant>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    /// Table-level multi-column foreign keys
    pub multi_column_fks: Vec<MultiColumnForeignKey>,
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
    /// CHECK constraint (Phase 1)
    pub check: Option<CheckConstraint>,
    /// GENERATED column (Phase 3)
    pub generated: Option<Generated>,
}

/// Foreign key reference definition.
#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
    pub on_delete: FkAction,
    pub on_update: FkAction,
    /// DEFERRABLE clause (Phase 2)
    pub deferrable: Deferrable,
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

#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    /// Index method (Phase 4): btree, hash, gin, gist, brin
    pub method: IndexMethod,
    /// Partial index WHERE clause
    pub where_clause: Option<CheckExpr>,
    /// INCLUDE columns (covering index)
    pub include: Vec<String>,
    /// CREATE CONCURRENTLY
    pub concurrently: bool,
    /// Expression columns (e.g. `(lower(email))`) — if set, these replace `columns`
    pub expressions: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum MigrationHint {
    /// Rename a column (not delete + add)
    Rename { from: String, to: String },
    /// Transform data with expression
    Transform { expression: String, target: String },
    /// Drop with confirmation
    Drop { target: String, confirmed: bool },
}

// ============================================================================
// Phase 1: CHECK Constraints (AST-native)
// ============================================================================

/// CHECK constraint expression (AST-native, no raw SQL)
#[derive(Debug, Clone)]
pub enum CheckExpr {
    /// column > value
    GreaterThan { column: String, value: i64 },
    /// column >= value
    GreaterOrEqual { column: String, value: i64 },
    /// column < value
    LessThan { column: String, value: i64 },
    /// column <= value
    LessOrEqual { column: String, value: i64 },
    Between { column: String, low: i64, high: i64 },
    In { column: String, values: Vec<String> },
    /// column ~ pattern (regex)
    Regex { column: String, pattern: String },
    /// LENGTH(column) <= max
    MaxLength { column: String, max: usize },
    /// LENGTH(column) >= min
    MinLength { column: String, min: usize },
    NotNull { column: String },
    And(Box<CheckExpr>, Box<CheckExpr>),
    Or(Box<CheckExpr>, Box<CheckExpr>),
    Not(Box<CheckExpr>),
}

/// CHECK constraint with optional name
#[derive(Debug, Clone)]
pub struct CheckConstraint {
    pub expr: CheckExpr,
    pub name: Option<String>,
}

// ============================================================================
// Phase 2: DEFERRABLE Constraints
// ============================================================================

/// Constraint deferral mode
#[derive(Debug, Clone, Default, PartialEq)]
pub enum Deferrable {
    #[default]
    NotDeferrable,
    Deferrable,
    InitiallyDeferred,
    InitiallyImmediate,
}

// ============================================================================
// Phase 3: GENERATED Columns
// ============================================================================

/// GENERATED column type
#[derive(Debug, Clone)]
pub enum Generated {
    /// GENERATED ALWAYS AS (expr) STORED
    AlwaysStored(String),
    /// GENERATED ALWAYS AS IDENTITY
    AlwaysIdentity,
    /// GENERATED BY DEFAULT AS IDENTITY
    ByDefaultIdentity,
}

// ============================================================================
// Phase 4: Advanced Index Types
// ============================================================================

/// Index method (USING clause)
#[derive(Debug, Clone, Default, PartialEq)]
pub enum IndexMethod {
    #[default]
    BTree,
    Hash,
    Gin,
    Gist,
    Brin,
    SpGist,
}

// ============================================================================
// Phase 7: Extensions, Comments, Sequences
// ============================================================================

/// PostgreSQL extension (e.g. `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
#[derive(Debug, Clone, PartialEq)]
pub struct Extension {
    pub name: String,
    pub schema: Option<String>,
    pub version: Option<String>,
}

impl Extension {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            schema: None,
            version: None,
        }
    }

    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }
}

/// COMMENT ON TABLE/COLUMN
#[derive(Debug, Clone, PartialEq)]
pub struct Comment {
    pub target: CommentTarget,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CommentTarget {
    Table(String),
    Column { table: String, column: String },
}

impl Comment {
    pub fn on_table(table: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            target: CommentTarget::Table(table.into()),
            text: text.into(),
        }
    }

    pub fn on_column(
        table: impl Into<String>,
        column: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Self {
            target: CommentTarget::Column {
                table: table.into(),
                column: column.into(),
            },
            text: text.into(),
        }
    }
}

/// Standalone sequence (CREATE SEQUENCE)
#[derive(Debug, Clone, PartialEq)]
pub struct Sequence {
    pub name: String,
    pub data_type: Option<String>,
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
    pub cache: Option<i64>,
    pub cycle: bool,
    pub owned_by: Option<String>,
}

impl Sequence {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: None,
            start: None,
            increment: None,
            min_value: None,
            max_value: None,
            cache: None,
            cycle: false,
            owned_by: None,
        }
    }

    pub fn start(mut self, v: i64) -> Self {
        self.start = Some(v);
        self
    }

    pub fn increment(mut self, v: i64) -> Self {
        self.increment = Some(v);
        self
    }

    pub fn min_value(mut self, v: i64) -> Self {
        self.min_value = Some(v);
        self
    }

    pub fn max_value(mut self, v: i64) -> Self {
        self.max_value = Some(v);
        self
    }

    pub fn cache(mut self, v: i64) -> Self {
        self.cache = Some(v);
        self
    }

    pub fn cycle(mut self) -> Self {
        self.cycle = true;
        self
    }

    pub fn owned_by(mut self, col: impl Into<String>) -> Self {
        self.owned_by = Some(col.into());
        self
    }
}

// ============================================================================
// Phase 8: Standalone Enums, Multi-Column FK
// ============================================================================

/// Standalone ENUM type (CREATE TYPE ... AS ENUM)
#[derive(Debug, Clone, PartialEq)]
pub struct EnumType {
    pub name: String,
    pub values: Vec<String>,
}

impl EnumType {
    pub fn new(name: impl Into<String>, values: Vec<String>) -> Self {
        Self {
            name: name.into(),
            values,
        }
    }

    /// Add a new value (for ALTER TYPE ADD VALUE)
    pub fn add_value(mut self, value: impl Into<String>) -> Self {
        self.values.push(value.into());
        self
    }
}

/// Table-level multi-column foreign key
#[derive(Debug, Clone, PartialEq)]
pub struct MultiColumnForeignKey {
    pub columns: Vec<String>,
    pub ref_table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: FkAction,
    pub on_update: FkAction,
    pub deferrable: Deferrable,
    pub name: Option<String>,
}

impl MultiColumnForeignKey {
    pub fn new(
        columns: Vec<String>,
        ref_table: impl Into<String>,
        ref_columns: Vec<String>,
    ) -> Self {
        Self {
            columns,
            ref_table: ref_table.into(),
            ref_columns,
            on_delete: FkAction::default(),
            on_update: FkAction::default(),
            deferrable: Deferrable::default(),
            name: None,
        }
    }

    pub fn on_delete(mut self, action: FkAction) -> Self {
        self.on_delete = action;
        self
    }

    pub fn on_update(mut self, action: FkAction) -> Self {
        self.on_update = action;
        self
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

// ============================================================================
// Phase 9: Views, Functions, Triggers, Grants
// ============================================================================

/// A SQL view definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewDef {
    pub name: String,
    pub query: String,
    pub materialized: bool,
}

impl ViewDef {
    pub fn new(name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            materialized: false,
        }
    }

    pub fn materialized(mut self) -> Self {
        self.materialized = true;
        self
    }
}

/// A PL/pgSQL function definition for the schema model.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaFunctionDef {
    pub name: String,
    pub args: Vec<String>,
    pub returns: String,
    pub body: String,
    pub language: String,
    pub volatility: Option<String>,
}

impl SchemaFunctionDef {
    pub fn new(
        name: impl Into<String>,
        returns: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            args: Vec::new(),
            returns: returns.into(),
            body: body.into(),
            language: "plpgsql".to_string(),
            volatility: None,
        }
    }

    pub fn language(mut self, lang: impl Into<String>) -> Self {
        self.language = lang.into();
        self
    }

    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    pub fn volatility(mut self, v: impl Into<String>) -> Self {
        self.volatility = Some(v.into());
        self
    }
}

/// A trigger definition for the schema model.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaTriggerDef {
    pub name: String,
    pub table: String,
    pub timing: String,
    pub events: Vec<String>,
    pub for_each_row: bool,
    pub execute_function: String,
    pub condition: Option<String>,
}

impl SchemaTriggerDef {
    pub fn new(
        name: impl Into<String>,
        table: impl Into<String>,
        execute_function: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            timing: "BEFORE".to_string(),
            events: vec!["INSERT".to_string()],
            for_each_row: true,
            execute_function: execute_function.into(),
            condition: None,
        }
    }

    pub fn timing(mut self, t: impl Into<String>) -> Self {
        self.timing = t.into();
        self
    }

    pub fn events(mut self, evts: Vec<String>) -> Self {
        self.events = evts;
        self
    }

    pub fn for_each_statement(mut self) -> Self {
        self.for_each_row = false;
        self
    }

    pub fn condition(mut self, cond: impl Into<String>) -> Self {
        self.condition = Some(cond.into());
        self
    }
}

/// GRANT or REVOKE permission.
#[derive(Debug, Clone, PartialEq)]
pub struct Grant {
    pub action: GrantAction,
    pub privileges: Vec<Privilege>,
    pub on_object: String,
    pub to_role: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GrantAction {
    Grant,
    Revoke,
}

impl Default for GrantAction {
    fn default() -> Self {
        GrantAction::Grant
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Privilege {
    All,
    Select,
    Insert,
    Update,
    Delete,
    Usage,
    Execute,
}

impl std::fmt::Display for Privilege {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Privilege::All => write!(f, "ALL"),
            Privilege::Select => write!(f, "SELECT"),
            Privilege::Insert => write!(f, "INSERT"),
            Privilege::Update => write!(f, "UPDATE"),
            Privilege::Delete => write!(f, "DELETE"),
            Privilege::Usage => write!(f, "USAGE"),
            Privilege::Execute => write!(f, "EXECUTE"),
        }
    }
}

impl Grant {
    pub fn new(
        privileges: Vec<Privilege>,
        on_object: impl Into<String>,
        to_role: impl Into<String>,
    ) -> Self {
        Self {
            action: GrantAction::Grant,
            privileges,
            on_object: on_object.into(),
            to_role: to_role.into(),
        }
    }

    pub fn revoke(
        privileges: Vec<Privilege>,
        on_object: impl Into<String>,
        from_role: impl Into<String>,
    ) -> Self {
        Self {
            action: GrantAction::Revoke,
            privileges,
            on_object: on_object.into(),
            to_role: from_role.into(),
        }
    }
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

    pub fn add_extension(&mut self, ext: Extension) {
        self.extensions.push(ext);
    }

    pub fn add_comment(&mut self, comment: Comment) {
        self.comments.push(comment);
    }

    pub fn add_sequence(&mut self, seq: Sequence) {
        self.sequences.push(seq);
    }

    pub fn add_enum(&mut self, enum_type: EnumType) {
        self.enums.push(enum_type);
    }

    pub fn add_view(&mut self, view: ViewDef) {
        self.views.push(view);
    }

    pub fn add_function(&mut self, func: SchemaFunctionDef) {
        self.functions.push(func);
    }

    pub fn add_trigger(&mut self, trigger: SchemaTriggerDef) {
        self.triggers.push(trigger);
    }

    pub fn add_grant(&mut self, grant: Grant) {
        self.grants.push(grant);
    }

    /// Validate all foreign key references in the schema.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        for table in self.tables.values() {
            for col in &table.columns {
                if let Some(ref fk) = col.foreign_key {
                    if !self.tables.contains_key(&fk.table) {
                        errors.push(format!(
                            "FK error: {}.{} references non-existent table '{}'",
                            table.name, col.name, fk.table
                        ));
                    } else {
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

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            multi_column_fks: Vec::new(),
        }
    }

    pub fn column(mut self, col: Column) -> Self {
        self.columns.push(col);
        self
    }

    /// Add a table-level multi-column foreign key
    pub fn foreign_key(mut self, fk: MultiColumnForeignKey) -> Self {
        self.multi_column_fks.push(fk);
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
            check: None,
            generated: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Set as primary key with compile-time validation.
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
    /// # Example
    /// ```ignore
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
            deferrable: Deferrable::default(),
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

    // ==================== Phase 1: CHECK ====================

    /// Add a CHECK constraint (AST-native)
    pub fn check(mut self, expr: CheckExpr) -> Self {
        self.check = Some(CheckConstraint { expr, name: None });
        self
    }

    /// Add a named CHECK constraint
    pub fn check_named(mut self, name: impl Into<String>, expr: CheckExpr) -> Self {
        self.check = Some(CheckConstraint {
            expr,
            name: Some(name.into()),
        });
        self
    }

    // ==================== Phase 2: DEFERRABLE ====================

    /// Make foreign key DEFERRABLE
    pub fn deferrable(mut self) -> Self {
        if let Some(ref mut fk) = self.foreign_key {
            fk.deferrable = Deferrable::Deferrable;
        }
        self
    }

    /// Make foreign key DEFERRABLE INITIALLY DEFERRED
    pub fn initially_deferred(mut self) -> Self {
        if let Some(ref mut fk) = self.foreign_key {
            fk.deferrable = Deferrable::InitiallyDeferred;
        }
        self
    }

    /// Make foreign key DEFERRABLE INITIALLY IMMEDIATE
    pub fn initially_immediate(mut self) -> Self {
        if let Some(ref mut fk) = self.foreign_key {
            fk.deferrable = Deferrable::InitiallyImmediate;
        }
        self
    }

    // ==================== Phase 3: GENERATED ====================

    /// GENERATED ALWAYS AS (expr) STORED
    pub fn generated_stored(mut self, expr: impl Into<String>) -> Self {
        self.generated = Some(Generated::AlwaysStored(expr.into()));
        self
    }

    /// GENERATED ALWAYS AS IDENTITY
    pub fn generated_identity(mut self) -> Self {
        self.generated = Some(Generated::AlwaysIdentity);
        self
    }

    /// GENERATED BY DEFAULT AS IDENTITY
    pub fn generated_by_default(mut self) -> Self {
        self.generated = Some(Generated::ByDefaultIdentity);
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
            method: IndexMethod::default(),
            where_clause: None,
            include: Vec::new(),
            concurrently: false,
            expressions: Vec::new(),
        }
    }

    /// Create an expression index (e.g. `CREATE INDEX ON t ((lower(email)))`)
    pub fn expression(
        name: impl Into<String>,
        table: impl Into<String>,
        expressions: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            columns: Vec::new(),
            unique: false,
            method: IndexMethod::default(),
            where_clause: None,
            include: Vec::new(),
            concurrently: false,
            expressions,
        }
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    // ==================== Phase 4: Advanced Index Options ====================

    /// Set index method (USING clause)
    pub fn using(mut self, method: IndexMethod) -> Self {
        self.method = method;
        self
    }

    /// Create a partial index with WHERE clause
    pub fn partial(mut self, expr: CheckExpr) -> Self {
        self.where_clause = Some(expr);
        self
    }

    /// Add INCLUDE columns (covering index)
    pub fn include(mut self, cols: Vec<String>) -> Self {
        self.include = cols;
        self
    }

    /// Create index CONCURRENTLY
    pub fn concurrently(mut self) -> Self {
        self.concurrently = true;
        self
    }
}

/// Format a Schema to .qail format string.
pub fn to_qail_string(schema: &Schema) -> String {
    let mut output = String::new();
    output.push_str("# QAIL Schema\n\n");

    // Extensions first (must be created before any DDL)
    for ext in &schema.extensions {
        let mut line = format!("extension \"{}\"", ext.name);
        if let Some(ref s) = ext.schema {
            line.push_str(&format!(" schema {}", s));
        }
        if let Some(ref v) = ext.version {
            line.push_str(&format!(" version \"{}\"", v));
        }
        output.push_str(&line);
        output.push('\n');
    }
    if !schema.extensions.is_empty() {
        output.push('\n');
    }

    // Enums (CREATE TYPE ... AS ENUM, must precede tables)
    for enum_type in &schema.enums {
        let values = enum_type
            .values
            .iter()
            .map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        output.push_str(&format!("enum {} {{ {} }}\n", enum_type.name, values));
    }
    if !schema.enums.is_empty() {
        output.push('\n');
    }

    // Sequences (before tables, since columns may reference them)
    for seq in &schema.sequences {
        if seq.start.is_some()
            || seq.increment.is_some()
            || seq.min_value.is_some()
            || seq.max_value.is_some()
            || seq.cache.is_some()
            || seq.cycle
            || seq.owned_by.is_some()
        {
            let mut opts = Vec::new();
            if let Some(v) = seq.start {
                opts.push(format!("start {}", v));
            }
            if let Some(v) = seq.increment {
                opts.push(format!("increment {}", v));
            }
            if let Some(v) = seq.min_value {
                opts.push(format!("minvalue {}", v));
            }
            if let Some(v) = seq.max_value {
                opts.push(format!("maxvalue {}", v));
            }
            if let Some(v) = seq.cache {
                opts.push(format!("cache {}", v));
            }
            if seq.cycle {
                opts.push("cycle".to_string());
            }
            if let Some(ref o) = seq.owned_by {
                opts.push(format!("owned_by {}", o));
            }
            output.push_str(&format!("sequence {} {{ {} }}\n", seq.name, opts.join(" ")));
        } else {
            output.push_str(&format!("sequence {}\n", seq.name));
        }
    }
    if !schema.sequences.is_empty() {
        output.push('\n');
    }

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
            if let Some(ref fk) = col.foreign_key {
                constraints.push(format!("references {}({})", fk.table, fk.column));
            }

            let constraint_str = if constraints.is_empty() {
                String::new()
            } else {
                format!(" {}", constraints.join(" "))
            };

            output.push_str(&format!(
                "  {} {}{}\n",
                col.name,
                col.data_type.to_pg_type(),
                constraint_str
            ));
        }
        // Multi-column foreign keys
        for fk in &table.multi_column_fks {
            output.push_str(&format!(
                "  foreign_key ({}) references {}({})\n",
                fk.columns.join(", "),
                fk.ref_table,
                fk.ref_columns.join(", ")
            ));
        }
        output.push_str("}\n\n");
    }

    for idx in &schema.indexes {
        let unique = if idx.unique { "unique " } else { "" };
        let cols = if !idx.expressions.is_empty() {
            idx.expressions.join(", ")
        } else {
            idx.columns.join(", ")
        };
        output.push_str(&format!(
            "{}index {} on {} ({})\n",
            unique, idx.name, idx.table, cols
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

    // Views
    for view in &schema.views {
        let prefix = if view.materialized {
            "materialized view"
        } else {
            "view"
        };
        output.push_str(&format!("{} {} $$\n{}\n$$\n\n", prefix, view.name, view.query));
    }

    // Functions
    for func in &schema.functions {
        let args = func.args.join(", ");
        output.push_str(&format!(
            "function {}({}) returns {} language {} $$\n{}\n$$\n\n",
            func.name, args, func.returns, func.language, func.body
        ));
    }

    // Triggers
    for trigger in &schema.triggers {
        let events = trigger.events.join(" or ");
        output.push_str(&format!(
            "trigger {} on {} {} {} execute {}\n",
            trigger.name, trigger.table, trigger.timing.to_lowercase(),
            events.to_lowercase(), trigger.execute_function
        ));
    }
    if !schema.triggers.is_empty() {
        output.push('\n');
    }

    // Grants
    for grant in &schema.grants {
        let privs: Vec<String> = grant.privileges.iter().map(|p| p.to_string().to_lowercase()).collect();
        match grant.action {
            GrantAction::Grant => {
                output.push_str(&format!(
                    "grant {} on {} to {}\n",
                    privs.join(", "), grant.on_object, grant.to_role
                ));
            }
            GrantAction::Revoke => {
                output.push_str(&format!(
                    "revoke {} on {} from {}\n",
                    privs.join(", "), grant.on_object, grant.to_role
                ));
            }
        }
    }
    if !schema.grants.is_empty() {
        output.push('\n');
    }

    // Comments last (tables must exist first)
    for comment in &schema.comments {
        match &comment.target {
            CommentTarget::Table(t) => {
                output.push_str(&format!("comment on {} \"{}\"\n", t, comment.text));
            }
            CommentTarget::Column { table, column } => {
                output.push_str(&format!(
                    "comment on {}.{} \"{}\"\n",
                    table, column, comment.text
                ));
            }
        }
    }

    output
}


/// Convert a Schema to a list of Qail commands (CREATE TABLE, CREATE INDEX).
/// Used by shadow migration to apply the base schema before applying diffs.
pub fn schema_to_commands(schema: &Schema) -> Vec<crate::ast::Qail> {
    use crate::ast::{Action, Constraint, Expr, IndexDef, Qail};
    
    let mut cmds = Vec::new();
    
    // Sort tables to handle dependencies (tables with FK refs should come after their targets)
    let mut table_order: Vec<&Table> = schema.tables.values().collect();
    table_order.sort_by(|a, b| {
        let a_has_fk = a.columns.iter().any(|c| c.foreign_key.is_some());
        let b_has_fk = b.columns.iter().any(|c| c.foreign_key.is_some());
        a_has_fk.cmp(&b_has_fk)
    });
    
    for table in table_order {
        // Build columns using Expr::Def exactly like diff.rs does
        let columns: Vec<Expr> = table.columns.iter().map(|col| {
            let mut constraints = Vec::new();
            
            if col.primary_key {
                constraints.push(Constraint::PrimaryKey);
            }
            if col.nullable {
                constraints.push(Constraint::Nullable);
            }
            if col.unique {
                constraints.push(Constraint::Unique);
            }
            if let Some(def) = &col.default {
                constraints.push(Constraint::Default(def.clone()));
            }
            if let Some(ref fk) = col.foreign_key {
                constraints.push(Constraint::References(format!(
                    "{}({})",
                    fk.table, fk.column
                )));
            }
            
            Expr::Def {
                name: col.name.clone(),
                data_type: col.data_type.to_pg_type(),
                constraints,
            }
        }).collect();
        
        cmds.push(Qail {
            action: Action::Make,
            table: table.name.clone(),
            columns,
            ..Default::default()
        });
    }
    
    // Add indexes using IndexDef like diff.rs
    for idx in &schema.indexes {
        cmds.push(Qail {
            action: Action::Index,
            table: String::new(),
            index_def: Some(IndexDef {
                name: idx.name.clone(),
                table: idx.table.clone(),
                columns: idx.columns.clone(),
                unique: idx.unique,
                index_type: None,
            }),
            ..Default::default()
        });
    }
    
    cmds
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

        schema.add_table(
            Table::new("users").column(Column::new("id", ColumnType::Uuid).primary_key()),
        );

        schema.add_table(
            Table::new("posts")
                .column(Column::new("id", ColumnType::Uuid).primary_key())
                .column(
                    Column::new("user_id", ColumnType::Uuid)
                        .references("users", "id")
                        .on_delete(FkAction::Cascade),
                ),
        );

        // Should pass validation
        assert!(schema.validate().is_ok());
    }

    #[test]
    fn test_foreign_key_invalid_table() {
        let mut schema = Schema::new();

        schema.add_table(
            Table::new("posts")
                .column(Column::new("id", ColumnType::Uuid).primary_key())
                .column(Column::new("user_id", ColumnType::Uuid).references("nonexistent", "id")),
        );

        // Should fail validation
        let result = schema.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err()[0].contains("non-existent table"));
    }

    #[test]
    fn test_foreign_key_invalid_column() {
        let mut schema = Schema::new();

        schema.add_table(
            Table::new("users").column(Column::new("id", ColumnType::Uuid).primary_key()),
        );

        schema.add_table(
            Table::new("posts")
                .column(Column::new("id", ColumnType::Uuid).primary_key())
                .column(
                    Column::new("user_id", ColumnType::Uuid).references("users", "wrong_column"),
                ),
        );

        // Should fail validation
        let result = schema.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err()[0].contains("non-existent column"));
    }
}
