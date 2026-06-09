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

use super::policy::{PolicyPermissiveness, PolicyTarget, RlsPolicy};
use super::types::ColumnType;
use std::collections::HashMap;

/// A complete database schema.
#[derive(Debug, Clone, Default)]
pub struct Schema {
    /// Declared tables.
    pub tables: HashMap<String, Table>,
    /// Declared indexes.
    pub indexes: Vec<Index>,
    /// Migration hints (renames, transforms, drops).
    pub migrations: Vec<MigrationHint>,
    /// PostgreSQL extensions (e.g. uuid-ossp, pgcrypto, PostGIS)
    pub extensions: Vec<Extension>,
    /// Schema-level comments on tables/columns
    pub comments: Vec<Comment>,
    /// Standalone sequences
    pub sequences: Vec<Sequence>,
    /// Standalone ENUM types
    pub enums: Vec<EnumType>,
    /// SQL views (CREATE VIEW / CREATE MATERIALIZED VIEW).
    pub views: Vec<ViewDef>,
    /// PL/pgSQL functions
    pub functions: Vec<SchemaFunctionDef>,
    /// Database triggers (CREATE TRIGGER).
    pub triggers: Vec<SchemaTriggerDef>,
    /// GRANT/REVOKE permissions
    pub grants: Vec<Grant>,
    /// RLS policies
    pub policies: Vec<RlsPolicy>,
    /// Infrastructure resources (buckets, queues, topics)
    pub resources: Vec<ResourceDef>,
}

// ============================================================================
// Infrastructure Resources
// ============================================================================

/// Kind of infrastructure resource declared in schema.qail.
#[derive(Debug, Clone, PartialEq)]
pub enum ResourceKind {
    /// Object storage bucket.
    Bucket,
    /// Message queue.
    Queue,
    /// Pub/sub topic.
    Topic,
}

impl std::fmt::Display for ResourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bucket => write!(f, "bucket"),
            Self::Queue => write!(f, "queue"),
            Self::Topic => write!(f, "topic"),
        }
    }
}

/// An infrastructure resource declaration.
///
/// ```qail
/// bucket avatars {
///     provider s3
///     region "ap-southeast-1"
/// }
/// ```
#[derive(Debug, Clone)]
pub struct ResourceDef {
    /// Resource name (e.g. `"avatars"`).
    pub name: String,
    /// Kind of resource.
    pub kind: ResourceKind,
    /// Cloud provider (e.g. `"s3"`, `"gcs"`).
    pub provider: Option<String>,
    /// Arbitrary key-value properties.
    pub properties: HashMap<String, String>,
}

/// A table definition in the schema.
#[derive(Debug, Clone)]
pub struct Table {
    /// Table name.
    pub name: String,
    /// Column definitions.
    pub columns: Vec<Column>,
    /// Table-level multi-column foreign keys
    pub multi_column_fks: Vec<MultiColumnForeignKey>,
    /// ENABLE ROW LEVEL SECURITY
    pub enable_rls: bool,
    /// FORCE ROW LEVEL SECURITY
    pub force_rls: bool,
}

/// A column definition with compile-time type safety.
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Compile-time validated data type.
    pub data_type: ColumnType,
    /// Whether the column accepts NULL.
    pub nullable: bool,
    /// Whether this column is a primary key.
    pub primary_key: bool,
    /// Whether this column has a UNIQUE constraint.
    pub unique: bool,
    /// Default value expression.
    pub default: Option<String>,
    /// Foreign key reference.
    pub foreign_key: Option<ForeignKey>,
    /// CHECK constraint (Phase 1)
    pub check: Option<CheckConstraint>,
    /// GENERATED column (Phase 3)
    pub generated: Option<Generated>,
}

/// Foreign key reference definition.
#[derive(Debug, Clone)]
pub struct ForeignKey {
    /// Referenced table name.
    pub table: String,
    /// Referenced column name.
    pub column: String,
    /// Action taken when the referenced row is deleted.
    pub on_delete: FkAction,
    /// Action taken when the referenced row is updated.
    pub on_update: FkAction,
    /// DEFERRABLE clause (Phase 2)
    pub deferrable: Deferrable,
}

/// Foreign key action on DELETE/UPDATE.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum FkAction {
    #[default]
    /// No action on referenced row change.
    NoAction,
    /// Cascade the delete/update to referencing rows.
    Cascade,
    /// Set referencing column to NULL.
    SetNull,
    /// Set referencing column to its DEFAULT.
    SetDefault,
    /// Prevent the action (raises error).
    Restrict,
}

/// An index definition.
#[derive(Debug, Clone)]
pub struct Index {
    /// Index name.
    pub name: String,
    /// Table the index belongs to.
    pub table: String,
    /// Columns covered by the index.
    pub columns: Vec<String>,
    /// Whether the index enforces uniqueness.
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

/// Hints for the migration diff engine to improve migration quality.
#[derive(Debug, Clone)]
pub enum MigrationHint {
    /// Rename a column (not delete + add)
    Rename {
        /// Original column name.
        from: String,
        /// New column name.
        to: String,
    },
    /// Transform data with expression
    Transform {
        /// SQL expression for data transformation.
        expression: String,
        /// Target column name.
        target: String,
    },
    /// Drop with confirmation
    Drop {
        /// Target name to drop.
        target: String,
        /// Whether the drop has been confirmed.
        confirmed: bool,
    },
}

// ============================================================================
// Phase 1: CHECK Constraints (AST-native)
// ============================================================================

/// Binary comparison operator used by AST-native CHECK constraints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckComparisonOp {
    /// Equality (`=`)
    Equal,
    /// Inequality (`<>`)
    NotEqual,
    /// Greater than (`>`)
    GreaterThan,
    /// Greater than or equal (`>=`)
    GreaterOrEqual,
    /// Less than (`<`)
    LessThan,
    /// Less than or equal (`<=`)
    LessOrEqual,
}

impl CheckComparisonOp {
    /// SQL spelling for this comparison operator.
    pub fn as_sql_str(self) -> &'static str {
        match self {
            CheckComparisonOp::Equal => "=",
            CheckComparisonOp::NotEqual => "<>",
            CheckComparisonOp::GreaterThan => ">",
            CheckComparisonOp::GreaterOrEqual => ">=",
            CheckComparisonOp::LessThan => "<",
            CheckComparisonOp::LessOrEqual => "<=",
        }
    }
}

/// CHECK constraint expression (AST-native where possible, raw SQL fallback when needed)
#[derive(Debug, Clone)]
pub enum CheckExpr {
    /// column > value
    GreaterThan {
        /// Column name.
        column: String,
        /// Comparison value.
        value: i64,
    },
    /// column >= value
    GreaterOrEqual {
        /// Column name.
        column: String,
        /// Comparison value.
        value: i64,
    },
    /// column < value
    LessThan {
        /// Column name.
        column: String,
        /// Comparison value.
        value: i64,
    },
    /// column <= value
    LessOrEqual {
        /// Column name.
        column: String,
        /// Comparison value.
        value: i64,
    },
    /// value BETWEEN low AND high
    Between {
        /// Column name.
        column: String,
        /// Lower bound.
        low: i64,
        /// Upper bound.
        high: i64,
    },
    /// column IN (values)
    In {
        /// Column name.
        column: String,
        /// Allowed values.
        values: Vec<String>,
    },
    /// column IN (integer values)
    InIntegers {
        /// Column name.
        column: String,
        /// Allowed integer values.
        values: Vec<i64>,
    },
    /// left_column op right_column
    CompareColumns {
        /// Left-hand column.
        left_column: String,
        /// Comparison operator.
        op: CheckComparisonOp,
        /// Right-hand column.
        right_column: String,
    },
    /// column op 'text'
    TextCompare {
        /// Column name.
        column: String,
        /// Comparison operator.
        op: CheckComparisonOp,
        /// Text literal.
        value: String,
    },
    /// column op COALESCE(other_column, 'fallback'::type)
    CompareColumnToCoalesce {
        /// Left-hand column.
        left_column: String,
        /// Comparison operator.
        op: CheckComparisonOp,
        /// Column used as the first COALESCE argument.
        coalesce_column: String,
        /// Text fallback value.
        fallback: String,
        /// Optional fallback type cast, such as `date`.
        fallback_cast: Option<String>,
    },
    /// column = lower(btrim(column))
    LowerTrimEquals {
        /// Column name.
        column: String,
    },
    /// column ~ pattern (regex)
    Regex {
        /// Column name.
        column: String,
        /// Regex pattern.
        pattern: String,
    },
    /// LENGTH(column) <= max
    MaxLength {
        /// Column name.
        column: String,
        /// Maximum allowed length.
        max: usize,
    },
    /// LENGTH(column) >= min
    MinLength {
        /// Column name.
        column: String,
        /// Minimum required length.
        min: usize,
    },
    /// column IS NOT NULL
    NotNull {
        /// Column name.
        column: String,
    },
    /// Logical AND of two expressions.
    And(Box<CheckExpr>, Box<CheckExpr>),
    /// Logical OR of two expressions.
    Or(Box<CheckExpr>, Box<CheckExpr>),
    /// Logical NOT of an expression.
    Not(Box<CheckExpr>),
    /// SQL boolean expression (preserved as-is).
    Sql(String),
}

/// CHECK constraint with optional name
#[derive(Debug, Clone)]
pub struct CheckConstraint {
    /// The constraint expression.
    pub expr: CheckExpr,
    /// Optional constraint name.
    pub name: Option<String>,
}

// ============================================================================
// Phase 2: DEFERRABLE Constraints
// ============================================================================

/// Constraint deferral mode
#[derive(Debug, Clone, Default, PartialEq)]
pub enum Deferrable {
    #[default]
    /// Not deferrable (default).
    NotDeferrable,
    /// DEFERRABLE (initially immediate).
    Deferrable,
    /// DEFERRABLE INITIALLY DEFERRED.
    InitiallyDeferred,
    /// DEFERRABLE INITIALLY IMMEDIATE.
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
    /// B-tree (default for most columns).
    BTree,
    /// Hash (equality-only lookups).
    Hash,
    /// GIN (full-text search, JSONB).
    Gin,
    /// GiST (geometric, range types).
    Gist,
    /// BRIN (large, naturally-ordered tables).
    Brin,
    /// SP-GiST (space-partitioned).
    SpGist,
    /// HNSW vector index (pgvector).
    Hnsw,
    /// IVFFlat vector index (pgvector).
    IvfFlat,
}

pub(crate) fn index_method_str(method: &IndexMethod) -> &'static str {
    match method {
        IndexMethod::BTree => "btree",
        IndexMethod::Hash => "hash",
        IndexMethod::Gin => "gin",
        IndexMethod::Gist => "gist",
        IndexMethod::Brin => "brin",
        IndexMethod::SpGist => "spgist",
        IndexMethod::Hnsw => "hnsw",
        IndexMethod::IvfFlat => "ivfflat",
    }
}

// ============================================================================
// Phase 7: Extensions, Comments, Sequences
// ============================================================================

/// PostgreSQL extension (e.g. `CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
#[derive(Debug, Clone, PartialEq)]
pub struct Extension {
    /// Extension name (e.g. `"uuid-ossp"`).
    pub name: String,
    /// Target schema.
    pub schema: Option<String>,
    /// Pinned version.
    pub version: Option<String>,
}

impl Extension {
    /// Create a new extension declaration.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            schema: None,
            version: None,
        }
    }

    /// Set the target schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Pin to a specific version.
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }
}

/// COMMENT ON TABLE/COLUMN
#[derive(Debug, Clone, PartialEq)]
pub struct Comment {
    /// What the comment is attached to.
    pub target: CommentTarget,
    /// Comment text.
    pub text: String,
}

/// Target of a COMMENT ON statement.
#[derive(Debug, Clone, PartialEq)]
pub enum CommentTarget {
    /// COMMENT ON TABLE.
    Table(String),
    /// COMMENT ON COLUMN.
    Column {
        /// Table name.
        table: String,
        /// Column name.
        column: String,
    },
    /// COMMENT ON arbitrary object target (e.g. FUNCTION/POLICY/TYPE/CONSTRAINT).
    Raw(String),
}

impl Comment {
    /// Create a comment on a table.
    pub fn on_table(table: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            target: CommentTarget::Table(table.into()),
            text: text.into(),
        }
    }

    /// Create a comment on a column.
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

    /// Create a comment on an arbitrary object target.
    pub fn on_raw(target: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            target: CommentTarget::Raw(target.into()),
            text: text.into(),
        }
    }
}

/// Standalone sequence (CREATE SEQUENCE)
#[derive(Debug, Clone, PartialEq)]
pub struct Sequence {
    /// Sequence name.
    pub name: String,
    /// Data type (e.g. `"bigint"`).
    pub data_type: Option<String>,
    /// START WITH value.
    pub start: Option<i64>,
    /// INCREMENT BY value.
    pub increment: Option<i64>,
    /// Minimum value for the sequence (MINVALUE clause).
    pub min_value: Option<i64>,
    /// Maximum value for the sequence (MAXVALUE clause).
    pub max_value: Option<i64>,
    /// CACHE size.
    pub cache: Option<i64>,
    /// Whether the sequence wraps around.
    pub cycle: bool,
    /// OWNED BY column reference.
    pub owned_by: Option<String>,
}

impl Sequence {
    /// Create a new sequence.
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

    /// Set the START WITH value.
    pub fn start(mut self, v: i64) -> Self {
        self.start = Some(v);
        self
    }

    /// Set the INCREMENT BY value.
    pub fn increment(mut self, v: i64) -> Self {
        self.increment = Some(v);
        self
    }

    /// Set the MINVALUE.
    pub fn min_value(mut self, v: i64) -> Self {
        self.min_value = Some(v);
        self
    }

    /// Set the MAXVALUE.
    pub fn max_value(mut self, v: i64) -> Self {
        self.max_value = Some(v);
        self
    }

    /// Set the CACHE size.
    pub fn cache(mut self, v: i64) -> Self {
        self.cache = Some(v);
        self
    }

    /// Enable CYCLE (wrap around at limit).
    pub fn cycle(mut self) -> Self {
        self.cycle = true;
        self
    }

    /// Set the OWNED BY column reference.
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
    /// Type name.
    pub name: String,
    /// Allowed values.
    pub values: Vec<String>,
}

impl EnumType {
    /// Create a new enum type.
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
    /// Source columns.
    pub columns: Vec<String>,
    /// Referenced table.
    pub ref_table: String,
    /// Referenced columns.
    pub ref_columns: Vec<String>,
    /// ON DELETE action.
    pub on_delete: FkAction,
    /// ON UPDATE action.
    pub on_update: FkAction,
    /// Deferral mode.
    pub deferrable: Deferrable,
    /// Optional constraint name.
    pub name: Option<String>,
}

impl MultiColumnForeignKey {
    /// Create a new multi-column foreign key.
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

    /// Set the ON DELETE action.
    pub fn on_delete(mut self, action: FkAction) -> Self {
        self.on_delete = action;
        self
    }

    /// Set the ON UPDATE action.
    pub fn on_update(mut self, action: FkAction) -> Self {
        self.on_update = action;
        self
    }

    /// Set an explicit constraint name.
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Make the foreign key DEFERRABLE.
    pub fn deferrable(mut self) -> Self {
        self.deferrable = Deferrable::Deferrable;
        self
    }

    /// Make the foreign key DEFERRABLE INITIALLY DEFERRED.
    pub fn initially_deferred(mut self) -> Self {
        self.deferrable = Deferrable::InitiallyDeferred;
        self
    }

    /// Make the foreign key DEFERRABLE INITIALLY IMMEDIATE.
    pub fn initially_immediate(mut self) -> Self {
        self.deferrable = Deferrable::InitiallyImmediate;
        self
    }
}

// ============================================================================
// Phase 9: Views, Functions, Triggers, Grants
// ============================================================================

/// A SQL view definition.
#[derive(Debug, Clone, PartialEq)]
pub struct ViewDef {
    /// View name.
    pub name: String,
    /// Underlying SQL query.
    pub query: String,
    /// Whether this is a MATERIALIZED VIEW.
    pub materialized: bool,
}

impl ViewDef {
    /// Create a standard (non-materialized) view.
    pub fn new(name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            materialized: false,
        }
    }

    /// Mark as MATERIALIZED VIEW.
    pub fn materialized(mut self) -> Self {
        self.materialized = true;
        self
    }
}

/// A PL/pgSQL function definition for the schema model.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaFunctionDef {
    /// Function name.
    pub name: String,
    /// Function arguments (e.g. `"p_id uuid"`).
    pub args: Vec<String>,
    /// Return type.
    pub returns: String,
    /// Function body.
    pub body: String,
    /// Language (default `"plpgsql"`).
    pub language: String,
    /// Volatility category (VOLATILE, STABLE, IMMUTABLE).
    pub volatility: Option<String>,
}

impl SchemaFunctionDef {
    /// Create a new function definition.
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

    /// Set the function language.
    pub fn language(mut self, lang: impl Into<String>) -> Self {
        self.language = lang.into();
        self
    }

    /// Add a function argument.
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Set the volatility category.
    pub fn volatility(mut self, v: impl Into<String>) -> Self {
        self.volatility = Some(v.into());
        self
    }
}

/// A trigger definition for the schema model.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaTriggerDef {
    /// Trigger name.
    pub name: String,
    /// Target table.
    pub table: String,
    /// Timing (BEFORE, AFTER, INSTEAD OF).
    pub timing: String,
    /// Events that fire the trigger (INSERT, UPDATE, DELETE).
    pub events: Vec<String>,
    /// Optional column list for `UPDATE OF` triggers.
    pub update_columns: Vec<String>,
    /// Whether the trigger fires FOR EACH ROW (vs. FOR EACH STATEMENT).
    pub for_each_row: bool,
    /// Function to execute.
    pub execute_function: String,
    /// Optional WHEN condition.
    pub condition: Option<String>,
}

impl SchemaTriggerDef {
    /// Create a new trigger definition.
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
            update_columns: Vec::new(),
            for_each_row: true,
            execute_function: execute_function.into(),
            condition: None,
        }
    }

    /// Set the trigger timing.
    pub fn timing(mut self, t: impl Into<String>) -> Self {
        self.timing = t.into();
        self
    }

    /// Set the trigger events.
    pub fn events(mut self, evts: Vec<String>) -> Self {
        self.events = evts;
        self
    }

    /// Fire FOR EACH STATEMENT instead of FOR EACH ROW.
    pub fn for_each_statement(mut self) -> Self {
        self.for_each_row = false;
        self
    }

    /// Set an optional WHEN condition.
    pub fn condition(mut self, cond: impl Into<String>) -> Self {
        self.condition = Some(cond.into());
        self
    }
}

/// GRANT or REVOKE permission.
#[derive(Debug, Clone, PartialEq)]
pub struct Grant {
    /// GRANT or REVOKE.
    pub action: GrantAction,
    /// Privileges being granted/revoked.
    pub privileges: Vec<Privilege>,
    /// Target object (table, schema, sequence).
    pub on_object: String,
    /// Role receiving (or losing) the privileges.
    pub to_role: String,
}

/// Whether a permission statement is a GRANT or REVOKE.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum GrantAction {
    #[default]
    /// Grant privileges.
    Grant,
    /// Revoke privileges.
    Revoke,
}

/// SQL privilege type.
#[derive(Debug, Clone, PartialEq)]
pub enum Privilege {
    /// ALL PRIVILEGES.
    All,
    /// SELECT.
    Select,
    /// INSERT.
    Insert,
    /// UPDATE.
    Update,
    /// DELETE.
    Delete,
    /// USAGE (on schemas, sequences).
    Usage,
    /// EXECUTE (on functions).
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
    /// Create a GRANT statement.
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

    /// Create a REVOKE statement.
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
    /// Create an empty schema.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a table definition.
    pub fn add_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), table);
    }

    /// Add an index definition.
    pub fn add_index(&mut self, index: Index) {
        self.indexes.push(index);
    }

    /// Add a migration hint.
    pub fn add_hint(&mut self, hint: MigrationHint) {
        self.migrations.push(hint);
    }

    /// Add a PostgreSQL extension.
    pub fn add_extension(&mut self, ext: Extension) {
        self.extensions.push(ext);
    }

    /// Add a schema comment.
    pub fn add_comment(&mut self, comment: Comment) {
        self.comments.push(comment);
    }

    /// Add a standalone sequence.
    pub fn add_sequence(&mut self, seq: Sequence) {
        self.sequences.push(seq);
    }

    /// Add a standalone ENUM type.
    pub fn add_enum(&mut self, enum_type: EnumType) {
        self.enums.push(enum_type);
    }

    /// Add a view definition.
    pub fn add_view(&mut self, view: ViewDef) {
        self.views.push(view);
    }

    /// Add a function definition.
    pub fn add_function(&mut self, func: SchemaFunctionDef) {
        self.functions.push(func);
    }

    /// Add a trigger definition.
    pub fn add_trigger(&mut self, trigger: SchemaTriggerDef) {
        self.triggers.push(trigger);
    }

    /// Add a GRANT or REVOKE.
    pub fn add_grant(&mut self, grant: Grant) {
        self.grants.push(grant);
    }

    /// Add an infrastructure resource declaration.
    pub fn add_resource(&mut self, resource: ResourceDef) {
        self.resources.push(resource);
    }

    /// Add an RLS policy definition.
    pub fn add_policy(&mut self, policy: RlsPolicy) {
        self.policies.push(policy);
    }

    /// Validate all foreign key references in the schema.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        for table in self.tables.values() {
            let mut seen_columns = std::collections::BTreeSet::new();
            for col in &table.columns {
                if !seen_columns.insert(col.name.as_str()) {
                    errors.push(format!(
                        "Schema error: table '{}' has duplicate column '{}'",
                        table.name, col.name
                    ));
                }
            }

            let table_columns = table
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<std::collections::BTreeSet<_>>();

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

                if let Some(check) = &col.check {
                    for referenced in check_expr_column_references(&check.expr) {
                        let referenced_column = check_expr_reference_name(referenced);
                        if !table_columns.contains(referenced_column.as_str()) {
                            errors.push(format!(
                                "CHECK error: {}.{} references non-existent column '{}.{}'",
                                table.name, col.name, table.name, referenced_column
                            ));
                        }
                    }
                }
            }

            for fk in &table.multi_column_fks {
                if fk.columns.is_empty() {
                    errors.push(format!(
                        "Multi-column FK error: {} has no source columns",
                        table.name
                    ));
                }
                if fk.ref_columns.is_empty() {
                    errors.push(format!(
                        "Multi-column FK error: {} references '{}' with no target columns",
                        table.name, fk.ref_table
                    ));
                }
                if fk.columns.len() != fk.ref_columns.len() {
                    errors.push(format!(
                        "Multi-column FK error: {} column count {} does not match referenced column count {}",
                        table.name,
                        fk.columns.len(),
                        fk.ref_columns.len()
                    ));
                }

                for source_col in &fk.columns {
                    if !table.columns.iter().any(|c| c.name == *source_col) {
                        errors.push(format!(
                            "Multi-column FK error: {} references non-existent source column '{}.{}'",
                            table.name, table.name, source_col
                        ));
                    }
                }

                let Some(ref_table) = self.tables.get(&fk.ref_table) else {
                    errors.push(format!(
                        "Multi-column FK error: {} references non-existent table '{}'",
                        table.name, fk.ref_table
                    ));
                    continue;
                };

                for ref_col in &fk.ref_columns {
                    if !ref_table.columns.iter().any(|c| c.name == *ref_col) {
                        errors.push(format!(
                            "Multi-column FK error: {} references non-existent column '{}.{}'",
                            table.name, fk.ref_table, ref_col
                        ));
                    }
                }
            }
        }

        let mut seen_index_names = std::collections::BTreeSet::new();
        for index in &self.indexes {
            if !seen_index_names.insert(index.name.as_str()) {
                errors.push(format!(
                    "Index error: duplicate index name '{}'",
                    index.name
                ));
            }

            let Some(table) = self.tables.get(&index.table) else {
                errors.push(format!(
                    "Index error: {} references non-existent table '{}'",
                    index.name, index.table
                ));
                continue;
            };

            if index.columns.is_empty() && index.expressions.is_empty() {
                errors.push(format!(
                    "Index error: {} must define at least one column or expression",
                    index.name
                ));
            }
            if !index.columns.is_empty() && !index.expressions.is_empty() {
                errors.push(format!(
                    "Index error: {} cannot mix columns and expressions",
                    index.name
                ));
            }

            for column in &index.columns {
                if column.trim().is_empty() {
                    errors.push(format!("Index error: {} has empty column", index.name));
                    continue;
                }
                let Some(column_name) = index_column_reference_name(column) else {
                    continue;
                };
                if !table.columns.iter().any(|c| c.name == column_name) {
                    errors.push(format!(
                        "Index error: {} references non-existent column '{}.{}'",
                        index.name, index.table, column_name
                    ));
                }
            }

            for expression in &index.expressions {
                if expression.trim().is_empty() {
                    errors.push(format!("Index error: {} has empty expression", index.name));
                }
            }

            for include_column in &index.include {
                let Some(column_name) = index_column_reference_name(include_column) else {
                    errors.push(format!(
                        "Index error: {} has invalid INCLUDE column '{}'",
                        index.name, include_column
                    ));
                    continue;
                };
                if !table.columns.iter().any(|c| c.name == column_name) {
                    errors.push(format!(
                        "Index error: {} references non-existent INCLUDE column '{}.{}'",
                        index.name, index.table, column_name
                    ));
                }
            }

            if let Some(where_clause) = &index.where_clause {
                for referenced in check_expr_column_references(where_clause) {
                    let referenced_column = check_expr_reference_name(referenced);
                    if !table.columns.iter().any(|c| c.name == referenced_column) {
                        errors.push(format!(
                            "Index error: {} WHERE references non-existent column '{}.{}'",
                            index.name, index.table, referenced_column
                        ));
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

fn check_expr_column_references(expr: &CheckExpr) -> Vec<&str> {
    let mut refs = Vec::new();
    collect_check_expr_column_references(expr, &mut refs);
    refs.sort_unstable();
    refs.dedup();
    refs
}

fn collect_check_expr_column_references<'a>(expr: &'a CheckExpr, refs: &mut Vec<&'a str>) {
    match expr {
        CheckExpr::GreaterThan { column, .. }
        | CheckExpr::GreaterOrEqual { column, .. }
        | CheckExpr::LessThan { column, .. }
        | CheckExpr::LessOrEqual { column, .. }
        | CheckExpr::Between { column, .. }
        | CheckExpr::In { column, .. }
        | CheckExpr::InIntegers { column, .. }
        | CheckExpr::TextCompare { column, .. }
        | CheckExpr::LowerTrimEquals { column }
        | CheckExpr::Regex { column, .. }
        | CheckExpr::MaxLength { column, .. }
        | CheckExpr::MinLength { column, .. }
        | CheckExpr::NotNull { column } => refs.push(column),
        CheckExpr::CompareColumns {
            left_column,
            right_column,
            ..
        } => {
            refs.push(left_column);
            refs.push(right_column);
        }
        CheckExpr::CompareColumnToCoalesce {
            left_column,
            coalesce_column,
            ..
        } => {
            refs.push(left_column);
            refs.push(coalesce_column);
        }
        CheckExpr::And(left, right) | CheckExpr::Or(left, right) => {
            collect_check_expr_column_references(left, refs);
            collect_check_expr_column_references(right, refs);
        }
        CheckExpr::Not(inner) => collect_check_expr_column_references(inner, refs),
        CheckExpr::Sql(_) => {}
    }
}

fn check_expr_reference_name(reference: &str) -> String {
    let trimmed = reference.trim();
    let unqualified = trimmed.rsplit('.').next().unwrap_or(trimmed);
    unquote_identifier(unqualified)
}

fn index_column_reference_name(fragment: &str) -> Option<String> {
    let fragment = fragment.trim();
    if fragment.is_empty() || fragment.contains('(') || fragment.contains("->") {
        return None;
    }

    let token = first_index_column_token(fragment)?;
    let unqualified = token.rsplit('.').next().unwrap_or(token);
    Some(unquote_identifier(unqualified))
}

fn first_index_column_token(fragment: &str) -> Option<&str> {
    let fragment = fragment.trim_start();
    if fragment.starts_with('"') {
        let mut escaped = false;
        for (idx, ch) in fragment.char_indices().skip(1) {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '"' {
                if fragment[idx + ch.len_utf8()..].starts_with('"') {
                    escaped = true;
                    continue;
                }
                return Some(&fragment[..=idx]);
            }
        }
        return None;
    }

    let end = fragment
        .find(|ch: char| ch.is_whitespace() || ch == '-' || ch == '>')
        .unwrap_or(fragment.len());
    (end > 0).then_some(&fragment[..end])
}

fn unquote_identifier(identifier: &str) -> String {
    identifier
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .map(|s| s.replace("\"\"", "\""))
        .unwrap_or_else(|| identifier.to_string())
}

impl Table {
    /// Create a new empty table.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            multi_column_fks: Vec::new(),
            enable_rls: false,
            force_rls: false,
        }
    }

    /// Add a column (builder pattern).
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
    fn primary_key_type_error(&self) -> String {
        format!(
            "Column '{}' of type {} cannot be a primary key. \
             Valid PK types: scalar/indexable types \
             (UUID, TEXT, VARCHAR, INT, BIGINT, SERIAL, BIGSERIAL, BOOLEAN, FLOAT, DECIMAL, \
             TIMESTAMP, TIMESTAMPTZ, DATE, TIME, ENUM, INET, CIDR, MACADDR)",
            self.name,
            self.data_type.name()
        )
    }

    fn unique_type_error(&self) -> String {
        format!(
            "Column '{}' of type {} cannot have UNIQUE constraint. \
             JSONB and BYTEA types do not support standard indexing.",
            self.name,
            self.data_type.name()
        )
    }

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

    /// Mark as NOT NULL.
    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    /// Set as primary key with compile-time validation.
    /// Validates that the column type can be a primary key.
    ///
    /// This method is fail-soft: invalid type combinations are allowed to
    /// continue without panicking so production callers cannot crash on
    /// dynamic schema input. Use [`Column::try_primary_key`] for strict mode.
    pub fn primary_key(mut self) -> Self {
        if !self.data_type.can_be_primary_key() {
            #[cfg(debug_assertions)]
            eprintln!("QAIL: {}", self.primary_key_type_error());
        }
        self.primary_key = true;
        self.nullable = false;
        self
    }

    /// Strict variant of [`Column::primary_key`].
    ///
    /// Returns an error instead of panicking when type policy disallows PK.
    pub fn try_primary_key(mut self) -> Result<Self, String> {
        if !self.data_type.can_be_primary_key() {
            return Err(self.primary_key_type_error());
        }
        self.primary_key = true;
        self.nullable = false;
        Ok(self)
    }

    /// Set as unique with compile-time validation.
    /// Validates that the column type supports indexing.
    ///
    /// This method is fail-soft: invalid type combinations are allowed to
    /// continue without panicking so production callers cannot crash on
    /// dynamic schema input. Use [`Column::try_unique`] for strict mode.
    pub fn unique(mut self) -> Self {
        if !self.data_type.supports_indexing() {
            #[cfg(debug_assertions)]
            eprintln!("QAIL: {}", self.unique_type_error());
        }
        self.unique = true;
        self
    }

    /// Strict variant of [`Column::unique`].
    ///
    /// Returns an error instead of panicking when type policy disallows UNIQUE.
    pub fn try_unique(mut self) -> Result<Self, String> {
        if !self.data_type.supports_indexing() {
            return Err(self.unique_type_error());
        }
        self.unique = true;
        Ok(self)
    }

    /// Set a DEFAULT value expression.
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
    /// Create a new index on the given columns.
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

    /// Mark this index as UNIQUE.
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
/// Convert FkAction to its QAIL string representation
fn fk_action_str(action: &FkAction) -> &'static str {
    match action {
        FkAction::NoAction => "no_action",
        FkAction::Cascade => "cascade",
        FkAction::SetNull => "set_null",
        FkAction::SetDefault => "set_default",
        FkAction::Restrict => "restrict",
    }
}

fn format_qail_value_token(value: &str, extra_special: &[char]) -> String {
    let needs_quotes = value.is_empty()
        || value.chars().any(|ch| {
            ch.is_whitespace() || matches!(ch, ',' | '\'' | '"') || extra_special.contains(&ch)
        });

    if needs_quotes {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn format_check_in_value(value: &str) -> String {
    format_qail_value_token(value, &['[', ']'])
}

fn format_sql_text_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn format_sql_text_literal_with_cast(value: &str, cast: &Option<String>) -> String {
    let literal = format_sql_text_literal(value);
    match cast {
        Some(cast) => format!("{literal}::{cast}"),
        None => literal,
    }
}

/// Serialize CheckExpr to QAIL check syntax
fn check_expr_str(expr: &CheckExpr) -> String {
    match expr {
        CheckExpr::GreaterThan { column, value } => format!("{} > {}", column, value),
        CheckExpr::GreaterOrEqual { column, value } => format!("{} >= {}", column, value),
        CheckExpr::LessThan { column, value } => format!("{} < {}", column, value),
        CheckExpr::LessOrEqual { column, value } => format!("{} <= {}", column, value),
        CheckExpr::Between { column, low, high } => format!("{} between {} {}", column, low, high),
        CheckExpr::In { column, values } => format!(
            "{} in [{}]",
            column,
            values
                .iter()
                .map(|value| format_check_in_value(value))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        CheckExpr::InIntegers { column, values } => format!(
            "{} = ANY (ARRAY[{}])",
            column,
            values
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        CheckExpr::CompareColumns {
            left_column,
            op,
            right_column,
        } => format!("{} {} {}", left_column, op.as_sql_str(), right_column),
        CheckExpr::TextCompare { column, op, value } => {
            format!(
                "{} {} {}",
                column,
                op.as_sql_str(),
                format_sql_text_literal(value)
            )
        }
        CheckExpr::CompareColumnToCoalesce {
            left_column,
            op,
            coalesce_column,
            fallback,
            fallback_cast,
        } => format!(
            "{} {} COALESCE({}, {})",
            left_column,
            op.as_sql_str(),
            coalesce_column,
            format_sql_text_literal_with_cast(fallback, fallback_cast)
        ),
        CheckExpr::LowerTrimEquals { column } => format!("{column} = lower(btrim({column}))"),
        CheckExpr::Regex { column, pattern } => {
            format!("{} ~ {}", column, format_sql_text_literal(pattern))
        }
        CheckExpr::MaxLength { column, max } => format!("length({}) <= {}", column, max),
        CheckExpr::MinLength { column, min } => format!("length({}) >= {}", column, min),
        CheckExpr::NotNull { column } => format!("{} not_null", column),
        CheckExpr::And(l, r) => format!("{} and {}", check_expr_str(l), check_expr_str(r)),
        CheckExpr::Or(l, r) => format!("{} or {}", check_expr_str(l), check_expr_str(r)),
        CheckExpr::Not(e) => format!("not {}", check_expr_str(e)),
        CheckExpr::Sql(sql) => sql.clone(),
    }
}

fn format_enum_value(value: &str) -> String {
    format_qail_value_token(value, &['{', '}'])
}

fn dollar_quote_qail_body(body: &str) -> String {
    let delimiter = if !body.contains("$$") {
        "$$".to_string()
    } else {
        let mut idx = 0usize;
        loop {
            let candidate = if idx == 0 {
                "$qail$".to_string()
            } else {
                format!("$qail{idx}$")
            };
            if !body.contains(&candidate) {
                break candidate;
            }
            idx = idx.saturating_add(1);
        }
    };

    format!("{delimiter}\n{body}\n{delimiter}")
}

/// Serialize a `Schema` back to a QAIL-format string.
pub fn to_qail_string(schema: &Schema) -> String {
    let mut output = String::new();
    output.push_str("# QAIL Schema\n\n");

    // Extensions first (must be created before any DDL)
    for ext in &schema.extensions {
        let mut line = format!("extension {}", quote_qail_string(&ext.name));
        if let Some(ref s) = ext.schema {
            line.push_str(&format!(" schema {}", quote_qail_string(s)));
        }
        if let Some(ref v) = ext.version {
            line.push_str(&format!(" version {}", quote_qail_string(v)));
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
            .map(|v| format_enum_value(v))
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

    let mut table_names: Vec<&String> = schema.tables.keys().collect();
    table_names.sort();
    for table_name in table_names {
        let table = &schema.tables[table_name];
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
            if let Some(generated) = &col.generated {
                match generated {
                    Generated::AlwaysStored(expr) => {
                        constraints.push(format!("generated_stored({})", expr));
                    }
                    Generated::AlwaysIdentity => {
                        constraints.push("generated_identity".to_string());
                    }
                    Generated::ByDefaultIdentity => {
                        constraints.push("generated_by_default_identity".to_string());
                    }
                }
            }
            if let Some(ref fk) = col.foreign_key {
                let mut fk_str = format!("references {}({})", fk.table, fk.column);
                if fk.on_delete != FkAction::NoAction {
                    fk_str.push_str(&format!(" on_delete {}", fk_action_str(&fk.on_delete)));
                }
                if fk.on_update != FkAction::NoAction {
                    fk_str.push_str(&format!(" on_update {}", fk_action_str(&fk.on_update)));
                }
                match &fk.deferrable {
                    Deferrable::Deferrable => fk_str.push_str(" deferrable"),
                    Deferrable::InitiallyDeferred => fk_str.push_str(" initially_deferred"),
                    Deferrable::InitiallyImmediate => fk_str.push_str(" initially_immediate"),
                    Deferrable::NotDeferrable => {} // default, omit
                }
                constraints.push(fk_str);
            }
            if let Some(ref check) = col.check {
                constraints.push(format!("check({})", check_expr_str(&check.expr)));
                if let Some(name) = &check.name {
                    constraints.push(format!("check_name {}", name));
                }
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
            let mut fk_line = format!(
                "  foreign_key ({}) references {}({})\n",
                fk.columns.join(", "),
                fk.ref_table,
                fk.ref_columns.join(", ")
            );
            if fk.name.is_some()
                || fk.on_delete != FkAction::NoAction
                || fk.on_update != FkAction::NoAction
                || fk.deferrable != Deferrable::NotDeferrable
            {
                fk_line.pop();
                if let Some(name) = &fk.name {
                    fk_line.push_str(&format!(" constraint {}", name));
                }
                if fk.on_delete != FkAction::NoAction {
                    fk_line.push_str(&format!(" on_delete {}", fk_action_str(&fk.on_delete)));
                }
                if fk.on_update != FkAction::NoAction {
                    fk_line.push_str(&format!(" on_update {}", fk_action_str(&fk.on_update)));
                }
                match &fk.deferrable {
                    Deferrable::Deferrable => fk_line.push_str(" deferrable"),
                    Deferrable::InitiallyDeferred => fk_line.push_str(" initially_deferred"),
                    Deferrable::InitiallyImmediate => fk_line.push_str(" initially_immediate"),
                    Deferrable::NotDeferrable => {}
                }
                fk_line.push('\n');
            }
            output.push_str(&fk_line);
        }
        // RLS directives
        if table.enable_rls {
            output.push_str("  enable_rls\n");
        }
        if table.force_rls {
            output.push_str("  force_rls\n");
        }
        output.push_str("}\n\n");
    }

    for idx in &schema.indexes {
        let unique = if idx.unique { "unique " } else { "" };
        let concurrently = if idx.concurrently {
            "concurrently "
        } else {
            ""
        };
        let cols = if !idx.expressions.is_empty() {
            idx.expressions.join(", ")
        } else {
            idx.columns.join(", ")
        };
        let mut line = format!(
            "{}index {}{} on {}",
            unique, concurrently, idx.name, idx.table
        );
        if idx.method != IndexMethod::BTree {
            line.push_str(" using ");
            line.push_str(index_method_str(&idx.method));
        }
        line.push_str(" (");
        line.push_str(&cols);
        line.push(')');
        if !idx.include.is_empty() {
            line.push_str(" include (");
            line.push_str(&idx.include.join(", "));
            line.push(')');
        }
        if let Some(where_clause) = &idx.where_clause {
            line.push_str(" where ");
            line.push_str(&check_expr_str(where_clause));
        }
        output.push_str(&line);
        output.push('\n');
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
        let body = dollar_quote_qail_body(&view.query);
        output.push_str(&format!("{} {} {}\n\n", prefix, view.name, body));
    }

    // Functions
    for func in &schema.functions {
        let args = func.args.join(", ");
        let volatility = func
            .volatility
            .as_deref()
            .filter(|v| !v.trim().is_empty())
            .map(|v| format!(" {}", v))
            .unwrap_or_default();
        let body = dollar_quote_qail_body(&func.body);
        output.push_str(&format!(
            "function {}({}) returns {} language {}{} {}\n\n",
            func.name, args, func.returns, func.language, volatility, body
        ));
    }

    // Triggers
    for trigger in &schema.triggers {
        let mut events = Vec::new();
        for evt in &trigger.events {
            if evt.eq_ignore_ascii_case("UPDATE") && !trigger.update_columns.is_empty() {
                events.push(format!("UPDATE OF {}", trigger.update_columns.join(", ")));
            } else {
                events.push(evt.clone());
            }
        }
        output.push_str(&format!(
            "trigger {} on {} {} {} execute {}\n",
            trigger.name,
            trigger.table,
            trigger.timing.to_lowercase(),
            events.join(" or ").to_lowercase(),
            trigger.execute_function
        ));
    }
    if !schema.triggers.is_empty() {
        output.push('\n');
    }

    // Policies
    for policy in &schema.policies {
        let cmd = match policy.target {
            PolicyTarget::All => "all",
            PolicyTarget::Select => "select",
            PolicyTarget::Insert => "insert",
            PolicyTarget::Update => "update",
            PolicyTarget::Delete => "delete",
        };
        let perm = match policy.permissiveness {
            PolicyPermissiveness::Permissive => "",
            PolicyPermissiveness::Restrictive => " restrictive",
        };
        let role_str = match &policy.role {
            Some(r) => format!(" to {}", r),
            None => String::new(),
        };
        output.push_str(&format!(
            "policy {} on {} for {}{}{}",
            policy.name, policy.table, cmd, role_str, perm
        ));
        if let Some(ref using) = policy.using {
            output.push_str(&format!("\n  using $$ {} $$", using));
        }
        if let Some(ref wc) = policy.with_check {
            output.push_str(&format!("\n  with_check $$ {} $$", wc));
        }
        output.push_str("\n\n");
    }

    // Grants
    for grant in &schema.grants {
        let privs: Vec<String> = grant
            .privileges
            .iter()
            .map(|p| p.to_string().to_lowercase())
            .collect();
        match grant.action {
            GrantAction::Grant => {
                output.push_str(&format!(
                    "grant {} on {} to {}\n",
                    privs.join(", "),
                    grant.on_object,
                    grant.to_role
                ));
            }
            GrantAction::Revoke => {
                output.push_str(&format!(
                    "revoke {} on {} from {}\n",
                    privs.join(", "),
                    grant.on_object,
                    grant.to_role
                ));
            }
        }
    }
    if !schema.grants.is_empty() {
        output.push('\n');
    }

    // Comments last (tables must exist first)
    for comment in &schema.comments {
        let text = quote_qail_string(&comment.text);
        match &comment.target {
            CommentTarget::Table(t) => {
                output.push_str(&format!("comment on {} {}\n", t, text));
            }
            CommentTarget::Column { table, column } => {
                output.push_str(&format!("comment on {}.{} {}\n", table, column, text));
            }
            CommentTarget::Raw(target) => {
                output.push_str(&format!("comment on {} {}\n", target, text));
            }
        }
    }

    output
}

fn quote_qail_string(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

/// Convert a Schema to a list of Qail commands (CREATE TABLE, CREATE INDEX).
/// Used by shadow migration to apply the base schema before applying diffs.
pub fn schema_to_commands(schema: &Schema) -> Vec<crate::ast::Qail> {
    use crate::ast::{Action, ColumnGeneration, Constraint, Expr, IndexDef, Qail};

    let mut cmds = Vec::new();

    // Topologically sort tables by FK dependencies:
    // referenced targets must be created before dependent tables.
    let mut indegree: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut reverse_adj: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    for name in schema.tables.keys() {
        indegree.insert(name.clone(), 0);
    }

    for table in schema.tables.values() {
        let mut deps = std::collections::HashSet::new();
        for col in &table.columns {
            if let Some(fk) = &col.foreign_key
                && fk.table != table.name
                && schema.tables.contains_key(&fk.table)
            {
                deps.insert(fk.table.clone());
            }
        }
        for fk in &table.multi_column_fks {
            if fk.ref_table != table.name && schema.tables.contains_key(&fk.ref_table) {
                deps.insert(fk.ref_table.clone());
            }
        }

        indegree.insert(table.name.clone(), deps.len());
        for dep in deps {
            reverse_adj.entry(dep).or_default().push(table.name.clone());
        }
    }

    let mut ready = std::collections::BTreeSet::new();
    for (name, deg) in &indegree {
        if *deg == 0 {
            ready.insert(name.clone());
        }
    }

    let mut ordered_names: Vec<String> = Vec::with_capacity(schema.tables.len());
    while let Some(next) = ready.pop_first() {
        ordered_names.push(next.clone());
        if let Some(dependents) = reverse_adj.get(&next) {
            for dep_name in dependents {
                if let Some(d) = indegree.get_mut(dep_name)
                    && *d > 0
                {
                    *d -= 1;
                    if *d == 0 {
                        ready.insert(dep_name.clone());
                    }
                }
            }
        }
    }

    // If there is an FK cycle, append remaining names in lexical order
    // so output is deterministic (runtime may still reject unresolved cycle).
    if ordered_names.len() < schema.tables.len() {
        let mut leftovers: Vec<String> = schema
            .tables
            .keys()
            .filter(|name| !ordered_names.contains(*name))
            .cloned()
            .collect();
        leftovers.sort();
        ordered_names.extend(leftovers);
    }

    for table_name in ordered_names {
        let table = &schema.tables[&table_name];
        // Build columns using Expr::Def exactly like diff.rs does
        let columns: Vec<Expr> = table
            .columns
            .iter()
            .map(|col| {
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
                    constraints.push(Constraint::References(foreign_key_to_sql(fk)));
                }
                if let Some(check) = &col.check {
                    let check_sql = check_expr_to_sql(&check.expr);
                    if let Some(name) = &check.name {
                        constraints.push(Constraint::Check(vec![format!(
                            "CONSTRAINT {} CHECK ({})",
                            name, check_sql
                        )]));
                    } else {
                        constraints.push(Constraint::Check(vec![check_sql]));
                    }
                }
                if let Some(generated) = &col.generated {
                    let gen_constraint = match generated {
                        Generated::AlwaysStored(expr) => {
                            Constraint::Generated(ColumnGeneration::Stored(expr.clone()))
                        }
                        Generated::AlwaysIdentity => {
                            Constraint::Generated(ColumnGeneration::Stored("identity".to_string()))
                        }
                        Generated::ByDefaultIdentity => Constraint::Generated(
                            ColumnGeneration::Stored("identity_by_default".to_string()),
                        ),
                    };
                    constraints.push(gen_constraint);
                }

                Expr::Def {
                    name: col.name.clone(),
                    data_type: col.data_type.to_pg_type(),
                    constraints,
                }
            })
            .collect();

        cmds.push(Qail {
            action: Action::Make,
            table: table.name.clone(),
            columns,
            ..Default::default()
        });

        if table.enable_rls {
            cmds.push(Qail {
                action: Action::AlterEnableRls,
                table: table.name.clone(),
                ..Default::default()
            });
        }
        if table.force_rls {
            cmds.push(Qail {
                action: Action::AlterForceRls,
                table: table.name.clone(),
                ..Default::default()
            });
        }
    }

    // Add indexes using IndexDef like diff.rs
    for idx in &schema.indexes {
        cmds.push(Qail {
            action: Action::Index,
            table: String::new(),
            index_def: Some(IndexDef {
                name: idx.name.clone(),
                table: idx.table.clone(),
                columns: if !idx.expressions.is_empty() {
                    idx.expressions.clone()
                } else {
                    idx.columns.clone()
                },
                unique: idx.unique,
                index_type: Some(index_method_str(&idx.method).to_string()),
                include: idx.include.clone(),
                concurrently: idx.concurrently,
                where_clause: idx.where_clause.as_ref().map(check_expr_to_sql),
            }),
            ..Default::default()
        });
    }

    let mut fk_table_names: Vec<&String> = schema
        .tables
        .iter()
        .filter(|(_, table)| !table.multi_column_fks.is_empty())
        .map(|(name, _)| name)
        .collect();
    fk_table_names.sort();
    for table_name in fk_table_names {
        let table = &schema.tables[table_name];
        for fk in &table.multi_column_fks {
            cmds.push(multi_column_fk_to_alter_command(&table.name, fk));
        }
    }

    cmds
}

pub(super) fn multi_column_fk_to_table_constraint(
    fk: &MultiColumnForeignKey,
) -> crate::ast::TableConstraint {
    crate::ast::TableConstraint::ForeignKey {
        name: fk.name.clone(),
        columns: fk.columns.clone(),
        ref_table: fk.ref_table.clone(),
        ref_columns: fk.ref_columns.clone(),
        on_delete: (fk.on_delete != FkAction::NoAction)
            .then(|| fk_action_to_sql(&fk.on_delete).to_string()),
        on_update: (fk.on_update != FkAction::NoAction)
            .then(|| fk_action_to_sql(&fk.on_update).to_string()),
        deferrable: deferrable_to_sql(&fk.deferrable).map(str::to_string),
    }
}

pub(super) fn multi_column_fk_to_alter_command(
    table_name: &str,
    fk: &MultiColumnForeignKey,
) -> crate::ast::Qail {
    crate::ast::Qail {
        action: crate::ast::Action::Alter,
        table: table_name.to_string(),
        table_constraints: vec![multi_column_fk_to_table_constraint(fk)],
        ..Default::default()
    }
}

fn fk_action_to_sql(action: &FkAction) -> &'static str {
    match action {
        FkAction::NoAction => "NO ACTION",
        FkAction::Cascade => "CASCADE",
        FkAction::SetNull => "SET NULL",
        FkAction::SetDefault => "SET DEFAULT",
        FkAction::Restrict => "RESTRICT",
    }
}

fn deferrable_to_sql(deferrable: &Deferrable) -> Option<&'static str> {
    match deferrable {
        Deferrable::NotDeferrable => None,
        Deferrable::Deferrable => Some("DEFERRABLE"),
        Deferrable::InitiallyDeferred => Some("DEFERRABLE INITIALLY DEFERRED"),
        Deferrable::InitiallyImmediate => Some("DEFERRABLE INITIALLY IMMEDIATE"),
    }
}

pub(crate) fn foreign_key_to_sql(fk: &ForeignKey) -> String {
    let mut target = format!("{}({})", fk.table, fk.column);
    if fk.on_delete != FkAction::NoAction {
        target.push_str(" ON DELETE ");
        target.push_str(fk_action_to_sql(&fk.on_delete));
    }
    if fk.on_update != FkAction::NoAction {
        target.push_str(" ON UPDATE ");
        target.push_str(fk_action_to_sql(&fk.on_update));
    }
    if let Some(def) = deferrable_to_sql(&fk.deferrable) {
        target.push(' ');
        target.push_str(def);
    }
    target
}

pub(crate) fn check_expr_to_sql(expr: &CheckExpr) -> String {
    match expr {
        CheckExpr::GreaterThan { column, value } => format!("{column} > {value}"),
        CheckExpr::GreaterOrEqual { column, value } => format!("{column} >= {value}"),
        CheckExpr::LessThan { column, value } => format!("{column} < {value}"),
        CheckExpr::LessOrEqual { column, value } => format!("{column} <= {value}"),
        CheckExpr::Between { column, low, high } => format!("{column} BETWEEN {low} AND {high}"),
        CheckExpr::In { column, values } => {
            if values.len() == 1 && looks_like_raw_check_expr(&values[0]) {
                return values[0].clone();
            }
            let quoted = values
                .iter()
                .map(|v| format!("'{}'", v.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{column} IN ({quoted})")
        }
        CheckExpr::InIntegers { column, values } => format!(
            "{column} IN ({})",
            values
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        CheckExpr::CompareColumns {
            left_column,
            op,
            right_column,
        } => format!("{left_column} {} {right_column}", op.as_sql_str()),
        CheckExpr::TextCompare { column, op, value } => {
            format!(
                "{column} {} {}",
                op.as_sql_str(),
                format_sql_text_literal(value)
            )
        }
        CheckExpr::CompareColumnToCoalesce {
            left_column,
            op,
            coalesce_column,
            fallback,
            fallback_cast,
        } => format!(
            "{left_column} {} COALESCE({coalesce_column}, {})",
            op.as_sql_str(),
            format_sql_text_literal_with_cast(fallback, fallback_cast)
        ),
        CheckExpr::LowerTrimEquals { column } => format!("{column} = lower(btrim({column}))"),
        CheckExpr::Regex { column, pattern } => {
            format!("{column} ~ {}", format_sql_text_literal(pattern))
        }
        CheckExpr::MaxLength { column, max } => format!("char_length({column}) <= {max}"),
        CheckExpr::MinLength { column, min } => format!("char_length({column}) >= {min}"),
        CheckExpr::NotNull { column } => format!("{column} IS NOT NULL"),
        CheckExpr::And(left, right) => {
            format!(
                "({}) AND ({})",
                check_expr_to_sql(left),
                check_expr_to_sql(right)
            )
        }
        CheckExpr::Or(left, right) => {
            format!(
                "({}) OR ({})",
                check_expr_to_sql(left),
                check_expr_to_sql(right)
            )
        }
        CheckExpr::Not(inner) => format!("NOT ({})", check_expr_to_sql(inner)),
        CheckExpr::Sql(sql) => sql.clone(),
    }
}

fn looks_like_raw_check_expr(s: &str) -> bool {
    s.chars()
        .any(|c| c.is_whitespace() || matches!(c, '<' | '>' | '=' | '!' | '(' | ')' | ':'))
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
    fn test_to_qail_string_preserves_vector_index_methods() {
        let mut schema = Schema::new();
        schema.add_index(
            Index::new(
                "idx_docs_embedding_hnsw",
                "documents",
                vec!["embedding vector_l2_ops".into()],
            )
            .using(IndexMethod::Hnsw),
        );
        schema.add_index(
            Index::new(
                "idx_docs_embedding_ivfflat",
                "documents",
                vec!["embedding vector_cosine_ops".into()],
            )
            .using(IndexMethod::IvfFlat),
        );

        let output = to_qail_string(&schema);

        assert!(output.contains(
            "index idx_docs_embedding_hnsw on documents using hnsw (embedding vector_l2_ops)"
        ));
        assert!(output.contains(
            "index idx_docs_embedding_ivfflat on documents using ivfflat (embedding vector_cosine_ops)"
        ));
    }

    #[test]
    fn test_to_qail_string_preserves_covering_concurrent_index_options() {
        let mut schema = Schema::new();
        schema.add_index(
            Index::new("idx_users_email_cover", "users", vec!["email".into()])
                .unique()
                .include(vec!["name".into(), "created_at".into()])
                .concurrently()
                .partial(CheckExpr::Sql("deleted_at IS NULL".to_string())),
        );

        let output = to_qail_string(&schema);

        assert!(output.contains(
            "unique index concurrently idx_users_email_cover on users (email) include (name, created_at) where deleted_at IS NULL"
        ));
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
    fn test_to_qail_string_includes_function_volatility() {
        let mut schema = Schema::new();
        let func = SchemaFunctionDef::new(
            "is_super_admin",
            "boolean",
            "BEGIN RETURN true; END;".to_string(),
        )
        .language("plpgsql")
        .volatility("stable");
        schema.add_function(func);

        let output = to_qail_string(&schema);
        assert!(
            output.contains("function is_super_admin() returns boolean language plpgsql stable $$")
        );
    }

    #[test]
    fn test_invalid_primary_key_type_strict() {
        let err = Column::new("data", ColumnType::Jsonb)
            .try_primary_key()
            .expect_err("JSONB should be rejected by strict PK policy");
        assert!(err.contains("cannot be a primary key"));
    }

    #[test]
    fn test_invalid_primary_key_type_fail_soft() {
        let col = Column::new("data", ColumnType::Jsonb).primary_key();
        assert!(col.primary_key);
        assert!(!col.nullable);
    }

    #[test]
    fn test_invalid_unique_type_strict() {
        let err = Column::new("data", ColumnType::Jsonb)
            .try_unique()
            .expect_err("JSONB should be rejected by strict UNIQUE policy");
        assert!(err.contains("cannot have UNIQUE"));
    }

    #[test]
    fn test_invalid_unique_type_fail_soft() {
        let col = Column::new("data", ColumnType::Jsonb).unique();
        assert!(col.unique);
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

    #[test]
    fn test_multi_column_foreign_key_invalid_table_and_columns() {
        let mut schema = Schema::new();
        schema.add_table(
            Table::new("trips")
                .column(Column::new("route_id", ColumnType::Text))
                .foreign_key(MultiColumnForeignKey::new(
                    vec!["route_id".to_string(), "schedule_id".to_string()],
                    "schedules",
                    vec!["route_id".to_string(), "schedule_id".to_string()],
                )),
        );

        let errors = schema
            .validate()
            .expect_err("invalid composite FK should fail validation");
        assert!(
            errors
                .iter()
                .any(|err| err.contains("non-existent source column 'trips.schedule_id'")),
            "{errors:?}"
        );
        assert!(
            errors
                .iter()
                .any(|err| err.contains("non-existent table 'schedules'")),
            "{errors:?}"
        );
    }

    #[test]
    fn test_multi_column_foreign_key_invalid_target_column_and_arity() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("schedules").column(Column::new("route_id", ColumnType::Text)));
        schema.add_table(
            Table::new("trips")
                .column(Column::new("route_id", ColumnType::Text))
                .foreign_key(MultiColumnForeignKey::new(
                    vec!["route_id".to_string()],
                    "schedules",
                    vec!["route_id".to_string(), "schedule_id".to_string()],
                )),
        );

        let errors = schema
            .validate()
            .expect_err("invalid composite FK should fail validation");
        assert!(
            errors.iter().any(|err| err.contains("column count 1")),
            "{errors:?}"
        );
        assert!(
            errors
                .iter()
                .any(|err| err.contains("non-existent column 'schedules.schedule_id'")),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_rejects_duplicate_columns() {
        let mut schema = Schema::new();
        schema.add_table(
            Table::new("users")
                .column(Column::new("email", ColumnType::Text))
                .column(Column::new("email", ColumnType::Text)),
        );

        let errors = schema
            .validate()
            .expect_err("duplicate columns should fail validation");
        assert!(
            errors
                .iter()
                .any(|err| err.contains("duplicate column 'email'")),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_rejects_duplicate_index_names() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("users").column(Column::new("email", ColumnType::Text)));
        schema.add_index(Index::new(
            "idx_users_email",
            "users",
            vec!["email".to_string()],
        ));
        schema.add_index(Index::new(
            "idx_users_email",
            "users",
            vec!["email".to_string()],
        ));

        let errors = schema
            .validate()
            .expect_err("duplicate indexes should fail validation");
        assert!(
            errors
                .iter()
                .any(|err| err.contains("duplicate index name 'idx_users_email'")),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_rejects_check_on_missing_column() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("orders").column(
            Column::new("status", ColumnType::Text).check(CheckExpr::In {
                column: "missing_status".to_string(),
                values: vec!["paid".to_string(), "pending".to_string()],
            }),
        ));

        let errors = schema
            .validate()
            .expect_err("CHECK references should fail validation");
        assert!(
            errors.iter().any(|err| {
                err.contains("CHECK error")
                    && err.contains("orders.status")
                    && err.contains("orders.missing_status")
            }),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_rejects_nested_check_on_missing_column() {
        let mut schema = Schema::new();
        schema.add_table(
            Table::new("pricing_plans")
                .column(Column::new("start_date", ColumnType::Date))
                .column(
                    Column::new("end_date", ColumnType::Date).check(CheckExpr::And(
                        Box::new(CheckExpr::CompareColumns {
                            left_column: "end_date".to_string(),
                            op: CheckComparisonOp::GreaterOrEqual,
                            right_column: "start_date".to_string(),
                        }),
                        Box::new(CheckExpr::CompareColumnToCoalesce {
                            left_column: "end_date".to_string(),
                            op: CheckComparisonOp::GreaterOrEqual,
                            coalesce_column: "missing_fallback_date".to_string(),
                            fallback: "1970-01-01".to_string(),
                            fallback_cast: Some("date".to_string()),
                        }),
                    )),
                ),
        );

        let errors = schema
            .validate()
            .expect_err("nested CHECK references should fail validation");
        assert!(
            errors
                .iter()
                .any(|err| err.contains("pricing_plans.missing_fallback_date")),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_rejects_index_on_missing_table_or_column() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("users").column(Column::new("email", ColumnType::Text)));
        schema.add_index(Index::new(
            "idx_missing_table",
            "profiles",
            vec!["email".to_string()],
        ));
        schema.add_index(Index::new(
            "idx_missing_column",
            "users",
            vec!["username".to_string()],
        ));

        let errors = schema
            .validate()
            .expect_err("invalid indexes should fail validation");
        assert!(
            errors
                .iter()
                .any(|err| err.contains("idx_missing_table") && err.contains("profiles")),
            "{errors:?}"
        );
        assert!(
            errors
                .iter()
                .any(|err| err.contains("idx_missing_column") && err.contains("users.username")),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_rejects_empty_index_definition() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("users").column(Column::new("email", ColumnType::Text)));
        schema.add_index(Index::new("idx_users_empty", "users", vec![]));

        let errors = schema
            .validate()
            .expect_err("empty index definitions should fail validation");
        assert!(
            errors.iter().any(|err| {
                err.contains("idx_users_empty") && err.contains("at least one column or expression")
            }),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_rejects_blank_index_column_fragment() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("users").column(Column::new("email", ColumnType::Text)));
        schema.add_index(Index::new(
            "idx_users_blank",
            "users",
            vec![" ".to_string()],
        ));

        let errors = schema
            .validate()
            .expect_err("blank index columns should fail validation");
        assert!(
            errors
                .iter()
                .any(|err| err.contains("idx_users_blank") && err.contains("empty column")),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_rejects_mixed_index_columns_and_expressions() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("users").column(Column::new("email", ColumnType::Text)));
        let mut index = Index::expression(
            "idx_users_email_lower",
            "users",
            vec!["lower(email)".to_string()],
        );
        index.columns.push("email".to_string());
        schema.add_index(index);

        let errors = schema
            .validate()
            .expect_err("mixed index keys should fail validation");
        assert!(
            errors.iter().any(|err| {
                err.contains("idx_users_email_lower")
                    && err.contains("cannot mix columns and expressions")
            }),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_rejects_missing_index_include_column() {
        let mut schema = Schema::new();
        schema.add_table(
            Table::new("users")
                .column(Column::new("email", ColumnType::Text))
                .column(Column::new("created_at", ColumnType::Timestamp)),
        );
        schema.add_index(
            Index::new("idx_users_email_cover", "users", vec!["email".to_string()])
                .include(vec!["name".to_string()]),
        );

        let errors = schema
            .validate()
            .expect_err("invalid INCLUDE column should fail validation");
        assert!(
            errors
                .iter()
                .any(|err| { err.contains("idx_users_email_cover") && err.contains("users.name") }),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_rejects_missing_partial_index_predicate_column() {
        let mut schema = Schema::new();
        schema.add_table(Table::new("users").column(Column::new("email", ColumnType::Text)));
        schema.add_index(
            Index::new("idx_users_active_email", "users", vec!["email".to_string()]).partial(
                CheckExpr::NotNull {
                    column: "deleted_at".to_string(),
                },
            ),
        );

        let errors = schema
            .validate()
            .expect_err("invalid partial-index predicates should fail validation");
        assert!(
            errors.iter().any(|err| {
                err.contains("idx_users_active_email") && err.contains("users.deleted_at")
            }),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_allows_index_sort_direction_and_opclass_columns() {
        let mut schema = Schema::new();
        schema.add_table(
            Table::new("documents")
                .column(Column::new(
                    "embedding",
                    ColumnType::Array(Box::new(ColumnType::Float)),
                ))
                .column(Column::new("created_at", ColumnType::Timestamptz)),
        );
        schema.add_index(
            Index::new(
                "idx_docs_embedding_hnsw",
                "documents",
                vec!["embedding vector_l2_ops".to_string()],
            )
            .using(IndexMethod::Hnsw),
        );
        schema.add_index(Index::new(
            "idx_docs_created_at",
            "documents",
            vec!["created_at DESC NULLS LAST".to_string()],
        ));

        assert!(schema.validate().is_ok());
    }

    #[test]
    fn test_schema_to_commands_preserves_fk_actions_and_checks() {
        let mut schema = Schema::new();
        schema.add_table(
            Table::new("orgs").column(Column::new("id", ColumnType::Uuid).primary_key()),
        );
        schema.add_table(
            Table::new("users")
                .column(Column::new("id", ColumnType::Uuid).primary_key())
                .column(
                    Column::new("org_id", ColumnType::Uuid)
                        .references("orgs", "id")
                        .on_delete(FkAction::Cascade)
                        .on_update(FkAction::Restrict),
                )
                .column(
                    Column::new("age", ColumnType::Int).check(CheckExpr::GreaterOrEqual {
                        column: "age".to_string(),
                        value: 18,
                    }),
                ),
        );

        let cmds = schema_to_commands(&schema);
        let users_cmd = cmds
            .iter()
            .find(|c| c.action == crate::ast::Action::Make && c.table == "users")
            .expect("users create command should exist");
        let org_id_constraints = users_cmd
            .columns
            .iter()
            .find_map(|e| match e {
                crate::ast::Expr::Def {
                    name, constraints, ..
                } if name == "org_id" => Some(constraints),
                _ => None,
            })
            .expect("org_id should exist");
        let age_constraints = users_cmd
            .columns
            .iter()
            .find_map(|e| match e {
                crate::ast::Expr::Def {
                    name, constraints, ..
                } if name == "age" => Some(constraints),
                _ => None,
            })
            .expect("age should exist");

        assert!(
            org_id_constraints.iter().any(|c| matches!(
                c,
                crate::ast::Constraint::References(target)
                if target.contains("orgs(id)")
                    && target.contains("ON DELETE CASCADE")
                    && target.contains("ON UPDATE RESTRICT")
            )),
            "foreign key action clauses should be preserved"
        );
        assert!(
            age_constraints
                .iter()
                .any(|c| matches!(c, crate::ast::Constraint::Check(vals) if vals.len() == 1)),
            "check expressions should be preserved"
        );
    }

    #[test]
    fn schema_to_commands_preserves_table_rls_flags() {
        let mut docs = Table::new("docs").column(Column::new("id", ColumnType::Uuid).primary_key());
        docs.enable_rls = true;
        docs.force_rls = true;

        let mut schema = Schema::new();
        schema.add_table(docs);

        let cmds = schema_to_commands(&schema);
        let make_idx = cmds
            .iter()
            .position(|cmd| cmd.action == crate::ast::Action::Make && cmd.table == "docs")
            .expect("table create command should exist");
        let enable_idx = cmds
            .iter()
            .position(|cmd| cmd.action == crate::ast::Action::AlterEnableRls && cmd.table == "docs")
            .expect("enable RLS command should exist");
        let force_idx = cmds
            .iter()
            .position(|cmd| cmd.action == crate::ast::Action::AlterForceRls && cmd.table == "docs")
            .expect("force RLS command should exist");

        assert!(make_idx < enable_idx);
        assert!(enable_idx < force_idx);
    }

    #[test]
    fn schema_to_commands_preserves_multi_column_foreign_keys() {
        use crate::transpiler::ToSql;

        let mut schema = Schema::new();
        schema.add_table(
            Table::new("schedules")
                .column(Column::new("route_id", ColumnType::Text))
                .column(Column::new("schedule_id", ColumnType::Text)),
        );
        schema.add_index(
            Index::new(
                "idx_schedules_route_schedule",
                "schedules",
                vec!["route_id".to_string(), "schedule_id".to_string()],
            )
            .unique(),
        );
        schema.add_table(
            Table::new("trips")
                .column(Column::new("route_id", ColumnType::Text))
                .column(Column::new("schedule_id", ColumnType::Text))
                .foreign_key(
                    MultiColumnForeignKey::new(
                        vec!["route_id".to_string(), "schedule_id".to_string()],
                        "schedules",
                        vec!["route_id".to_string(), "schedule_id".to_string()],
                    )
                    .named("fk_trips_schedule")
                    .on_delete(FkAction::Cascade)
                    .on_update(FkAction::Restrict)
                    .initially_deferred(),
                ),
        );

        let cmds = schema_to_commands(&schema);
        let schedules_idx = cmds
            .iter()
            .position(|c| c.action == crate::ast::Action::Make && c.table == "schedules")
            .expect("schedules create command should exist");
        let trips_idx = cmds
            .iter()
            .position(|c| c.action == crate::ast::Action::Make && c.table == "trips")
            .expect("trips create command should exist");
        let unique_idx = cmds
            .iter()
            .position(|c| {
                c.action == crate::ast::Action::Index
                    && c.index_def
                        .as_ref()
                        .is_some_and(|idx| idx.name == "idx_schedules_route_schedule")
            })
            .expect("unique index command should exist");
        let add_fk_idx = cmds
            .iter()
            .position(|c| c.action == crate::ast::Action::Alter && c.table == "trips")
            .expect("trips composite foreign key ALTER command should exist");

        assert!(schedules_idx < unique_idx);
        assert!(trips_idx < unique_idx);
        assert!(unique_idx < add_fk_idx);

        let trips_cmd = cmds
            .iter()
            .find(|c| c.action == crate::ast::Action::Make && c.table == "trips")
            .expect("trips create command should exist");
        assert!(
            trips_cmd.table_constraints.is_empty(),
            "composite foreign keys should not be emitted inline on CREATE TABLE"
        );

        let add_fk_cmd = &cmds[add_fk_idx];
        assert!(
            add_fk_cmd
                .table_constraints
                .iter()
                .any(|constraint| matches!(
                        constraint,
                        crate::ast::TableConstraint::ForeignKey {
                            name,
                            columns,
                            ref_table,
                            ref_columns,
                            on_delete,
                            on_update,
                            deferrable,
                        } if columns == &["route_id", "schedule_id"]
                            && name.as_deref() == Some("fk_trips_schedule")
                            && ref_table == "schedules"
                            && ref_columns == &["route_id", "schedule_id"]
                            && on_delete.as_deref() == Some("CASCADE")
                            && on_update.as_deref() == Some("RESTRICT")
                            && deferrable.as_deref() == Some("DEFERRABLE INITIALLY DEFERRED")
                )),
            "multi-column foreign key should be represented in generated commands"
        );

        let sql = add_fk_cmd.to_sql();
        assert!(
            sql.contains(
                "ALTER TABLE trips ADD CONSTRAINT fk_trips_schedule FOREIGN KEY (route_id, schedule_id) REFERENCES schedules(route_id, schedule_id) ON DELETE CASCADE ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED"
            ),
            "generated SQL should include composite foreign key, got: {sql}"
        );
    }

    #[test]
    fn test_check_expr_sql_renders_integer_in_and_column_comparison() {
        assert_eq!(
            check_expr_to_sql(&CheckExpr::InIntegers {
                column: "duration_hours".to_string(),
                values: vec![8, 10, 12],
            }),
            "duration_hours IN (8, 10, 12)"
        );

        assert_eq!(
            check_expr_to_sql(&CheckExpr::CompareColumns {
                left_column: "origin_harbor_id".to_string(),
                op: CheckComparisonOp::NotEqual,
                right_column: "destination_harbor_id".to_string(),
            }),
            "origin_harbor_id <> destination_harbor_id"
        );

        assert_eq!(
            check_expr_to_sql(&CheckExpr::TextCompare {
                column: "module".to_string(),
                op: CheckComparisonOp::NotEqual,
                value: "charter".to_string(),
            }),
            "module <> 'charter'"
        );

        assert_eq!(
            check_expr_to_sql(&CheckExpr::CompareColumnToCoalesce {
                left_column: "start_date".to_string(),
                op: CheckComparisonOp::LessOrEqual,
                coalesce_column: "end_date".to_string(),
                fallback: "2099-12-31".to_string(),
                fallback_cast: Some("date".to_string()),
            }),
            "start_date <= COALESCE(end_date, '2099-12-31'::date)"
        );

        assert_eq!(
            check_expr_to_sql(&CheckExpr::LowerTrimEquals {
                column: "slug".to_string(),
            }),
            "slug = lower(btrim(slug))"
        );
    }
}
