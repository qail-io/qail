use crate::ast::{
    Action, Cage, CageKind, Condition, Distance, Expr, GroupByMode, IndexDef, Join, LockMode,
    LogicalOp, Operator, OverridingKind, SampleMethod, SetOp, TableConstraint, Value,
};

/// The core Qail AST node representing a single database operation.
#[derive(Debug, Clone, PartialEq)]
pub struct Qail {
    /// SQL action to perform.
    pub action: Action,
    /// Target table name.
    pub table: String,
    /// Selected / inserted / modified columns.
    pub columns: Vec<Expr>,
    /// Join clauses.
    pub joins: Vec<Join>,
    /// Filter / sort / group / limit cages.
    pub cages: Vec<Cage>,
    /// SELECT DISTINCT.
    pub distinct: bool,
    /// Index definition for CREATE INDEX.
    pub index_def: Option<IndexDef>,
    /// Table-level constraints (composite UNIQUE / PK).
    pub table_constraints: Vec<TableConstraint>,
    /// UNION / INTERSECT / EXCEPT operations.
    pub set_ops: Vec<(SetOp, Box<Qail>)>,
    /// HAVING clause conditions.
    pub having: Vec<Condition>,
    /// GROUP BY mode (simple, rollup, cube, grouping sets).
    pub group_by_mode: GroupByMode,
    /// Common table expressions (WITH).
    pub ctes: Vec<CTEDef>,
    /// DISTINCT ON columns.
    pub distinct_on: Vec<Expr>,
    /// RETURNING clause.
    pub returning: Option<Vec<Expr>>,
    /// ON CONFLICT clause for upsert.
    pub on_conflict: Option<OnConflict>,
    /// INSERT … SELECT source query.
    pub source_query: Option<Box<Qail>>,
    /// LISTEN/NOTIFY channel.
    pub channel: Option<String>,
    /// NOTIFY payload.
    pub payload: Option<String>,
    /// SAVEPOINT name.
    pub savepoint_name: Option<String>,
    /// UPDATE … FROM additional tables.
    pub from_tables: Vec<String>,
    /// DELETE … USING additional tables.
    pub using_tables: Vec<String>,
    /// Row locking (FOR UPDATE / FOR SHARE).
    pub lock_mode: Option<LockMode>,
    /// SKIP LOCKED modifier for row locking (FOR UPDATE SKIP LOCKED).
    pub skip_locked: bool,
    /// FETCH FIRST n ROWS [ONLY|WITH TIES].
    pub fetch: Option<(u64, bool)>,
    /// INSERT with DEFAULT VALUES.
    pub default_values: bool,
    /// OVERRIDING clause for generated columns.
    pub overriding: Option<OverridingKind>,
    /// TABLESAMPLE method, percentage, and optional seed.
    pub sample: Option<(SampleMethod, f64, Option<u64>)>,
    /// SELECT FROM ONLY (exclude inheritance).
    pub only_table: bool,
    // Vector database fields (Qdrant)
    /// Search vector for similarity queries.
    pub vector: Option<Vec<f32>>,
    /// Minimum score threshold.
    pub score_threshold: Option<f32>,
    /// Named vector in multi-vector collections.
    pub vector_name: Option<String>,
    /// Include vector data in results.
    pub with_vector: bool,
    /// Vector dimensionality.
    pub vector_size: Option<u64>,
    /// Distance metric.
    pub distance: Option<Distance>,
    /// Store vectors on disk.
    pub on_disk: Option<bool>,
    // PostgreSQL procedural objects
    /// Function definition.
    pub function_def: Option<crate::ast::FunctionDef>,
    /// Trigger definition.
    pub trigger_def: Option<crate::ast::TriggerDef>,
}

/// Common Table Expression (WITH clause) definition.
#[derive(Debug, Clone, PartialEq)]
pub struct CTEDef {
    /// Alias name used to reference this CTE elsewhere in the query.
    pub name: String,
    /// Whether this is a recursive CTE.
    pub recursive: bool,
    /// Explicit column list.
    pub columns: Vec<String>,
    /// The base query.
    pub base_query: Box<Qail>,
    /// Recursive part (UNION ALL).
    pub recursive_query: Option<Box<Qail>>,
    /// Source table for data-modifying CTEs.
    pub source_table: Option<String>,
}

/// ON CONFLICT clause for upsert.
#[derive(Debug, Clone, PartialEq)]
pub struct OnConflict {
    /// Conflict target columns.
    pub columns: Vec<String>,
    /// What to do on conflict.
    pub action: ConflictAction,
}

/// Action to take on an INSERT conflict.
#[derive(Debug, Clone, PartialEq)]
pub enum ConflictAction {
    /// DO NOTHING.
    DoNothing,
    /// DO UPDATE SET.
    DoUpdate {
        /// Column = expression assignments.
        assignments: Vec<(String, Expr)>,
    },
}

impl Default for OnConflict {
    fn default() -> Self {
        Self {
            columns: vec![],
            action: ConflictAction::DoNothing,
        }
    }
}

impl Default for Qail {
    fn default() -> Self {
        Self {
            action: Action::Get,
            table: String::new(),
            columns: vec![],
            joins: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
            channel: None,
            payload: None,
            savepoint_name: None,
            from_tables: vec![],
            using_tables: vec![],
            lock_mode: None,
            skip_locked: false,
            fetch: None,
            default_values: false,
            overriding: None,
            sample: None,
            only_table: false,
            // Vector database fields
            vector: None,
            score_threshold: None,
            vector_name: None,
            with_vector: false,
            vector_size: None,
            distance: None,
            on_disk: None,
            // Procedural objects
            function_def: None,
            trigger_def: None,
        }
    }
}

// Submodules with builder methods
mod advanced;
mod constructors;
mod cte;
mod query;
mod rls;
mod vector;

// Deprecated methods kept in main module for backward compatibility
impl Qail {
    /// Set columns for the query (deprecated alias for `.columns()`).
    #[deprecated(since = "0.11.0", note = "Use .columns([...]) instead")]
    pub fn hook(mut self, cols: &[&str]) -> Self {
        self.columns = cols.iter().map(|c| Expr::Named(c.to_string())).collect();
        self
    }

    /// Add an equality filter (deprecated alias for `.where_eq()`).
    #[deprecated(
        since = "0.11.0",
        note = "Use .filter(column, Operator::Eq, value) or .where_eq(column, value) instead"
    )]
    pub fn cage(mut self, column: &str, value: impl Into<Value>) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named(column.to_string()),
                op: Operator::Eq,
                value: value.into(),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });
        self
    }
}

impl std::fmt::Display for Qail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use the Formatter from the fmt module for canonical output
        use crate::fmt::Formatter;
        match Formatter::new().format(self) {
            Ok(s) => write!(f, "{}", s),
            Err(_) => write!(f, "{:?}", self), // Fallback to Debug
        }
    }
}
