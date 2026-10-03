use crate::ast::{
    Action, Cage, Condition, Distance, Expr, GroupByMode, IndexDef, Join, LockMode, OverridingKind,
    SampleMethod, SetOp, TableConstraint,
};

/// The core Qail AST node representing a single database operation.
#[derive(Debug, Clone, PartialEq)]
pub struct Qail {
    /// SQL action to perform.
    pub action: Action,
    /// Target table name.
    pub table: String,
    /// Typed FROM item that replaces `table` in a SELECT (`FROM (subquery) AS t`,
    /// `FROM generate_series(..) AS t`). `table` must equal the source alias:
    /// scoping and policy lookups key on `table`, so a mismatch is rejected.
    pub from_source: Option<FromSource>,
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
    /// `RETURNING WITH (OLD AS .., NEW AS ..)` row-alias renames (PostgreSQL 18).
    pub returning_aliases: Option<ReturningAliases>,
    /// ON CONFLICT clause for upsert.
    pub on_conflict: Option<OnConflict>,
    /// Applied INSERT scope retained for later conflict-builder calls.
    ///
    /// These guards survive replacing the conflict action or target. The
    /// builders copy them into `OnConflict::where_conditions` for DO UPDATE.
    pub conflict_update_scope: Vec<Condition>,
    /// PostgreSQL MERGE specification.
    pub merge: Option<Merge>,
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
    /// NOWAIT modifier for row locking; exclusive with `skip_locked`.
    pub lock_nowait: bool,
    /// `FOR ... OF name, ...`: unqualified FROM names (tables or aliases) to lock.
    pub lock_of: Vec<String>,
    /// FETCH FIRST n ROWS [ONLY|WITH TIES].
    pub fetch: Option<(u64, bool)>,
    /// INSERT with DEFAULT VALUES. MERGE rejects it; set it on the INSERT arm.
    pub default_values: bool,
    /// OVERRIDING clause for generated columns. MERGE rejects it; set it on the INSERT arm.
    pub overriding: Option<OverridingKind>,
    /// TABLESAMPLE method, percentage, and optional seed.
    pub sample: Option<(SampleMethod, f64, Option<u64>)>,
    /// ONLY on the SELECT/UPDATE/DELETE/MERGE target (exclude inheritance).
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
    /// RLS policy definition.
    pub policy_def: Option<crate::migrate::policy::RlsPolicy>,
    /// `CREATE VIEW … WITH (security_invoker = true)`.
    ///
    /// Postgres evaluates a plain view against its base tables with the VIEW
    /// OWNER's privileges, so row-level security on those tables is checked as
    /// the owner rather than the caller — a view over an RLS-protected table is
    /// an RLS bypass unless this is set. Only meaningful for [`Action::CreateView`].
    pub view_security_invoker: bool,
    /// `CREATE VIEW … WITH (security_barrier = true)`: keeps caller-supplied
    /// functions from seeing rows the view's own WHERE filters out.
    /// Only meaningful for [`Action::CreateView`].
    pub view_security_barrier: bool,
    /// `CREATE VIEW … WITH LOCAL|CASCADED CHECK OPTION`.
    /// Only meaningful for [`Action::CreateView`].
    pub view_check_option: Option<crate::ast::ViewCheckOption>,
}

/// Common Table Expression (WITH clause) definition.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
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
    /// `AS [NOT] MATERIALIZED`; `None` leaves the choice to the planner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization: Option<CteMaterialization>,
    /// `SEARCH { DEPTH | BREADTH } FIRST BY ... SET ...` (recursive CTEs only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<CteSearch>,
    /// `CYCLE ... SET ... USING ...` (recursive CTEs only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cycle: Option<CteCycle>,
}

/// CTE materialization hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CteMaterialization {
    /// `AS MATERIALIZED`: evaluate once, as an optimization fence.
    Materialized,
    /// `AS NOT MATERIALIZED`: allow inlining into the outer query.
    NotMaterialized,
}

/// Recursive CTE traversal order for `SEARCH`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CteSearchOrder {
    /// `SEARCH DEPTH FIRST`.
    DepthFirst,
    /// `SEARCH BREADTH FIRST`.
    BreadthFirst,
}

/// `SEARCH { DEPTH | BREADTH } FIRST BY by... SET set_column`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CteSearch {
    /// Traversal order.
    pub order: CteSearchOrder,
    /// CTE output columns that order the traversal.
    pub by: Vec<String>,
    /// Added sequence column to `ORDER BY` in the outer query.
    pub set_column: String,
}

/// `CYCLE columns... SET set_column USING using_column`.
///
/// The mark column takes PostgreSQL's default boolean values; the
/// `TO value DEFAULT value` form is not modeled.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CteCycle {
    /// CTE output columns compared to detect a cycle.
    pub columns: Vec<String>,
    /// Added boolean column, true on the row that closes a cycle.
    pub set_column: String,
    /// Added path column that records visited rows.
    pub using_column: String,
}

/// Renames for the PostgreSQL 18 `OLD` / `NEW` row aliases in RETURNING:
/// `RETURNING WITH (OLD AS before, NEW AS after) ...`. Needed when the
/// target table or a source is itself named `old` or `new`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReturningAliases {
    /// Name for the `OLD` row (values before the write; NULL for INSERT).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    /// Name for the `NEW` row (values after the write; NULL for DELETE).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

impl ReturningAliases {
    /// The ` WITH (OLD AS x, NEW AS y)` text that follows `RETURNING`, or
    /// `None` when neither alias is set. Identifiers are emitted as given and
    /// must be validated by the caller.
    pub fn sql_parts(&self) -> Option<Vec<(&'static str, &str)>> {
        let mut parts = Vec::new();
        if let Some(before) = &self.before {
            parts.push(("OLD", before.as_str()));
        }
        if let Some(after) = &self.after {
            parts.push(("NEW", after.as_str()));
        }
        if parts.is_empty() { None } else { Some(parts) }
    }
}

/// Typed FROM item for a SELECT (PostgreSQL table expressions).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum FromSource {
    /// `FROM (SELECT ...) AS alias [(col, ...)]`.
    Subquery {
        /// Derived-table query (read-only SELECT).
        query: Box<Qail>,
        /// Required alias.
        alias: String,
        /// Optional column renames.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        column_aliases: Vec<String>,
    },
    /// `FROM func(args) [WITH ORDINALITY] AS alias [(col, ...)]` for a
    /// set-returning function such as `generate_series` or `unnest`.
    Function {
        /// Function name (optionally schema-qualified).
        name: String,
        /// Arguments.
        args: Vec<Expr>,
        /// Append an ordinality (`bigint`, 1-based) column.
        #[serde(default)]
        with_ordinality: bool,
        /// Required alias.
        alias: String,
        /// Optional column renames.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        column_aliases: Vec<String>,
    },
}

impl FromSource {
    /// `FROM (query) AS alias`.
    pub fn subquery(query: Qail, alias: impl Into<String>) -> Self {
        FromSource::Subquery {
            query: Box::new(query),
            alias: alias.into(),
            column_aliases: Vec::new(),
        }
    }

    /// `FROM name(args) AS alias`.
    pub fn function<I, E>(name: impl Into<String>, args: I, alias: impl Into<String>) -> Self
    where
        I: IntoIterator<Item = E>,
        E: Into<Expr>,
    {
        FromSource::Function {
            name: name.into(),
            args: args.into_iter().map(Into::into).collect(),
            with_ordinality: false,
            alias: alias.into(),
            column_aliases: Vec::new(),
        }
    }

    /// Rename the source's columns: `AS alias (c1, c2, ...)`.
    pub fn column_aliases<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let columns: Vec<String> = columns.into_iter().map(Into::into).collect();
        match &mut self {
            FromSource::Subquery { column_aliases, .. }
            | FromSource::Function { column_aliases, .. } => *column_aliases = columns,
        }
        self
    }

    /// `WITH ORDINALITY` (functions only; a no-op on a subquery source).
    pub fn with_ordinality(mut self) -> Self {
        if let FromSource::Function {
            with_ordinality, ..
        } = &mut self
        {
            *with_ordinality = true;
        }
        self
    }

    /// The source alias.
    pub fn alias(&self) -> &str {
        match self {
            FromSource::Subquery { alias, .. } | FromSource::Function { alias, .. } => alias,
        }
    }

    /// Column renames after the alias.
    pub fn column_alias_list(&self) -> &[String] {
        match self {
            FromSource::Subquery { column_aliases, .. }
            | FromSource::Function { column_aliases, .. } => column_aliases,
        }
    }
}

/// ON CONFLICT clause for upsert.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OnConflict {
    /// Conflict target columns.
    pub columns: Vec<String>,
    /// `ON CONFLICT ON CONSTRAINT <name>` target. Exclusive with `columns`.
    #[serde(default)]
    pub constraint: Option<String>,
    /// What to do on conflict.
    pub action: ConflictAction,
    /// `DO UPDATE ... WHERE <conditions>` — predicates over the EXISTING row.
    ///
    /// This is how RLS scoping reaches the update arm of an upsert: the
    /// insert payload is stamped with the scope, and the conflicting row
    /// must satisfy the same scope or the update is skipped. Ignored for
    /// `DO NOTHING`.
    #[serde(default)]
    pub where_conditions: Vec<Condition>,
}

/// Action to take on an INSERT conflict.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ConflictAction {
    /// DO NOTHING.
    DoNothing,
    /// DO UPDATE SET.
    DoUpdate {
        /// Column = expression assignments.
        assignments: Vec<(String, Expr)>,
    },
}

/// PostgreSQL `MERGE` specification.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Merge {
    /// Optional target table alias.
    pub target_alias: Option<String>,
    /// `USING` data source.
    pub source: MergeSource,
    /// `ON` join conditions.
    pub on: Vec<Condition>,
    /// Ordered `WHEN ... THEN ...` clauses.
    pub clauses: Vec<MergeClause>,
}

/// PostgreSQL `MERGE USING` source.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MergeSource {
    /// Table or view source.
    Table {
        /// Source relation name.
        name: String,
        /// Optional source alias.
        alias: Option<String>,
        /// `USING ONLY name`: exclude inheritance children.
        #[serde(default)]
        only: bool,
    },
    /// Subquery source.
    Query {
        /// Source query.
        query: Box<Qail>,
        /// Optional source alias.
        alias: Option<String>,
    },
}

/// One ordered PostgreSQL `MERGE WHEN` clause.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MergeClause {
    /// Match class for the candidate row.
    pub match_kind: MergeMatchKind,
    /// Optional `AND` conditions after the match class.
    pub condition: Vec<Condition>,
    /// Action to execute for this clause.
    pub action: MergeAction,
}

/// PostgreSQL `MERGE WHEN` match class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MergeMatchKind {
    /// `WHEN MATCHED`.
    Matched,
    /// `WHEN NOT MATCHED [BY TARGET]`.
    NotMatchedByTarget,
    /// `WHEN NOT MATCHED BY SOURCE`.
    NotMatchedBySource,
}

/// PostgreSQL `MERGE THEN` action.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MergeAction {
    /// `UPDATE SET ...`.
    Update {
        /// Column = expression assignments.
        assignments: Vec<(String, Expr)>,
    },
    /// `INSERT [(...)] [OVERRIDING ... VALUE] VALUES (...)` or `INSERT DEFAULT VALUES`.
    Insert {
        /// Optional target columns.
        columns: Vec<String>,
        /// Insert value expressions.
        values: Vec<Expr>,
        /// `OVERRIDING { SYSTEM | USER } VALUE`; not allowed with `default_values`.
        #[serde(default)]
        overriding: Option<OverridingKind>,
        /// `INSERT DEFAULT VALUES`; requires empty `columns` and `values`.
        #[serde(default)]
        default_values: bool,
    },
    /// `DELETE`.
    Delete,
    /// `DO NOTHING`.
    DoNothing,
}

impl Default for OnConflict {
    fn default() -> Self {
        Self {
            columns: vec![],
            constraint: None,
            action: ConflictAction::DoNothing,
            where_conditions: Vec::new(),
        }
    }
}

impl ConflictAction {
    pub(crate) fn update_assignments(&self) -> Option<&[(String, Expr)]> {
        match self {
            Self::DoNothing => None,
            Self::DoUpdate { assignments } => Some(assignments),
        }
    }
}

impl Default for Qail {
    fn default() -> Self {
        Self {
            action: Action::Get,
            table: String::new(),
            from_source: None,
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
            returning_aliases: None,
            on_conflict: None,
            conflict_update_scope: Vec::new(),
            merge: None,
            source_query: None,
            channel: None,
            payload: None,
            savepoint_name: None,
            from_tables: vec![],
            using_tables: vec![],
            lock_mode: None,
            skip_locked: false,
            lock_nowait: false,
            lock_of: vec![],
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
            policy_def: None,
            view_security_invoker: false,
            view_security_barrier: false,
            view_check_option: None,
        }
    }
}

// Submodules with builder methods
mod advanced;
mod constructors;
mod cte;
mod grouping;
mod merge;
mod query;

pub use grouping::GroupByClause;
mod rls;
mod serialization;
mod vector;

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
