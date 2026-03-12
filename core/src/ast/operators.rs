use serde::{Deserialize, Serialize};

/// The action type (SQL operation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    /// SELECT query.
    Get,
    /// COUNT query.
    Cnt,
    /// UPDATE statement.
    Set,
    /// DELETE statement.
    Del,
    /// INSERT statement.
    Add,
    /// Code generation.
    Gen,
    /// CREATE TABLE.
    Make,
    /// DROP TABLE.
    Drop,
    /// ALTER TABLE (add column).
    Mod,
    /// UPSERT / ON CONFLICT … DO UPDATE.
    Over,
    /// CTE / WITH clause.
    With,
    /// CREATE INDEX.
    Index,
    /// DROP INDEX.
    DropIndex,
    /// ALTER TABLE (generic).
    Alter,
    /// ALTER TABLE … DROP COLUMN.
    AlterDrop,
    /// ALTER TABLE … ALTER COLUMN TYPE.
    AlterType,
    /// BEGIN TRANSACTION.
    TxnStart,
    /// COMMIT.
    TxnCommit,
    /// ROLLBACK.
    TxnRollback,
    /// Bulk INSERT / COPY.
    Put,
    /// DROP COLUMN.
    DropCol,
    /// ALTER TABLE … RENAME COLUMN.
    RenameCol,
    /// JSONB_TO_RECORDSET.
    JsonTable,
    /// COPY … TO STDOUT.
    Export,
    /// TRUNCATE TABLE.
    Truncate,
    /// EXPLAIN.
    Explain,
    /// EXPLAIN ANALYZE.
    ExplainAnalyze,
    /// LOCK TABLE.
    Lock,
    /// CREATE MATERIALIZED VIEW.
    CreateMaterializedView,
    /// REFRESH MATERIALIZED VIEW.
    RefreshMaterializedView,
    /// DROP MATERIALIZED VIEW.
    DropMaterializedView,
    /// LISTEN (async notifications).
    Listen,
    /// NOTIFY (async notifications).
    Notify,
    /// UNLISTEN (async notifications).
    Unlisten,
    /// SAVEPOINT.
    Savepoint,
    /// RELEASE SAVEPOINT.
    ReleaseSavepoint,
    /// ROLLBACK TO SAVEPOINT.
    RollbackToSavepoint,
    /// CREATE VIEW.
    CreateView,
    /// DROP VIEW.
    DropView,
    /// Full-text or vector search.
    Search,
    /// INSERT … ON CONFLICT DO UPDATE.
    Upsert,
    /// Cursor-based scrolling.
    Scroll,
    /// Create vector collection (Qdrant).
    CreateCollection,
    /// Delete vector collection (Qdrant).
    DeleteCollection,
    /// CREATE FUNCTION.
    CreateFunction,
    /// DROP FUNCTION.
    DropFunction,
    /// CREATE TRIGGER.
    CreateTrigger,
    /// DROP TRIGGER.
    DropTrigger,
    /// CREATE EXTENSION.
    CreateExtension,
    /// DROP EXTENSION.
    DropExtension,
    /// COMMENT ON.
    CommentOn,
    /// CREATE SEQUENCE.
    CreateSequence,
    /// DROP SEQUENCE.
    DropSequence,
    /// CREATE TYPE … AS ENUM.
    CreateEnum,
    /// DROP TYPE.
    DropEnum,
    /// ALTER TYPE … ADD VALUE.
    AlterEnumAddValue,
    /// ALTER COLUMN SET NOT NULL.
    AlterSetNotNull,
    /// ALTER COLUMN DROP NOT NULL.
    AlterDropNotNull,
    /// ALTER COLUMN SET DEFAULT.
    AlterSetDefault,
    /// ALTER COLUMN DROP DEFAULT.
    AlterDropDefault,
    /// ALTER TABLE ENABLE ROW LEVEL SECURITY.
    AlterEnableRls,
    /// ALTER TABLE DISABLE ROW LEVEL SECURITY.
    AlterDisableRls,
    /// ALTER TABLE FORCE ROW LEVEL SECURITY.
    AlterForceRls,
    /// ALTER TABLE NO FORCE ROW LEVEL SECURITY.
    AlterNoForceRls,
    // Session & procedural commands
    /// CALL procedure.
    Call,
    /// DO anonymous block.
    Do,
    /// SET session variable.
    SessionSet,
    /// SHOW session variable.
    SessionShow,
    /// RESET session variable.
    SessionReset,
    /// CREATE DATABASE.
    CreateDatabase,
    /// DROP DATABASE.
    DropDatabase,
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Get => write!(f, "GET"),
            Action::Cnt => write!(f, "CNT"),
            Action::Set => write!(f, "SET"),
            Action::Del => write!(f, "DEL"),
            Action::Add => write!(f, "ADD"),
            Action::Gen => write!(f, "GEN"),
            Action::Make => write!(f, "MAKE"),
            Action::Drop => write!(f, "DROP"),
            Action::Mod => write!(f, "MOD"),
            Action::Over => write!(f, "OVER"),
            Action::With => write!(f, "WITH"),
            Action::Index => write!(f, "INDEX"),
            Action::DropIndex => write!(f, "DROP_INDEX"),
            Action::Alter => write!(f, "ALTER"),
            Action::AlterDrop => write!(f, "ALTER_DROP"),
            Action::AlterType => write!(f, "ALTER_TYPE"),
            Action::TxnStart => write!(f, "TXN_START"),
            Action::TxnCommit => write!(f, "TXN_COMMIT"),
            Action::TxnRollback => write!(f, "TXN_ROLLBACK"),
            Action::Put => write!(f, "PUT"),
            Action::DropCol => write!(f, "DROP_COL"),
            Action::RenameCol => write!(f, "RENAME_COL"),
            Action::JsonTable => write!(f, "JSON_TABLE"),
            Action::Export => write!(f, "EXPORT"),
            Action::Truncate => write!(f, "TRUNCATE"),
            Action::Explain => write!(f, "EXPLAIN"),
            Action::ExplainAnalyze => write!(f, "EXPLAIN_ANALYZE"),
            Action::Lock => write!(f, "LOCK"),
            Action::CreateMaterializedView => write!(f, "CREATE_MATERIALIZED_VIEW"),
            Action::RefreshMaterializedView => write!(f, "REFRESH_MATERIALIZED_VIEW"),
            Action::DropMaterializedView => write!(f, "DROP_MATERIALIZED_VIEW"),
            Action::Listen => write!(f, "LISTEN"),
            Action::Notify => write!(f, "NOTIFY"),
            Action::Unlisten => write!(f, "UNLISTEN"),
            Action::Savepoint => write!(f, "SAVEPOINT"),
            Action::ReleaseSavepoint => write!(f, "RELEASE_SAVEPOINT"),
            Action::RollbackToSavepoint => write!(f, "ROLLBACK_TO_SAVEPOINT"),
            Action::CreateView => write!(f, "CREATE_VIEW"),
            Action::DropView => write!(f, "DROP_VIEW"),
            Action::Search => write!(f, "SEARCH"),
            Action::Upsert => write!(f, "UPSERT"),
            Action::Scroll => write!(f, "SCROLL"),
            Action::CreateCollection => write!(f, "CREATE_COLLECTION"),
            Action::DeleteCollection => write!(f, "DELETE_COLLECTION"),
            Action::CreateFunction => write!(f, "CREATE_FUNCTION"),
            Action::DropFunction => write!(f, "DROP_FUNCTION"),
            Action::CreateTrigger => write!(f, "CREATE_TRIGGER"),
            Action::DropTrigger => write!(f, "DROP_TRIGGER"),
            Action::CreateExtension => write!(f, "CREATE_EXTENSION"),
            Action::DropExtension => write!(f, "DROP_EXTENSION"),
            Action::CommentOn => write!(f, "COMMENT_ON"),
            Action::CreateSequence => write!(f, "CREATE_SEQUENCE"),
            Action::DropSequence => write!(f, "DROP_SEQUENCE"),
            Action::CreateEnum => write!(f, "CREATE_ENUM"),
            Action::DropEnum => write!(f, "DROP_ENUM"),
            Action::AlterEnumAddValue => write!(f, "ALTER_ENUM_ADD_VALUE"),
            Action::AlterSetNotNull => write!(f, "ALTER_SET_NOT_NULL"),
            Action::AlterDropNotNull => write!(f, "ALTER_DROP_NOT_NULL"),
            Action::AlterSetDefault => write!(f, "ALTER_SET_DEFAULT"),
            Action::AlterDropDefault => write!(f, "ALTER_DROP_DEFAULT"),
            Action::AlterEnableRls => write!(f, "ALTER_ENABLE_RLS"),
            Action::AlterDisableRls => write!(f, "ALTER_DISABLE_RLS"),
            Action::AlterForceRls => write!(f, "ALTER_FORCE_RLS"),
            Action::AlterNoForceRls => write!(f, "ALTER_NO_FORCE_RLS"),
            Action::Call => write!(f, "CALL"),
            Action::Do => write!(f, "DO"),
            Action::SessionSet => write!(f, "SESSION_SET"),
            Action::SessionShow => write!(f, "SESSION_SHOW"),
            Action::SessionReset => write!(f, "SESSION_RESET"),
            Action::CreateDatabase => write!(f, "CREATE_DATABASE"),
            Action::DropDatabase => write!(f, "DROP_DATABASE"),
        }
    }
}

/// Logical operator between conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LogicalOp {
    #[default]
    /// Logical AND.
    And,
    /// Logical OR.
    Or,
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortOrder {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
    /// Ascending, NULLs first.
    AscNullsFirst,
    /// Ascending, NULLs last.
    AscNullsLast,
    /// Descending, NULLs first.
    DescNullsFirst,
    /// Descending, NULLs last.
    DescNullsLast,
}

/// Comparison / filtering operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operator {
    /// `=`
    Eq,
    /// `!=`
    Ne,
    /// `>`
    Gt,
    /// `>=`
    Gte,
    /// `<`
    Lt,
    /// `<=`
    Lte,
    /// Case-insensitive LIKE (legacy alias for [`ILike`](Operator::ILike)).
    Fuzzy,
    /// IN (list).
    In,
    /// NOT IN (list).
    NotIn,
    /// IS NULL.
    IsNull,
    /// IS NOT NULL.
    IsNotNull,
    /// JSONB `@>` containment.
    Contains,
    /// JSONB `?` key existence.
    KeyExists,
    /// JSON_EXISTS path check.
    JsonExists,
    /// JSON_QUERY path extraction.
    JsonQuery,
    /// JSON_VALUE scalar extraction.
    JsonValue,
    /// LIKE pattern match.
    Like,
    /// NOT LIKE.
    NotLike,
    /// ILIKE (case-insensitive).
    ILike,
    /// NOT ILIKE.
    NotILike,
    /// BETWEEN low AND high.
    Between,
    /// NOT BETWEEN.
    NotBetween,
    /// EXISTS (subquery).
    Exists,
    /// NOT EXISTS (subquery).
    NotExists,
    /// POSIX regex `~`.
    Regex,
    /// Case-insensitive regex `~*`.
    RegexI,
    /// SIMILAR TO.
    SimilarTo,
    /// JSONB `<@` contained-by.
    ContainedBy,
    /// Array `&&` overlap.
    Overlaps,
    /// Full-text search `@@`.
    TextSearch,
    /// `?|` — does JSONB contain ANY of the given keys?
    KeyExistsAny,
    /// `?&` — does JSONB contain ALL of the given keys?
    KeyExistsAll,
    /// `#>` — JSONB path extraction → jsonb
    JsonPath,
    /// `#>>` — JSONB path extraction → text
    JsonPathText,
    /// `LOWER(text) LIKE '%' || LOWER(array_element) || '%'` over `unnest(array_column)`.
    /// Used for "does input text contain any keyword token?" matching.
    ArrayElemContainedInText,
}

impl Operator {
    /// For simple operators, returns the symbol directly.
    /// For complex operators (BETWEEN, EXISTS), returns the keyword.
    pub fn sql_symbol(&self) -> &'static str {
        match self {
            Operator::Eq => "=",
            Operator::Ne => "!=",
            Operator::Gt => ">",
            Operator::Gte => ">=",
            Operator::Lt => "<",
            Operator::Lte => "<=",
            Operator::Fuzzy => "ILIKE",
            Operator::In => "IN",
            Operator::NotIn => "NOT IN",
            Operator::IsNull => "IS NULL",
            Operator::IsNotNull => "IS NOT NULL",
            Operator::Contains => "@>",
            Operator::KeyExists => "?",
            Operator::JsonExists => "JSON_EXISTS",
            Operator::JsonQuery => "JSON_QUERY",
            Operator::JsonValue => "JSON_VALUE",
            Operator::Like => "LIKE",
            Operator::NotLike => "NOT LIKE",
            Operator::ILike => "ILIKE",
            Operator::NotILike => "NOT ILIKE",
            Operator::Between => "BETWEEN",
            Operator::NotBetween => "NOT BETWEEN",
            Operator::Exists => "EXISTS",
            Operator::NotExists => "NOT EXISTS",
            Operator::Regex => "~",
            Operator::RegexI => "~*",
            Operator::SimilarTo => "SIMILAR TO",
            Operator::ContainedBy => "<@",
            Operator::Overlaps => "&&",
            Operator::TextSearch => "@@",
            Operator::KeyExistsAny => "?|",
            Operator::KeyExistsAll => "?&",
            Operator::JsonPath => "#>",
            Operator::JsonPathText => "#>>",
            Operator::ArrayElemContainedInText => "CONTAINS_ANY_TOKEN",
        }
    }

    /// IS NULL, IS NOT NULL, EXISTS, NOT EXISTS don't need values.
    pub fn needs_value(&self) -> bool {
        !matches!(
            self,
            Operator::IsNull | Operator::IsNotNull | Operator::Exists | Operator::NotExists
        )
    }

    /// Returns `true` for simple binary operators (=, !=, >, <, LIKE, ILIKE, etc.).
    pub fn is_simple_binary(&self) -> bool {
        matches!(
            self,
            Operator::Eq
                | Operator::Ne
                | Operator::Gt
                | Operator::Gte
                | Operator::Lt
                | Operator::Lte
                | Operator::Like
                | Operator::NotLike
                | Operator::ILike
                | Operator::NotILike
        )
    }
}

/// Aggregate function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunc {
    /// COUNT(*).
    Count,
    /// SUM.
    Sum,
    /// AVG.
    Avg,
    /// MIN.
    Min,
    /// MAX.
    Max,
    /// ARRAY_AGG.
    ArrayAgg,
    /// STRING_AGG.
    StringAgg,
    /// JSON_AGG.
    JsonAgg,
    /// JSONB_AGG.
    JsonbAgg,
    /// BOOL_AND.
    BoolAnd,
    /// BOOL_OR.
    BoolOr,
}

impl std::fmt::Display for AggregateFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateFunc::Count => write!(f, "COUNT"),
            AggregateFunc::Sum => write!(f, "SUM"),
            AggregateFunc::Avg => write!(f, "AVG"),
            AggregateFunc::Min => write!(f, "MIN"),
            AggregateFunc::Max => write!(f, "MAX"),
            AggregateFunc::ArrayAgg => write!(f, "ARRAY_AGG"),
            AggregateFunc::StringAgg => write!(f, "STRING_AGG"),
            AggregateFunc::JsonAgg => write!(f, "JSON_AGG"),
            AggregateFunc::JsonbAgg => write!(f, "JSONB_AGG"),
            AggregateFunc::BoolAnd => write!(f, "BOOL_AND"),
            AggregateFunc::BoolOr => write!(f, "BOOL_OR"),
        }
    }
}

/// Join Type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinKind {
    /// INNER JOIN.
    Inner,
    /// LEFT (OUTER) JOIN.
    Left,
    /// RIGHT (OUTER) JOIN.
    Right,
    /// LATERAL join.
    Lateral,
    /// FULL (OUTER) JOIN.
    Full,
    /// CROSS JOIN.
    Cross,
}

/// Set operation type for combining queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOp {
    /// UNION (de-duplicated).
    Union,
    /// UNION ALL.
    UnionAll,
    /// INTERSECT.
    Intersect,
    /// EXCEPT.
    Except,
}

/// ALTER TABLE modification kind.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ModKind {
    /// ADD.
    Add,
    /// DROP.
    Drop,
}

/// GROUP BY mode for advanced aggregations
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum GroupByMode {
    #[default]
    /// Standard GROUP BY.
    Simple,
    /// GROUP BY ROLLUP.
    Rollup,
    /// GROUP BY CUBE.
    Cube,
    /// GROUP BY GROUPING SETS.
    GroupingSets(Vec<Vec<String>>),
}

impl GroupByMode {
    /// Check if this is the default Simple mode (for serde skip)
    pub fn is_simple(&self) -> bool {
        matches!(self, GroupByMode::Simple)
    }
}

/// Row locking mode for SELECT...FOR UPDATE/SHARE
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockMode {
    /// FOR UPDATE.
    Update,
    /// FOR NO KEY UPDATE.
    NoKeyUpdate,
    /// FOR SHARE.
    Share,
    /// FOR KEY SHARE.
    KeyShare,
}

/// OVERRIDING clause for INSERT with GENERATED columns
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OverridingKind {
    /// OVERRIDING SYSTEM VALUE.
    SystemValue,
    /// OVERRIDING USER VALUE.
    UserValue,
}

/// TABLESAMPLE sampling method
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SampleMethod {
    /// Random row sampling (row-level).
    Bernoulli,
    /// Block-level sampling.
    System,
}

/// Distance metric for vector similarity
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Distance {
    #[default]
    /// Cosine similarity.
    Cosine,
    /// Euclidean distance.
    Euclid,
    /// Dot product.
    Dot,
}
