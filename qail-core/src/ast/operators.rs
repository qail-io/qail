use serde::{Deserialize, Serialize};

/// The action type (SQL operation).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    /// SELECT query
    Get,
    /// UPDATE query  
    Set,
    /// DELETE query
    Del,
    /// INSERT query
    Add,
    /// Generate Rust struct from table schema
    Gen,
    /// Create Table (Make)
    Make,
    /// Drop Table (Drop)
    Drop,
    /// Modify Table (Mod)
    Mod,
    /// Window Function (Over)
    Over,
    /// CTE (With)
    With,
    /// Create Index
    Index,
    /// Drop Index
    DropIndex,
    /// ALTER TABLE ADD COLUMN
    Alter,
    /// ALTER TABLE DROP COLUMN
    AlterDrop,
    /// ALTER TABLE ALTER COLUMN TYPE
    AlterType,
    // Transactions
    TxnStart,
    TxnCommit,
    TxnRollback,
    Put,
    DropCol,
    RenameCol,
    // Additional clauses
    /// JSON_TABLE - convert JSON to relational rows
    JsonTable,
    /// COPY TO STDOUT - bulk export data (AST-native)
    Export,
    /// TRUNCATE TABLE - fast delete all rows
    Truncate,
    /// EXPLAIN - query plan analysis
    Explain,
    /// EXPLAIN ANALYZE - execute and analyze query plan
    ExplainAnalyze,
    /// LOCK TABLE - explicit table locking
    Lock,
    /// CREATE MATERIALIZED VIEW
    CreateMaterializedView,
    /// REFRESH MATERIALIZED VIEW
    RefreshMaterializedView,
    /// DROP MATERIALIZED VIEW
    DropMaterializedView,
    // Pub/Sub (LISTEN/NOTIFY)
    /// LISTEN channel - subscribe to notifications
    Listen,
    /// NOTIFY channel, 'payload' - send notification
    Notify,
    /// UNLISTEN channel - unsubscribe from notifications
    Unlisten,
    // Savepoints
    /// SAVEPOINT name
    Savepoint,
    /// RELEASE SAVEPOINT name
    ReleaseSavepoint,
    /// ROLLBACK TO SAVEPOINT name
    RollbackToSavepoint,
}

impl std::fmt::Display for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Get => write!(f, "GET"),
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
        }
    }
}

/// Logical operator between conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LogicalOp {
    #[default]
    And,
    Or,
}

/// Sort order direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortOrder {
    Asc,
    Desc,
    /// ASC NULLS FIRST (nulls at top)
    AscNullsFirst,
    /// ASC NULLS LAST (nulls at bottom)
    AscNullsLast,
    /// DESC NULLS FIRST (nulls at top)
    DescNullsFirst,
    /// DESC NULLS LAST (nulls at bottom)
    DescNullsLast,
}

/// Comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operator {
    /// Equal (=)
    Eq,
    /// Not equal (!=, <>)
    Ne,
    /// Greater than (>)
    Gt,
    /// Greater than or equal (>=)
    Gte,
    /// Less than (<)
    Lt,
    /// Less than or equal (<=)  
    Lte,
    /// Fuzzy match (~) -> ILIKE
    Fuzzy,
    /// IN array
    In,
    /// NOT IN array
    NotIn,
    /// IS NULL
    IsNull,
    /// IS NOT NULL
    IsNotNull,
    /// JSON/Array Contains (@>)
    Contains,
    /// JSON Key Exists (?)
    KeyExists,
    /// JSON_EXISTS - check if path exists (Postgres 17+)
    JsonExists,
    /// JSON_QUERY - extract JSON object/array at path (Postgres 17+)
    JsonQuery,
    /// JSON_VALUE - extract scalar value at path (Postgres 17+)
    JsonValue,
    /// LIKE pattern match
    Like,
    /// NOT LIKE pattern match
    NotLike,
    /// ILIKE case-insensitive pattern match (Postgres)
    ILike,
    /// NOT ILIKE case-insensitive pattern match (Postgres)
    NotILike,
    /// BETWEEN x AND y - range check (value stored as Value::Array with 2 elements)
    Between,
    /// NOT BETWEEN x AND y
    NotBetween,
    /// EXISTS (subquery) - check if subquery returns any rows
    Exists,
    /// NOT EXISTS (subquery)
    NotExists,
    /// Regular expression match (~ in Postgres)
    Regex,
    /// Case-insensitive regex (~* in Postgres)
    RegexI,
    /// SIMILAR TO pattern match
    SimilarTo,
    /// Array/JSON is contained by (<@)
    ContainedBy,
    /// Array overlap (&&)
    Overlaps,
}

impl Operator {
    /// Returns the SQL symbol/keyword for this operator.
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
        }
    }

    /// Returns true if this operator requires a value on the right side.
    /// IS NULL, IS NOT NULL, EXISTS, NOT EXISTS don't need values.
    pub fn needs_value(&self) -> bool {
        !matches!(self, 
            Operator::IsNull | 
            Operator::IsNotNull | 
            Operator::Exists | 
            Operator::NotExists
        )
    }

    /// Returns true if this operator is a simple binary operator (col OP value).
    pub fn is_simple_binary(&self) -> bool {
        matches!(self,
            Operator::Eq | Operator::Ne | Operator::Gt | Operator::Gte |
            Operator::Lt | Operator::Lte | Operator::Like | Operator::NotLike |
            Operator::ILike | Operator::NotILike
        )
    }
}

/// Aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// ARRAY_AGG - collect values into array
    ArrayAgg,
    /// STRING_AGG - concatenate strings with delimiter
    StringAgg,
    /// JSON_AGG - aggregate values as JSON array
    JsonAgg,
    /// JSONB_AGG - aggregate values as JSONB array
    JsonbAgg,
    /// BOOL_AND - logical AND of all values
    BoolAnd,
    /// BOOL_OR - logical OR of all values
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
    Inner,
    Left,
    Right,
    /// LATERAL join (Postgres, MySQL 8+)
    Lateral,
    /// FULL OUTER JOIN
    Full,
    /// CROSS JOIN
    Cross,
}

/// Set operation type for combining queries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOp {
    /// UNION (removes duplicates)
    Union,
    /// UNION ALL (keeps duplicates)
    UnionAll,
    /// INTERSECT (common rows)
    Intersect,
    /// EXCEPT (rows in first but not second)
    Except,
}

/// Column modification type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ModKind {
    Add,
    Drop,
}

/// GROUP BY mode for advanced aggregations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum GroupByMode {
    /// Standard GROUP BY
    #[default]
    Simple,
    /// ROLLUP - hierarchical subtotals
    Rollup,
    /// CUBE - all combinations of subtotals
    Cube,
}
