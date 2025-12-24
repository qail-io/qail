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
            Action::TxnStart => write!(f, "TXN_START"),
            Action::TxnCommit => write!(f, "TXN_COMMIT"),
            Action::TxnRollback => write!(f, "TXN_ROLLBACK"),
            Action::Put => write!(f, "PUT"),
            Action::DropCol => write!(f, "DROP_COL"),
            Action::RenameCol => write!(f, "RENAME_COL"),
            Action::JsonTable => write!(f, "JSON_TABLE"),
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
}

/// Aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl std::fmt::Display for AggregateFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateFunc::Count => write!(f, "COUNT"),
            AggregateFunc::Sum => write!(f, "SUM"),
            AggregateFunc::Avg => write!(f, "AVG"),
            AggregateFunc::Min => write!(f, "MIN"),
            AggregateFunc::Max => write!(f, "MAX"),
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
