//! Abstract Syntax Tree for QAIL commands.
//!
//! This module defines the core data structures that represent
//! a parsed QAIL query.

use serde::{Deserialize, Serialize};

/// The primary command structure representing a parsed QAIL query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QailCmd {
    /// The action to perform (GET, SET, DEL, ADD)
    pub action: Action,
    /// Target table name
    pub table: String,
    /// Columns to select/return
    pub columns: Vec<Column>,
    /// Joins to other tables
    #[serde(default)]
    pub joins: Vec<Join>,
    /// Cages (filters, sorts, limits, payloads)
    pub cages: Vec<Cage>,
    /// Whether to use DISTINCT in SELECT
    #[serde(default)]
    pub distinct: bool,
}

impl QailCmd {
    /// Create a new GET command for the given table.
    pub fn get(table: impl Into<String>) -> Self {
        Self {
            action: Action::Get,
            table: table.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
        }
    }

    /// Create a new SET (update) command for the given table.
    pub fn set(table: impl Into<String>) -> Self {
        Self {
            action: Action::Set,
            table: table.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
        }
    }

    /// Create a new DEL (delete) command for the given table.
    pub fn del(table: impl Into<String>) -> Self {
        Self {
            action: Action::Del,
            table: table.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
        }
    }

    /// Create a new ADD (insert) command for the given table.
    pub fn add(table: impl Into<String>) -> Self {
        Self {
            action: Action::Add,
            table: table.into(),
            joins: vec![],
            columns: vec![],
            cages: vec![],
            distinct: false,
        }
    }
    /// Add columns to hook (select).
    pub fn hook(mut self, cols: &[&str]) -> Self {
        self.columns = cols.iter().map(|c| Column::Named(c.to_string())).collect();
        self
    }

    /// Add a filter cage.
    pub fn cage(mut self, column: &str, value: impl Into<Value>) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                column: column.to_string(),
                op: Operator::Eq,
                value: value.into(),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });
        self
    }

    /// Add a limit cage.
    pub fn limit(mut self, n: i64) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Limit(n as usize),
            conditions: vec![],
            logical_op: LogicalOp::And,
        });
        self
    }

    /// Add a sort cage (ascending).
    pub fn sort_asc(mut self, column: &str) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Sort(SortOrder::Asc),
            conditions: vec![Condition {
                column: column.to_string(),
                op: Operator::Eq,
                value: Value::Null,
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });
        self
    }

    /// Add a sort cage (descending).
    pub fn sort_desc(mut self, column: &str) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Sort(SortOrder::Desc),
            conditions: vec![Condition {
                column: column.to_string(),
                op: Operator::Eq,
                value: Value::Null,
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });
        self
    }
}

/// A join definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Join {
    pub table: String,
    pub kind: JoinKind,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
}

/// A column reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Column {
    /// All columns (*)
    Star,
    /// A named column
    Named(String),
    /// An aliased column (col AS alias)
    Aliased { name: String, alias: String },
    /// An aggregate function (COUNT(col))
    Aggregate { col: String, func: AggregateFunc },
    /// Column Definition (for Make keys)
    Def {
        name: String,
        data_type: String,
        constraints: Vec<Constraint>,
    },
    /// Column Modification (for Mod keys)
    Mod {
        kind: ModKind,
        col: Box<Column>,
    },
    /// Window Function Definition
    Window {
        name: String,
        func: String,
        params: Vec<Value>,
        partition: Vec<String>,
        order: Vec<Cage>,
    },
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Column::Star => write!(f, "*"),
            Column::Named(name) => write!(f, "{}", name),
            Column::Aliased { name, alias } => write!(f, "{} AS {}", name, alias),
            Column::Aggregate { col, func } => write!(f, "{}({})", func, col),
            Column::Def {
                name,
                data_type,
                constraints,
            } => {
                write!(f, "{}:{}", name, data_type)?;
                for c in constraints {
                    write!(f, "^{}", c)?;
                }
                Ok(())
            }
            Column::Mod { kind, col } => match kind {
                ModKind::Add => write!(f, "+{}", col),
                ModKind::Drop => write!(f, "-{}", col),
            },
            Column::Window { name, func, params, partition, order } => {
                write!(f, "{}:{}(", name, func)?;
                for (i, p) in params.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}", p)?;
                }
                write!(f, ")")?;
                
                // Print partitions if any (custom syntax for display?)
                if !partition.is_empty() {
                    write!(f, "{{Part=")?;
                    for (i, p) in partition.iter().enumerate() {
                        if i > 0 { write!(f, ",")?; }
                        write!(f, "{}", p)?;
                    }
                    write!(f, "}}")?;
                }

                // Print order cages (TODO: implement proper order display)
                for _cage in order {
                    // Order cages are sort cages - display format TBD
                }
                Ok(())
            }
        }
    }
}

/// Column modification type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ModKind {
    Add,
    Drop,
}

/// Column definition constraints
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Constraint {
    PrimaryKey,
    Unique,
    Nullable,
    /// DEFAULT value (e.g., `= uuid()`, `= 0`, `= now()`)
    Default(String),
    /// CHECK constraint with allowed values (e.g., `^check("a","b")`)
    Check(Vec<String>),
}

impl std::fmt::Display for Constraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Constraint::PrimaryKey => write!(f, "pk"),
            Constraint::Unique => write!(f, "uniq"),
            Constraint::Nullable => write!(f, "?"),
            Constraint::Default(val) => write!(f, "={}", val),
            Constraint::Check(vals) => write!(f, "check({})", vals.join(",")),
        }
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
    /// Modify Table (Mod)
    Mod,
    /// Window Function (Over)
    Over,
    /// CTE (With)
    With,
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
            Action::Mod => write!(f, "MOD"),
            Action::Over => write!(f, "OVER"),
            Action::With => write!(f, "WITH"),
        }
    }
}



/// A cage (constraint block) in the query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cage {
    /// The type of cage
    pub kind: CageKind,
    /// Conditions within this cage
    pub conditions: Vec<Condition>,
    /// Logical operator between conditions (AND or OR)
    pub logical_op: LogicalOp,
}

/// The type of cage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CageKind {
    /// WHERE filter
    Filter,
    /// SET payload (for updates)
    Payload,
    /// ORDER BY
    Sort(SortOrder),
    /// LIMIT
    Limit(usize),
    /// OFFSET
    Offset(usize),
}

/// Sort order direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Logical operator between conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LogicalOp {
    #[default]
    And,
    Or,
}

/// A single condition within a cage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    /// Column name
    pub column: String,
    /// Comparison operator
    pub op: Operator,
    /// Value to compare against
    pub value: Value,
    /// Whether this is an array unnest operation (column[*])
    #[serde(default)]
    pub is_array_unnest: bool,
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
}

/// A value in a condition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// NULL value
    Null,
    /// Boolean
    Bool(bool),
    /// Integer
    Int(i64),
    /// Float
    Float(f64),
    /// String
    String(String),
    /// Parameter reference ($1, $2, etc.)
    Param(usize),
    /// SQL function call (e.g., now())
    Function(String),
    /// Array of values
    Array(Vec<Value>),
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<i32> for Value {
    fn from(n: i32) -> Self {
        Value::Int(n as i64)
    }
}

impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Int(n)
    }
}

impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Float(n)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(n) => write!(f, "{}", n),
            Value::Float(n) => write!(f, "{}", n),
            Value::String(s) => write!(f, "'{}'", s.replace('\'', "''")),
            Value::Param(n) => write!(f, "${}", n),
            Value::Function(name) => write!(f, "{}()", name),
            Value::Array(arr) => {
                write!(f, "ARRAY[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_pattern() {
        let cmd = QailCmd::get("users")
            .hook(&["id", "email"])
            .cage("active", true)
            .limit(10);

        assert_eq!(cmd.action, Action::Get);
        assert_eq!(cmd.table, "users");
        assert_eq!(cmd.columns.len(), 2);
        assert_eq!(cmd.cages.len(), 2);
    }
}
