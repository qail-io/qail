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
    /// Index definition (for Action::Index)
    #[serde(default)]
    pub index_def: Option<IndexDef>,
    /// Table-level constraints (for Action::Make)
    #[serde(default)]
    pub table_constraints: Vec<TableConstraint>,
    /// Set operations (UNION, INTERSECT, EXCEPT) chained queries
    #[serde(default)]
    pub set_ops: Vec<(SetOp, Box<QailCmd>)>,
    /// HAVING clause conditions (filter on aggregates)
    #[serde(default)]
    pub having: Vec<Condition>,
    /// GROUP BY mode (Simple, Rollup, Cube)
    #[serde(default)]
    pub group_by_mode: GroupByMode,
    /// CTE definitions (for WITH/WITH RECURSIVE queries)
    #[serde(default)]
    pub ctes: Vec<CTEDef>,
    /// DISTINCT ON columns (Postgres-specific)
    #[serde(default)]
    pub distinct_on: Vec<String>,
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
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
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
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
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
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
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
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
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

    // =========================================================================
    // CTE (Common Table Expression) Builder Methods
    // =========================================================================

    /// Wrap this query as a CTE with the given name.
    /// 
    /// # Example
    /// ```ignore
    /// let cte = QailCmd::get("employees")
    ///     .hook(&["id", "name"])
    ///     .cage("manager_id", Value::Null)
    ///     .as_cte("emp_tree");
    /// ```
    pub fn as_cte(self, name: impl Into<String>) -> Self {
        let cte_name = name.into();
        let columns: Vec<String> = self.columns.iter().filter_map(|c| {
            match c {
                Column::Named(n) => Some(n.clone()),
                Column::Aliased { alias, .. } => Some(alias.clone()),
                _ => None,
            }
        }).collect();
        
        Self {
            action: Action::With,
            table: cte_name.clone(),
            columns: vec![],
            joins: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            distinct_on: vec![],
            ctes: vec![CTEDef {
                name: cte_name,
                recursive: false,
                columns,
                base_query: Box::new(self),
                recursive_query: None,
                source_table: None,
            }],
        }
    }

    /// Make this CTE recursive and add the recursive part.
    /// 
    /// # Example
    /// ```ignore
    /// let recursive_cte = base_query
    ///     .as_cte("emp_tree")
    ///     .recursive(recursive_query);
    /// ```
    pub fn recursive(mut self, recursive_part: QailCmd) -> Self {
        if let Some(cte) = self.ctes.last_mut() {
            cte.recursive = true;
            cte.recursive_query = Some(Box::new(recursive_part));
        }
        self
    }

    /// Set the source table for recursive join (self-reference).
    pub fn from_cte(mut self, cte_name: impl Into<String>) -> Self {
        if let Some(cte) = self.ctes.last_mut() {
            cte.source_table = Some(cte_name.into());
        }
        self
    }

    /// Chain a final SELECT from the CTE.
    /// 
    /// # Example
    /// ```ignore
    /// let final_query = cte.select_from_cte(&["id", "name", "level"]);
    /// ```
    pub fn select_from_cte(mut self, columns: &[&str]) -> Self {
        self.columns = columns.iter().map(|c| Column::Named(c.to_string())).collect();
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

/// CTE (Common Table Expression) definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CTEDef {
    /// CTE name (the alias used in the query)
    pub name: String,
    /// Whether this is a RECURSIVE CTE
    pub recursive: bool,
    /// Column list for the CTE (optional)
    pub columns: Vec<String>,
    /// Base query (non-recursive part)
    pub base_query: Box<QailCmd>,
    /// Recursive part (UNION ALL with self-reference)
    pub recursive_query: Option<Box<QailCmd>>,
    /// Source table for recursive join (references CTE name)
    pub source_table: Option<String>,
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
        frame: Option<WindowFrame>,
    },
    /// CASE WHEN expression
    Case {
        /// WHEN condition THEN value pairs
        when_clauses: Vec<(Condition, Value)>,
        /// ELSE value (optional)
        else_value: Option<Box<Value>>,
        /// Optional alias
        alias: Option<String>,
    },
    /// JSON accessor (data->>'key' or data->'key')
    JsonAccess {
        /// Base column name
        column: String,
        /// JSON path/key
        path: String,
        /// true for ->> (as text), false for -> (as JSON)
        as_text: bool,
        /// Optional alias
        alias: Option<String>,
    },
    /// Function call expression (COALESCE, NULLIF, etc.)
    FunctionCall {
        /// Function name (coalesce, nullif, etc.)
        name: String,
        /// Arguments to the function
        args: Vec<String>,
        /// Optional alias
        alias: Option<String>,
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
            Column::Window { name, func, params, partition, order, frame } => {
                write!(f, "{}:{}(", name, func)?;
                for (i, p) in params.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}", p)?;
                }
                write!(f, ")")?;
                
                // Print partitions if any
                if !partition.is_empty() {
                    write!(f, "{{Part=")?;
                    for (i, p) in partition.iter().enumerate() {
                        if i > 0 { write!(f, ",")?; }
                        write!(f, "{}", p)?;
                    }
                    if let Some(fr) = frame {
                        write!(f, ", Frame={:?}", fr)?; // Debug format for now
                    }
                    write!(f, "}}")?;
                } else if frame.is_some() {
                     write!(f, "{{Frame={:?}}}", frame.as_ref().unwrap())?;
                }

                // Print order cages
                for _cage in order {
                    // Order cages are sort cages - display format TBD
                }
                Ok(())
            }
            Column::Case { when_clauses, else_value, alias } => {
                write!(f, "CASE")?;
                for (cond, val) in when_clauses {
                    write!(f, " WHEN {} THEN {}", cond.column, val)?;
                }
                if let Some(e) = else_value {
                    write!(f, " ELSE {}", e)?;
                }
                write!(f, " END")?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Column::JsonAccess { column, path, as_text, alias } => {
                let op = if *as_text { "->>" } else { "->" };
                write!(f, "{}{}{}", column, op, path)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Column::FunctionCall { name, args, alias } => {
                write!(f, "{}({})", name.to_uppercase(), args.join(", "))?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
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
    /// Column comment (COMMENT ON COLUMN)
    Comment(String),
    /// Generated column expression (GENERATED ALWAYS AS)
    Generated(ColumnGeneration),
}

/// Generated column type (STORED or VIRTUAL)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ColumnGeneration {
    /// GENERATED ALWAYS AS (expr) STORED - computed and stored
    Stored(String),
    /// GENERATED ALWAYS AS (expr) - computed at query time (default in Postgres 18+)
    Virtual(String),
}

/// Window frame definition for window functions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowFrame {
    /// ROWS BETWEEN start AND end
    Rows { start: FrameBound, end: FrameBound },
    /// RANGE BETWEEN start AND end
    Range { start: FrameBound, end: FrameBound },
}

/// Window frame boundary
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(i32),
    CurrentRow,
    Following(i32),
    UnboundedFollowing,
}

impl std::fmt::Display for Constraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Constraint::PrimaryKey => write!(f, "pk"),
            Constraint::Unique => write!(f, "uniq"),
            Constraint::Nullable => write!(f, "?"),
            Constraint::Default(val) => write!(f, "={}", val),
            Constraint::Check(vals) => write!(f, "check({})", vals.join(",")),
            Constraint::Comment(text) => write!(f, "comment(\"{}\")", text),
            Constraint::Generated(generation) => match generation {
                ColumnGeneration::Stored(expr) => write!(f, "gen({})", expr),
                ColumnGeneration::Virtual(expr) => write!(f, "vgen({})", expr),
            },
        }
    }
}

/// Index definition for CREATE INDEX
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct IndexDef {
    /// Index name
    pub name: String,
    /// Target table
    pub table: String,
    /// Columns to index (ordered)
    pub columns: Vec<String>,
    /// Whether this is a UNIQUE index
    pub unique: bool,
}

/// Table-level constraints for composite keys
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableConstraint {
    /// UNIQUE (col1, col2, ...)
    Unique(Vec<String>),
    /// PRIMARY KEY (col1, col2, ...)
    PrimaryKey(Vec<String>),
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
    /// Drop Table (Drop)
    Drop,
    /// Modify Table (Mod)
    Mod,
    /// Window Function (Over)
    Over,
    /// CTE (With)
    With,
    /// Create Index
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
    /// TABLESAMPLE - percentage of rows
    Sample(usize),
    /// QUALIFY - filter on window function results
    Qualify,
    /// PARTITION BY - window function partitioning
    Partition,
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
    /// Subquery for IN/EXISTS expressions (e.g., WHERE id IN (SELECT ...))
    Subquery(Box<QailCmd>),
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
            Value::Subquery(cmd) => write!(f, "({})", cmd.table), // Placeholder display
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
