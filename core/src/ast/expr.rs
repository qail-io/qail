use crate::ast::{AggregateFunc, Cage, Condition, ModKind, Value};
use serde::{Deserialize, Serialize};

/// Binary operators for expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    Concat,
    Add,
    Sub,
    Mul,
    Div,
    /// Modulo (%)
    Rem,
}

impl std::fmt::Display for BinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinaryOp::Concat => write!(f, "||"),
            BinaryOp::Add => write!(f, "+"),
            BinaryOp::Sub => write!(f, "-"),
            BinaryOp::Mul => write!(f, "*"),
            BinaryOp::Div => write!(f, "/"),
            BinaryOp::Rem => write!(f, "%"),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// All columns (*)
    Star,
    Named(String),
    /// An aliased expression (expr AS alias)
    Aliased { name: String, alias: String },
    /// An aggregate function (COUNT(col)) with optional FILTER and DISTINCT
    Aggregate {
        col: String,
        func: AggregateFunc,
        distinct: bool,
        /// PostgreSQL FILTER (WHERE ...) clause for aggregates
        filter: Option<Vec<Condition>>,
        alias: Option<String>,
    },
    /// Type cast expression (expr::type)
    Cast {
        expr: Box<Expr>,
        target_type: String,
        alias: Option<String>,
    },
    Def {
        name: String,
        data_type: String,
        constraints: Vec<Constraint>,
    },
    Mod { kind: ModKind, col: Box<Expr> },
    /// Window Function Definition
    Window {
        name: String,
        func: String,
        /// Function arguments as expressions (e.g., for SUM(amount), use Expr::Named("amount"))
        params: Vec<Expr>,
        partition: Vec<String>,
        order: Vec<Cage>,
        frame: Option<WindowFrame>,
    },
    /// CASE WHEN expression
    Case {
        /// WHEN condition THEN expr pairs (Expr allows functions, values, identifiers)
        when_clauses: Vec<(Condition, Box<Expr>)>,
        /// ELSE expr (optional)
        else_value: Option<Box<Expr>>,
        /// Optional alias
        alias: Option<String>,
    },
    /// JSON accessor (data->>'key' or data->'key' or chained data->'a'->0->>'b')
    JsonAccess {
        /// Base column name
        column: String,
        /// JSON path segments: (key, as_text)
        /// as_text: true for ->> (extract as text), false for -> (extract as JSON)
        /// For chained access like x->'a'->0->>'b', this is [("a", false), ("0", false), ("b", true)]
        path_segments: Vec<(String, bool)>,
        /// Optional alias
        alias: Option<String>,
    },
    /// Function call expression (COALESCE, NULLIF, etc.)
    FunctionCall {
        /// Function name (coalesce, nullif, etc.)
        name: String,
        /// Arguments to the function (now supports nested expressions)
        args: Vec<Expr>,
        /// Optional alias
        alias: Option<String>,
    },
    /// Special SQL function with keyword arguments (SUBSTRING, EXTRACT, TRIM, etc.)
    /// e.g., SUBSTRING(expr FROM pos [FOR len]), EXTRACT(YEAR FROM date)
    SpecialFunction {
        /// Function name (SUBSTRING, EXTRACT, TRIM, etc.)
        name: String,
        /// Arguments as (optional_keyword, expr) pairs
        /// e.g., [(None, col), (Some("FROM"), 2), (Some("FOR"), 5)]
        args: Vec<(Option<String>, Box<Expr>)>,
        /// Optional alias
        alias: Option<String>,
    },
    /// Binary expression (left op right)
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
        alias: Option<String>,
    },
    /// Literal value (string, number) for use in expressions
    /// e.g., '62', 0, 'active'
    Literal(Value),
    /// Array constructor: ARRAY[expr1, expr2, ...]
    ArrayConstructor {
        elements: Vec<Expr>,
        alias: Option<String>,
    },
    /// Row constructor: ROW(expr1, expr2, ...) or (expr1, expr2, ...)
    RowConstructor {
        elements: Vec<Expr>,
        alias: Option<String>,
    },
    /// Array/string subscript: arr[index]
    Subscript {
        expr: Box<Expr>,
        index: Box<Expr>,
        alias: Option<String>,
    },
    /// Collation: expr COLLATE "collation_name"
    Collate {
        expr: Box<Expr>,
        collation: String,
        alias: Option<String>,
    },
    /// Field selection from composite: (row).field
    FieldAccess {
        expr: Box<Expr>,
        field: String,
        alias: Option<String>,
    },
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Star => write!(f, "*"),
            Expr::Named(name) => write!(f, "{}", name),
            Expr::Aliased { name, alias } => write!(f, "{} AS {}", name, alias),
            Expr::Aggregate {
                col,
                func,
                distinct,
                filter,
                alias,
            } => {
                if *distinct {
                    write!(f, "{}(DISTINCT {})", func, col)?;
                } else {
                    write!(f, "{}({})", func, col)?;
                }
                if let Some(conditions) = filter {
                    write!(
                        f,
                        " FILTER (WHERE {})",
                        conditions
                            .iter()
                            .map(|c| c.to_string())
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    )?;
                }
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Cast {
                expr,
                target_type,
                alias,
            } => {
                write!(f, "{}::{}", expr, target_type)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Def {
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
            Expr::Mod { kind, col } => match kind {
                ModKind::Add => write!(f, "+{}", col),
                ModKind::Drop => write!(f, "-{}", col),
            },
            Expr::Window {
                name,
                func,
                params,
                partition,
                order,
                frame,
            } => {
                write!(f, "{}:{}(", name, func)?;
                for (i, p) in params.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", p)?;
                }
                write!(f, ")")?;

                // Print partitions if any
                if !partition.is_empty() {
                    write!(f, "{{Part=")?;
                    for (i, p) in partition.iter().enumerate() {
                        if i > 0 {
                            write!(f, ",")?;
                        }
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
            Expr::Case {
                when_clauses,
                else_value,
                alias,
            } => {
                write!(f, "CASE")?;
                for (cond, val) in when_clauses {
                    write!(f, " WHEN {} THEN {}", cond.left, val)?;
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
            Expr::JsonAccess {
                column,
                path_segments,
                alias,
            } => {
                write!(f, "{}", column)?;
                for (path, as_text) in path_segments {
                    let op = if *as_text { "->>" } else { "->" };
                    // Integer indices should NOT be quoted (array access)
                    // String keys should be quoted (object access)
                    if path.parse::<i64>().is_ok() {
                        write!(f, "{}{}", op, path)?;
                    } else {
                        write!(f, "{}'{}'", op, path)?;
                    }
                }
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::FunctionCall { name, args, alias } => {
                let args_str: Vec<String> = args.iter().map(|a| a.to_string()).collect();
                write!(f, "{}({})", name.to_uppercase(), args_str.join(", "))?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::SpecialFunction { name, args, alias } => {
                write!(f, "{}(", name.to_uppercase())?;
                for (i, (keyword, expr)) in args.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                    }
                    if let Some(kw) = keyword {
                        write!(f, "{} ", kw)?;
                    }
                    write!(f, "{}", expr)?;
                }
                write!(f, ")")?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Binary {
                left,
                op,
                right,
                alias,
            } => {
                write!(f, "({} {} {})", left, op, right)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Literal(value) => write!(f, "{}", value),
            Expr::ArrayConstructor { elements, alias } => {
                write!(f, "ARRAY[")?;
                for (i, elem) in elements.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", elem)?;
                }
                write!(f, "]")?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::RowConstructor { elements, alias } => {
                write!(f, "ROW(")?;
                for (i, elem) in elements.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", elem)?;
                }
                write!(f, ")")?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Subscript { expr, index, alias } => {
                write!(f, "{}[{}]", expr, index)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::Collate { expr, collation, alias } => {
                write!(f, "{} COLLATE \"{}\"", expr, collation)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
            Expr::FieldAccess { expr, field, alias } => {
                write!(f, "({}).{}", expr, field)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Constraint {
    PrimaryKey,
    Unique,
    Nullable,
    Default(String),
    Check(Vec<String>),
    Comment(String),
    References(String),
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
            Constraint::References(target) => write!(f, "ref({})", target),
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
    pub unique: bool,
    /// Index type (e.g., "keyword", "integer", "float", "geo", "text")
    #[serde(default)]
    pub index_type: Option<String>,
}

/// Table-level constraints for composite keys
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableConstraint {
    Unique(Vec<String>),
    PrimaryKey(Vec<String>),
}

// ==================== From Implementations for Ergonomic API ====================

impl From<&str> for Expr {
    /// Convert a string reference to a Named expression.
    /// Enables: `.select(["id", "name"])` instead of `.select([col("id"), col("name")])`
    fn from(s: &str) -> Self {
        Expr::Named(s.to_string())
    }
}

impl From<String> for Expr {
    fn from(s: String) -> Self {
        Expr::Named(s)
    }
}

impl From<&String> for Expr {
    fn from(s: &String) -> Self {
        Expr::Named(s.clone())
    }
}

// ==================== Function and Trigger Definitions ====================

/// PostgreSQL function definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionDef {
    pub name: String,
    pub returns: String,  // e.g., "trigger", "integer", "void"
    pub body: String,     // The function body (PL/pgSQL code)
    pub language: Option<String>,  // Default: plpgsql
}

/// Trigger timing (BEFORE or AFTER)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

/// Trigger event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// PostgreSQL trigger definition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TriggerDef {
    pub name: String,
    pub table: String,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub for_each_row: bool,
    pub execute_function: String,
}

