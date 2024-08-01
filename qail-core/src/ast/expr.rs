use serde::{Deserialize, Serialize};
use crate::ast::{AggregateFunc, Cage, Condition, ModKind, Value};

/// Binary operators for expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinaryOp {
    /// String concatenation (||)
    Concat,
    /// Addition (+)
    Add,
    /// Subtraction (-)
    Sub,
    /// Multiplication (*)
    Mul,
    /// Division (/)
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
/// A general expression node (column, value, function, etc.).
/// Formerly `Column`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// All columns (*)
    Star,
    /// A named column
    Named(String),
    /// An aliased expression (expr AS alias)
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
        col: Box<Expr>,
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
        /// WHEN condition THEN expr pairs (Expr allows functions, values, identifiers)
        when_clauses: Vec<(Condition, Box<Expr>)>,
        /// ELSE expr (optional)
        else_value: Option<Box<Expr>>,
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
}

impl std::fmt::Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::Star => write!(f, "*"),
            Expr::Named(name) => write!(f, "{}", name),
            Expr::Aliased { name, alias } => write!(f, "{} AS {}", name, alias),
            Expr::Aggregate { col, func } => write!(f, "{}({})", func, col),
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
            Expr::Window { name, func, params, partition, order, frame } => {
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
            Expr::Case { when_clauses, else_value, alias } => {
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
            Expr::JsonAccess { column, path, as_text, alias } => {
                let op = if *as_text { "->>" } else { "->" };
                write!(f, "{}{}{}", column, op, path)?;
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
                    if i > 0 { write!(f, " ")?; }
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
            Expr::Binary { left, op, right, alias } => {
                write!(f, "({} {} {})", left, op, right)?;
                if let Some(a) = alias {
                    write!(f, " AS {}", a)?;
                }
                Ok(())
            }
        }
    }
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
