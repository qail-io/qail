use serde::{Deserialize, Serialize};
use crate::ast::{AggregateFunc, Cage, Condition, ModKind, Value};

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
