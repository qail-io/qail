use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::ast::QailCmd;

/// Time interval unit for duration expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntervalUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl std::fmt::Display for IntervalUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IntervalUnit::Second => write!(f, "seconds"),
            IntervalUnit::Minute => write!(f, "minutes"),
            IntervalUnit::Hour => write!(f, "hours"),
            IntervalUnit::Day => write!(f, "days"),
            IntervalUnit::Week => write!(f, "weeks"),
            IntervalUnit::Month => write!(f, "months"),
            IntervalUnit::Year => write!(f, "years"),
        }
    }
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
    /// Named parameter reference (:name, :id, etc.)
    NamedParam(String),
    /// SQL function call (e.g., now())
    Function(String),
    /// Array of values
    Array(Vec<Value>),
    /// Subquery for IN/EXISTS expressions (e.g., WHERE id IN (SELECT ...))
    Subquery(Box<QailCmd>),
    /// Column reference (e.g. JOIN ... ON a.id = b.id)
    Column(String),
    /// UUID value
    Uuid(Uuid),
    /// Null UUID value (for typed NULL in UUID columns)
    NullUuid,
    /// Time interval (e.g., 24 hours, 7 days)
    Interval { amount: i64, unit: IntervalUnit },
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(n) => write!(f, "{}", n),
            Value::Float(n) => write!(f, "{}", n),
            Value::String(s) => write!(f, "'{}'", s),
            Value::Param(n) => write!(f, "${}", n),
            Value::NamedParam(name) => write!(f, ":{}", name),
            Value::Function(s) => write!(f, "{}", s),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            },
            Value::Subquery(_) => write!(f, "(SUBQUERY)"),
            Value::Column(s) => write!(f, "{}", s),
            Value::Uuid(u) => write!(f, "'{}'", u),
            Value::NullUuid => write!(f, "NULL"),
            Value::Interval { amount, unit } => write!(f, "INTERVAL '{} {}'", amount, unit),
        }
    }
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


impl From<Uuid> for Value {
    fn from(u: Uuid) -> Self {
        Value::Uuid(u)
    }
}

impl From<Option<Uuid>> for Value {
    fn from(opt: Option<Uuid>) -> Self {
        match opt {
            Some(u) => Value::Uuid(u),
            None => Value::NullUuid,
        }
    }
}
