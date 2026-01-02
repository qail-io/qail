use crate::ast::Qail;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Param(usize),
    /// Named parameter reference (:name, :id, etc.)
    NamedParam(String),
    Function(String),
    Array(Vec<Value>),
    Subquery(Box<Qail>),
    Column(String),
    Uuid(Uuid),
    NullUuid,
    /// Time interval (e.g., 24 hours, 7 days)
    Interval { amount: i64, unit: IntervalUnit },
    Timestamp(String),
    /// Binary data (bytea)
    Bytes(Vec<u8>),
    /// AST Expression (for complex expression comparisons like col > NOW() - INTERVAL)
    Expr(Box<crate::ast::Expr>),
    /// Vector embedding for similarity search (Qdrant)
    Vector(Vec<f32>),
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
                write!(f, "(")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, ")")
            }
            Value::Subquery(_) => write!(f, "(SUBQUERY)"),
            Value::Column(s) => write!(f, "{}", s),
            Value::Uuid(u) => write!(f, "'{}'", u),
            Value::NullUuid => write!(f, "NULL"),
            Value::Interval { amount, unit } => write!(f, "INTERVAL '{} {}'", amount, unit),
            Value::Timestamp(ts) => write!(f, "'{}'", ts),
            Value::Bytes(bytes) => {
                write!(f, "'\\x")?;
                for byte in bytes {
                    write!(f, "{:02x}", byte)?;
                }
                write!(f, "'")
            }
            Value::Expr(expr) => write!(f, "{}", expr),
            Value::Vector(v) => {
                write!(f, "[")?;
                for (i, val) in v.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}", val)?;
                }
                write!(f, "]")
            }
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

impl From<Option<String>> for Value {
    fn from(opt: Option<String>) -> Self {
        match opt {
            Some(s) => Value::String(s),
            None => Value::Null,
        }
    }
}

impl<'a> From<Option<&'a str>> for Value {
    fn from(opt: Option<&'a str>) -> Self {
        match opt {
            Some(s) => Value::String(s.to_string()),
            None => Value::Null,
        }
    }
}

impl From<Option<i64>> for Value {
    fn from(opt: Option<i64>) -> Self {
        match opt {
            Some(n) => Value::Int(n),
            None => Value::Null,
        }
    }
}

impl From<Option<i32>> for Value {
    fn from(opt: Option<i32>) -> Self {
        match opt {
            Some(n) => Value::Int(n as i64),
            None => Value::Null,
        }
    }
}

impl From<Option<bool>> for Value {
    fn from(opt: Option<bool>) -> Self {
        match opt {
            Some(b) => Value::Bool(b),
            None => Value::Null,
        }
    }
}
