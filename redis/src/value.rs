//! Redis value types returned from commands.

/// A value returned from Redis.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Null value (key doesn't exist, etc.)
    Null,
    /// Simple string (status replies like "OK")
    String(String),
    /// Bulk string (actual data)
    Bulk(Vec<u8>),
    /// Integer
    Integer(i64),
    /// Boolean (RESP3)
    Boolean(bool),
    /// Double (RESP3)
    Double(f64),
    /// Array of values
    Array(Vec<Value>),
    /// Map of key-value pairs (RESP3)
    Map(Vec<(Value, Value)>),
    /// Error from Redis
    Error(String),
}

impl Value {
    /// Try to get as string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            Value::Bulk(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }

    /// Try to get as bytes.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bulk(b) => Some(b),
            Value::String(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// Try to get as integer.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to get as boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            Value::Integer(i) => Some(*i != 0),
            _ => None,
        }
    }

    /// Check if null.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if error.
    pub fn is_error(&self) -> bool {
        matches!(self, Value::Error(_))
    }
}
