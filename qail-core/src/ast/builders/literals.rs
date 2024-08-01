//! Literal value builders.

use crate::ast::{Expr, Value};

/// Create an integer literal expression
pub fn int(value: i64) -> Expr {
    Expr::Literal(Value::Int(value))
}

/// Create a float literal expression  
pub fn float(value: f64) -> Expr {
    Expr::Literal(Value::Float(value))
}

/// Create a string literal expression
pub fn text(value: &str) -> Expr {
    Expr::Literal(Value::String(value.to_string()))
}

/// Create a boolean literal
pub fn boolean(value: bool) -> Expr {
    Expr::Literal(Value::Bool(value))
}

/// Create a NULL literal
pub fn null() -> Expr {
    Expr::Literal(Value::Null)
}

/// Create a parameter value for binding
pub fn bind<V: Into<Value>>(value: V) -> Value {
    value.into()
}
