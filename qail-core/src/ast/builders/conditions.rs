//! Condition builders for WHERE clauses.

use crate::ast::{Condition, Expr, Operator, Value};

/// Helper to create a condition
fn make_condition(column: &str, op: Operator, value: Value) -> Condition {
    Condition {
        left: Expr::Named(column.to_string()),
        op,
        value,
        is_array_unnest: false,
    }
}

/// Create an equality condition (column = value)
pub fn eq(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Eq, value.into())
}

/// Create a not-equal condition (column != value)
pub fn ne(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Ne, value.into())
}

/// Create a greater-than condition (column > value)
pub fn gt(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Gt, value.into())
}

/// Create a greater-than-or-equal condition (column >= value)
pub fn gte(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Gte, value.into())
}

/// Create a less-than condition (column < value)
pub fn lt(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Lt, value.into())
}

/// Create a less-than-or-equal condition (column <= value)
pub fn lte(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Lte, value.into())
}

/// Create an IN condition (column IN (values))
pub fn is_in<V: Into<Value>>(column: &str, values: impl IntoIterator<Item = V>) -> Condition {
    let vals: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
    make_condition(column, Operator::In, Value::Array(vals))
}

/// Create a NOT IN condition (column NOT IN (values))
pub fn not_in<V: Into<Value>>(column: &str, values: impl IntoIterator<Item = V>) -> Condition {
    let vals: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
    make_condition(column, Operator::NotIn, Value::Array(vals))
}

/// Create an IS NULL condition
pub fn is_null(column: &str) -> Condition {
    make_condition(column, Operator::IsNull, Value::Null)
}

/// Create an IS NOT NULL condition
pub fn is_not_null(column: &str) -> Condition {
    make_condition(column, Operator::IsNotNull, Value::Null)
}

/// Create a LIKE condition (column LIKE pattern)
pub fn like(column: &str, pattern: &str) -> Condition {
    make_condition(column, Operator::Like, Value::String(pattern.to_string()))
}

/// Create an ILIKE condition (case-insensitive LIKE)
pub fn ilike(column: &str, pattern: &str) -> Condition {
    make_condition(column, Operator::ILike, Value::String(pattern.to_string()))
}

/// Create a condition with an expression on the left side (for JSON, functions, etc.)
pub fn cond(left: Expr, op: Operator, value: impl Into<Value>) -> Condition {
    Condition {
        left,
        op,
        value: value.into(),
        is_array_unnest: false,
    }
}
