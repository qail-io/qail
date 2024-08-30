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

/// Create a NOT LIKE condition
pub fn not_like(column: &str, pattern: &str) -> Condition {
    make_condition(column, Operator::NotLike, Value::String(pattern.to_string()))
}

/// Create a BETWEEN condition (column BETWEEN low AND high)
pub fn between(column: &str, low: impl Into<Value>, high: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Between, Value::Array(vec![low.into(), high.into()]))
}

/// Create a NOT BETWEEN condition
pub fn not_between(column: &str, low: impl Into<Value>, high: impl Into<Value>) -> Condition {
    make_condition(column, Operator::NotBetween, Value::Array(vec![low.into(), high.into()]))
}

/// Create a regex match condition (column ~ pattern)
pub fn regex(column: &str, pattern: &str) -> Condition {
    make_condition(column, Operator::Regex, Value::String(pattern.to_string()))
}

/// Create a case-insensitive regex match (column ~* pattern)
pub fn regex_i(column: &str, pattern: &str) -> Condition {
    make_condition(column, Operator::RegexI, Value::String(pattern.to_string()))
}

/// Create an array/JSONB containment condition (column @> value)
pub fn contains<V: Into<Value>>(column: &str, values: impl IntoIterator<Item = V>) -> Condition {
    let vals: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
    make_condition(column, Operator::Contains, Value::Array(vals))
}

/// Create an array overlap condition (column && values)
pub fn overlaps<V: Into<Value>>(column: &str, values: impl IntoIterator<Item = V>) -> Condition {
    let vals: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
    make_condition(column, Operator::Overlaps, Value::Array(vals))
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

/// Create a SIMILAR TO pattern condition
pub fn similar_to(column: &str, pattern: &str) -> Condition {
    make_condition(column, Operator::SimilarTo, Value::String(pattern.to_string()))
}

/// Create a JSON key exists condition (column ? 'key')
pub fn key_exists(column: &str, key: &str) -> Condition {
    make_condition(column, Operator::KeyExists, Value::String(key.to_string()))
}
