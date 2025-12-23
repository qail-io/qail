use serde::{Deserialize, Serialize};
use crate::ast::{Operator, Value, Expr};

/// A single condition within a cage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    /// Left hand side expression (usually a column)
    pub left: Expr,
    /// Comparison operator
    pub op: Operator,
    /// Value to compare against
    pub value: Value,
    /// Whether this is an array unnest operation (column[*])
    #[serde(default)]
    pub is_array_unnest: bool,
}
