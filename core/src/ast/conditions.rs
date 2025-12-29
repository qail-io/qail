use crate::ast::{Expr, Operator, Value};
use serde::{Deserialize, Serialize};

/// A single condition within a cage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    /// Left hand side expression (usually a column)
    pub left: Expr,
    /// Comparison operator
    pub op: Operator,
    /// Value to compare against
    pub value: Value,
    #[serde(default)]
    pub is_array_unnest: bool,
}

impl std::fmt::Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use the operator's sql_symbol() method - single source of truth
        write!(f, "{} {} {}", self.left, self.op.sql_symbol(), self.value)
    }
}
