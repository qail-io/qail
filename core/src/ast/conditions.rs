use crate::ast::{Expr, Operator, Value};

/// A single condition within a cage.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Condition {
    /// Left-hand expression.
    pub left: Expr,
    /// Comparison operator.
    pub op: Operator,
    /// Right-hand value.
    pub value: Value,
    /// Whether to unnest array values.
    pub is_array_unnest: bool,
}

impl std::fmt::Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op.sql_symbol(), self.value)
    }
}
