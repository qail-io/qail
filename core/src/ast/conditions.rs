use crate::ast::{Expr, Operator, Value};
use serde::{Deserialize, Serialize};

/// A single condition within a cage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    pub left: Expr,
    pub op: Operator,
    pub value: Value,
    #[serde(default)]
    pub is_array_unnest: bool,
}

impl std::fmt::Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op.sql_symbol(), self.value)
    }
}
