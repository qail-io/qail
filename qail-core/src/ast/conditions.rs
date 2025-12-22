use serde::{Deserialize, Serialize};
use crate::ast::{Operator, Value};

/// A single condition within a cage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    /// Column name
    pub column: String,
    /// Comparison operator
    pub op: Operator,
    /// Value to compare against
    pub value: Value,
    /// Whether this is an array unnest operation (column[*])
    #[serde(default)]
    pub is_array_unnest: bool,
}
