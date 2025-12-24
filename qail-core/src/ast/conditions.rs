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

impl std::fmt::Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let op_str = match self.op {
            Operator::Eq => "=",
            Operator::Ne => "!=",
            Operator::Gt => ">",
            Operator::Gte => ">=",
            Operator::Lt => "<",
            Operator::Lte => "<=",
            Operator::Like => "LIKE",
            Operator::ILike => "ILIKE",
            Operator::NotLike => "NOT LIKE",
            Operator::NotILike => "NOT ILIKE",
            Operator::In => "IN",
            Operator::NotIn => "NOT IN",
            Operator::IsNull => "IS NULL",
            Operator::IsNotNull => "IS NOT NULL",
            Operator::Fuzzy => "ILIKE",
            Operator::Contains => "@>",
            Operator::KeyExists => "?",
            Operator::JsonExists => "JSON_EXISTS",
            Operator::JsonQuery => "JSON_QUERY",
            Operator::JsonValue => "JSON_VALUE",
            Operator::Between => "BETWEEN",
            Operator::NotBetween => "NOT BETWEEN",
        };
        write!(f, "{} {} {}", self.left, op_str, self.value)
    }
}
