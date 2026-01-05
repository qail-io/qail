use crate::ast::{Condition, JoinKind};
use serde::{Deserialize, Serialize};

/// A JOIN clause in the query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Join {
    /// Target table (may include alias).
    pub table: String,
    /// Join kind (LEFT, INNER, etc.).
    pub kind: JoinKind,
    /// ON conditions.
    #[serde(default)]
    pub on: Option<Vec<Condition>>,
    /// If true, use ON TRUE (unconditional join). Used for joining CTEs.
    #[serde(default)]
    pub on_true: bool,
}
