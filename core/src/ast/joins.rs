use crate::ast::{Condition, JoinKind};

/// A JOIN clause in the query.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    /// Target table (may include alias).
    pub table: String,
    /// Join kind (LEFT, INNER, etc.).
    pub kind: JoinKind,
    /// ON conditions.
    pub on: Option<Vec<Condition>>,
    /// If true, use ON TRUE (unconditional join). Used for joining CTEs.
    pub on_true: bool,
}
