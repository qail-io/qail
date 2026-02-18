use crate::ast::{Condition, LogicalOp, SortOrder};
use serde::{Deserialize, Serialize};

/// A cage (constraint block) in the query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cage {
    /// Kind of constraint.
    pub kind: CageKind,
    /// Conditions within this cage.
    pub conditions: Vec<Condition>,
    /// Logical operator joining conditions.
    pub logical_op: LogicalOp,
}

/// The type of cage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CageKind {
    /// WHERE filter.
    Filter,
    /// SET/INSERT payload.
    Payload,
    /// ORDER BY.
    Sort(SortOrder),
    /// LIMIT.
    Limit(usize),
    /// OFFSET.
    Offset(usize),
    /// TABLESAMPLE.
    Sample(usize),
    /// Window QUALIFY.
    Qualify,
    /// GROUP BY.
    Partition,
}
