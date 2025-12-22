use serde::{Deserialize, Serialize};
use crate::ast::{Condition, LogicalOp, SortOrder};

/// A cage (constraint block) in the query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cage {
    /// The type of cage
    pub kind: CageKind,
    /// Conditions within this cage
    pub conditions: Vec<Condition>,
    /// Logical operator between conditions (AND or OR)
    pub logical_op: LogicalOp,
}

/// The type of cage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CageKind {
    /// WHERE filter
    Filter,
    /// SET payload (for updates)
    Payload,
    /// ORDER BY
    Sort(SortOrder),
    /// LIMIT
    Limit(usize),
    /// OFFSET
    Offset(usize),
    /// TABLESAMPLE - percentage of rows
    Sample(usize),
    /// QUALIFY - filter on window function results
    Qualify,
    /// PARTITION BY - window function partitioning
    Partition,
}
