use crate::ast::{Condition, LogicalOp, SortOrder};
use serde::{Deserialize, Serialize};

/// A cage (constraint block) in the query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cage {
    pub kind: CageKind,
    pub conditions: Vec<Condition>,
    pub logical_op: LogicalOp,
}

/// The type of cage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CageKind {
    Filter,
    Payload,
    Sort(SortOrder),
    Limit(usize),
    Offset(usize),
    Sample(usize),
    Qualify,
    Partition,
}
