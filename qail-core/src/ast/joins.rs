use crate::ast::{Condition, JoinKind};
use serde::{Deserialize, Serialize};

/// A join definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Join {
    pub table: String,
    pub kind: JoinKind,
    #[serde(default)]
    pub on: Option<Vec<Condition>>,
    /// If true, use ON TRUE (unconditional join). Used for joining CTEs.
    #[serde(default)]
    pub on_true: bool,
}
