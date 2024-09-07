use crate::ast::{
    Action, Cage, CageKind, Condition, Expr, GroupByMode, IndexDef, Join, LockMode,
    LogicalOp, Operator, OverridingKind, SampleMethod, SetOp, TableConstraint, Value,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Qail {
    pub action: Action,
    pub table: String,
    pub columns: Vec<Expr>,
    #[serde(default)]
    pub joins: Vec<Join>,
    pub cages: Vec<Cage>,
    #[serde(default)]
    pub distinct: bool,
    #[serde(default)]
    pub index_def: Option<IndexDef>,
    #[serde(default)]
    pub table_constraints: Vec<TableConstraint>,
    #[serde(default)]
    pub set_ops: Vec<(SetOp, Box<Qail>)>,
    #[serde(default)]
    pub having: Vec<Condition>,
    #[serde(default)]
    pub group_by_mode: GroupByMode,
    #[serde(default)]
    pub ctes: Vec<CTEDef>,
    #[serde(default)]
    pub distinct_on: Vec<Expr>,
    #[serde(default)]
    pub returning: Option<Vec<Expr>>,
    #[serde(default)]
    pub on_conflict: Option<OnConflict>,
    #[serde(default)]
    pub source_query: Option<Box<Qail>>,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub payload: Option<String>,
    #[serde(default)]
    pub savepoint_name: Option<String>,
    #[serde(default)]
    pub from_tables: Vec<String>,
    #[serde(default)]
    pub using_tables: Vec<String>,
    #[serde(default)]
    pub lock_mode: Option<LockMode>,
    #[serde(default)]
    pub fetch: Option<(u64, bool)>,
    #[serde(default)]
    pub default_values: bool,
    #[serde(default)]
    pub overriding: Option<OverridingKind>,
    #[serde(default)]
    pub sample: Option<(SampleMethod, f64, Option<u64>)>,
    #[serde(default)]
    pub only_table: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CTEDef {
    pub name: String,
    pub recursive: bool,
    pub columns: Vec<String>,
    pub base_query: Box<Qail>,
    pub recursive_query: Option<Box<Qail>>,
    pub source_table: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnConflict {
    pub columns: Vec<String>,
    pub action: ConflictAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConflictAction {
    DoNothing,
    DoUpdate {
        assignments: Vec<(String, Expr)>,
    },
}

impl Default for OnConflict {
    fn default() -> Self {
        Self {
            columns: vec![],
            action: ConflictAction::DoNothing,
        }
    }
}

impl Default for Qail {
    fn default() -> Self {
        Self {
            action: Action::Get,
            table: String::new(),
            columns: vec![],
            joins: vec![],
            cages: vec![],
            distinct: false,
            index_def: None,
            table_constraints: vec![],
            set_ops: vec![],
            having: vec![],
            group_by_mode: GroupByMode::Simple,
            ctes: vec![],
            distinct_on: vec![],
            returning: None,
            on_conflict: None,
            source_query: None,
            channel: None,
            payload: None,
            savepoint_name: None,
            from_tables: vec![],
            using_tables: vec![],
            lock_mode: None,
            fetch: None,
            default_values: false,
            overriding: None,
            sample: None,
            only_table: false,
        }
    }
}

// Submodules with builder methods
mod advanced;
mod constructors;
mod cte;
mod query;

// Deprecated methods kept in main module for backward compatibility
impl Qail {
    #[deprecated(since = "0.11.0", note = "Use .columns([...]) instead")]
    pub fn hook(mut self, cols: &[&str]) -> Self {
        self.columns = cols.iter().map(|c| Expr::Named(c.to_string())).collect();
        self
    }

    #[deprecated(since = "0.11.0", note = "Use .filter(column, Operator::Eq, value) or .where_eq(column, value) instead")]
    pub fn cage(mut self, column: &str, value: impl Into<Value>) -> Self {
        self.cages.push(Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named(column.to_string()),
                op: Operator::Eq,
                value: value.into(),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        });
        self
    }
}
