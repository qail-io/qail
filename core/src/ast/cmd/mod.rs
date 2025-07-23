use crate::ast::{
    Action, Cage, CageKind, Condition, Distance, Expr, GroupByMode, IndexDef, Join, LockMode,
    LogicalOp, Operator, OverridingKind, SampleMethod, SetOp, TableConstraint, Value,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Qail {
    pub action: Action,
    pub table: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<Expr>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub joins: Vec<Join>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cages: Vec<Cage>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub distinct: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_def: Option<IndexDef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub table_constraints: Vec<TableConstraint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub set_ops: Vec<(SetOp, Box<Qail>)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub having: Vec<Condition>,
    #[serde(default, skip_serializing_if = "GroupByMode::is_simple")]
    pub group_by_mode: GroupByMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ctes: Vec<CTEDef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub distinct_on: Vec<Expr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returning: Option<Vec<Expr>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_conflict: Option<OnConflict>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_query: Option<Box<Qail>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub savepoint_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from_tables: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub using_tables: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock_mode: Option<LockMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch: Option<(u64, bool)>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub default_values: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overriding: Option<OverridingKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample: Option<(SampleMethod, f64, Option<u64>)>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub only_table: bool,
    // Vector database fields (Qdrant)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score_threshold: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_name: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub with_vector: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub distance: Option<Distance>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_disk: Option<bool>,
    // PostgreSQL procedural objects
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_def: Option<crate::ast::FunctionDef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_def: Option<crate::ast::TriggerDef>,
    // Redis fields
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_value: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redis_ttl: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redis_set_condition: Option<String>,
}

/// Helper for skip_serializing_if on bool fields
fn is_false(b: &bool) -> bool {
    !*b
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
            // Vector database fields
            vector: None,
            score_threshold: None,
            vector_name: None,
            with_vector: false,
            vector_size: None,
            distance: None,
            on_disk: None,
            // Procedural objects
            function_def: None,
            trigger_def: None,
            // Redis fields
            raw_value: None,
            redis_ttl: None,
            redis_set_condition: None,
        }
    }
}

// Submodules with builder methods
mod advanced;
mod constructors;
mod cte;
mod query;
mod vector;

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

impl std::fmt::Display for Qail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Use the Formatter from the fmt module for canonical output
        use crate::fmt::Formatter;
        match Formatter::new().format(self) {
            Ok(s) => write!(f, "{}", s),
            Err(_) => write!(f, "{:?}", self), // Fallback to Debug
        }
    }
}
