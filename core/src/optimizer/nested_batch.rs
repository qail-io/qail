use std::collections::HashSet;

use crate::ast::{Condition, Expr, LogicalOp, Operator, Value};
use crate::schema::RelationRegistry;

use super::{FilterClause, NormalizedSelect};

/// Direction and shape of nested relation expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NestedRelationKind {
    /// Parent row references a single related row (object expansion).
    ForwardObject,
    /// Related rows reference parent row (array expansion).
    ReverseArray,
}

/// Planned batched fetch for nested relation expansion.
#[derive(Debug, Clone, PartialEq)]
pub struct NestedBatchPlan {
    pub kind: NestedRelationKind,
    pub parent_table: String,
    pub related_table: String,
    /// Column read from parent rows for key extraction.
    pub parent_key_column: String,
    /// Column filtered on related table with `IN (...)`.
    pub related_match_column: String,
    /// Canonicalized batched related fetch query.
    pub query: NormalizedSelect,
}

impl NestedBatchPlan {
    /// Convert the planned query into executable QAIL.
    pub fn to_qail(&self) -> crate::ast::Qail {
        self.query.to_qail()
    }
}

/// Errors from nested batch planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchPlanError {
    RelationNotFound {
        parent_table: String,
        related_table: String,
    },
}

impl std::fmt::Display for BatchPlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RelationNotFound {
                parent_table,
                related_table,
            } => write!(
                f,
                "no relation found between '{}' and '{}'",
                parent_table, related_table
            ),
        }
    }
}

impl std::error::Error for BatchPlanError {}

/// Plan a batched nested-relation fetch using relation metadata and parent keys.
///
/// Returns `Ok(None)` when the provided keys are empty after null-elision.
pub fn plan_nested_batch_fetch(
    relations: &RelationRegistry,
    parent_table: &str,
    related_table: &str,
    parent_keys: Vec<Value>,
) -> Result<Option<NestedBatchPlan>, BatchPlanError> {
    let normalized_keys = canonicalize_non_null_values(parent_keys);
    if normalized_keys.is_empty() {
        return Ok(None);
    }

    let (kind, parent_key_column, related_match_column) =
        if let Some((fk_col, ref_col)) = relations.get(parent_table, related_table) {
            (
                NestedRelationKind::ForwardObject,
                fk_col.to_string(),
                ref_col.to_string(),
            )
        } else if let Some((fk_col, ref_col)) = relations.get(related_table, parent_table) {
            (
                NestedRelationKind::ReverseArray,
                ref_col.to_string(),
                fk_col.to_string(),
            )
        } else {
            return Err(BatchPlanError::RelationNotFound {
                parent_table: parent_table.to_string(),
                related_table: related_table.to_string(),
            });
        };

    let query = NormalizedSelect {
        table: related_table.to_string(),
        columns: vec![Expr::Star],
        joins: Vec::new(),
        filters: vec![FilterClause {
            logical_op: LogicalOp::And,
            conditions: vec![Condition {
                left: Expr::Named(related_match_column.clone()),
                op: Operator::In,
                value: Value::Array(normalized_keys),
                is_array_unnest: false,
            }],
        }],
        order_by: Vec::new(),
        limit: None,
        offset: None,
    }
    .cleaned();

    Ok(Some(NestedBatchPlan {
        kind,
        parent_table: parent_table.to_string(),
        related_table: related_table.to_string(),
        parent_key_column,
        related_match_column,
        query,
    }))
}

fn canonicalize_non_null_values(values: Vec<Value>) -> Vec<Value> {
    let mut pairs: Vec<(String, Value)> = values
        .into_iter()
        .filter(|v| !is_null_like(v))
        .map(|v| (value_signature(&v), v))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));

    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(pairs.len());
    for (sig, value) in pairs {
        if seen.insert(sig) {
            deduped.push(value);
        }
    }
    deduped
}

fn is_null_like(value: &Value) -> bool {
    matches!(value, Value::Null | Value::NullUuid)
}

fn value_signature(value: &Value) -> String {
    format!("{}", value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Action, Qail};
    use crate::optimizer::normalize_select;
    use uuid::Uuid;

    #[test]
    fn forward_relation_builds_object_plan() {
        let mut relations = RelationRegistry::new();
        relations.register("orders", "user_id", "users", "id");

        let plan = plan_nested_batch_fetch(
            &relations,
            "orders",
            "users",
            vec![
                Value::String("u2".to_string()),
                Value::Null,
                Value::String("u1".to_string()),
                Value::String("u2".to_string()),
            ],
        )
        .expect("planning should succeed")
        .expect("keys are non-empty");

        assert_eq!(plan.kind, NestedRelationKind::ForwardObject);
        assert_eq!(plan.parent_key_column, "user_id");
        assert_eq!(plan.related_match_column, "id");
        assert_eq!(plan.related_table, "users");
        assert_eq!(plan.query.table, "users");

        let Some(filter) = plan.query.filters.first() else {
            panic!("missing filter");
        };
        assert_eq!(filter.conditions.len(), 1);
        assert_eq!(filter.conditions[0].left, Expr::Named("id".to_string()));
        assert_eq!(filter.conditions[0].op, Operator::In);
        assert_eq!(
            filter.conditions[0].value,
            Value::Array(vec![
                Value::String("u1".to_string()),
                Value::String("u2".to_string()),
            ])
        );
    }

    #[test]
    fn reverse_relation_builds_array_plan() {
        let mut relations = RelationRegistry::new();
        relations.register("posts", "user_id", "users", "id");

        let plan = plan_nested_batch_fetch(
            &relations,
            "users",
            "posts",
            vec![Value::Int(2), Value::Int(1)],
        )
        .expect("planning should succeed")
        .expect("keys are non-empty");

        assert_eq!(plan.kind, NestedRelationKind::ReverseArray);
        assert_eq!(plan.parent_key_column, "id");
        assert_eq!(plan.related_match_column, "user_id");
        assert_eq!(plan.related_table, "posts");

        let Some(filter) = plan.query.filters.first() else {
            panic!("missing filter");
        };
        assert_eq!(
            filter.conditions[0].left,
            Expr::Named("user_id".to_string())
        );
        assert_eq!(
            filter.conditions[0].value,
            Value::Array(vec![Value::Int(1), Value::Int(2)])
        );
    }

    #[test]
    fn missing_relation_returns_error() {
        let relations = RelationRegistry::new();
        let err = plan_nested_batch_fetch(
            &relations,
            "users",
            "invoices",
            vec![Value::Int(1), Value::Int(2)],
        )
        .expect_err("relation should be required");

        assert_eq!(
            err,
            BatchPlanError::RelationNotFound {
                parent_table: "users".to_string(),
                related_table: "invoices".to_string(),
            }
        );
    }

    #[test]
    fn null_only_keys_skip_plan() {
        let mut relations = RelationRegistry::new();
        relations.register("posts", "user_id", "users", "id");

        let plan = plan_nested_batch_fetch(
            &relations,
            "users",
            "posts",
            vec![Value::Null, Value::NullUuid],
        )
        .expect("planning should succeed");

        assert!(plan.is_none());
    }

    #[test]
    fn plan_query_roundtrips_through_qail() {
        let mut relations = RelationRegistry::new();
        relations.register("posts", "user_id", "users", "id");

        let plan = plan_nested_batch_fetch(
            &relations,
            "users",
            "posts",
            vec![Value::Uuid(Uuid::nil()), Value::Uuid(Uuid::nil())],
        )
        .expect("planning should succeed")
        .expect("keys are non-empty");

        let qail: Qail = plan.to_qail();
        assert_eq!(qail.action, Action::Get);
        let normalized = normalize_select(&qail).expect("planned query should normalize");
        assert!(plan.query.equivalent_shape(&normalized));
    }
}
