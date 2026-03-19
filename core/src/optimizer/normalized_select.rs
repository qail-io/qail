use crate::ast::{
    Action, Cage, CageKind, Condition, Expr, Join, JoinKind, LogicalOp, Qail, SortOrder, Value,
};
use std::collections::HashSet;

/// Canonical representation for the rewrite-safe subset of `SELECT`.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedSelect {
    pub table: String,
    pub columns: Vec<Expr>,
    pub joins: Vec<NormalizedJoin>,
    pub filters: Vec<FilterClause>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Canonical JOIN item.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedJoin {
    pub table: String,
    pub kind: JoinKind,
    pub on: Vec<Condition>,
    pub on_true: bool,
}

/// Canonical WHERE clause block.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterClause {
    pub logical_op: LogicalOp,
    pub conditions: Vec<Condition>,
}

/// Canonical ORDER BY item.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub order: SortOrder,
}

/// Errors returned when a query cannot be normalized into the supported subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizeError {
    UnsupportedAction(Action),
    UnsupportedFeature(&'static str),
    DuplicateClause(&'static str),
}

impl std::fmt::Display for NormalizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedAction(action) => {
                write!(f, "normalization only supports SELECT, got {}", action)
            }
            Self::UnsupportedFeature(feature) => {
                write!(f, "unsupported SELECT feature: {}", feature)
            }
            Self::DuplicateClause(clause) => {
                write!(f, "duplicate {} clause is not supported", clause)
            }
        }
    }
}

impl std::error::Error for NormalizeError {}

/// Normalize a QAIL `SELECT` into a canonical representation.
pub fn normalize_select(qail: &Qail) -> Result<NormalizedSelect, NormalizeError> {
    NormalizedSelect::try_from(qail)
}

impl TryFrom<&Qail> for NormalizedSelect {
    type Error = NormalizeError;

    fn try_from(qail: &Qail) -> Result<Self, Self::Error> {
        if qail.action != Action::Get {
            return Err(NormalizeError::UnsupportedAction(qail.action));
        }

        reject_unsupported_select_features(qail)?;

        let mut filters = Vec::new();
        let mut order_by = Vec::new();
        let mut limit = None;
        let mut offset = None;

        for cage in &qail.cages {
            match &cage.kind {
                CageKind::Filter => filters.push(FilterClause {
                    logical_op: cage.logical_op,
                    conditions: cage.conditions.clone(),
                }),
                CageKind::Sort(order) => {
                    for condition in &cage.conditions {
                        order_by.push(OrderByItem {
                            expr: condition.left.clone(),
                            order: *order,
                        });
                    }
                }
                CageKind::Limit(value) => {
                    if limit.replace(*value).is_some() {
                        return Err(NormalizeError::DuplicateClause("LIMIT"));
                    }
                }
                CageKind::Offset(value) => {
                    if offset.replace(*value).is_some() {
                        return Err(NormalizeError::DuplicateClause("OFFSET"));
                    }
                }
                CageKind::Payload => {
                    return Err(NormalizeError::UnsupportedFeature("payload cages"));
                }
                CageKind::Sample(_) => {
                    return Err(NormalizeError::UnsupportedFeature("sample cages"));
                }
                CageKind::Qualify => {
                    return Err(NormalizeError::UnsupportedFeature("QUALIFY cages"));
                }
                CageKind::Partition => {
                    return Err(NormalizeError::UnsupportedFeature("GROUP BY cages"));
                }
            }
        }

        let columns = if qail.columns.is_empty() {
            vec![Expr::Star]
        } else {
            qail.columns.clone()
        };

        let joins = qail
            .joins
            .iter()
            .map(|join| NormalizedJoin {
                table: join.table.clone(),
                kind: join.kind.clone(),
                on: join.on.clone().unwrap_or_default(),
                on_true: join.on_true,
            })
            .collect();

        Ok(Self {
            table: qail.table.clone(),
            columns,
            joins,
            filters,
            order_by,
            limit,
            offset,
        })
    }
}

impl NormalizedSelect {
    /// Apply a deterministic cleanup rewrite that preserves the supported-subset semantics.
    ///
    /// Cleanup rules:
    /// - merge all `AND` filter clauses into one clause
    /// - merge all `OR` filter clauses into one clause
    /// - remove empty filter clauses
    /// - sort and dedupe conditions in filter and join ON clauses
    /// - dedupe repeated ORDER BY items while preserving the first occurrence
    pub fn cleaned(&self) -> Self {
        let mut cleaned = self.clone();

        for join in &mut cleaned.joins {
            if join.on_true {
                join.on.clear();
            } else {
                join.on = dedupe_conditions_sorted(std::mem::take(&mut join.on));
            }
        }

        let mut and_conditions = Vec::new();
        let mut or_conditions = Vec::new();
        for filter in &cleaned.filters {
            match filter.logical_op {
                LogicalOp::And => and_conditions.extend(filter.conditions.clone()),
                LogicalOp::Or => or_conditions.extend(filter.conditions.clone()),
            }
        }

        and_conditions = dedupe_conditions_sorted(and_conditions);
        or_conditions = dedupe_conditions_sorted(or_conditions);

        cleaned.filters.clear();
        if !and_conditions.is_empty() {
            cleaned.filters.push(FilterClause {
                logical_op: LogicalOp::And,
                conditions: and_conditions,
            });
        }
        if !or_conditions.is_empty() {
            cleaned.filters.push(FilterClause {
                logical_op: LogicalOp::Or,
                conditions: or_conditions,
            });
        }

        cleaned.order_by = dedupe_order_by_stable(std::mem::take(&mut cleaned.order_by));

        cleaned
    }

    /// Return a canonicalized clone for structural comparison.
    ///
    /// This only reorders semantically commutative components:
    /// - condition order within a filter clause
    /// - condition order within a join `ON` conjunction
    ///
    /// It intentionally preserves the order of:
    /// - projection columns
    /// - joins
    /// - filter clauses
    /// - ORDER BY items
    pub fn canonicalized(&self) -> Self {
        self.cleaned()
    }

    /// Compare two normalized queries under the supported-subset semantics.
    pub fn equivalent_shape(&self, other: &Self) -> bool {
        self.canonicalized() == other.canonicalized()
    }

    /// Lower the normalized form back into a canonical QAIL `SELECT`.
    pub fn to_qail(&self) -> Qail {
        let mut qail = Qail {
            action: Action::Get,
            table: self.table.clone(),
            columns: self.columns.clone(),
            joins: self
                .joins
                .iter()
                .map(|join| Join {
                    table: join.table.clone(),
                    kind: join.kind.clone(),
                    on: if join.on_true {
                        None
                    } else {
                        Some(join.on.clone())
                    },
                    on_true: join.on_true,
                })
                .collect(),
            ..Default::default()
        };

        for filter in &self.filters {
            qail.cages.push(Cage {
                kind: CageKind::Filter,
                conditions: filter.conditions.clone(),
                logical_op: filter.logical_op,
            });
        }

        for item in &self.order_by {
            qail.cages.push(Cage {
                kind: CageKind::Sort(item.order),
                conditions: vec![Condition {
                    left: item.expr.clone(),
                    op: crate::ast::Operator::Eq,
                    value: Value::Null,
                    is_array_unnest: false,
                }],
                logical_op: LogicalOp::And,
            });
        }

        if let Some(limit) = self.limit {
            qail.cages.push(Cage {
                kind: CageKind::Limit(limit),
                conditions: Vec::new(),
                logical_op: LogicalOp::And,
            });
        }

        if let Some(offset) = self.offset {
            qail.cages.push(Cage {
                kind: CageKind::Offset(offset),
                conditions: Vec::new(),
                logical_op: LogicalOp::And,
            });
        }

        qail
    }
}

fn reject_unsupported_select_features(qail: &Qail) -> Result<(), NormalizeError> {
    if qail.distinct {
        return Err(NormalizeError::UnsupportedFeature("DISTINCT"));
    }
    if !qail.distinct_on.is_empty() {
        return Err(NormalizeError::UnsupportedFeature("DISTINCT ON"));
    }
    if !qail.table_constraints.is_empty() {
        return Err(NormalizeError::UnsupportedFeature("table constraints"));
    }
    if !qail.set_ops.is_empty() {
        return Err(NormalizeError::UnsupportedFeature("set operations"));
    }
    if !qail.having.is_empty() {
        return Err(NormalizeError::UnsupportedFeature("HAVING"));
    }
    if !qail.group_by_mode.is_simple() {
        return Err(NormalizeError::UnsupportedFeature("GROUP BY mode"));
    }
    if !qail.ctes.is_empty() {
        return Err(NormalizeError::UnsupportedFeature("CTEs"));
    }
    if qail.returning.is_some() {
        return Err(NormalizeError::UnsupportedFeature("RETURNING"));
    }
    if qail.on_conflict.is_some() {
        return Err(NormalizeError::UnsupportedFeature("ON CONFLICT"));
    }
    if qail.source_query.is_some() {
        return Err(NormalizeError::UnsupportedFeature("source query"));
    }
    if qail.channel.is_some() || qail.payload.is_some() {
        return Err(NormalizeError::UnsupportedFeature("LISTEN/NOTIFY metadata"));
    }
    if qail.savepoint_name.is_some() {
        return Err(NormalizeError::UnsupportedFeature("savepoint metadata"));
    }
    if !qail.from_tables.is_empty() || !qail.using_tables.is_empty() {
        return Err(NormalizeError::UnsupportedFeature(
            "auxiliary FROM/USING tables",
        ));
    }
    if qail.lock_mode.is_some() || qail.skip_locked {
        return Err(NormalizeError::UnsupportedFeature("row locks"));
    }
    if qail.fetch.is_some() {
        return Err(NormalizeError::UnsupportedFeature("FETCH FIRST"));
    }
    if qail.default_values {
        return Err(NormalizeError::UnsupportedFeature("DEFAULT VALUES"));
    }
    if qail.overriding.is_some() {
        return Err(NormalizeError::UnsupportedFeature("OVERRIDING"));
    }
    if qail.sample.is_some() {
        return Err(NormalizeError::UnsupportedFeature("TABLESAMPLE"));
    }
    if qail.only_table {
        return Err(NormalizeError::UnsupportedFeature("ONLY"));
    }
    if qail.vector.is_some()
        || qail.score_threshold.is_some()
        || qail.vector_name.is_some()
        || qail.with_vector
        || qail.vector_size.is_some()
        || qail.distance.is_some()
        || qail.on_disk.is_some()
    {
        return Err(NormalizeError::UnsupportedFeature("vector search fields"));
    }
    if qail.function_def.is_some() || qail.trigger_def.is_some() {
        return Err(NormalizeError::UnsupportedFeature("procedural objects"));
    }

    Ok(())
}

fn condition_signature(condition: &Condition) -> String {
    format!(
        "{}|{}|{}|{}",
        expr_signature(&condition.left),
        condition.op.sql_symbol(),
        value_signature(&condition.value),
        condition.is_array_unnest
    )
}

fn expr_signature(expr: &Expr) -> String {
    format!("{}", expr)
}

fn value_signature(value: &Value) -> String {
    format!("{}", value)
}

fn order_item_signature(item: &OrderByItem) -> String {
    format!("{}|{:?}", expr_signature(&item.expr), item.order)
}

fn dedupe_conditions_sorted(mut conditions: Vec<Condition>) -> Vec<Condition> {
    conditions.sort_by_key(condition_signature);

    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(conditions.len());
    for condition in conditions {
        let signature = condition_signature(&condition);
        if seen.insert(signature) {
            deduped.push(condition);
        }
    }
    deduped
}

fn dedupe_order_by_stable(items: Vec<OrderByItem>) -> Vec<OrderByItem> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(items.len());
    for item in items {
        let signature = order_item_signature(&item);
        if seen.insert(signature) {
            deduped.push(item);
        }
    }
    deduped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Cage, Operator};
    use crate::optimizer::cleanup_select;

    #[test]
    fn normalize_supported_select_into_canonical_form() {
        let qail = Qail::get("users")
            .filter("active", Operator::Eq, true)
            .or_filter("role", Operator::Eq, "admin")
            .left_join("teams", "users.team_id", "teams.id")
            .order_desc("created_at")
            .limit(10)
            .offset(5);

        let normalized = normalize_select(&qail).expect("supported select should normalize");

        assert_eq!(normalized.table, "users");
        assert_eq!(normalized.columns, vec![Expr::Star]);
        assert_eq!(normalized.joins.len(), 1);
        assert_eq!(normalized.filters.len(), 2);
        assert_eq!(normalized.filters[0].logical_op, LogicalOp::And);
        assert_eq!(normalized.filters[1].logical_op, LogicalOp::Or);
        assert_eq!(normalized.order_by.len(), 1);
        assert_eq!(normalized.limit, Some(10));
        assert_eq!(normalized.offset, Some(5));
    }

    #[test]
    fn normalize_flattens_multi_expr_sort_cages() {
        let qail = Qail {
            action: Action::Get,
            table: "users".to_string(),
            cages: vec![Cage {
                kind: CageKind::Sort(SortOrder::Asc),
                conditions: vec![
                    Condition {
                        left: Expr::Named("last_name".to_string()),
                        op: Operator::Eq,
                        value: Value::Null,
                        is_array_unnest: false,
                    },
                    Condition {
                        left: Expr::Named("first_name".to_string()),
                        op: Operator::Eq,
                        value: Value::Null,
                        is_array_unnest: false,
                    },
                ],
                logical_op: LogicalOp::And,
            }],
            ..Default::default()
        };

        let normalized = normalize_select(&qail).expect("sort cage should normalize");

        assert_eq!(
            normalized.order_by,
            vec![
                OrderByItem {
                    expr: Expr::Named("last_name".to_string()),
                    order: SortOrder::Asc,
                },
                OrderByItem {
                    expr: Expr::Named("first_name".to_string()),
                    order: SortOrder::Asc,
                },
            ]
        );
    }

    #[test]
    fn normalize_rejects_unsupported_features() {
        let qail = Qail::get("users").distinct_on(["email"]);
        let err = normalize_select(&qail).expect_err("DISTINCT ON should be rejected");
        assert_eq!(err, NormalizeError::UnsupportedFeature("DISTINCT ON"));
    }

    #[test]
    fn normalize_rejects_duplicate_limit() {
        let qail = Qail::get("users").limit(10).limit(20);
        let err = normalize_select(&qail).expect_err("duplicate LIMIT should be rejected");
        assert_eq!(err, NormalizeError::DuplicateClause("LIMIT"));
    }

    #[test]
    fn normalized_select_roundtrips_to_canonical_qail() {
        let qail = Qail::get("users")
            .column("id")
            .column("email")
            .filter("active", Operator::Eq, true)
            .order_asc("email")
            .limit(25);

        let normalized = normalize_select(&qail).expect("supported select should normalize");
        let roundtrip = normalized.to_qail();

        assert_eq!(roundtrip.action, Action::Get);
        assert_eq!(roundtrip.table, "users");
        assert_eq!(
            roundtrip.columns,
            vec![Expr::Named("id".into()), Expr::Named("email".into())]
        );
        assert_eq!(
            normalize_select(&roundtrip).expect("roundtrip should normalize"),
            normalized
        );
    }

    #[test]
    fn equivalent_shape_ignores_condition_order_within_filter_clause() {
        let left = Qail::get("users")
            .filter("active", Operator::Eq, true)
            .filter("role", Operator::Eq, "admin");

        let right = Qail {
            action: Action::Get,
            table: "users".to_string(),
            cages: vec![Cage {
                kind: CageKind::Filter,
                conditions: vec![
                    Condition {
                        left: Expr::Named("role".to_string()),
                        op: Operator::Eq,
                        value: Value::String("admin".to_string()),
                        is_array_unnest: false,
                    },
                    Condition {
                        left: Expr::Named("active".to_string()),
                        op: Operator::Eq,
                        value: Value::Bool(true),
                        is_array_unnest: false,
                    },
                ],
                logical_op: LogicalOp::And,
            }],
            ..Default::default()
        };

        let left = normalize_select(&left).expect("left query should normalize");
        let right = normalize_select(&right).expect("right query should normalize");

        assert!(left.equivalent_shape(&right));
    }

    #[test]
    fn equivalent_shape_preserves_order_sensitive_parts() {
        let left = normalize_select(
            &Qail::get("users")
                .column("id")
                .column("email")
                .order_asc("email"),
        )
        .expect("left query should normalize");

        let right = normalize_select(
            &Qail::get("users")
                .column("email")
                .column("id")
                .order_asc("email"),
        )
        .expect("right query should normalize");

        assert!(!left.equivalent_shape(&right));
    }

    #[test]
    fn cleanup_merges_filter_clauses_and_dedupes_conditions() {
        let qail = Qail {
            action: Action::Get,
            table: "users".to_string(),
            cages: vec![
                Cage {
                    kind: CageKind::Filter,
                    conditions: vec![Condition {
                        left: Expr::Named("active".to_string()),
                        op: Operator::Eq,
                        value: Value::Bool(true),
                        is_array_unnest: false,
                    }],
                    logical_op: LogicalOp::And,
                },
                Cage {
                    kind: CageKind::Filter,
                    conditions: vec![Condition {
                        left: Expr::Named("active".to_string()),
                        op: Operator::Eq,
                        value: Value::Bool(true),
                        is_array_unnest: false,
                    }],
                    logical_op: LogicalOp::And,
                },
                Cage {
                    kind: CageKind::Filter,
                    conditions: vec![Condition {
                        left: Expr::Named("role".to_string()),
                        op: Operator::Eq,
                        value: Value::String("admin".to_string()),
                        is_array_unnest: false,
                    }],
                    logical_op: LogicalOp::Or,
                },
            ],
            ..Default::default()
        };

        let normalized = normalize_select(&qail).expect("query should normalize");
        let cleaned = cleanup_select(&normalized);

        assert_eq!(cleaned.filters.len(), 2);
        assert_eq!(cleaned.filters[0].logical_op, LogicalOp::And);
        assert_eq!(cleaned.filters[0].conditions.len(), 1);
        assert_eq!(cleaned.filters[1].logical_op, LogicalOp::Or);
        assert_eq!(cleaned.filters[1].conditions.len(), 1);
    }

    #[test]
    fn cleanup_dedupes_order_by_stably() {
        let qail = Qail::get("users")
            .order_asc("email")
            .order_asc("email")
            .order_desc("created_at");
        let normalized = normalize_select(&qail).expect("query should normalize");
        let cleaned = cleanup_select(&normalized);

        assert_eq!(
            cleaned.order_by,
            vec![
                OrderByItem {
                    expr: Expr::Named("email".to_string()),
                    order: SortOrder::Asc,
                },
                OrderByItem {
                    expr: Expr::Named("created_at".to_string()),
                    order: SortOrder::Desc,
                },
            ]
        );
    }

    #[test]
    fn cleanup_is_idempotent() {
        let qail = Qail::get("users")
            .filter("active", Operator::Eq, true)
            .filter("active", Operator::Eq, true)
            .order_asc("email")
            .order_asc("email");

        let normalized = normalize_select(&qail).expect("query should normalize");
        let cleaned = cleanup_select(&normalized);
        let cleaned_twice = cleanup_select(&cleaned);

        assert_eq!(cleaned, cleaned_twice);
    }

    #[test]
    fn cleanup_preserves_equivalent_shape() {
        let qail = Qail::get("users")
            .filter("active", Operator::Eq, true)
            .filter("active", Operator::Eq, true)
            .or_filter("role", Operator::Eq, "admin");

        let normalized = normalize_select(&qail).expect("query should normalize");
        let cleaned = cleanup_select(&normalized);

        assert!(normalized.equivalent_shape(&cleaned));
    }
}
