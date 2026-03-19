use crate::ast::{
    Action, Cage, CageKind, Condition, Expr, LogicalOp, OnConflict, OverridingKind, Qail, Value,
};
use std::collections::HashSet;

/// Canonical condition clause used by mutation normalization.
#[derive(Debug, Clone, PartialEq)]
pub struct MutationClause {
    pub logical_op: LogicalOp,
    pub conditions: Vec<Condition>,
}

/// Canonical representation for the rewrite-safe subset of mutation queries.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedMutation {
    pub action: Action,
    pub table: String,
    pub columns: Vec<Expr>,
    pub payload: Vec<MutationClause>,
    pub filters: Vec<MutationClause>,
    pub returning: Option<Vec<Expr>>,
    pub on_conflict: Option<OnConflict>,
    pub source_query: Option<Box<Qail>>,
    pub default_values: bool,
    pub overriding: Option<OverridingKind>,
    pub from_tables: Vec<String>,
    pub using_tables: Vec<String>,
    pub only_table: bool,
}

/// Errors returned when a query cannot be normalized into the supported mutation subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizeMutationError {
    UnsupportedAction(Action),
    UnsupportedFeature(&'static str),
    InvalidShape(&'static str),
}

impl std::fmt::Display for NormalizeMutationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedAction(action) => {
                write!(
                    f,
                    "mutation normalization only supports ADD/SET/DEL, got {}",
                    action
                )
            }
            Self::UnsupportedFeature(feature) => {
                write!(f, "unsupported mutation feature: {}", feature)
            }
            Self::InvalidShape(shape) => write!(f, "invalid mutation shape: {}", shape),
        }
    }
}

impl std::error::Error for NormalizeMutationError {}

/// Normalize a QAIL mutation (`ADD`, `SET`, `DEL`) into a canonical representation.
pub fn normalize_mutation(qail: &Qail) -> Result<NormalizedMutation, NormalizeMutationError> {
    NormalizedMutation::try_from(qail)
}

impl TryFrom<&Qail> for NormalizedMutation {
    type Error = NormalizeMutationError;

    fn try_from(qail: &Qail) -> Result<Self, Self::Error> {
        if !matches!(qail.action, Action::Add | Action::Set | Action::Del) {
            return Err(NormalizeMutationError::UnsupportedAction(qail.action));
        }

        reject_unsupported_mutation_features(qail)?;

        let mut payload = Vec::new();
        let mut filters = Vec::new();
        for cage in &qail.cages {
            match &cage.kind {
                CageKind::Payload => payload.push(MutationClause {
                    logical_op: cage.logical_op,
                    conditions: cage.conditions.clone(),
                }),
                CageKind::Filter => filters.push(MutationClause {
                    logical_op: cage.logical_op,
                    conditions: cage.conditions.clone(),
                }),
                CageKind::Sort(_) => {
                    return Err(NormalizeMutationError::UnsupportedFeature("ORDER BY cages"));
                }
                CageKind::Limit(_) => {
                    return Err(NormalizeMutationError::UnsupportedFeature("LIMIT cages"));
                }
                CageKind::Offset(_) => {
                    return Err(NormalizeMutationError::UnsupportedFeature("OFFSET cages"));
                }
                CageKind::Sample(_) => {
                    return Err(NormalizeMutationError::UnsupportedFeature("sample cages"));
                }
                CageKind::Qualify => {
                    return Err(NormalizeMutationError::UnsupportedFeature("QUALIFY cages"));
                }
                CageKind::Partition => {
                    return Err(NormalizeMutationError::UnsupportedFeature("GROUP BY cages"));
                }
            }
        }

        match qail.action {
            Action::Add => {
                if !qail.from_tables.is_empty() || !qail.using_tables.is_empty() || qail.only_table
                {
                    return Err(NormalizeMutationError::UnsupportedFeature(
                        "INSERT with FROM/USING/ONLY",
                    ));
                }

                if !filters.is_empty() {
                    return Err(NormalizeMutationError::UnsupportedFeature(
                        "INSERT filter cages",
                    ));
                }

                let inserts_from_non_payload =
                    qail.default_values || qail.source_query.is_some() || qail.columns.is_empty();

                if inserts_from_non_payload {
                    if !payload.is_empty() {
                        return Err(NormalizeMutationError::InvalidShape(
                            "payload cages with DEFAULT VALUES or INSERT ... SELECT",
                        ));
                    }
                } else if payload.len() != 1 {
                    return Err(NormalizeMutationError::InvalidShape(
                        "INSERT requires exactly one payload cage",
                    ));
                }
            }
            Action::Set => {
                if !qail.using_tables.is_empty() {
                    return Err(NormalizeMutationError::UnsupportedFeature("UPDATE USING"));
                }
                if qail.on_conflict.is_some() {
                    return Err(NormalizeMutationError::UnsupportedFeature(
                        "UPDATE ON CONFLICT",
                    ));
                }
                if qail.source_query.is_some() {
                    return Err(NormalizeMutationError::UnsupportedFeature(
                        "UPDATE source query",
                    ));
                }
                if qail.default_values {
                    return Err(NormalizeMutationError::UnsupportedFeature(
                        "UPDATE DEFAULT VALUES",
                    ));
                }
                if qail.overriding.is_some() {
                    return Err(NormalizeMutationError::UnsupportedFeature(
                        "UPDATE OVERRIDING",
                    ));
                }
            }
            Action::Del => {
                if !qail.from_tables.is_empty() {
                    return Err(NormalizeMutationError::UnsupportedFeature("DELETE FROM"));
                }
                if !payload.is_empty() {
                    return Err(NormalizeMutationError::UnsupportedFeature(
                        "DELETE payload cages",
                    ));
                }
                if qail.on_conflict.is_some() {
                    return Err(NormalizeMutationError::UnsupportedFeature(
                        "DELETE ON CONFLICT",
                    ));
                }
                if qail.source_query.is_some() {
                    return Err(NormalizeMutationError::UnsupportedFeature(
                        "DELETE source query",
                    ));
                }
                if qail.default_values {
                    return Err(NormalizeMutationError::UnsupportedFeature(
                        "DELETE DEFAULT VALUES",
                    ));
                }
                if qail.overriding.is_some() {
                    return Err(NormalizeMutationError::UnsupportedFeature(
                        "DELETE OVERRIDING",
                    ));
                }
            }
            _ => unreachable!("unsupported action already rejected"),
        }

        Ok(Self {
            action: qail.action,
            table: qail.table.clone(),
            columns: qail.columns.clone(),
            payload,
            filters,
            returning: qail.returning.clone(),
            on_conflict: qail.on_conflict.clone(),
            source_query: qail.source_query.clone(),
            default_values: qail.default_values,
            overriding: qail.overriding,
            from_tables: qail.from_tables.clone(),
            using_tables: qail.using_tables.clone(),
            only_table: qail.only_table,
        })
    }
}

impl NormalizedMutation {
    /// Apply deterministic cleanup on supported mutation shapes.
    ///
    /// Cleanup rules:
    /// - merge multiple `SET` payload cages into one (preserving assignment order)
    /// - merge all `AND` filter clauses into one clause
    /// - merge all `OR` filter clauses into one clause
    /// - remove empty filter clauses
    /// - sort and dedupe filter conditions
    pub fn cleaned(&self) -> Self {
        let mut cleaned = self.clone();

        if cleaned.action == Action::Set {
            let mut merged_payload = Vec::new();
            for clause in &cleaned.payload {
                merged_payload.extend(clause.conditions.clone());
            }
            cleaned.payload = if merged_payload.is_empty() {
                Vec::new()
            } else {
                vec![MutationClause {
                    logical_op: LogicalOp::And,
                    conditions: merged_payload,
                }]
            };
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
            cleaned.filters.push(MutationClause {
                logical_op: LogicalOp::And,
                conditions: and_conditions,
            });
        }
        if !or_conditions.is_empty() {
            cleaned.filters.push(MutationClause {
                logical_op: LogicalOp::Or,
                conditions: or_conditions,
            });
        }

        cleaned
    }

    /// Return a canonicalized clone for structural comparison.
    pub fn canonicalized(&self) -> Self {
        self.cleaned()
    }

    /// Compare two normalized mutations under the supported-subset semantics.
    pub fn equivalent_shape(&self, other: &Self) -> bool {
        self.canonicalized() == other.canonicalized()
    }

    /// Lower the normalized form back into canonical QAIL.
    pub fn to_qail(&self) -> Qail {
        let mut qail = Qail {
            action: self.action,
            table: self.table.clone(),
            columns: self.columns.clone(),
            returning: self.returning.clone(),
            on_conflict: self.on_conflict.clone(),
            source_query: self.source_query.clone(),
            default_values: self.default_values,
            overriding: self.overriding,
            from_tables: self.from_tables.clone(),
            using_tables: self.using_tables.clone(),
            only_table: self.only_table,
            ..Default::default()
        };

        for payload in &self.payload {
            qail.cages.push(Cage {
                kind: CageKind::Payload,
                conditions: payload.conditions.clone(),
                logical_op: payload.logical_op,
            });
        }

        for filter in &self.filters {
            qail.cages.push(Cage {
                kind: CageKind::Filter,
                conditions: filter.conditions.clone(),
                logical_op: filter.logical_op,
            });
        }

        qail
    }
}

fn reject_unsupported_mutation_features(qail: &Qail) -> Result<(), NormalizeMutationError> {
    if !qail.joins.is_empty() {
        return Err(NormalizeMutationError::UnsupportedFeature("joins"));
    }
    if qail.distinct {
        return Err(NormalizeMutationError::UnsupportedFeature("DISTINCT"));
    }
    if qail.index_def.is_some() {
        return Err(NormalizeMutationError::UnsupportedFeature(
            "index definitions",
        ));
    }
    if !qail.table_constraints.is_empty() {
        return Err(NormalizeMutationError::UnsupportedFeature(
            "table constraints",
        ));
    }
    if !qail.set_ops.is_empty() {
        return Err(NormalizeMutationError::UnsupportedFeature("set operations"));
    }
    if !qail.having.is_empty() {
        return Err(NormalizeMutationError::UnsupportedFeature("HAVING"));
    }
    if !qail.group_by_mode.is_simple() {
        return Err(NormalizeMutationError::UnsupportedFeature("GROUP BY mode"));
    }
    if !qail.ctes.is_empty() {
        return Err(NormalizeMutationError::UnsupportedFeature("CTEs"));
    }
    if !qail.distinct_on.is_empty() {
        return Err(NormalizeMutationError::UnsupportedFeature("DISTINCT ON"));
    }
    if qail.channel.is_some() || qail.payload.is_some() {
        return Err(NormalizeMutationError::UnsupportedFeature(
            "LISTEN/NOTIFY metadata",
        ));
    }
    if qail.savepoint_name.is_some() {
        return Err(NormalizeMutationError::UnsupportedFeature(
            "savepoint metadata",
        ));
    }
    if qail.lock_mode.is_some() || qail.skip_locked {
        return Err(NormalizeMutationError::UnsupportedFeature("row locks"));
    }
    if qail.fetch.is_some() {
        return Err(NormalizeMutationError::UnsupportedFeature("FETCH FIRST"));
    }
    if qail.sample.is_some() {
        return Err(NormalizeMutationError::UnsupportedFeature("TABLESAMPLE"));
    }
    if qail.vector.is_some()
        || qail.score_threshold.is_some()
        || qail.vector_name.is_some()
        || qail.with_vector
        || qail.vector_size.is_some()
        || qail.distance.is_some()
        || qail.on_disk.is_some()
    {
        return Err(NormalizeMutationError::UnsupportedFeature(
            "vector search fields",
        ));
    }
    if qail.function_def.is_some() || qail.trigger_def.is_some() {
        return Err(NormalizeMutationError::UnsupportedFeature(
            "procedural objects",
        ));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Cage, ConflictAction, Operator};

    fn eq(col: &str, value: Value) -> Condition {
        Condition {
            left: Expr::Named(col.to_string()),
            op: Operator::Eq,
            value,
            is_array_unnest: false,
        }
    }

    #[test]
    fn normalize_supported_insert_shape() {
        let qail = Qail {
            action: Action::Add,
            table: "users".to_string(),
            columns: vec![
                Expr::Named("id".to_string()),
                Expr::Named("email".to_string()),
            ],
            cages: vec![Cage {
                kind: CageKind::Payload,
                logical_op: LogicalOp::And,
                conditions: vec![
                    eq("id", Value::Int(1)),
                    eq("email", Value::String("a@b.com".to_string())),
                ],
            }],
            on_conflict: Some(OnConflict {
                columns: vec!["id".to_string()],
                action: ConflictAction::DoNothing,
            }),
            returning: Some(vec![Expr::Star]),
            ..Default::default()
        };

        let normalized = normalize_mutation(&qail).expect("insert should normalize");
        assert_eq!(normalized.action, Action::Add);
        assert_eq!(normalized.table, "users");
        assert_eq!(normalized.payload.len(), 1);
        assert!(normalized.filters.is_empty());
        assert!(normalized.on_conflict.is_some());
    }

    #[test]
    fn normalize_rejects_insert_with_multiple_payload_cages() {
        let qail = Qail {
            action: Action::Add,
            table: "users".to_string(),
            columns: vec![Expr::Named("id".to_string())],
            cages: vec![
                Cage {
                    kind: CageKind::Payload,
                    logical_op: LogicalOp::And,
                    conditions: vec![eq("id", Value::Int(1))],
                },
                Cage {
                    kind: CageKind::Payload,
                    logical_op: LogicalOp::And,
                    conditions: vec![eq("id", Value::Int(2))],
                },
            ],
            ..Default::default()
        };

        let err = normalize_mutation(&qail).expect_err("multiple payload cages must be rejected");
        assert_eq!(
            err,
            NormalizeMutationError::InvalidShape("INSERT requires exactly one payload cage")
        );
    }

    #[test]
    fn normalize_supported_update_shape() {
        let qail = Qail {
            action: Action::Set,
            table: "users".to_string(),
            cages: vec![
                Cage {
                    kind: CageKind::Payload,
                    logical_op: LogicalOp::And,
                    conditions: vec![eq("name", Value::String("Alice".to_string()))],
                },
                Cage {
                    kind: CageKind::Filter,
                    logical_op: LogicalOp::And,
                    conditions: vec![eq("id", Value::Int(7))],
                },
            ],
            from_tables: vec!["teams".to_string()],
            ..Default::default()
        };

        let normalized = normalize_mutation(&qail).expect("update should normalize");
        assert_eq!(normalized.action, Action::Set);
        assert_eq!(normalized.from_tables, vec!["teams".to_string()]);
        assert_eq!(normalized.payload.len(), 1);
        assert_eq!(normalized.filters.len(), 1);
    }

    #[test]
    fn normalize_supported_delete_shape() {
        let qail = Qail {
            action: Action::Del,
            table: "users".to_string(),
            cages: vec![Cage {
                kind: CageKind::Filter,
                logical_op: LogicalOp::And,
                conditions: vec![eq("id", Value::Int(9))],
            }],
            using_tables: vec!["teams".to_string()],
            only_table: true,
            ..Default::default()
        };

        let normalized = normalize_mutation(&qail).expect("delete should normalize");
        assert_eq!(normalized.action, Action::Del);
        assert_eq!(normalized.using_tables, vec!["teams".to_string()]);
        assert!(normalized.payload.is_empty());
        assert!(normalized.only_table);
    }

    #[test]
    fn cleanup_merges_update_payload_and_filter_clauses() {
        let qail = Qail {
            action: Action::Set,
            table: "users".to_string(),
            cages: vec![
                Cage {
                    kind: CageKind::Payload,
                    logical_op: LogicalOp::And,
                    conditions: vec![eq("name", Value::String("Alice".to_string()))],
                },
                Cage {
                    kind: CageKind::Payload,
                    logical_op: LogicalOp::And,
                    conditions: vec![eq("active", Value::Bool(true))],
                },
                Cage {
                    kind: CageKind::Filter,
                    logical_op: LogicalOp::And,
                    conditions: vec![eq("id", Value::Int(1)), eq("id", Value::Int(1))],
                },
                Cage {
                    kind: CageKind::Filter,
                    logical_op: LogicalOp::Or,
                    conditions: vec![eq("role", Value::String("admin".to_string()))],
                },
            ],
            ..Default::default()
        };

        let normalized = normalize_mutation(&qail).expect("update should normalize");
        let cleaned = normalized.cleaned();

        assert_eq!(cleaned.payload.len(), 1);
        assert_eq!(cleaned.payload[0].conditions.len(), 2);
        assert_eq!(cleaned.filters.len(), 2);
        assert_eq!(cleaned.filters[0].logical_op, LogicalOp::And);
        assert_eq!(cleaned.filters[0].conditions.len(), 1);
        assert_eq!(cleaned.filters[1].logical_op, LogicalOp::Or);
    }

    #[test]
    fn normalized_mutation_roundtrips_to_canonical_qail() {
        let qail = Qail {
            action: Action::Set,
            table: "users".to_string(),
            cages: vec![
                Cage {
                    kind: CageKind::Payload,
                    logical_op: LogicalOp::And,
                    conditions: vec![eq("email", Value::String("x@y.com".to_string()))],
                },
                Cage {
                    kind: CageKind::Filter,
                    logical_op: LogicalOp::And,
                    conditions: vec![eq("id", Value::Int(42))],
                },
            ],
            ..Default::default()
        };

        let normalized = normalize_mutation(&qail).expect("update should normalize");
        let roundtrip = normalized.to_qail();
        let roundtrip_normalized =
            normalize_mutation(&roundtrip).expect("roundtrip should normalize");

        assert!(normalized.equivalent_shape(&roundtrip_normalized));
    }

    #[test]
    fn cleanup_is_idempotent() {
        let qail = Qail {
            action: Action::Del,
            table: "users".to_string(),
            cages: vec![
                Cage {
                    kind: CageKind::Filter,
                    logical_op: LogicalOp::And,
                    conditions: vec![eq("id", Value::Int(1)), eq("id", Value::Int(1))],
                },
                Cage {
                    kind: CageKind::Filter,
                    logical_op: LogicalOp::And,
                    conditions: vec![eq("active", Value::Bool(true))],
                },
            ],
            ..Default::default()
        };

        let normalized = normalize_mutation(&qail).expect("delete should normalize");
        let cleaned = normalized.cleaned();
        let cleaned_twice = cleaned.cleaned();
        assert_eq!(cleaned, cleaned_twice);
    }

    #[test]
    fn equivalent_shape_ignores_filter_condition_order() {
        let left = Qail {
            action: Action::Del,
            table: "users".to_string(),
            cages: vec![Cage {
                kind: CageKind::Filter,
                logical_op: LogicalOp::And,
                conditions: vec![
                    eq("active", Value::Bool(true)),
                    eq("tenant_id", Value::String("t1".to_string())),
                ],
            }],
            ..Default::default()
        };
        let right = Qail {
            action: Action::Del,
            table: "users".to_string(),
            cages: vec![Cage {
                kind: CageKind::Filter,
                logical_op: LogicalOp::And,
                conditions: vec![
                    eq("tenant_id", Value::String("t1".to_string())),
                    eq("active", Value::Bool(true)),
                ],
            }],
            ..Default::default()
        };

        let left = normalize_mutation(&left).expect("left mutation should normalize");
        let right = normalize_mutation(&right).expect("right mutation should normalize");

        assert!(left.equivalent_shape(&right));
    }
}
