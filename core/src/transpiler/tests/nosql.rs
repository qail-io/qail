use crate::transpiler::nosql::qdrant::ToQdrant;

#[test]
fn test_qdrant_search() {
    use crate::ast::*;
    // Qdrant with vector search uses special syntax, use manual construction
    let mut cmd = Qail::get("points");
    cmd.columns.push(Expr::Named("id".to_string()));
    cmd.columns.push(Expr::Named("score".to_string()));
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![
            Condition {
                left: Expr::Named("vector".to_string()),
                op: Operator::Fuzzy,
                value: Value::String("cute cat".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("city".to_string()),
                op: Operator::Eq,
                value: Value::String("London".to_string()),
                is_array_unnest: false,
            },
        ],
        logical_op: LogicalOp::And,
    });
    cmd.cages.push(Cage {
        kind: CageKind::Limit(10),
        conditions: vec![],
        logical_op: LogicalOp::And,
    });
    let qdrant = cmd.to_qdrant_search();

    assert!(qdrant.contains("{{EMBED:cute cat}}"));
    assert!(qdrant.contains("\"filter\": { \"must\": ["));
    assert!(qdrant.contains("\"key\": \"city\", \"match\": { \"value\": \"London\" }"));
}

#[test]
fn test_qdrant_or_filter_output() {
    use crate::ast::{Operator, Qail};

    let qdrant = Qail::get("points")
        .or_filter("city", Operator::Eq, "London")
        .or_filter("country", Operator::Eq, "UK")
        .to_qdrant_search();

    assert!(
        qdrant.contains("\"should\": ["),
        "Expected should group: {qdrant}"
    );
}

#[test]
fn test_qdrant_and_plus_or_filter_output() {
    use crate::ast::{Operator, Qail};

    let qdrant = Qail::get("points")
        .filter("is_active", Operator::Eq, true)
        .or_filter("city", Operator::Eq, "London")
        .or_filter("country", Operator::Eq, "UK")
        .to_qdrant_search();

    assert!(
        qdrant.contains("\"must\": ["),
        "Expected must group: {qdrant}"
    );
    assert!(
        qdrant.contains("\"should\": ["),
        "Expected should group: {qdrant}"
    );
}

#[test]
fn test_qdrant_multiple_or_cages_remain_separate_groups() {
    use crate::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let qdrant = Qail {
        table: "points".to_string(),
        cages: vec![
            Cage {
                kind: CageKind::Filter,
                conditions: vec![Condition {
                    left: Expr::Named("tenant_id".to_string()),
                    op: Operator::Eq,
                    value: Value::String("t1".to_string()),
                    is_array_unnest: false,
                }],
                logical_op: LogicalOp::And,
            },
            Cage {
                kind: CageKind::Filter,
                conditions: vec![
                    Condition {
                        left: Expr::Named("city".to_string()),
                        op: Operator::Eq,
                        value: Value::String("London".to_string()),
                        is_array_unnest: false,
                    },
                    Condition {
                        left: Expr::Named("city".to_string()),
                        op: Operator::Eq,
                        value: Value::String("Paris".to_string()),
                        is_array_unnest: false,
                    },
                ],
                logical_op: LogicalOp::Or,
            },
            Cage {
                kind: CageKind::Filter,
                conditions: vec![
                    Condition {
                        left: Expr::Named("country".to_string()),
                        op: Operator::Eq,
                        value: Value::String("UK".to_string()),
                        is_array_unnest: false,
                    },
                    Condition {
                        left: Expr::Named("country".to_string()),
                        op: Operator::Eq,
                        value: Value::String("FR".to_string()),
                        is_array_unnest: false,
                    },
                ],
                logical_op: LogicalOp::Or,
            },
        ],
        ..Default::default()
    }
    .to_qdrant_search();

    let should_count = qdrant.matches("\"should\": [").count();
    assert!(
        should_count >= 2,
        "Expected multiple nested OR groups, got {should_count}: {qdrant}"
    );
}

#[test]
fn test_qdrant_json_strings_are_escaped() {
    use crate::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let search = Qail {
        table: "points".to_string(),
        columns: vec![Expr::Named("payload\"key".to_string())],
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![
                Condition {
                    left: Expr::Named("vector".to_string()),
                    op: Operator::Fuzzy,
                    value: Value::String("cute \"cat\"".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("city\", \"must\": [".to_string()),
                    op: Operator::Eq,
                    value: Value::String("London\"}, \"must\": []".to_string()),
                    is_array_unnest: false,
                },
            ],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_qdrant_search();

    let parsed: serde_json::Value =
        serde_json::from_str(&search).expect("qdrant search JSON must stay valid");
    assert_eq!(parsed["vector"], "{{EMBED:cute \"cat\"}}");
    assert_eq!(parsed["with_payload"]["include"][0], "payload\"key");
    assert_eq!(parsed["filter"]["must"][0]["key"], "city\", \"must\": [");
    assert_eq!(
        parsed["filter"]["must"][0]["match"]["value"],
        "London\"}, \"must\": []"
    );

    let upsert = Qail {
        action: crate::ast::Action::Add,
        table: "points".to_string(),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![Condition {
                left: Expr::Named("name\"bad".to_string()),
                op: Operator::Eq,
                value: Value::String("Ana\"bad".to_string()),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_qdrant_search();

    let parsed: serde_json::Value =
        serde_json::from_str(&upsert).expect("qdrant upsert JSON must stay valid");
    assert_eq!(parsed["points"][0]["payload"]["name\"bad"], "Ana\"bad");
}
