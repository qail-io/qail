use crate::transpiler::nosql::{dynamo::ToDynamo, mongo::ToMongo, qdrant::ToQdrant};

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

#[test]
fn test_qdrant_transpiler_rejects_invalid_json_values() {
    use crate::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let search = Qail {
        table: "points".to_string(),
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("score".to_string()),
                op: Operator::Eq,
                value: Value::Float(f64::NAN),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_qdrant_search();

    let parsed: serde_json::Value =
        serde_json::from_str(&search).expect("qdrant error JSON must be valid");
    assert!(
        parsed["error"]
            .as_str()
            .expect("error should be a string")
            .contains("non-finite")
    );
}

#[test]
fn test_qdrant_transpiler_preserves_payload_arrays() {
    use crate::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let upsert = Qail {
        action: Action::Add,
        table: "points".to_string(),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![Condition {
                left: Expr::Named("tags".to_string()),
                op: Operator::Eq,
                value: Value::Array(vec![
                    Value::String("blue".to_string()),
                    Value::Bool(true),
                    Value::Int(7),
                ]),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_qdrant_search();

    let parsed: serde_json::Value =
        serde_json::from_str(&upsert).expect("qdrant upsert JSON must stay valid");
    assert_eq!(
        parsed["points"][0]["payload"]["tags"],
        serde_json::json!(["blue", true, 7])
    );
}

#[test]
fn test_qdrant_transpiler_rejects_invalid_vector_values() {
    use crate::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let search = Qail {
        table: "points".to_string(),
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("vector".to_string()),
                op: Operator::Fuzzy,
                value: Value::Array(vec![Value::String("oops".to_string())]),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_qdrant_search();

    let parsed: serde_json::Value =
        serde_json::from_str(&search).expect("qdrant error JSON must be valid");
    assert!(
        parsed["error"]
            .as_str()
            .expect("error should be a string")
            .contains("vector values must be numeric")
    );
}

#[test]
fn test_qdrant_transpiler_rejects_unsupported_filter_operator() {
    use crate::ast::{Operator, Qail};

    let search = Qail::get("points")
        .filter("city", Operator::Like, "%Lon%")
        .to_qdrant_search();

    let parsed: serde_json::Value =
        serde_json::from_str(&search).expect("qdrant error JSON must be valid");
    assert!(
        parsed["error"]
            .as_str()
            .expect("error should be a string")
            .contains("unsupported Qdrant filter operator")
    );
}

#[test]
fn test_qdrant_delete_without_filter_returns_error_json() {
    use crate::ast::{Action, Qail};

    let delete = Qail {
        action: Action::Del,
        table: "points".to_string(),
        ..Default::default()
    }
    .to_qdrant_search();

    let parsed: serde_json::Value =
        serde_json::from_str(&delete).expect("qdrant error JSON must be valid");
    assert!(
        parsed["error"]
            .as_str()
            .expect("error should be a string")
            .contains("requires an id or filter")
    );
}

#[test]
fn test_mongo_shell_fragments_are_escaped() {
    use crate::ast::{
        Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, SortOrder, Value,
    };

    let insert = Qail {
        action: Action::Add,
        table: "users\"); db.dropDatabase(); //".to_string(),
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
    .to_mongo();

    assert!(insert.starts_with("db.getCollection("), "{insert}");
    assert!(
        insert.contains("\"users\\\"); db.dropDatabase(); //\""),
        "{insert}"
    );
    assert!(
        insert.contains("\"name\\\"bad\": \"Ana\\\"bad\""),
        "{insert}"
    );

    let find = Qail {
        table: "events; db.evil()".to_string(),
        columns: vec![Expr::Named("payload\"key".to_string())],
        cages: vec![
            Cage {
                kind: CageKind::Filter,
                conditions: vec![Condition {
                    left: Expr::Named("city\", $where: evil".to_string()),
                    op: Operator::Eq,
                    value: Value::String("London\" }".to_string()),
                    is_array_unnest: false,
                }],
                logical_op: LogicalOp::And,
            },
            Cage {
                kind: CageKind::Sort(SortOrder::Desc),
                conditions: vec![Condition {
                    left: Expr::Named("score\"bad".to_string()),
                    op: Operator::Eq,
                    value: Value::Null,
                    is_array_unnest: false,
                }],
                logical_op: LogicalOp::And,
            },
        ],
        ..Default::default()
    }
    .to_mongo();

    assert!(find.starts_with("db.getCollection("), "{find}");
    assert!(find.contains("\"events; db.evil()\""), "{find}");
    assert!(find.contains("\"payload\\\"key\": 1"), "{find}");
    assert!(
        find.contains("\"city\\\", $where: evil\": \"London\\\" }\""),
        "{find}"
    );
    assert!(find.contains(".sort({ \"score\\\"bad\": -1 })"), "{find}");
}

#[test]
fn test_dynamo_json_and_expression_names_are_escaped() {
    use crate::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let get = Qail {
        table: "users\"bad".to_string(),
        columns: vec![Expr::Named("payload\"key".to_string())],
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![
                Condition {
                    left: Expr::Named("city\", #x = :evil".to_string()),
                    op: Operator::Eq,
                    value: Value::String("London\"bad".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("index".to_string()),
                    op: Operator::Eq,
                    value: Value::String("gsi\"bad".to_string()),
                    is_array_unnest: false,
                },
            ],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_dynamo();

    let parsed: serde_json::Value =
        serde_json::from_str(&get).expect("dynamo get JSON must stay valid");
    assert_eq!(parsed["TableName"], "users\"bad");
    assert_eq!(parsed["IndexName"], "gsi\"bad");
    assert_eq!(parsed["FilterExpression"], "#f1 = :v1");
    assert_eq!(
        parsed["ExpressionAttributeNames"]["#f1"],
        "city\", #x = :evil"
    );
    assert_eq!(parsed["ExpressionAttributeNames"]["#p1"], "payload\"key");
    assert_eq!(
        parsed["ExpressionAttributeValues"][":v1"]["S"],
        "London\"bad"
    );
    assert_eq!(parsed["ProjectionExpression"], "#p1");

    let update = Qail {
        action: Action::Set,
        table: "users".to_string(),
        cages: vec![
            Cage {
                kind: CageKind::Filter,
                conditions: vec![Condition {
                    left: Expr::Named("pk\"bad".to_string()),
                    op: Operator::Eq,
                    value: Value::String("user\"1".to_string()),
                    is_array_unnest: false,
                }],
                logical_op: LogicalOp::And,
            },
            Cage {
                kind: CageKind::Payload,
                conditions: vec![Condition {
                    left: Expr::Named("set\", danger = :x".to_string()),
                    op: Operator::Eq,
                    value: Value::String("active\"yes".to_string()),
                    is_array_unnest: false,
                }],
                logical_op: LogicalOp::And,
            },
        ],
        ..Default::default()
    }
    .to_dynamo();

    let parsed: serde_json::Value =
        serde_json::from_str(&update).expect("dynamo update JSON must stay valid");
    assert_eq!(parsed["Key"]["pk\"bad"]["S"], "user\"1");
    assert_eq!(parsed["UpdateExpression"], "SET #u101 = :u101");
    assert_eq!(
        parsed["ExpressionAttributeNames"]["#u101"],
        "set\", danger = :x"
    );
    assert_eq!(
        parsed["ExpressionAttributeValues"][":u101"]["S"],
        "active\"yes"
    );
}

#[test]
fn test_dynamo_rejects_non_finite_numbers() {
    use crate::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let put = Qail {
        action: Action::Add,
        table: "users".to_string(),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![Condition {
                left: Expr::Named("score".to_string()),
                op: Operator::Eq,
                value: Value::Float(f64::INFINITY),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_dynamo();

    let parsed: serde_json::Value =
        serde_json::from_str(&put).expect("dynamo error JSON must stay valid");
    assert!(
        parsed["error"]
            .as_str()
            .expect("error should be a string")
            .contains("non-finite")
    );
}

#[test]
fn test_dynamo_rejects_unsupported_filter_operator() {
    use crate::ast::{Operator, Qail};

    let get = Qail::get("users")
        .filter("name", Operator::Like, "%ana%")
        .to_dynamo();

    let parsed: serde_json::Value =
        serde_json::from_str(&get).expect("dynamo error JSON must stay valid");
    assert!(
        parsed["error"]
            .as_str()
            .expect("error should be a string")
            .contains("unsupported DynamoDB filter operator")
    );
}

#[test]
fn test_dynamo_delete_without_key_filter_returns_error_json() {
    use crate::ast::{Action, Qail};

    let delete = Qail {
        action: Action::Del,
        table: "users".to_string(),
        ..Default::default()
    }
    .to_dynamo();

    let parsed: serde_json::Value =
        serde_json::from_str(&delete).expect("dynamo error JSON must stay valid");
    assert!(
        parsed["error"]
            .as_str()
            .expect("error should be a string")
            .contains("requires an equality key filter")
    );
}

#[test]
fn test_dynamo_preserves_array_payload_values() {
    use crate::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let put = Qail {
        action: Action::Add,
        table: "users".to_string(),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![Condition {
                left: Expr::Named("tags".to_string()),
                op: Operator::Eq,
                value: Value::Array(vec![
                    Value::String("blue".to_string()),
                    Value::Bool(true),
                    Value::Int(7),
                ]),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_dynamo();

    let parsed: serde_json::Value =
        serde_json::from_str(&put).expect("dynamo put JSON must stay valid");
    assert_eq!(parsed["Item"]["tags"]["L"][0]["S"], "blue");
    assert_eq!(parsed["Item"]["tags"]["L"][1]["BOOL"], true);
    assert_eq!(parsed["Item"]["tags"]["L"][2]["N"], "7");
}
