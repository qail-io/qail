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
fn test_qdrant_native_vector_builders_are_supported() {
    use crate::ast::{Operator, Qail, Value};

    let search = Qail::search("points")
        .vector(vec![0.1, 0.2])
        .filter("city", Operator::Eq, "London")
        .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&search).expect("native qdrant search JSON must be valid");
    assert!(parsed.get("error").is_none(), "{search}");
    assert_eq!(parsed["vector"], serde_json::json!([0.1, 0.2]));
    assert_eq!(parsed["filter"]["must"][0]["key"], "city");

    let upsert = Qail::upsert("points")
        .set_value("id", "point-1")
        .set_value("vector", Value::Vector(vec![0.3, 0.4]))
        .set_value("title", "Native builder")
        .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&upsert).expect("native qdrant upsert JSON must be valid");
    assert!(parsed.get("error").is_none(), "{upsert}");
    assert_eq!(parsed["points"][0]["id"], "point-1");
    assert_eq!(parsed["points"][0]["vector"], serde_json::json!([0.3, 0.4]));
    assert_eq!(parsed["points"][0]["payload"]["title"], "Native builder");

    let scroll = Qail::scroll("points")
        .filter("city", Operator::Eq, "London")
        .limit(25)
        .offset(42)
        .columns(["id", "title", "vector"])
        .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&scroll).expect("native qdrant scroll JSON must be valid");
    assert!(parsed.get("error").is_none(), "{scroll}");
    assert_eq!(parsed["filter"]["must"][0]["key"], "city");
    assert_eq!(parsed["limit"], 25);
    assert_eq!(parsed["offset"], 42);
    assert_eq!(
        parsed["with_payload"]["include"],
        serde_json::json!(["title"])
    );
    assert_eq!(parsed["with_vector"], true);
}

#[test]
fn test_qdrant_or_filter_output() {
    use crate::ast::{Operator, Qail};

    let qdrant = Qail::get("points")
        .vector(vec![0.1, 0.2])
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
        .vector(vec![0.1, 0.2])
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
        vector: Some(vec![0.1, 0.2]),
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
            conditions: vec![
                Condition {
                    left: Expr::Named("id".to_string()),
                    op: Operator::Eq,
                    value: Value::String("point-1".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("vector".to_string()),
                    op: Operator::Eq,
                    value: Value::Vector(vec![0.1, 0.2]),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("name\"bad".to_string()),
                    op: Operator::Eq,
                    value: Value::String("Ana\"bad".to_string()),
                    is_array_unnest: false,
                },
            ],
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
        vector: Some(vec![0.1]),
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("score".to_string()),
                op: Operator::Gt,
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
            .contains("finite")
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
            conditions: vec![
                Condition {
                    left: Expr::Named("id".to_string()),
                    op: Operator::Eq,
                    value: Value::String("point-1".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("vector".to_string()),
                    op: Operator::Eq,
                    value: Value::Vector(vec![0.1, 0.2]),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("tags".to_string()),
                    op: Operator::Eq,
                    value: Value::Array(vec![
                        Value::String("blue".to_string()),
                        Value::Bool(true),
                        Value::Int(7),
                    ]),
                    is_array_unnest: false,
                },
            ],
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
        .vector(vec![0.1])
        .filter("city", Operator::NotILike, "%Lon%")
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
fn test_qdrant_transpiler_encodes_negative_filters() {
    use crate::ast::{Operator, Qail, Value};

    let search = Qail::get("points")
        .vector(vec![0.1])
        .filter("city", Operator::Ne, "London")
        .filter(
            "priority",
            Operator::NotIn,
            Value::Array(vec![Value::Int(1), Value::Int(2)]),
        )
        .filter("deleted_at", Operator::IsNotNull, Value::Null)
        .filter("summary", Operator::NotLike, "refund")
        .filter(
            "id",
            Operator::NotIn,
            Value::Array(vec![
                Value::Int(7),
                Value::String("uuid-like-id".to_string()),
            ]),
        )
        .to_qdrant_search();

    let parsed: serde_json::Value =
        serde_json::from_str(&search).expect("qdrant search JSON must be valid");
    let filter = &parsed["filter"];
    let must = filter["must"]
        .as_array()
        .expect("negative filters should be nested must clauses");

    assert!(must.iter().all(|clause| clause["must_not"].is_array()));
    assert!(search.contains("\"match\": { \"value\": \"London\" }"));
    assert!(search.contains("\"match\": { \"any\": [1, 2] }"));
    assert!(search.contains("\"is_null\": { \"key\": \"deleted_at\" }"));
    assert!(search.contains("\"match\": { \"text\": \"refund\" }"));
    assert!(search.contains("\"has_id\": [7, \"uuid-like-id\"]"));
}

#[test]
fn test_qdrant_search_encodes_native_filter_contracts() {
    use crate::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let owner_id = uuid::Uuid::parse_str("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa").unwrap();
    let reviewer_id = uuid::Uuid::parse_str("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb").unwrap();
    let search = Qail {
        table: "points".to_string(),
        vector: Some(vec![0.1, 0.2]),
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![
                Condition {
                    left: Expr::Named("ID".to_string()),
                    op: Operator::Eq,
                    value: Value::Int(7),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("status".to_string()),
                    op: Operator::In,
                    value: Value::Array(vec![
                        Value::String("open".to_string()),
                        Value::String("closed".to_string()),
                    ]),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("owner_id".to_string()),
                    op: Operator::Eq,
                    value: Value::Uuid(owner_id),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("reviewer_id".to_string()),
                    op: Operator::In,
                    value: Value::Array(vec![
                        Value::Uuid(reviewer_id),
                        Value::String("external-reviewer".to_string()),
                    ]),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("summary".to_string()),
                    op: Operator::Contains,
                    value: Value::String("refund".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("deleted_at".to_string()),
                    op: Operator::IsNull,
                    value: Value::NullUuid,
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
    let must = parsed["filter"]["must"].as_array().expect("must clauses");

    assert_eq!(must[0]["has_id"][0], 7);
    assert_eq!(
        must[1]["match"]["any"],
        serde_json::json!(["open", "closed"])
    );
    assert_eq!(must[2]["match"]["value"], owner_id.to_string());
    assert_eq!(
        must[3]["match"]["any"],
        serde_json::json!([reviewer_id.to_string(), "external-reviewer"])
    );
    assert_eq!(must[4]["match"]["text"], "refund");
    assert_eq!(must[5]["is_null"]["key"], "deleted_at");
}

#[test]
fn test_qdrant_search_rejects_invalid_filter_value_shapes() {
    use crate::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    for (value, expected) in [
        (Value::Null, "IS NULL"),
        (
            Value::Array(vec![Value::String("open".to_string())]),
            "equality filters",
        ),
        (
            Value::Json(r#"{"status":"open"}"#.to_string()),
            "equality filters",
        ),
        (Value::Float(1.5), "equality filters"),
    ] {
        let search = Qail {
            table: "points".to_string(),
            vector: Some(vec![0.1]),
            cages: vec![Cage {
                kind: CageKind::Filter,
                conditions: vec![Condition {
                    left: Expr::Named("status".to_string()),
                    op: Operator::Eq,
                    value,
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
            parsed["error"].as_str().unwrap().contains(expected),
            "{parsed}"
        );
    }

    for value in [
        Value::Array(vec![]),
        Value::Array(vec![Value::Null]),
        Value::Array(vec![Value::String("open".to_string()), Value::Int(1)]),
        Value::Array(vec![Value::Bool(true)]),
        Value::String("not-array".to_string()),
    ] {
        let search = Qail {
            table: "points".to_string(),
            vector: Some(vec![0.1]),
            cages: vec![Cage {
                kind: CageKind::Filter,
                conditions: vec![Condition {
                    left: Expr::Named("status".to_string()),
                    op: Operator::In,
                    value,
                    is_array_unnest: false,
                }],
                logical_op: LogicalOp::And,
            }],
            ..Default::default()
        }
        .to_qdrant_search();

        let parsed: serde_json::Value =
            serde_json::from_str(&search).expect("qdrant error JSON must be valid");
        assert!(parsed["error"].as_str().unwrap().contains("IN filters"));
    }
}

#[test]
fn test_qdrant_search_encodes_native_id_in_filter() {
    use crate::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let search = Qail {
        table: "points".to_string(),
        vector: Some(vec![0.1, 0.2]),
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("ID".to_string()),
                op: Operator::In,
                value: Value::Array(vec![
                    Value::Int(7),
                    Value::String("uuid-like-id".to_string()),
                ]),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_qdrant_search();

    let parsed: serde_json::Value =
        serde_json::from_str(&search).expect("qdrant search JSON must stay valid");
    assert_eq!(
        parsed["filter"]["must"][0]["has_id"],
        serde_json::json!([7, "uuid-like-id"])
    );

    let bad = Qail {
        table: "points".to_string(),
        vector: Some(vec![0.1, 0.2]),
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("id".to_string()),
                op: Operator::In,
                value: Value::Array(vec![]),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&bad).expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("id IN"));
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
fn test_qdrant_search_rejects_missing_or_duplicate_vectors_and_limits() {
    use crate::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let missing_vector = Qail::get("points").filter("city", Operator::Eq, "London");
    let parsed: serde_json::Value = serde_json::from_str(&missing_vector.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(
        parsed["error"]
            .as_str()
            .unwrap()
            .contains("search requires")
    );

    let zero_limit = Qail::get("points").vector(vec![0.1]).limit(0);
    let parsed: serde_json::Value = serde_json::from_str(&zero_limit.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("limit"));

    let duplicate_limit = Qail {
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        cages: vec![
            Cage {
                kind: CageKind::Limit(1),
                conditions: vec![],
                logical_op: LogicalOp::And,
            },
            Cage {
                kind: CageKind::Limit(2),
                conditions: vec![],
                logical_op: LogicalOp::And,
            },
        ],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&duplicate_limit.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("Duplicate"));

    let duplicate_vector = Qail {
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("vector".to_string()),
                op: Operator::Fuzzy,
                value: Value::Vector(vec![0.2]),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&duplicate_vector.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("Duplicate"));

    let empty_prompt = Qail {
        table: "points".to_string(),
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("vector".to_string()),
                op: Operator::Fuzzy,
                value: Value::String(" ".to_string()),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&empty_prompt.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("prompt"));
}

#[test]
fn test_qdrant_search_preserves_score_threshold_vector_and_projection_contracts() {
    use crate::ast::{Expr, Qail};

    let search = Qail::get("points")
        .vector(vec![0.1, 0.2])
        .score_threshold(0.8)
        .with_vectors()
        .columns(["id", "score", "vector", "title"])
        .to_qdrant_search();

    let parsed: serde_json::Value =
        serde_json::from_str(&search).expect("qdrant search JSON must stay valid");
    assert_eq!(parsed["score_threshold"], 0.8);
    assert_eq!(parsed["with_vector"], true);
    assert_eq!(
        parsed["with_payload"]["include"],
        serde_json::json!(["title"])
    );

    let wildcard = Qail::get("points")
        .vector(vec![0.1, 0.2])
        .columns(["points.*"])
        .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&wildcard).expect("qdrant search JSON must stay valid");
    assert_eq!(parsed["with_payload"], true);

    let system_only = Qail::get("points")
        .vector(vec![0.1, 0.2])
        .columns(["id", "score"])
        .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&system_only).expect("qdrant search JSON must stay valid");
    assert_eq!(parsed["with_payload"], false);
    assert!(parsed.get("with_vector").is_none());

    let quoted_star = Qail {
        table: "points".to_string(),
        vector: Some(vec![0.1, 0.2]),
        columns: vec![Expr::Named("\"points.*\"".to_string())],
        ..Default::default()
    }
    .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&quoted_star).expect("qdrant search JSON must stay valid");
    assert_eq!(
        parsed["with_payload"]["include"],
        serde_json::json!(["points.*"])
    );
}

#[test]
fn test_qdrant_search_rejects_unsupported_or_invalid_vector_options() {
    use crate::ast::Qail;

    let named = Qail::get("points")
        .vector(vec![0.1, 0.2])
        .vector_name("image")
        .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&named).expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("named vector"));

    let mut blank_name = Qail::get("points").vector(vec![0.1, 0.2]);
    blank_name.vector_name = Some(" ".to_string());
    let parsed: serde_json::Value = serde_json::from_str(&blank_name.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("vector name"));

    let bad_threshold = Qail::get("points")
        .vector(vec![0.1, 0.2])
        .score_threshold(f32::NAN)
        .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&bad_threshold).expect("qdrant error JSON must be valid");
    assert!(
        parsed["error"]
            .as_str()
            .unwrap()
            .contains("score threshold")
    );
}

#[test]
fn test_qdrant_search_rejects_invalid_filter_and_projection_shapes() {
    use crate::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let range_string =
        Qail::get("points")
            .vector(vec![0.1])
            .filter("price", Operator::Gt, "expensive");
    let parsed: serde_json::Value = serde_json::from_str(&range_string.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("range filter"));

    let fuzzy_non_vector = Qail {
        table: "points".to_string(),
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("title".to_string()),
                op: Operator::Fuzzy,
                value: Value::String("boat".to_string()),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&fuzzy_non_vector.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("fuzzy"));

    let quoted_empty_field = Qail {
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("\"   \"".to_string()),
                op: Operator::Eq,
                value: Value::String("bad".to_string()),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&quoted_empty_field.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("field name"));

    let bad_projection = Qail {
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        columns: vec![Expr::Literal(Value::String("payload".to_string()))],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&bad_projection.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("named"));

    let empty_projection = Qail {
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        columns: vec![Expr::Named("\"   \"".to_string())],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&empty_projection.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("field name"));
}

#[test]
fn test_qdrant_upsert_rejects_missing_duplicate_and_invalid_contract_fields() {
    use crate::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let missing_id = Qail {
        action: Action::Add,
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&missing_id.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("id"));

    let missing_vector = Qail {
        action: Action::Add,
        table: "points".to_string(),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![Condition {
                left: Expr::Named("id".to_string()),
                op: Operator::Eq,
                value: Value::String("point-1".to_string()),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&missing_vector.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("vector"));

    let duplicate_id = Qail {
        action: Action::Add,
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![
                Condition {
                    left: Expr::Named("id".to_string()),
                    op: Operator::Eq,
                    value: Value::String("a".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("id".to_string()),
                    op: Operator::Eq,
                    value: Value::String("b".to_string()),
                    is_array_unnest: false,
                },
            ],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&duplicate_id.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("Duplicate"));

    let duplicate_vector = Qail {
        action: Action::Add,
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![
                Condition {
                    left: Expr::Named("id".to_string()),
                    op: Operator::Eq,
                    value: Value::String("a".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("vector".to_string()),
                    op: Operator::Eq,
                    value: Value::Vector(vec![0.2]),
                    is_array_unnest: false,
                },
            ],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&duplicate_vector.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("Duplicate"));

    let invalid_id = Qail {
        action: Action::Add,
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![Condition {
                left: Expr::Named("id".to_string()),
                op: Operator::Eq,
                value: Value::String(" ".to_string()),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&invalid_id.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("point id"));
}

#[test]
fn test_qdrant_upsert_treats_case_variant_reserved_fields_as_control_fields() {
    use crate::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let upsert = Qail {
        action: Action::Add,
        table: "points".to_string(),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![
                Condition {
                    left: Expr::Named("ID".to_string()),
                    op: Operator::Eq,
                    value: Value::Int(7),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("VECTOR".to_string()),
                    op: Operator::Eq,
                    value: Value::Vector(vec![0.1, 0.2]),
                    is_array_unnest: false,
                },
            ],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_qdrant_search();

    let parsed: serde_json::Value =
        serde_json::from_str(&upsert).expect("qdrant upsert JSON must stay valid");
    assert_eq!(parsed["points"][0]["id"], 7);
    assert_eq!(parsed["points"][0]["vector"], serde_json::json!([0.1, 0.2]));
    assert_eq!(parsed["points"][0]["payload"], serde_json::json!({}));

    let reserved_payload = Qail {
        action: Action::Add,
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![
                Condition {
                    left: Expr::Named("id".to_string()),
                    op: Operator::Eq,
                    value: Value::Int(7),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("_QAIL_ORIGINAL_POINT_ID".to_string()),
                    op: Operator::Eq,
                    value: Value::String("spoof".to_string()),
                    is_array_unnest: false,
                },
            ],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    }
    .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&reserved_payload).expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("reserved"));
}

#[test]
fn test_qdrant_upsert_filter_fallbacks_fail_closed() {
    use crate::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let ignored_conditional_filter = Qail::add("points")
        .set_value("id", 7)
        .set_value("vector", Value::Vector(vec![0.1, 0.2]))
        .filter("tenant_id", Operator::Eq, "tenant-a")
        .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&ignored_conditional_filter).expect("qdrant error JSON must be valid");
    assert!(
        parsed["error"]
            .as_str()
            .unwrap()
            .contains("conditional writes")
    );

    let ambiguous_or_id = Qail {
        action: Action::Add,
        table: "points".to_string(),
        vector: Some(vec![0.1, 0.2]),
        cages: vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![
                Condition {
                    left: Expr::Named("ID".to_string()),
                    op: Operator::Eq,
                    value: Value::Int(7),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("tenant_id".to_string()),
                    op: Operator::Eq,
                    value: Value::String("tenant-a".to_string()),
                    is_array_unnest: false,
                },
            ],
            logical_op: LogicalOp::Or,
        }],
        ..Default::default()
    }
    .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&ambiguous_or_id).expect("qdrant error JSON must be valid");
    assert!(
        parsed["error"]
            .as_str()
            .unwrap()
            .contains("multi-condition OR")
    );

    let conflicting_id = Qail::add("points")
        .set_value("id", 7)
        .set_value("vector", Value::Vector(vec![0.1, 0.2]))
        .filter("ID", Operator::Eq, 8)
        .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&conflicting_id).expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("conflicts"));

    let conflicting_vector = Qail {
        action: Action::Add,
        table: "points".to_string(),
        cages: vec![
            Cage {
                kind: CageKind::Payload,
                conditions: vec![
                    Condition {
                        left: Expr::Named("id".to_string()),
                        op: Operator::Eq,
                        value: Value::Int(7),
                        is_array_unnest: false,
                    },
                    Condition {
                        left: Expr::Named("vector".to_string()),
                        op: Operator::Eq,
                        value: Value::Vector(vec![0.1, 0.2]),
                        is_array_unnest: false,
                    },
                ],
                logical_op: LogicalOp::And,
            },
            Cage {
                kind: CageKind::Filter,
                conditions: vec![Condition {
                    left: Expr::Named("VECTOR".to_string()),
                    op: Operator::Eq,
                    value: Value::Vector(vec![0.3, 0.4]),
                    is_array_unnest: false,
                }],
                logical_op: LogicalOp::And,
            },
        ],
        ..Default::default()
    }
    .to_qdrant_search();
    let parsed: serde_json::Value =
        serde_json::from_str(&conflicting_vector).expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("conflicts"));
}

#[test]
fn test_qdrant_upsert_rejects_payload_shape_drift() {
    use crate::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let duplicate_payload = Qail {
        action: Action::Add,
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![
                Condition {
                    left: Expr::Named("id".to_string()),
                    op: Operator::Eq,
                    value: Value::String("point-1".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("status".to_string()),
                    op: Operator::Eq,
                    value: Value::String("open".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("status".to_string()),
                    op: Operator::Eq,
                    value: Value::String("closed".to_string()),
                    is_array_unnest: false,
                },
            ],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&duplicate_payload.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("Duplicate"));

    let quoted_empty_payload = Qail {
        action: Action::Add,
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![
                Condition {
                    left: Expr::Named("id".to_string()),
                    op: Operator::Eq,
                    value: Value::String("point-1".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("\"   \"".to_string()),
                    op: Operator::Eq,
                    value: Value::String("bad".to_string()),
                    is_array_unnest: false,
                },
            ],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&quoted_empty_payload.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("field name"));

    let non_eq_payload = Qail {
        action: Action::Add,
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![Condition {
                left: Expr::Named("id".to_string()),
                op: Operator::Gt,
                value: Value::String("point-1".to_string()),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&non_eq_payload.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("equality"));

    let bad_json_payload = Qail {
        action: Action::Add,
        table: "points".to_string(),
        vector: Some(vec![0.1]),
        cages: vec![Cage {
            kind: CageKind::Payload,
            conditions: vec![
                Condition {
                    left: Expr::Named("id".to_string()),
                    op: Operator::Eq,
                    value: Value::String("point-1".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("metadata".to_string()),
                    op: Operator::Eq,
                    value: Value::Json(r#"{" ":"bad"}"#.to_string()),
                    is_array_unnest: false,
                },
            ],
            logical_op: LogicalOp::And,
        }],
        ..Default::default()
    };
    let parsed: serde_json::Value = serde_json::from_str(&bad_json_payload.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("object keys"));
}

#[test]
fn test_qdrant_delete_rejects_invalid_id_filters() {
    use crate::ast::{Action, Operator, Qail, Value};

    let uppercase_id = Qail {
        action: Action::Del,
        table: "points".to_string(),
        ..Default::default()
    }
    .filter("ID", Operator::Eq, 7);
    let parsed: serde_json::Value = serde_json::from_str(&uppercase_id.to_qdrant_search())
        .expect("qdrant delete JSON must be valid");
    assert_eq!(
        parsed["filter"]["must"][0]["has_id"],
        serde_json::json!([7])
    );

    let scoped_id_delete = Qail {
        action: Action::Del,
        table: "points".to_string(),
        ..Default::default()
    }
    .filter("id", Operator::Eq, 7)
    .filter("tenant_id", Operator::Eq, "tenant-a");
    let parsed: serde_json::Value = serde_json::from_str(&scoped_id_delete.to_qdrant_search())
        .expect("scoped qdrant delete JSON must be valid");
    assert!(parsed.get("points").is_none());
    assert_eq!(
        parsed["filter"]["must"][0]["has_id"],
        serde_json::json!([7])
    );
    assert_eq!(parsed["filter"]["must"][1]["key"], "tenant_id");
    assert_eq!(parsed["filter"]["must"][1]["match"]["value"], "tenant-a");

    let id_in_delete = Qail {
        action: Action::Del,
        table: "points".to_string(),
        ..Default::default()
    }
    .filter(
        "id",
        Operator::In,
        Value::Array(vec![Value::Int(7), Value::String("point-8".to_string())]),
    );
    let parsed: serde_json::Value = serde_json::from_str(&id_in_delete.to_qdrant_search())
        .expect("id IN qdrant delete JSON must be valid");
    assert_eq!(
        parsed["filter"]["must"][0]["has_id"],
        serde_json::json!([7, "point-8"])
    );

    let bad_id_operator = Qail {
        action: Action::Del,
        table: "points".to_string(),
        ..Default::default()
    }
    .filter("id", Operator::Gt, 7);
    let parsed: serde_json::Value = serde_json::from_str(&bad_id_operator.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("id filters"));

    let empty_id = Qail {
        action: Action::Del,
        table: "points".to_string(),
        ..Default::default()
    }
    .filter("id", Operator::Eq, " ");
    let parsed: serde_json::Value = serde_json::from_str(&empty_id.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("point id"));

    let fuzzy_delete = Qail {
        action: Action::Del,
        table: "points".to_string(),
        ..Default::default()
    }
    .filter("title", Operator::Fuzzy, "boat");
    let parsed: serde_json::Value = serde_json::from_str(&fuzzy_delete.to_qdrant_search())
        .expect("qdrant error JSON must be valid");
    assert!(parsed["error"].as_str().unwrap().contains("fuzzy"));
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
fn test_mongo_rejects_non_finite_numbers() {
    use crate::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let insert = Qail {
        action: Action::Add,
        table: "events".to_string(),
        cages: vec![Cage {
            kind: CageKind::Payload,
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
    .to_mongo();

    assert!(insert.starts_with("throw new Error("), "{insert}");
    assert!(insert.contains("non-finite"), "{insert}");
}

#[test]
fn test_mongo_rejects_unsupported_filter_operator() {
    use crate::ast::{Operator, Qail};

    let find = Qail::get("events")
        .filter("name", Operator::Like, "%ana%")
        .to_mongo();

    assert!(find.starts_with("throw new Error("), "{find}");
    assert!(
        find.contains("unsupported MongoDB filter operator"),
        "{find}"
    );
}

#[test]
fn test_mongo_delete_without_filter_returns_error() {
    use crate::ast::{Action, Qail};

    let delete = Qail {
        action: Action::Del,
        table: "events".to_string(),
        ..Default::default()
    }
    .to_mongo();

    assert!(delete.starts_with("throw new Error("), "{delete}");
    assert!(
        delete.contains("delete requires at least one filter"),
        "{delete}"
    );
}

#[test]
fn test_mongo_preserves_array_payload_values() {
    use crate::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    let insert = Qail {
        action: Action::Add,
        table: "events".to_string(),
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
    .to_mongo();

    assert!(insert.contains("\"tags\": [\"blue\", true, 7]"), "{insert}");
}

#[test]
fn test_mongo_update_does_not_write_filter_fields_into_set_payload() {
    use crate::ast::Qail;

    let update = Qail::set("users")
        .set_value("name", "Ana")
        .eq("id", 1)
        .to_mongo();

    assert_eq!(
        update,
        "db.users.updateMany({ \"id\": 1 }, { $set: { \"name\": \"Ana\" } })"
    );
}

#[test]
fn test_mongo_or_filters_are_rendered_as_or_clauses() {
    use crate::ast::{Operator, Qail};

    let find = Qail::get("events")
        .or_filter("city", Operator::Eq, "London")
        .or_filter("city", Operator::Eq, "Paris")
        .to_mongo();

    assert_eq!(
        find,
        "db.events.find({ \"$or\": [{ \"city\": \"London\" }, { \"city\": \"Paris\" }] }, {})"
    );
}

#[test]
fn test_mongo_repeated_field_and_filters_are_not_flattened() {
    use crate::ast::{Operator, Qail};

    let find = Qail::get("events")
        .filter("score", Operator::Gte, 10)
        .filter("score", Operator::Lt, 20)
        .to_mongo();

    assert_eq!(
        find,
        "db.events.find({ \"$and\": [{ \"score\": { \"$gte\": 10 } }, { \"score\": { \"$lt\": 20 } }] }, {})"
    );
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
