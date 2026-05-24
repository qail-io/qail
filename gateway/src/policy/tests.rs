use super::*;
use crate::auth::AuthContext;
use qail_core::ast::{Action, CageKind, Expr, LogicalOp, MergeAction, Operator, Qail, Value};

#[test]
fn test_policy_expands_user_id() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user123".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let result = engine.expand_filter("user_id = $user_id", &auth);
    assert_eq!(result, "user_id = 'user123'");
}

#[test]
fn test_policy_injects_filter() {
    let engine = PolicyEngine::new();
    let mut cmd = Qail::get("orders").columns(["id", "total"]);

    engine
        .inject_filter(&mut cmd, "user_id = 'user123'")
        .unwrap();

    assert_eq!(cmd.cages.len(), 1);
    assert!(matches!(cmd.cages[0].kind, CageKind::Filter));
    assert_eq!(cmd.cages[0].conditions.len(), 1);
}

#[test]
fn test_apply_policies_adds_filter() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "tenant_isolation".to_string(),
        table: "orders".to_string(),
        filter: Some("user_id = $user_id".to_string()),
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user456".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("orders").columns(["id"]);
    engine.apply_policies(&auth, &mut cmd).unwrap();

    // Check that filter was added
    assert_eq!(cmd.cages.len(), 1);
    let condition = &cmd.cages[0].conditions[0];
    assert_eq!(condition.left, Expr::Named("user_id".to_string()));
    assert_eq!(condition.value, Value::String("user456".to_string()));
}

#[test]
fn test_apply_policies_treats_cnt_as_read_operation() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "orders_read".to_string(),
        table: "orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user_cnt".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("orders");
    cmd.action = Action::Cnt;
    let result = engine.apply_policies(&auth, &mut cmd);
    assert!(result.is_ok(), "cnt should be treated as read");
}

#[test]
fn test_apply_policies_recurses_into_cte_body() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "orders_read".to_string(),
        table: "orders".to_string(),
        filter: Some("tenant_id = $tenant_id".to_string()),
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user_cte".to_string(),
        role: "user".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd =
        Qail::get("summary").with("summary", Qail::get("orders").columns(["id", "total"]));
    engine.apply_policies(&auth, &mut cmd).unwrap();

    let cte_body = &cmd.ctes[0].base_query;
    assert_eq!(cte_body.cages.len(), 1);
    let condition = &cte_body.cages[0].conditions[0];
    assert_eq!(condition.left, Expr::Named("tenant_id".to_string()));
    assert_eq!(condition.value, Value::String("tenant-1".to_string()));
}

#[test]
fn test_apply_policies_recurses_into_expression_subquery_body() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "orders_open".to_string(),
        table: "orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "source_tenant".to_string(),
        table: "source_orders".to_string(),
        filter: Some("tenant_id = $tenant_id".to_string()),
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user_expr".to_string(),
        role: "user".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("orders").columns(["id"]);
    cmd.columns.push(Expr::Subquery {
        query: Box::new(Qail::get("source_orders").columns(["total"])),
        alias: Some("source_total".to_string()),
    });
    engine.apply_policies(&auth, &mut cmd).unwrap();

    let subquery = cmd
        .columns
        .iter()
        .find_map(|expr| {
            if let Expr::Subquery { query, .. } = expr {
                Some(query)
            } else {
                None
            }
        })
        .expect("expression subquery");
    let condition = &subquery.cages[0].conditions[0];
    assert_eq!(condition.left, Expr::Named("tenant_id".to_string()));
    assert_eq!(condition.value, Value::String("tenant-1".to_string()));
}

#[test]
fn test_apply_policies_recurses_into_merge_query_source() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "orders_update".to_string(),
        table: "orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Update],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "source_tenant".to_string(),
        table: "source_orders".to_string(),
        filter: Some("tenant_id = $tenant_id".to_string()),
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user_merge_source".to_string(),
        role: "user".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let source = Qail::get("source_orders").columns(["id", "total"]);
    let mut cmd = Qail::merge_into("orders")
        .using_query_as(source, "s")
        .merge_on_column("orders.id", Operator::Eq, "s.id")
        .when_matched_update(&[("total", Expr::Named("s.total".to_string()))]);
    engine.apply_policies(&auth, &mut cmd).unwrap();

    let merge = cmd.merge.as_ref().expect("merge spec");
    let qail_core::ast::MergeSource::Query { query, .. } = &merge.source else {
        panic!("expected query source");
    };
    let condition = &query.cages[0].conditions[0];
    assert_eq!(condition.left, Expr::Named("tenant_id".to_string()));
    assert_eq!(condition.value, Value::String("tenant-1".to_string()));
}

#[test]
fn test_apply_policies_adds_joined_table_filter_to_join_clause() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "orders_read".to_string(),
        table: "orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "users_tenant".to_string(),
        table: "users".to_string(),
        filter: Some("tenant_id = $tenant_id".to_string()),
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user_join".to_string(),
        role: "user".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("orders").left_join("users", "orders.user_id", "users.id");
    engine.apply_policies(&auth, &mut cmd).unwrap();

    let join = cmd.joins.first().expect("join");
    let on = join.on.as_ref().expect("join conditions");
    assert!(on.iter().any(|condition| {
        condition.left == Expr::Named("users.tenant_id".to_string())
            && condition.value == Value::String("tenant-1".to_string())
    }));
}

#[test]
fn test_apply_policies_rejects_joined_table_column_policy() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "orders_read".to_string(),
        table: "orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "users_columns".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec!["id".to_string()],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user_join".to_string(),
        role: "user".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("orders").left_join("users", "orders.user_id", "users.id");
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();
    assert!(err.to_string().contains("column policies"));
}

#[test]
fn test_create_policy_filter_is_injected_into_insert_payload() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "operator_create".to_string(),
        table: "orders".to_string(),
        filter: Some("operator_id = $user_id".to_string()),
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::add("orders")
        .set_value("id", "order-1")
        .set_value("operator_id", "attacker");
    engine.apply_policies(&auth, &mut cmd).unwrap();

    let payload = cmd
        .cages
        .iter()
        .find(|cage| matches!(cage.kind, CageKind::Payload))
        .expect("payload cage");
    assert!(payload.conditions.iter().any(|condition| {
        condition.left == Expr::Named("operator_id".to_string())
            && condition.value == Value::String("operator-1".to_string())
    }));
    assert!(
        !payload
            .conditions
            .iter()
            .any(|condition| { condition.value == Value::String("attacker".to_string()) })
    );
}

#[test]
fn test_create_policy_rejects_multiple_filtered_policies() {
    let mut engine = PolicyEngine::new();
    for name in ["operator_create", "region_create"] {
        engine.add_policy(PolicyDef {
            name: name.to_string(),
            table: "orders".to_string(),
            filter: Some(if name == "operator_create" {
                "operator_id = $user_id".to_string()
            } else {
                "region = 'west'".to_string()
            }),
            role: None,
            operations: vec![OperationType::Create],
            allowed_columns: vec![],
            denied_columns: vec![],
        });
    }

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::add("orders").set_value("id", "order-1");
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();
    assert!(
        err.to_string()
            .contains("Multiple filtered create policies")
    );
}

#[test]
fn test_create_policy_rewrites_insert_select_projection() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "source_read".to_string(),
        table: "source_orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "create_west_orders".to_string(),
        table: "orders".to_string(),
        filter: Some("region = 'west'".to_string()),
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::add("orders").columns(["id", "total"]);
    cmd.source_query = Some(Box::new(
        Qail::get("source_orders").columns(["id", "total"]),
    ));

    engine.apply_policies(&auth, &mut cmd).unwrap();

    assert_eq!(
        cmd.columns,
        vec![
            Expr::Named("id".to_string()),
            Expr::Named("total".to_string()),
            Expr::Named("region".to_string())
        ]
    );
    let source_query = cmd.source_query.as_ref().expect("source query");
    assert!(matches!(
        source_query.columns.last(),
        Some(Expr::Literal(Value::String(value))) if value == "west"
    ));

    let (sql, params) = qail_pg::protocol::ast_encoder::AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert_eq!(
        sql,
        "INSERT INTO orders (id, total, region) SELECT id, total, 'west' FROM source_orders"
    );
    assert!(params.is_empty());
}

#[test]
fn test_create_policy_replaces_insert_select_policy_column_projection() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "source_read".to_string(),
        table: "source_orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "create_west_orders".to_string(),
        table: "orders".to_string(),
        filter: Some("region = 'west'".to_string()),
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::add("orders").columns(["id", "region", "total"]);
    cmd.source_query = Some(Box::new(Qail::get("source_orders").columns([
        "id",
        "attacker_region",
        "total",
    ])));

    engine.apply_policies(&auth, &mut cmd).unwrap();

    assert_eq!(
        cmd.columns,
        vec![
            Expr::Named("id".to_string()),
            Expr::Named("region".to_string()),
            Expr::Named("total".to_string())
        ]
    );
    let source_query = cmd.source_query.as_ref().expect("source query");
    assert!(matches!(
        &source_query.columns[1],
        Expr::Literal(Value::String(value)) if value == "west"
    ));
    assert!(
        !source_query
            .columns
            .iter()
            .any(|expr| { matches!(expr, Expr::Named(name) if name == "attacker_region") }),
        "policy column projection should be replaced by policy literal"
    );
}

#[test]
fn test_create_policy_rejects_insert_select_source_projection_mismatch() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "source_read".to_string(),
        table: "source_orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "create_west_orders".to_string(),
        table: "orders".to_string(),
        filter: Some("region = 'west'".to_string()),
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::add("orders").columns(["id", "total"]);
    cmd.source_query = Some(Box::new(Qail::get("source_orders").columns(["id"])));

    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();
    assert!(
        err.to_string()
            .contains("target/source column count mismatch")
    );
}

#[test]
fn test_create_policy_rejects_insert_select_implicit_source_projection() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "source_read".to_string(),
        table: "source_orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "create_west_orders".to_string(),
        table: "orders".to_string(),
        filter: Some("region = 'west'".to_string()),
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::add("orders").columns(["id", "total"]);
    cmd.source_query = Some(Box::new(Qail::get("source_orders")));

    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();
    assert!(
        err.to_string()
            .contains("explicit non-star source projection")
    );
}

#[test]
fn test_upsert_conflict_update_requires_update_policy() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "create_only".to_string(),
        table: "orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::add("orders")
        .set_value("id", "order-1")
        .set_value("status", "paid")
        .on_conflict_update(
            &["id"],
            &[("status", Expr::Named("EXCLUDED.status".to_string()))],
        );
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();
    assert!(err.to_string().contains("Update"));
}

#[test]
fn test_upsert_conflict_update_injects_update_policy_filter() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "create_orders".to_string(),
        table: "orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "update_own_orders".to_string(),
        table: "orders".to_string(),
        filter: Some("operator_id = $user_id".to_string()),
        role: None,
        operations: vec![OperationType::Update],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::add("orders")
        .set_value("id", "order-1")
        .set_value("status", "paid")
        .on_conflict_update(
            &["id"],
            &[("status", Expr::Named("EXCLUDED.status".to_string()))],
        );
    engine.apply_policies(&auth, &mut cmd).unwrap();

    assert!(cmd.cages.iter().any(|cage| {
        matches!(cage.kind, CageKind::Filter)
            && cage.conditions.iter().any(|condition| {
                condition.left == Expr::Named("operator_id".to_string())
                    && condition.value == Value::String("operator-1".to_string())
            })
    }));
}

#[test]
fn test_vector_upsert_requires_create_policy() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "update_vectors".to_string(),
        table: "embeddings".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Update],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::upsert("embeddings");
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();
    assert!(err.to_string().contains("Create"));
}

#[test]
fn test_vector_upsert_requires_update_policy() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "create_vectors".to_string(),
        table: "embeddings".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::upsert("embeddings");
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();
    assert!(err.to_string().contains("Update"));
}

#[test]
fn test_vector_upsert_allows_create_and_update_policies() {
    let mut engine = PolicyEngine::new();
    for (name, operation) in [
        ("create_vectors", OperationType::Create),
        ("update_vectors", OperationType::Update),
    ] {
        engine.add_policy(PolicyDef {
            name: name.to_string(),
            table: "embeddings".to_string(),
            filter: None,
            role: None,
            operations: vec![operation],
            allowed_columns: vec![],
            denied_columns: vec![],
        });
    }

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::upsert("embeddings");
    engine.apply_policies(&auth, &mut cmd).unwrap();
}

fn test_merge_command() -> Qail {
    Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[("name", Expr::Named("s.name".to_string()))])
        .when_not_matched_insert(
            &["id", "name"],
            &[
                Expr::Named("s.id".to_string()),
                Expr::Named("s.name".to_string()),
            ],
        )
}

fn merge_policy_auth() -> AuthContext {
    AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    }
}

#[test]
fn test_merge_requires_create_and_update_policies_for_insert_update_merge() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "users_create".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let mut cmd = test_merge_command();
    let err = engine
        .apply_policies(&merge_policy_auth(), &mut cmd)
        .unwrap_err();

    assert!(err.to_string().contains("Update"));
}

#[test]
fn test_merge_rejects_filtered_policy_that_cannot_be_injected() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "users_create".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "users_update_tenant".to_string(),
        table: "users".to_string(),
        filter: Some("tenant_id = $tenant_id".to_string()),
        role: None,
        operations: vec![OperationType::Update],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let mut cmd = test_merge_command();
    let err = engine
        .apply_policies(&merge_policy_auth(), &mut cmd)
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("cannot be safely enforced on MERGE")
    );
}

#[test]
fn test_merge_enforces_column_policies_for_actions() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "users_create".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec!["id".into(), "name".into()],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "users_update".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Update],
        allowed_columns: vec!["email".into()],
        denied_columns: vec![],
    });

    let mut cmd = test_merge_command();
    let err = engine
        .apply_policies(&merge_policy_auth(), &mut cmd)
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("does not allow Update on column 'name'")
    );
}

#[test]
fn test_merge_delete_requires_delete_policy() {
    let mut engine = PolicyEngine::new();
    for operation in [OperationType::Create, OperationType::Update] {
        engine.add_policy(PolicyDef {
            name: format!("users_{operation:?}"),
            table: "users".to_string(),
            filter: None,
            role: None,
            operations: vec![operation],
            allowed_columns: vec![],
            denied_columns: vec![],
        });
    }

    let mut cmd = test_merge_command();
    let merge = cmd.merge.as_mut().expect("merge spec");
    merge.clauses.push(qail_core::ast::MergeClause {
        match_kind: qail_core::ast::MergeMatchKind::Matched,
        condition: vec![],
        action: MergeAction::Delete,
    });

    let err = engine
        .apply_policies(&merge_policy_auth(), &mut cmd)
        .unwrap_err();

    assert!(err.to_string().contains("Delete"));
}

#[test]
fn test_filter_cages_for_operation_returns_filtered_vector_policies() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "create_west_vectors".to_string(),
        table: "embeddings".to_string(),
        filter: Some("region = 'west'".to_string()),
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "update_operator_vectors".to_string(),
        table: "embeddings".to_string(),
        filter: Some("operator_id = $user_id".to_string()),
        role: None,
        operations: vec![OperationType::Update],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "operator-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let create_cages = engine
        .filter_cages_for_operation(&auth, "embeddings", OperationType::Create)
        .unwrap();
    let update_cages = engine
        .filter_cages_for_operation(&auth, "embeddings", OperationType::Update)
        .unwrap();

    assert_eq!(create_cages.len(), 1);
    assert_eq!(create_cages[0].logical_op, LogicalOp::Or);
    assert_eq!(
        create_cages[0].conditions[0].left,
        Expr::Named("region".to_string())
    );
    assert_eq!(
        create_cages[0].conditions[0].value,
        Value::String("west".to_string())
    );
    assert_eq!(update_cages.len(), 1);
    assert_eq!(
        update_cages[0].conditions[0].left,
        Expr::Named("operator_id".to_string())
    );
    assert_eq!(
        update_cages[0].conditions[0].value,
        Value::String("operator-1".to_string())
    );
}

#[test]
fn test_column_whitelist() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "hide_sensitive".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![],
        allowed_columns: vec!["id".into(), "name".into(), "email".into()],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    // SELECT * should be restricted
    let mut cmd = Qail::get("users");
    engine.apply_policies(&auth, &mut cmd).unwrap();
    assert_eq!(cmd.columns.len(), 3);
    assert!(cmd.columns.contains(&Expr::Named("id".to_string())));
    assert!(cmd.columns.contains(&Expr::Named("name".to_string())));
    assert!(cmd.columns.contains(&Expr::Named("email".to_string())));
}

#[test]
fn test_column_blacklist() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "hide_password".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![],
        allowed_columns: vec![],
        denied_columns: vec!["password_hash".into(), "secret_key".into()],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("users").columns(["id", "name", "password_hash", "secret_key"]);
    engine.apply_policies(&auth, &mut cmd).unwrap();
    assert_eq!(cmd.columns.len(), 2);
    assert!(cmd.columns.contains(&Expr::Named("id".to_string())));
    assert!(cmd.columns.contains(&Expr::Named("name".to_string())));
}

#[test]
fn test_column_whitelist_matches_qualified_projection_names() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "users_whitelist".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec!["users.id".into(), "email".into()],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("users").columns(["id", "users.email", "users.password_hash"]);
    engine.apply_policies(&auth, &mut cmd).unwrap();

    assert_eq!(
        cmd.columns,
        vec![
            Expr::Named("id".to_string()),
            Expr::Named("users.email".to_string())
        ]
    );
}

#[test]
fn test_column_blacklist_matches_qualified_projection_names() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "hide_password".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec!["password_hash".into()],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("users").columns(["users.id", "users.password_hash", "users.email"]);
    engine.apply_policies(&auth, &mut cmd).unwrap();

    assert_eq!(
        cmd.columns,
        vec![
            Expr::Named("users.id".to_string()),
            Expr::Named("users.email".to_string())
        ]
    );
}

#[test]
fn test_column_blacklist_rejects_wildcard_projection() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "hide_password".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec!["password_hash".into()],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("users");
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();
    assert!(err.to_string().contains("wildcard projection"));
}

#[test]
fn test_column_whitelist_rejects_expression_projection() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "users_whitelist".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec!["id".into(), "name".into()],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("users");
    cmd.columns = vec![Expr::FunctionCall {
        name: "coalesce".to_string(),
        args: vec![
            Expr::Named("name".to_string()),
            Expr::Literal(Value::String("n/a".to_string())),
        ],
        alias: Some("display_name".to_string()),
    }];
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();
    assert!(err.to_string().contains("expression projections"));
}

#[test]
fn test_column_blacklist_rejects_expression_projection() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "users_blacklist".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec!["password_hash".into()],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("users");
    cmd.columns = vec![Expr::FunctionCall {
        name: "lower".to_string(),
        args: vec![Expr::Named("email".to_string())],
        alias: Some("email_lc".to_string()),
    }];
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();
    assert!(err.to_string().contains("expression projections"));
}

#[test]
fn test_update_column_blacklist_rejects_payload_column() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "users_update".to_string(),
        table: "users".to_string(),
        filter: Some("tenant_id = $tenant_id".to_string()),
        role: None,
        operations: vec![OperationType::Update],
        allowed_columns: vec![],
        denied_columns: vec!["is_admin".into(), "password_hash".into()],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "support".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::set("users").set_value("is_admin", true);
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();

    assert!(
        err.to_string()
            .contains("denies Update on column 'is_admin'")
    );
}

#[test]
fn test_create_column_whitelist_rejects_unlisted_payload_column() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "users_create".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec!["email".into(), "name".into()],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "support".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::add("users")
        .set_value("email", "a@example.test")
        .set_value("is_admin", true);
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();

    assert!(
        err.to_string()
            .contains("does not allow Create on column 'is_admin'")
    );
}

#[test]
fn test_qdrant_upsert_create_column_blacklist_rejects_payload_column() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "embeddings_create".to_string(),
        table: "embeddings".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec!["moderation_state".into()],
    });
    engine.add_policy(PolicyDef {
        name: "embeddings_update".to_string(),
        table: "embeddings".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Update],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "support".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::upsert("embeddings")
        .set_value("id", 7)
        .set_value("vector", Value::Vector(vec![0.1, 0.2]))
        .set_value("moderation_state", "approved");
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();

    assert!(
        err.to_string()
            .contains("denies Create on column 'moderation_state'")
    );
}

#[test]
fn test_qdrant_upsert_update_column_blacklist_rejects_payload_column() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "embeddings_create".to_string(),
        table: "embeddings".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "embeddings_update".to_string(),
        table: "embeddings".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Update],
        allowed_columns: vec![],
        denied_columns: vec!["moderation_state".into()],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "support".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::upsert("embeddings")
        .set_value("id", 7)
        .set_value("vector", Value::Vector(vec![0.1, 0.2]))
        .set_value("moderation_state", "approved");
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();

    assert!(
        err.to_string()
            .contains("denies Update on column 'moderation_state'")
    );
}

#[test]
fn test_insert_select_column_blacklist_requires_safe_target_columns() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "users_create".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec!["is_admin".into()],
    });
    engine.add_policy(PolicyDef {
        name: "staging_users_read".to_string(),
        table: "staging_users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "support".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::add("users").columns(["email", "is_admin"]);
    cmd.source_query = Some(Box::new(
        Qail::get("staging_users").columns(["email", "is_admin"]),
    ));
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();

    assert!(
        err.to_string()
            .contains("denies Create on column 'is_admin'")
    );
}

#[test]
fn test_on_conflict_update_column_blacklist_rejects_assignment() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "users_create".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Create],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "users_update".to_string(),
        table: "users".to_string(),
        filter: Some("tenant_id = $tenant_id".to_string()),
        role: None,
        operations: vec![OperationType::Update],
        allowed_columns: vec![],
        denied_columns: vec!["is_admin".into()],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "support".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::add("users")
        .set_value("id", "user-1")
        .set_value("is_admin", true)
        .on_conflict_update(
            &["id"],
            &[("is_admin", Expr::Named("EXCLUDED.is_admin".into()))],
        );
    let err = engine.apply_policies(&auth, &mut cmd).unwrap_err();

    assert!(
        err.to_string()
            .contains("denies Update on column 'is_admin'")
    );
}

// ══════════════════════════════════════════════════════════════════
// SECURITY: expand_filter SQL injection hardening (G1)
// ══════════════════════════════════════════════════════════════════

#[test]
fn security_expand_filter_escapes_user_id_quotes() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "x' OR 1=1 --".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    let result = engine.expand_filter("user_id = $user_id", &auth);
    assert_eq!(result, "user_id = 'x'' OR 1=1 --'");
    assert!(
        !result.contains("x' OR"),
        "Unescaped quote in user_id: {}",
        result
    );
}

#[test]
fn security_expand_filter_escapes_role_quotes() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "admin'; DROP TABLE users; --".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    let result = engine.expand_filter("role = $role", &auth);
    assert!(
        result.contains("admin''; DROP TABLE users; --"),
        "Unescaped quote in role: {}",
        result
    );
}

#[test]
fn security_expand_filter_escapes_claim_string_quotes() {
    let engine = PolicyEngine::new();
    let mut claims = std::collections::HashMap::new();
    claims.insert("org_name".to_string(), serde_json::json!("Acme' OR '1'='1"));
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims,
    };
    let result = engine.expand_filter("org = $org_name", &auth);
    assert!(
        result.contains("Acme'' OR ''1''=''1"),
        "Unescaped quote in claim: {}",
        result
    );
}

#[test]
fn security_expand_filter_prefers_longest_claim_placeholder() {
    let engine = PolicyEngine::new();
    let mut claims = std::collections::HashMap::new();
    claims.insert("org".to_string(), serde_json::json!("wrong"));
    claims.insert("org_id".to_string(), serde_json::json!("org-123"));
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims,
    };

    let result = engine.expand_filter("org_id = $org_id", &auth);
    assert_eq!(result, "org_id = 'org-123'");
}

#[test]
fn security_expand_filter_does_not_partially_replace_unknown_longer_placeholder() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let result = engine.expand_filter("tenant_shadow = $tenant_id_shadow", &auth);
    assert_eq!(result, "tenant_shadow = $tenant_id_shadow");
}

#[test]
fn security_expand_filter_does_not_reexpand_replacement_values() {
    let engine = PolicyEngine::new();
    let mut claims = std::collections::HashMap::new();
    claims.insert("org_id".to_string(), serde_json::json!("$user_id"));
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims,
    };

    let result = engine.expand_filter("org_id = $org_id", &auth);
    assert_eq!(result, "org_id = '$user_id'");
}

#[test]
fn security_expand_filter_numeric_claim_no_quotes() {
    let engine = PolicyEngine::new();
    let mut claims = std::collections::HashMap::new();
    claims.insert("age".to_string(), serde_json::json!(42));
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims,
    };
    let result = engine.expand_filter("age = $age", &auth);
    assert_eq!(result, "age = 42");
}

#[test]
fn security_expand_filter_bool_claim_no_quotes() {
    let engine = PolicyEngine::new();
    let mut claims = std::collections::HashMap::new();
    claims.insert("active".to_string(), serde_json::json!(true));
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims,
    };
    let result = engine.expand_filter("active = $active", &auth);
    assert_eq!(result, "active = true");
}

#[test]
fn security_expand_filter_safe_values_unchanged() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        role: "operator".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    let result = engine.expand_filter("user_id = $user_id AND role = $role", &auth);
    assert_eq!(
        result,
        "user_id = '550e8400-e29b-41d4-a716-446655440000' AND role = 'operator'"
    );
}

// ══════════════════════════════════════════════════════════════════
// SECURITY: $tenant_id expansion (H1)
// ══════════════════════════════════════════════════════════════════

#[test]
fn test_expand_filter_tenant_id() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: Some("tenant_abc".to_string()),
        claims: std::collections::HashMap::new(),
    };
    let result = engine.expand_filter("operator_id = $tenant_id", &auth);
    assert_eq!(result, "operator_id = 'tenant_abc'");
}

#[test]
fn test_expand_filter_tenant_id_sql_injection() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: Some("O'Brien".to_string()),
        claims: std::collections::HashMap::new(),
    };
    let result = engine.expand_filter("operator_id = $tenant_id", &auth);
    assert_eq!(result, "operator_id = 'O''Brien'");
    assert!(
        !result.contains("O'B"),
        "Unescaped quote in tenant_id: {}",
        result
    );
}

#[test]
fn test_parse_expanded_filter_unescapes_quoted_tenant_id() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: Some("O'Brien".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let expanded = engine.expand_filter("operator_id = $tenant_id", &auth);
    let condition = engine
        .parse_filter_to_condition(&expanded)
        .expect("expanded policy filter should parse");

    assert_eq!(condition.value, Value::String("O'Brien".to_string()));
}

#[test]
fn test_parse_expanded_filter_keeps_operator_outside_quoted_user_id() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "alice != admin".to_string(),
        role: "user".to_string(),
        tenant_id: Some("tenant-1".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let expanded = engine.expand_filter("user_id = $user_id", &auth);
    let condition = engine
        .parse_filter_to_condition(&expanded)
        .expect("expanded policy filter should parse");

    assert_eq!(condition.op, Operator::Eq);
    assert_eq!(condition.value, Value::String("alice != admin".to_string()));
}

#[test]
fn test_parse_expanded_filter_keeps_equals_inside_quoted_inequality_value() {
    let engine = PolicyEngine::new();
    let condition = engine
        .parse_filter_to_condition("status != 'role = admin'")
        .expect("policy filter should parse");

    assert_eq!(condition.op, Operator::Ne);
    assert_eq!(condition.value, Value::String("role = admin".to_string()));
}

#[test]
fn test_expand_filter_tenant_id_missing() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    // When tenant_id is None, $tenant_id should NOT be expanded (stays literal)
    let result = engine.expand_filter("operator_id = $tenant_id", &auth);
    assert_eq!(result, "operator_id = $tenant_id");
}

#[test]
fn test_expand_filter_extra_tenant_id_claim_cannot_spoof_scope() {
    let engine = PolicyEngine::new();
    let mut claims = std::collections::HashMap::new();
    claims.insert("tenant_id".to_string(), serde_json::json!("evil-tenant"));
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims,
    };

    let result = engine.expand_filter("operator_id = $tenant_id", &auth);
    assert_eq!(result, "operator_id = $tenant_id");
}

#[test]
fn test_apply_policies_does_not_early_deny_when_later_policy_allows_operation() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "harbors_public_read".to_string(),
        table: "harbors".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "harbors_operator_crud".to_string(),
        table: "harbors".to_string(),
        filter: None,
        role: Some("operator".to_string()),
        operations: vec![
            OperationType::Read,
            OperationType::Create,
            OperationType::Update,
            OperationType::Delete,
        ],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user-op".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-op".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::set("harbors");
    let result = engine.apply_policies(&auth, &mut cmd);
    assert!(
        result.is_ok(),
        "expected update to be allowed by operator CRUD policy, got: {:?}",
        result
    );
}

#[test]
fn test_apply_policies_denies_when_matching_policies_exist_but_none_allow_operation() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "harbors_public_read".to_string(),
        table: "harbors".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "anon".to_string(),
        role: "anonymous".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::set("harbors");
    let result = engine.apply_policies(&auth, &mut cmd);
    assert!(
        result.is_err(),
        "expected update deny when only read policy matches"
    );
}

#[test]
fn test_apply_policies_denies_when_no_policy_matches_table() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "orders_read".to_string(),
        table: "orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "anon".to_string(),
        role: "anonymous".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("users");
    let result = engine.apply_policies(&auth, &mut cmd);
    assert!(
        result.is_err(),
        "expected deny when no policy matches target table"
    );
}

#[test]
fn test_apply_policies_allows_when_policy_engine_is_empty() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "anon".to_string(),
        role: "anonymous".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    let mut cmd = Qail::get("users");
    let result = engine.apply_policies(&auth, &mut cmd);
    assert!(
        result.is_ok(),
        "empty policy engine should preserve current behavior"
    );
}

#[test]
fn test_apply_policies_denies_unmapped_action_when_policies_exist() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "wildcard_read".to_string(),
        table: "*".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    let mut cmd = Qail::get("users");
    cmd.action = Action::Make;

    let result = engine.apply_policies(&auth, &mut cmd);
    assert!(
        result.is_err(),
        "unmapped actions must be denied when policy engine is enabled"
    );
}
