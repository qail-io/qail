use super::*;
use crate::ast::{Condition, Operator};
use crate::rls::SuperAdminToken;

fn read_policy(table: &str) -> AccessPolicy {
    AccessPolicy::new().with_table(
        table,
        TableAccessPolicy::new().allow_operations([AccessOperation::Read]),
    )
}

#[test]
fn deny_by_default_without_matching_table_policy() {
    let policy = AccessPolicy::new();
    let err = policy
        .check_command(&AccessContext::anonymous(), &Qail::get("orders"))
        .expect_err("missing table policy should fail closed");

    assert_eq!(err.kind, AccessErrorKind::NoPolicy);
    assert_eq!(err.operation, Some(AccessOperation::Read));
}

#[test]
fn role_and_scope_gates_are_enforced() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Read])
            .require_any_role(["operator", "admin"])
            .require_scopes(["orders:read"]),
    );

    let missing_role = AccessContext::subject("user-1").with_scope("orders:read");
    assert!(matches!(
        policy
            .check_command(&missing_role, &Qail::get("orders"))
            .expect_err("role gate should fail")
            .kind,
        AccessErrorKind::MissingRole { .. }
    ));

    let allowed = AccessContext::subject("user-1")
        .with_role("operator")
        .with_scope("orders:read");
    policy
        .check_command(&allowed, &Qail::get("orders"))
        .expect("matching role and scope should pass");
}

#[test]
fn read_column_allowlist_rejects_wildcard_and_denied_columns() {
    let policy = AccessPolicy::new().with_table(
        "users",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Read])
            .read_columns(ColumnRule::only(["id", "email"])),
    );

    let wildcard = Qail::get("users");
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &wildcard)
            .expect_err("implicit SELECT * should fail")
            .kind,
        AccessErrorKind::WildcardProjectionDenied
    );

    let denied = Qail::get("users").columns(["id", "password_hash"]);
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &denied)
            .expect_err("password_hash should be denied")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "password_hash".to_string()
        }
    );

    let denied_filter =
        Qail::get("users")
            .columns(["id"])
            .filter("password_hash", Operator::Eq, "secret");
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &denied_filter)
            .expect_err("filtering by a denied column should fail")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "password_hash".to_string()
        }
    );

    policy
        .check_command(
            &AccessContext::anonymous(),
            &Qail::get("users").columns(["id", "email"]),
        )
        .expect("allowed projection should pass");
}

#[test]
fn quoted_schema_table_refs_match_normalized_policy_keys() {
    let policy = AccessPolicy::new().with_table(
        "public.users",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Read])
            .read_columns(ColumnRule::only(["id"])),
    );

    policy
        .check_command(
            &AccessContext::anonymous(),
            &Qail::get("\"public\".\"users\"").columns(["id"]),
        )
        .expect("quoted schema-qualified table should match normalized policy key");
}

#[test]
fn write_column_allowlist_checks_update_insert_upsert_and_merge() {
    let policy = AccessPolicy::new()
        .with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([
                    AccessOperation::Create,
                    AccessOperation::Update,
                    AccessOperation::Delete,
                ])
                .write_columns(ColumnRule::only(["status", "total"])),
        )
        .with_table(
            "incoming_orders",
            TableAccessPolicy::new().allow_operations([AccessOperation::Read]),
        );

    let update = Qail::set("orders").set_value("admin_note", "nope");
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &update)
            .expect_err("update denied column should fail")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "admin_note".to_string()
        }
    );

    let insert = Qail::add("orders")
        .columns(["status"])
        .values(["paid"])
        .on_conflict_update(
            &["id"],
            &[("total", Expr::Named("EXCLUDED.total".to_string()))],
        );
    policy
        .check_command(&AccessContext::anonymous(), &insert)
        .expect("insert and conflict update columns should pass");

    let mixed_insert = Qail::add("orders")
        .columns(["status"])
        .set_value("admin_note", "hidden");
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &mixed_insert)
            .expect_err("named payload columns must still be checked when columns are set")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "admin_note".to_string()
        }
    );

    let merge = Qail::merge_into("orders")
        .using_table_as("incoming_orders", "src")
        .merge_on_condition(Condition {
            left: Expr::Named("orders.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("src.id".to_string()),
            is_array_unnest: false,
        })
        .when_matched_update(&[("private_note", Expr::Named("src.note".to_string()))])
        .when_not_matched_insert(
            &["status", "total"],
            &[
                Expr::Named("src.status".to_string()),
                Expr::Named("src.total".to_string()),
            ],
        );
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &merge)
            .expect_err("merge update denied column should fail")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "private_note".to_string()
        }
    );
}

#[test]
fn merge_write_targets_reject_qualified_builder_columns_before_policy_allowlist() {
    let policy = AccessPolicy::new()
        .with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Create, AccessOperation::Update])
                .write_columns(ColumnRule::only(["status"])),
        )
        .with_table(
            "incoming_orders",
            TableAccessPolicy::new().allow_operations([AccessOperation::Read]),
        );

    let qualified_update = Qail::merge_into("orders")
        .using_table_as("incoming_orders", "src")
        .merge_on_condition(Condition {
            left: Expr::Named("orders.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("src.id".to_string()),
            is_array_unnest: false,
        })
        .when_matched_update(&[("orders.status", Expr::Named("src.status".to_string()))]);

    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &qualified_update)
            .expect_err("qualified MERGE update target must fail closed")
            .kind,
        AccessErrorKind::UnsupportedColumnExpression {
            context: "merge update target"
        }
    );

    let qualified_insert = Qail::merge_into("orders")
        .using_table_as("incoming_orders", "src")
        .merge_on_condition(Condition {
            left: Expr::Named("orders.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("src.id".to_string()),
            is_array_unnest: false,
        })
        .when_not_matched_insert(&["orders.status"], &[Expr::Named("src.status".to_string())]);

    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &qualified_insert)
            .expect_err("qualified MERGE insert target must fail closed")
            .kind,
        AccessErrorKind::UnsupportedColumnExpression {
            context: "merge insert target"
        }
    );
}

#[test]
fn read_column_policy_does_not_block_write_only_payloads() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Update])
            .read_columns(ColumnRule::only(["id"]))
            .write_columns(ColumnRule::only(["status"])),
    );

    let allowed = Qail::set("orders")
        .set_value("status", "paid")
        .filter("id", Operator::Eq, 1);
    policy
        .check_command(&AccessContext::anonymous(), &allowed)
        .expect("write-only payload column should not require read access");

    let denied_filter =
        Qail::set("orders")
            .set_value("status", "paid")
            .filter("status", Operator::Eq, "draft");
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &denied_filter)
            .expect_err("filter column should still require read access")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "status".to_string()
        }
    );
}

#[test]
fn write_payload_values_require_read_access_for_column_refs() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Update])
            .read_columns(ColumnRule::only(["id"]))
            .write_columns(ColumnRule::only(["status"])),
    );

    let copy_denied_column = Qail::set("orders")
        .set_value("status", Value::Column("private_note".to_string()))
        .filter("id", Operator::Eq, 1);
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &copy_denied_column)
            .expect_err("payload RHS column refs should require read access")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "private_note".to_string()
        }
    );

    let raw_expr = Qail::set("orders")
        .set_value("status", Value::Function("private_note".to_string()))
        .filter("id", Operator::Eq, 1);
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &raw_expr)
            .expect_err("raw payload RHS SQL cannot be inspected under read column policy")
            .kind,
        AccessErrorKind::UnsupportedColumnExpression {
            context: "write payload value"
        }
    );
}

#[test]
fn conflict_update_values_require_read_access_for_target_column_refs() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Create, AccessOperation::Update])
            .read_columns(ColumnRule::only(["id", "status"]))
            .write_columns(ColumnRule::only(["status"])),
    );

    let copy_denied_column = Qail::add("orders")
        .columns(["status"])
        .values(["paid"])
        .on_conflict_update(
            &["id"],
            &[("status", Expr::Named("private_note".to_string()))],
        );

    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &copy_denied_column)
            .expect_err("conflict update RHS target refs should require read access")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "private_note".to_string()
        }
    );
}

#[test]
fn merge_action_values_require_read_access_for_target_column_refs() {
    let policy = AccessPolicy::new()
        .with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Create, AccessOperation::Update])
                .read_columns(ColumnRule::only(["id", "status"]))
                .write_columns(ColumnRule::only(["status"])),
        )
        .with_table(
            "incoming_orders",
            TableAccessPolicy::new().allow_operations([AccessOperation::Read]),
        );

    let update_denied_column = Qail::merge_into("orders")
        .using_table_as("incoming_orders", "src")
        .merge_on_condition(Condition {
            left: Expr::Named("orders.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("src.id".to_string()),
            is_array_unnest: false,
        })
        .when_matched_update(&[("status", Expr::Named("orders.private_note".to_string()))]);

    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &update_denied_column)
            .expect_err("merge update RHS target refs should require read access")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "private_note".to_string()
        }
    );

    let insert_denied_column = Qail::merge_into("orders")
        .using_table_as("incoming_orders", "src")
        .merge_on_condition(Condition {
            left: Expr::Named("orders.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("src.id".to_string()),
            is_array_unnest: false,
        })
        .when_not_matched_insert(
            &["status"],
            &[Expr::Named("orders.private_note".to_string())],
        );

    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &insert_denied_column)
            .expect_err("merge insert RHS target refs should require read access")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "private_note".to_string()
        }
    );
}

#[test]
fn update_from_and_delete_using_require_read_access_on_auxiliary_tables() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Update, AccessOperation::Delete]),
    );

    let update = Qail::set("orders")
        .set_value("status", "paid")
        .update_from(["accounts"])
        .filter(
            "orders.account_id",
            Operator::Eq,
            Value::Column("accounts.id".into()),
        );
    let err = policy
        .check_command(&AccessContext::anonymous(), &update)
        .expect_err("UPDATE FROM source table should require read policy");
    assert_eq!(err.table, "accounts");
    assert_eq!(err.operation, Some(AccessOperation::Read));

    let delete = Qail::del("orders").delete_using(["accounts"]).filter(
        "orders.account_id",
        Operator::Eq,
        Value::Column("accounts.id".into()),
    );
    let err = policy
        .check_command(&AccessContext::anonymous(), &delete)
        .expect_err("DELETE USING source table should require read policy");
    assert_eq!(err.table, "accounts");
    assert_eq!(err.operation, Some(AccessOperation::Read));
}

#[test]
fn auxiliary_tables_with_restrictive_read_columns_fail_closed() {
    let policy = AccessPolicy::new()
        .with_table(
            "orders",
            TableAccessPolicy::new().allow_operations([AccessOperation::Update]),
        )
        .with_table(
            "accounts",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id"])),
        );

    let cmd = Qail::set("orders")
        .set_value("status", "paid")
        .update_from(["accounts"])
        .filter(
            "orders.account_id",
            Operator::Eq,
            Value::Column("accounts.id".into()),
        );

    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &cmd)
            .expect_err("restrictive auxiliary source columns cannot be enforced precisely")
            .kind,
        AccessErrorKind::AuxiliaryTableColumnPolicyUnsupported
    );
}

#[test]
fn read_column_policy_checks_distinct_on_and_grouping_sets() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Read])
            .read_columns(ColumnRule::only(["id", "status"])),
    );

    let distinct = Qail::get("orders")
        .columns(["id"])
        .distinct_on(["private_note"]);
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &distinct)
            .expect_err("DISTINCT ON denied column should fail")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "private_note".to_string()
        }
    );

    let mut grouping = Qail::get("orders").columns(["id"]);
    grouping.group_by_mode =
        crate::ast::GroupByMode::GroupingSets(vec![vec!["private_note".to_string()]]);
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &grouping)
            .expect_err("GROUPING SETS denied column should fail")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "private_note".to_string()
        }
    );
}

#[test]
fn read_column_policy_checks_window_partition_columns() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Read])
            .read_columns(ColumnRule::only(["id", "status"])),
    );

    let window_sort = Expr::Window {
        name: "ranked_orders".to_string(),
        func: "row_number".to_string(),
        params: vec![],
        partition: vec!["private_note".to_string()],
        order: vec![],
        frame: None,
    };
    let cmd = Qail::get("orders")
        .columns(["id"])
        .order_by_expr(window_sort, crate::ast::SortOrder::Asc);

    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &cmd)
            .expect_err("window PARTITION BY denied column should fail")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "private_note".to_string()
        }
    );
}

#[test]
fn returning_uses_read_column_policy_even_on_writes() {
    let policy = AccessPolicy::new().with_table(
        "users",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Update])
            .write_columns(ColumnRule::only(["email"]))
            .read_columns(ColumnRule::only(["id", "email"])),
    );

    let cmd = Qail::set("users")
        .set_value("email", "a@example.com")
        .returning(["password_hash"]);
    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &cmd)
            .expect_err("RETURNING denied read column should fail")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "password_hash".to_string()
        }
    );
}

#[test]
fn subqueries_are_checked_recursively() {
    let policy = read_policy("orders");
    let mut cmd = Qail::get("users").columns_expr([Expr::Subquery {
        query: Box::new(Qail::get("orders").columns(["id"])),
        alias: None,
    }]);

    let err = policy
        .check_command(&AccessContext::anonymous(), &cmd)
        .expect_err("outer table still needs a policy");
    assert_eq!(err.table, "users");

    cmd.table = "orders".to_string();
    policy
        .check_command(&AccessContext::anonymous(), &cmd)
        .expect("outer and subquery table policies should pass");
}

#[test]
fn correlated_subqueries_enforce_outer_read_column_policy() {
    let policy = AccessPolicy::new()
        .with_table(
            "users",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id"])),
        )
        .with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id", "user_id"])),
        );

    let cmd = Qail::get("users").columns(["id"]).filter(
        "id",
        Operator::Exists,
        Value::Subquery(Box::new(Qail::get("orders").columns(["id"]).filter(
            "orders.user_id",
            Operator::Eq,
            Value::Column("users.password_hash".to_string()),
        ))),
    );

    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &cmd)
            .expect_err("correlated outer refs must obey the outer table read policy")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "password_hash".to_string()
        }
    );
}

#[test]
fn inner_unqualified_subquery_columns_do_not_match_outer_policy() {
    let policy = AccessPolicy::new()
        .with_table(
            "users",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id"])),
        )
        .with_table(
            "orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id", "private_note"])),
        );

    let cmd = Qail::get("users").columns(["id"]).filter(
        "id",
        Operator::Exists,
        Value::Subquery(Box::new(Qail::get("orders").columns(["id"]).filter(
            "private_note",
            Operator::Eq,
            "visible",
        ))),
    );

    policy
        .check_command(&AccessContext::anonymous(), &cmd)
        .expect("unqualified inner columns should be checked against the inner table only");
}

#[test]
fn cte_alias_reads_do_not_require_separate_table_policy() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Read])
            .read_columns(ColumnRule::only(["id", "status"])),
    );
    let cmd = Qail::get("recent_orders")
        .with(
            "recent_orders",
            Qail::get("orders").columns(["id", "status"]),
        )
        .columns(["id"]);

    policy
        .check_command(&AccessContext::anonymous(), &cmd)
        .expect("CTE alias should be treated as a checked derived relation");
}

#[test]
fn cte_body_still_enforces_base_table_policy() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Read])
            .read_columns(ColumnRule::only(["id"])),
    );
    let cmd = Qail::get("recent_orders")
        .with(
            "recent_orders",
            Qail::get("orders").columns(["id", "private_note"]),
        )
        .columns(["id"]);

    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &cmd)
            .expect_err("CTE body denied columns must still fail")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "private_note".to_string()
        }
    );
}

#[test]
fn super_admin_token_bypasses_policy_checks() {
    let token = SuperAdminToken::for_system_process("access-check-test");
    let ctx = AccessContext::super_admin(token);
    AccessPolicy::new()
        .check_command(&ctx, &Qail::get("missing"))
        .expect("super admin context should bypass access policy");
}

#[test]
fn merge_query_source_is_checked_as_read() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new().allow_operations([AccessOperation::Update]),
    );

    let cmd = Qail::merge_into("orders")
        .using_query_as(Qail::get("source_orders").columns(["id"]), "src")
        .merge_on_condition(Condition {
            left: Expr::Named("orders.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("src.id".to_string()),
            is_array_unnest: false,
        })
        .when_matched_update(&[("status", Expr::Named("src.status".to_string()))]);

    let err = policy
        .check_command(&AccessContext::anonymous(), &cmd)
        .expect_err("merge source query table should require read policy");
    assert_eq!(err.table, "source_orders");
    assert_eq!(err.operation, Some(AccessOperation::Read));
}

#[test]
fn merge_table_source_is_checked_as_read() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new().allow_operations([AccessOperation::Update]),
    );

    let cmd = Qail::merge_into("orders")
        .using_table_as("source_orders", "src")
        .merge_on_condition(Condition {
            left: Expr::Named("orders.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("src.id".to_string()),
            is_array_unnest: false,
        })
        .when_matched_update(&[("status", Expr::Named("src.status".to_string()))]);

    let err = policy
        .check_command(&AccessContext::anonymous(), &cmd)
        .expect_err("merge source table should require read policy");
    assert_eq!(err.table, "source_orders");
    assert_eq!(err.operation, Some(AccessOperation::Read));
}

#[test]
fn merge_table_source_with_restrictive_columns_requires_query_source() {
    let policy = AccessPolicy::new()
        .with_table(
            "orders",
            TableAccessPolicy::new().allow_operations([AccessOperation::Update]),
        )
        .with_table(
            "source_orders",
            TableAccessPolicy::new()
                .allow_operations([AccessOperation::Read])
                .read_columns(ColumnRule::only(["id"])),
        );

    let cmd = Qail::merge_into("orders")
        .using_table_as("source_orders", "src")
        .merge_on_condition(Condition {
            left: Expr::Named("orders.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("src.id".to_string()),
            is_array_unnest: false,
        })
        .when_matched_update(&[("status", Expr::Named("src.status".to_string()))]);

    assert_eq!(
        policy
            .check_command(&AccessContext::anonymous(), &cmd)
            .expect_err("restrictive source table columns need an explicit query source")
            .kind,
        AccessErrorKind::SourceTableColumnPolicyUnsupported
    );
}

#[test]
fn access_policy_loads_from_toml_and_json() {
    let toml_policy = r#"
default_decision = "deny"

[tables.Orders]
operations = ["read"]
read_columns = { only = ["id", "status"] }
require_any_role = ["operator"]
require_scopes = ["orders:read"]
"#;
    let policy = AccessPolicy::from_toml_str(toml_policy).unwrap();
    policy
        .check_command(
            &AccessContext::subject("user-1")
                .with_role("operator")
                .with_scope("orders:read"),
            &Qail::get("orders").columns(["id", "status"]),
        )
        .expect("TOML policy should allow declared columns");
    assert!(policy.tables.contains_key("orders"));

    let json_policy = r#"{
        "default_decision": "deny",
        "tables": {
            "orders": {
                "operations": ["read"],
                "read_columns": {"only": ["id"]}
            }
        }
    }"#;
    let policy = AccessPolicy::from_json_str(json_policy).unwrap();
    policy
        .check_command(
            &AccessContext::anonymous(),
            &Qail::get("orders").columns(["id"]),
        )
        .expect("JSON policy should allow declared column");
}

#[test]
fn access_policy_rejects_unsupported_file_extensions() {
    let path = std::env::temp_dir().join(format!(
        "qail-access-policy-{}-{}.yaml",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, "default_decision: deny").unwrap();
    let err = AccessPolicy::load_from_path(&path).unwrap_err();
    let _ = std::fs::remove_file(&path);

    assert!(matches!(
        err,
        AccessPolicyLoadError::UnsupportedExtension(extension) if extension == "yaml"
    ));
}
