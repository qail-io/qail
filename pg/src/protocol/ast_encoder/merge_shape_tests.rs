//! Native MERGE assignment DEFAULT, inheritance selection, and INSERT-arm shape.

use super::AstEncoder;
use qail_core::ast::{
    BinaryOp, Condition, Expr, MergeAction, Operator, OverridingKind, Qail, Value,
};
use qail_core::parser::parse;

fn native(cmd: &Qail) -> String {
    let (sql, params) = AstEncoder::encode_cmd_sql(cmd).expect("encode");
    assert!(params.is_empty(), "unexpected params: {params:?}");
    sql
}

#[test]
fn merge_update_assignment_default_is_the_keyword() {
    let cmd = parse(
        "merge users as u using staging_users as s on u.id = s.id \
         when matched then update set name = default",
    )
    .expect("parse");

    assert_eq!(
        native(&cmd),
        "MERGE INTO users AS u USING staging_users AS s ON u.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = DEFAULT"
    );
}

#[test]
fn merge_insert_value_default_is_the_keyword() {
    let cmd = parse(
        "merge users as u using staging_users as s on u.id = s.id \
         when not matched then insert (id, name) values (s.id, DEFAULT)",
    )
    .expect("parse");

    assert_eq!(
        native(&cmd),
        "MERGE INTO users AS u USING staging_users AS s ON u.id = s.id \
         WHEN NOT MATCHED BY TARGET THEN INSERT (id, name) VALUES (s.id, DEFAULT)"
    );
}

#[test]
fn merge_only_target_is_emitted() {
    let cmd = Qail::merge_into("users")
        .only()
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);

    assert_eq!(
        native(&cmd),
        "MERGE INTO ONLY users USING staging_users AS s ON users.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name"
    );
}

#[test]
fn merge_command_overriding_flag_is_not_dropped() {
    let cmd = Qail::merge_into("users")
        .overriding_system_value()
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_not_matched_insert(
            &["id", "name"],
            &[
                Expr::Named("s.id".to_string()),
                Expr::Named("s.name".to_string()),
            ],
        );

    match AstEncoder::encode_cmd_sql(&cmd) {
        Err(_) => {}
        Ok((sql, _)) => assert!(
            sql.contains("OVERRIDING SYSTEM VALUE"),
            "command OVERRIDING was silently dropped: {sql}"
        ),
    }
}

#[test]
fn merge_command_default_values_flag_is_not_dropped() {
    let cmd = Qail::merge_into("users")
        .default_values()
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_not_matched_insert(&["id"], &[Expr::Named("s.id".to_string())]);

    let result = AstEncoder::encode_cmd_sql(&cmd);
    assert!(
        result.is_err(),
        "command DEFAULT VALUES was silently dropped: {result:?}"
    );
}

#[test]
fn merge_parser_accepts_insert_default_values() {
    let cmd = parse(
        "merge users as u using staging_users as s on u.id = s.id \
         when not matched then insert default values",
    )
    .expect("parse");

    assert_eq!(
        native(&cmd),
        "MERGE INTO users AS u USING staging_users AS s ON u.id = s.id \
         WHEN NOT MATCHED BY TARGET THEN INSERT DEFAULT VALUES"
    );
}

#[test]
fn merge_parser_accepts_insert_overriding() {
    let cmd = parse(
        "merge users as u using staging_users as s on u.id = s.id \
         when not matched then insert (id, name) overriding system value values (s.id, s.name)",
    )
    .expect("parse");

    assert_eq!(
        native(&cmd),
        "MERGE INTO users AS u USING staging_users AS s ON u.id = s.id \
         WHEN NOT MATCHED BY TARGET THEN INSERT (id, name) OVERRIDING SYSTEM VALUE \
         VALUES (s.id, s.name)"
    );
}

fn base() -> Qail {
    Qail::merge_into("users")
        .target_alias("u")
        .using_table_as("staging_users", "s")
        .merge_on_column("u.id", Operator::Eq, "s.id")
}

fn encode_err(cmd: &Qail) -> String {
    AstEncoder::encode_cmd_sql(cmd)
        .expect_err("encode should fail")
        .to_string()
}

#[test]
fn merge_builders_emit_arm_shapes_and_source_only() {
    let cmd = Qail::merge_into("users")
        .only()
        .target_alias("u")
        .using_only_table_as("staging_users", "s")
        .merge_on_column("u.id", Operator::Eq, "s.id")
        .when_matched_update(&[("name", Expr::Default)])
        .when_not_matched_insert_overriding(
            OverridingKind::UserValue,
            &["id", "name"],
            &[Expr::Named("s.id".to_string()), Expr::Default],
        );
    assert_eq!(
        native(&cmd),
        "MERGE INTO ONLY users AS u USING ONLY staging_users AS s ON u.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = DEFAULT \
         WHEN NOT MATCHED BY TARGET THEN INSERT (id, name) OVERRIDING USER VALUE \
         VALUES (s.id, DEFAULT)"
    );

    assert_eq!(
        native(&base().when_not_matched_insert_default_values()),
        "MERGE INTO users AS u USING staging_users AS s ON u.id = s.id \
         WHEN NOT MATCHED BY TARGET THEN INSERT DEFAULT VALUES"
    );
}

#[test]
fn merge_default_keeps_param_numbering_in_order() {
    let cmd = base()
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("s.kind".to_string()),
                op: Operator::Eq,
                value: Value::String("a".to_string()),
                is_array_unnest: false,
            }],
            &[("name", Expr::Default), ("status", lookup("b"))],
        )
        .when_not_matched_insert_overriding(
            OverridingKind::SystemValue,
            &["id", "name", "status"],
            &[Expr::Named("s.id".to_string()), Expr::Default, lookup("c")],
        );
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).expect("encode");
    assert_eq!(
        sql,
        "MERGE INTO users AS u USING staging_users AS s ON u.id = s.id \
         WHEN MATCHED AND s.kind = $1 THEN UPDATE SET name = DEFAULT, \
         status = (SELECT label FROM statuses WHERE code = $2) \
         WHEN NOT MATCHED BY TARGET THEN INSERT (id, name, status) OVERRIDING SYSTEM VALUE \
         VALUES (s.id, DEFAULT, (SELECT label FROM statuses WHERE code = $3))"
    );
    assert_eq!(
        params,
        vec![
            Some(b"a".to_vec()),
            Some(b"b".to_vec()),
            Some(b"c".to_vec())
        ]
    );
}

fn lookup(code: &str) -> Expr {
    Expr::Subquery {
        query: Box::new(Qail::get("statuses").columns(["label"]).filter(
            "code",
            Operator::Eq,
            code,
        )),
        alias: None,
    }
}

#[test]
fn merge_default_outside_a_whole_write_value_is_rejected() {
    let nested = base().when_matched_update(&[(
        "name",
        Expr::Binary {
            left: Box::new(Expr::Default),
            op: BinaryOp::Concat,
            right: Box::new(Expr::Named("s.name".to_string())),
            alias: None,
        },
    )]);
    assert!(encode_err(&nested).contains("DEFAULT is only valid"));

    let condition = base()
        .merge_on_condition(Condition {
            left: Expr::Default,
            op: Operator::Eq,
            value: Value::Int(1),
            is_array_unnest: false,
        })
        .when_matched_delete();
    assert!(encode_err(&condition).contains("DEFAULT is only valid"));

    let mut returning = base().when_matched_delete();
    returning.returning = Some(vec![Expr::Default]);
    assert!(encode_err(&returning).contains("DEFAULT is only valid"));

    let select = Qail::get("users").columns_expr([Expr::Default]);
    assert!(encode_err(&select).contains("DEFAULT is only valid"));

    // ON CONFLICT DO UPDATE has no DEFAULT support yet; it must not slip through.
    let upsert = Qail::add("users")
        .set_value("id", 1)
        .on_conflict_update(&["id"], &[("name", Expr::Default)]);
    assert!(encode_err(&upsert).contains("DEFAULT is only valid"));
}

#[test]
fn merge_default_values_arm_rejects_columns_values_and_overriding() {
    for (columns, values, overriding) in [
        (vec!["id".to_string()], vec![], None),
        (vec![], vec![Expr::Named("s.id".to_string())], None),
        (vec![], vec![], Some(OverridingKind::SystemValue)),
    ] {
        let mut cmd = base().when_not_matched_insert_default_values();
        cmd.merge.as_mut().expect("merge").clauses[0].action = MergeAction::Insert {
            columns,
            values,
            overriding,
            default_values: true,
        };
        assert!(
            encode_err(&cmd)
                .contains("MERGE INSERT DEFAULT VALUES cannot have columns, values, or OVERRIDING")
        );
    }
}
