//! MERGE assignment DEFAULT, inheritance selection, and INSERT-arm shape.

use crate::ast::*;
use crate::parser::parse;
use crate::transpiler::{Dialect, ToSql};

fn preview(cmd: &Qail) -> String {
    cmd.to_sql_with_dialect(Dialect::Postgres)
}

#[test]
fn merge_update_assignment_default_is_the_keyword() {
    let cmd = parse(
        "merge users as u using staging_users as s on u.id = s.id \
         when matched then update set name = default",
    )
    .expect("parse");

    assert_eq!(
        preview(&cmd),
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
        preview(&cmd),
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
        preview(&cmd),
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

    let sql = preview(&cmd);
    assert!(
        sql.starts_with("/* ERROR:") || sql.contains("OVERRIDING SYSTEM VALUE"),
        "command OVERRIDING was silently dropped: {sql}"
    );
}

#[test]
fn merge_command_default_values_flag_is_not_dropped() {
    let cmd = Qail::merge_into("users")
        .default_values()
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_not_matched_insert(&["id"], &[Expr::Named("s.id".to_string())]);

    let sql = preview(&cmd);
    assert!(
        sql.starts_with("/* ERROR:"),
        "command DEFAULT VALUES was silently dropped: {sql}"
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
        preview(&cmd),
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
        preview(&cmd),
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
        preview(&cmd),
        "MERGE INTO ONLY users AS u USING ONLY staging_users AS s ON u.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = DEFAULT \
         WHEN NOT MATCHED BY TARGET THEN INSERT (id, name) OVERRIDING USER VALUE \
         VALUES (s.id, DEFAULT)"
    );

    let cmd = base().when_not_matched_insert_default_values();
    assert_eq!(
        preview(&cmd),
        "MERGE INTO users AS u USING staging_users AS s ON u.id = s.id \
         WHEN NOT MATCHED BY TARGET THEN INSERT DEFAULT VALUES"
    );
}

#[test]
fn merge_default_outside_a_whole_write_value_is_an_error() {
    let nested = base().when_matched_update(&[(
        "name",
        Expr::Binary {
            left: Box::new(Expr::Default),
            op: BinaryOp::Concat,
            right: Box::new(Expr::Named("s.name".to_string())),
            alias: None,
        },
    )]);
    assert!(
        preview(&nested).contains("/* ERROR: DEFAULT is only valid"),
        "{}",
        preview(&nested)
    );

    let condition = base()
        .merge_on_condition(Condition {
            left: Expr::Default,
            op: Operator::Eq,
            value: Value::Int(1),
            is_array_unnest: false,
        })
        .when_matched_delete();
    assert!(
        preview(&condition).contains("/* ERROR: DEFAULT is only valid"),
        "{}",
        preview(&condition)
    );

    let mut returning = base().when_matched_delete();
    returning.returning = Some(vec![Expr::Default]);
    assert!(
        preview(&returning).contains("/* ERROR: DEFAULT is only valid"),
        "{}",
        preview(&returning)
    );

    let select = Qail::get("users").columns_expr([Expr::Default]);
    assert!(
        preview(&select).contains("/* ERROR:"),
        "{}",
        preview(&select)
    );
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
        assert_eq!(
            preview(&cmd),
            "/* ERROR: MERGE INSERT DEFAULT VALUES cannot have columns, values, or OVERRIDING */"
        );
    }
}

#[test]
fn merge_parser_rejects_invalid_insert_shapes() {
    for text in [
        "merge users using s on users.id = s.id when not matched then insert (id) default values",
        "merge users using s on users.id = s.id when not matched then insert overriding system value default values",
        "merge users using s on users.id = s.id when not matched then insert values (default + 1)",
        "merge users using s on users.id = s.id when matched then update set name = default::text",
        "merge users using s on users.id = s.id when not matched then insert (id) overriding any value values (s.id)",
    ] {
        assert!(parse(text).is_err(), "should reject: {text}");
    }

    // `default_x` is a column, not the keyword.
    let cmd = parse(
        "merge users using s on users.id = s.id when matched then update set name = default_name",
    )
    .expect("parse");
    assert_eq!(
        preview(&cmd),
        "MERGE INTO users USING s ON users.id = s.id WHEN MATCHED THEN UPDATE SET name = default_name"
    );
}

#[test]
fn merge_language_reference_example() {
    let cmd = parse(
        "merge only users as u using staging_users as s on u.id = s.id \
         when matched then update set name = default \
         when not matched then insert (id, name) overriding system value values (s.id, s.name)",
    )
    .expect("parse");
    assert_eq!(
        preview(&cmd),
        "MERGE INTO ONLY users AS u USING staging_users AS s ON u.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = DEFAULT \
         WHEN NOT MATCHED BY TARGET THEN INSERT (id, name) OVERRIDING SYSTEM VALUE \
         VALUES (s.id, s.name)"
    );
}

#[test]
fn merge_formatter_round_trips_new_shapes() {
    let text = "merge only users as u using only staging_users as s on u.id = s.id \
                when matched then update set name = default \
                when not matched by target and s.kind = 'a' then insert (id, name) overriding system value values (s.id, default) \
                when not matched by target then insert default values";
    let cmd = parse(text).expect("parse");
    assert!(cmd.only_table);
    let formatted = crate::fmt::Formatter::new().format(&cmd).expect("format");
    let reparsed = parse(formatted.trim()).expect("reparse formatted text");
    assert_eq!(reparsed, cmd, "formatted: {formatted}");
}

#[test]
fn merge_payloads_without_shape_fields_still_decode() {
    let action: MergeAction =
        serde_json::from_str(r#"{"Insert":{"columns":["id"],"values":[{"Named":"s.id"}]}}"#)
            .expect("decode insert arm without overriding/default_values");
    assert_eq!(
        action,
        MergeAction::Insert {
            columns: vec!["id".to_string()],
            values: vec![Expr::Named("s.id".to_string())],
            overriding: None,
            default_values: false,
        }
    );

    let source: MergeSource =
        serde_json::from_str(r#"{"Table":{"name":"staging_users","alias":"s"}}"#)
            .expect("decode table source without only");
    assert_eq!(
        source,
        MergeSource::Table {
            name: "staging_users".to_string(),
            alias: Some("s".to_string()),
            only: false,
        }
    );

    let default: Expr = serde_json::from_str(r#""Default""#).expect("decode Default");
    assert_eq!(default, Expr::Default);
}
