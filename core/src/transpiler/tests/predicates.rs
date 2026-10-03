//! Predicate and RETURNING previews that must agree with the native encoder.

use crate::ast::*;
use crate::parser::parse;
use crate::transpiler::{Dialect, ToSql};

fn function(name: &str, args: Vec<Expr>, alias: Option<&str>) -> Expr {
    Expr::FunctionCall {
        name: name.to_string(),
        args,
        alias: alias.map(str::to_string),
    }
}

#[test]
fn insert_preview_without_returning_has_no_returning_clause() {
    let sql = Qail::add("orders").set_value("status", "paid").to_sql();
    assert!(!sql.contains("RETURNING"), "{sql}");

    let mut empty = Qail::add("orders").set_value("status", "paid");
    empty.returning = Some(Vec::new());
    let sql = empty.to_sql();
    assert!(!sql.contains("RETURNING"), "{sql}");

    let sql = Qail::add("orders")
        .set_value("status", "paid")
        .returning_all()
        .to_sql();
    assert!(sql.ends_with(" RETURNING *"), "{sql}");
}

#[test]
fn delete_preview_keeps_returning() {
    let sql = Qail::del("orders")
        .filter("id", Operator::Eq, 7)
        .returning(["id"])
        .to_sql();
    assert!(sql.ends_with(" RETURNING id"), "{sql}");

    let mut empty = Qail::del("orders").filter("id", Operator::Eq, 7);
    empty.returning = Some(Vec::new());
    assert!(!empty.to_sql().contains("RETURNING"));
}

#[test]
fn update_preview_renders_returning_expressions() {
    let mut cmd = Qail::set("orders")
        .set_value("status", "paid")
        .filter("id", Operator::Eq, 7);
    cmd.returning = Some(vec![
        Expr::Named("id".to_string()),
        function("upper", vec![Expr::Named("status".to_string())], Some("s")),
    ]);
    let sql = cmd.to_sql();
    assert!(sql.ends_with(" RETURNING id, UPPER(status) AS s"), "{sql}");
}

#[test]
fn merge_preview_keeps_returning_function_alias() {
    let mut cmd = Qail::merge_into("users")
        .target_alias("u")
        .using_table_as("staging_users", "s")
        .merge_on_column("u.id", Operator::Eq, "s.id")
        .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);
    cmd.returning = Some(vec![
        function("merge_action", Vec::new(), Some("action_taken")),
        Expr::Aliased {
            name: "u.id".to_string(),
            alias: "user_id".to_string(),
        },
    ]);
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(
        sql.ends_with(" RETURNING MERGE_ACTION() AS action_taken, u.id AS user_id"),
        "{sql}"
    );
}

#[test]
fn parser_accepts_is_distinct_from() {
    let cmd = parse("get orders where status is distinct from 'paid'").expect("parse");
    let condition = &cmd.cages[0].conditions[0];
    assert_eq!(format!("{:?}", condition.op), "IsDistinctFrom");
    assert_eq!(condition.value, Value::String("paid".to_string()));
}

#[test]
fn parser_reads_every_null_safe_and_boolean_predicate() {
    let cases = [
        ("a is not distinct from 1", Operator::IsNotDistinctFrom),
        ("a is distinct from 1", Operator::IsDistinctFrom),
        ("a is true", Operator::IsTrue),
        ("a is not true", Operator::IsNotTrue),
        ("a is false", Operator::IsFalse),
        ("a is not false", Operator::IsNotFalse),
        ("a is unknown", Operator::IsUnknown),
        ("a is not unknown", Operator::IsNotUnknown),
        ("a between symmetric 9 and 1", Operator::BetweenSymmetric),
        (
            "a not between symmetric 9 and 1",
            Operator::NotBetweenSymmetric,
        ),
    ];
    for (predicate, op) in cases {
        let cmd = parse(&format!("get t where {predicate}")).expect(predicate);
        let condition = &cmd.cages[0].conditions[0];
        assert_eq!(condition.op, op, "{predicate}");
        if op.is_range() {
            assert_eq!(
                condition.value,
                Value::Array(vec![Value::Int(9), Value::Int(1)])
            );
        }

        // Text form round-trips through the parser.
        let reparsed = parse(&cmd.to_string()).expect("reparse");
        assert_eq!(reparsed.cages[0].conditions[0], *condition, "{cmd}");
    }
}

#[test]
fn transpiler_renders_null_safe_and_boolean_predicates() {
    let sql = parse(
        "get t where a is distinct from 'x' and b is not distinct from null \
         and c is not true and d between symmetric 9 and 1",
    )
    .expect("parse")
    .to_sql();
    assert!(
        sql.ends_with(
            "WHERE a IS DISTINCT FROM 'x' AND b IS NOT DISTINCT FROM NULL \
             AND c IS NOT TRUE AND d BETWEEN SYMMETRIC 9 AND 1"
        ),
        "{sql}"
    );

    let projected = Qail::get("t").columns_expr([
        Expr::Binary {
            left: Box::new(Expr::Named("a".to_string())),
            op: BinaryOp::IsNotDistinctFrom,
            right: Box::new(Expr::Named("b".to_string())),
            alias: Some("same".to_string()),
        },
        Expr::Binary {
            left: Box::new(Expr::Named("flag".to_string())),
            op: BinaryOp::IsFalse,
            right: Box::new(Expr::Literal(Value::Null)),
            alias: None,
        },
    ]);
    assert_eq!(
        projected.to_sql(),
        "SELECT (a IS NOT DISTINCT FROM b) AS same, (flag IS FALSE) FROM t"
    );
}

#[test]
fn case_when_parses_between() {
    let cmd = parse("get t fields case when a between 1 and 3 then 1 else 0 end").expect("parse");
    let sql = cmd.to_sql();
    assert!(
        sql.contains("CASE WHEN a BETWEEN 1 AND 3 THEN 1 ELSE 0 END"),
        "{sql}"
    );
}

#[test]
fn array_membership_with_unsupported_operator_is_an_error_preview() {
    let cmd = Qail::get("t").filter_cond(Condition {
        left: Expr::Named("tags".to_string()),
        op: Operator::Like,
        value: Value::String("x".to_string()),
        is_array_unnest: true,
    });
    let sql = cmd.to_sql();
    assert!(sql.contains("FALSE /* ERROR: is_array_unnest"), "{sql}");
}
