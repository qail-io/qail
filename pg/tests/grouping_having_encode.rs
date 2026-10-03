//! Native SELECT encoding of HAVING, explicit GROUP BY keys, and grouping
//! modes, including the positional bind order they share with WHERE.
//!
//!   cargo test -p qail-pg --test grouping_having_encode

use qail_core::ast::{AggregateFunc, Condition, Expr, GroupByMode, Operator, Qail, Value};
use qail_pg::protocol::AstEncoder;

fn lower_name() -> Expr {
    Expr::FunctionCall {
        name: "lower".to_string(),
        args: vec![Expr::Named("name".to_string())],
        alias: None,
    }
}

fn count_star() -> Expr {
    Expr::Aggregate {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: None,
        alias: None,
        args: Vec::new(),
        order_by: Vec::new(),
        within_group: Vec::new(),
    }
}

fn cond(left: Expr, op: Operator, value: Value) -> Condition {
    Condition {
        left,
        op,
        value,
        is_array_unnest: false,
    }
}

fn bind(value: &str) -> Option<Vec<u8>> {
    Some(value.as_bytes().to_vec())
}

#[test]
fn having_is_encoded_with_its_bind_value() {
    let cmd = Qail::get("orders")
        .columns(["department"])
        .column_expr(count_star())
        .having_cond(cond(
            Expr::Named("department".to_string()),
            Operator::Eq,
            Value::String("sales".to_string()),
        ));

    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

    assert_eq!(
        sql,
        "SELECT department, COUNT(*) FROM orders GROUP BY department HAVING department = $1"
    );
    assert_eq!(params, vec![bind("sales")]);
}

#[test]
fn having_binds_follow_where_binds_and_precede_order_and_limit() {
    let cmd = Qail::get("orders")
        .columns(["department"])
        .column_expr(count_star())
        .filter("status", Operator::Eq, "paid")
        .filter("region", Operator::Ne, "test")
        .having_cond(cond(count_star(), Operator::Gt, Value::Int(5)))
        .having_cond(cond(
            Expr::Named("department".to_string()),
            Operator::Ne,
            Value::String("ops".to_string()),
        ))
        .order_asc("department")
        .limit(10);

    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

    assert_eq!(
        sql,
        "SELECT department, COUNT(*) FROM orders WHERE status = $1 AND region != $2 \
         GROUP BY department HAVING COUNT(*) > $3 AND department != $4 \
         ORDER BY department LIMIT 10"
    );
    assert_eq!(
        params,
        vec![bind("paid"), bind("test"), bind("5"), bind("ops")]
    );
}

#[test]
fn having_with_explicit_group_by_expression() {
    let cmd = Qail::get("orders")
        .column_expr(lower_name())
        .column_expr(count_star())
        .group_by_expr([lower_name()])
        .having_cond(cond(count_star(), Operator::Gte, Value::Int(2)));

    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

    assert_eq!(
        sql,
        "SELECT LOWER(name), COUNT(*) FROM orders GROUP BY LOWER(name) HAVING COUNT(*) >= $1"
    );
    assert_eq!(params, vec![bind("2")]);
}

#[test]
fn count_keeps_having() {
    let cmd = Qail {
        action: qail_core::ast::Action::Cnt,
        ..Qail::get("orders")
            .group_by(["department"])
            .having_cond(cond(count_star(), Operator::Gt, Value::Int(1)))
    };

    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

    assert_eq!(
        sql,
        "SELECT COUNT(*) FROM orders GROUP BY department HAVING COUNT(*) > $1"
    );
    assert_eq!(params, vec![bind("1")]);
}

#[test]
fn rollup_applies_to_explicit_group_by_keys() {
    let mut cmd = Qail::get("orders")
        .columns(["department"])
        .column_expr(count_star())
        .group_by_expr([Expr::Named("department".to_string())]);
    cmd.group_by_mode = GroupByMode::Rollup;

    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

    assert_eq!(
        sql,
        "SELECT department, COUNT(*) FROM orders GROUP BY ROLLUP(department)"
    );
    assert!(params.is_empty());
}

#[test]
fn cube_applies_to_explicit_group_by_expressions() {
    let mut cmd = Qail::get("orders")
        .column_expr(lower_name())
        .column_expr(count_star())
        .group_by_expr([lower_name()]);
    cmd.group_by_mode = GroupByMode::Cube;

    let (sql, _) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

    assert_eq!(
        sql,
        "SELECT LOWER(name), COUNT(*) FROM orders GROUP BY CUBE(LOWER(name))"
    );
}

#[test]
fn grouping_mode_without_projected_aggregate_is_encoded() {
    let mut rollup = Qail::get("orders").columns(["department", "region"]);
    rollup.group_by_mode = GroupByMode::Rollup;
    let (sql, _) = AstEncoder::encode_cmd_sql(&rollup).unwrap();
    assert_eq!(
        sql,
        "SELECT department, region FROM orders GROUP BY ROLLUP(department, region)"
    );

    let mut sets = Qail::get("orders").columns(["department"]);
    sets.group_by_mode = GroupByMode::GroupingSets(vec![vec!["department".to_string()], vec![]]);
    let (sql, _) = AstEncoder::encode_cmd_sql(&sets).unwrap();
    assert_eq!(
        sql,
        "SELECT department FROM orders GROUP BY GROUPING SETS ((department), ())"
    );
}

#[test]
fn grouping_mode_without_keys_is_rejected() {
    let mut cmd = Qail::get("orders").column_expr(count_star());
    cmd.group_by_mode = GroupByMode::Cube;

    let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("CUBE without keys must fail closed");
    assert!(
        err.to_string()
            .contains("CUBE requires at least one grouping key"),
        "{err}"
    );
}

#[test]
fn grouping_sets_with_explicit_keys_is_rejected() {
    let mut cmd = Qail::get("orders")
        .columns(["department"])
        .group_by(["department"]);
    cmd.group_by_mode = GroupByMode::GroupingSets(vec![vec!["department".to_string()]]);

    let err = AstEncoder::encode_cmd_sql(&cmd)
        .expect_err("GROUPING SETS plus explicit keys must fail closed");
    assert!(
        err.to_string()
            .contains("GROUPING SETS cannot be combined with explicit GROUP BY keys"),
        "{err}"
    );
}

#[test]
fn every_group_by_cage_contributes_keys() {
    let cmd = Qail::get("orders")
        .columns(["department"])
        .column_expr(count_star())
        .group_by(["department"])
        .group_by(["region"]);

    let (sql, _) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

    assert_eq!(
        sql,
        "SELECT department, COUNT(*) FROM orders GROUP BY department, region"
    );
}

#[test]
fn explicit_group_by_expression_matches_preview() {
    use qail_core::transpiler::ToSql;

    let cmd = Qail::get("orders")
        .column_expr(lower_name())
        .group_by_expr([lower_name()]);

    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();

    assert_eq!(sql, "SELECT LOWER(name) FROM orders GROUP BY LOWER(name)");
    assert!(params.is_empty());
    assert_eq!(cmd.to_sql(), sql);
}
