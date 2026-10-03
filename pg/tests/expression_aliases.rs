//! Output aliases must not become part of a grouping key or predicate operand.

use qail_core::ast::{AggregateFunc, Condition, Expr, GroupByMode, Operator, Qail, Value};
use qail_core::transpiler::ToSql;
use qail_pg::protocol::AstEncoder;

fn lower_name() -> Expr {
    Expr::FunctionCall {
        name: "lower".into(),
        args: vec![Expr::Aliased {
            name: "name".into(),
            alias: "input_name".into(),
        }],
        alias: Some("folded_name".into()),
    }
}

fn count_star() -> Expr {
    Expr::Aggregate {
        col: "*".into(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: None,
        alias: Some("row_count".into()),
    }
}

fn condition(left: Expr, value: Value) -> Condition {
    Condition {
        left,
        op: Operator::Gt,
        value,
        is_array_unnest: false,
    }
}

#[test]
fn grouping_keys_omit_aliases_at_every_expression_depth() {
    for (mode, clause) in [
        (GroupByMode::Simple, "LOWER(name)"),
        (GroupByMode::Rollup, "ROLLUP(LOWER(name))"),
        (GroupByMode::Cube, "CUBE(LOWER(name))"),
    ] {
        let mut cmd = Qail::get("items")
            .column_expr(lower_name())
            .group_by_expr([lower_name()]);
        cmd.group_by_mode = mode;
        let expected = format!("SELECT LOWER(name) AS folded_name FROM items GROUP BY {clause}");
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(sql, expected);
        assert!(params.is_empty());
        assert_eq!(cmd.to_sql(), expected);
    }
}

#[test]
fn having_omits_aliases_and_keeps_where_then_having_bind_order() {
    let cmd = Qail::get("items")
        .columns(["name"])
        .column_expr(count_star())
        .eq("active", true)
        .having_cond(condition(count_star(), Value::Int(1)));
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert_eq!(
        sql,
        "SELECT name, COUNT(*) AS row_count FROM items WHERE active = $1 GROUP BY name HAVING COUNT(*) > $2"
    );
    assert_eq!(params, vec![Some(b"t".to_vec()), Some(b"1".to_vec())]);
    assert_eq!(
        cmd.to_sql(),
        "SELECT name, COUNT(*) AS row_count FROM items WHERE active = true GROUP BY name HAVING COUNT(*) > 1"
    );
}

#[test]
fn predicate_expression_values_omit_output_aliases() {
    let cmd = Qail::get("items")
        .columns(["name"])
        .group_by(["name"])
        .having_cond(condition(lower_name(), Value::Expr(Box::new(lower_name()))));
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert_eq!(
        sql,
        "SELECT name FROM items GROUP BY name HAVING LOWER(name) > LOWER(name)"
    );
    assert!(params.is_empty());
    assert_eq!(cmd.to_sql(), sql);
}

#[test]
fn scalar_subquery_keeps_its_own_projection_alias() {
    let expr = Expr::Subquery {
        query: Box::new(Qail::get("items").column_expr(count_star())),
        alias: Some("total".into()),
    };
    let cmd = Qail::get("items")
        .column_expr(expr.clone())
        .having_cond(condition(expr, Value::Int(0)));
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert_eq!(
        sql,
        "SELECT (SELECT COUNT(*) AS row_count FROM items) AS total FROM items HAVING (SELECT COUNT(*) AS row_count FROM items) > $1"
    );
    assert_eq!(params, vec![Some(b"0".to_vec())]);
}

#[test]
fn compound_projection_aliases_are_preserved_but_nested_aliases_are_not() {
    use qail_core::ast::BinaryOp;

    let alias = Some("result".to_string());
    let cases = [
        (
            Expr::Cast {
                expr: Box::new(lower_name()),
                target_type: "text".into(),
                alias: alias.clone(),
            },
            "LOWER(name)::text",
        ),
        (
            Expr::Binary {
                left: Box::new(lower_name()),
                op: BinaryOp::Concat,
                right: Box::new(Expr::Literal(Value::String("!".into()))),
                alias: alias.clone(),
            },
            "(LOWER(name) || '!')",
        ),
        (
            Expr::Case {
                when_clauses: vec![(
                    condition(lower_name(), Value::String("a".into())),
                    Box::new(lower_name()),
                )],
                else_value: None,
                alias: alias.clone(),
            },
            "CASE WHEN LOWER(name) > 'a' THEN LOWER(name) END",
        ),
        (
            Expr::ArrayConstructor {
                elements: vec![lower_name()],
                alias: alias.clone(),
            },
            "ARRAY[LOWER(name)]",
        ),
        (
            Expr::RowConstructor {
                elements: vec![lower_name()],
                alias: alias.clone(),
            },
            "ROW(LOWER(name))",
        ),
        (
            Expr::Collate {
                expr: Box::new(lower_name()),
                collation: "default".into(),
                alias: alias.clone(),
            },
            "LOWER(name) COLLATE \"default\"",
        ),
        (
            Expr::SpecialFunction {
                name: "TRIM".into(),
                args: vec![(None, Box::new(lower_name()))],
                alias,
            },
            "TRIM(LOWER(name))",
        ),
    ];
    for (expr, operand) in cases {
        let cmd = Qail::get("items")
            .column_expr(expr.clone())
            .group_by_expr([expr]);
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(
            sql,
            format!("SELECT {operand} AS result FROM items GROUP BY {operand}")
        );
        assert!(params.is_empty());
        assert_eq!(cmd.to_sql(), sql);
    }
}

#[test]
fn projection_aliases_remain_in_returning() {
    let mut cmd = Qail::set("items").set_value("name", "A");
    cmd.returning = Some(vec![lower_name()]);
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert_eq!(
        sql,
        "UPDATE items SET name = $1 RETURNING LOWER(name) AS folded_name"
    );
    assert_eq!(params, vec![Some(b"A".to_vec())]);
}

#[test]
fn inline_expression_values_omit_output_aliases() {
    let cmd = Qail::get("items")
        .column_expr(lower_name())
        .group_by_expr([Expr::Literal(Value::Expr(Box::new(lower_name())))])
        .having_cond(condition(
            Expr::Literal(Value::Expr(Box::new(count_star()))),
            Value::Int(1),
        ));
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert_eq!(
        sql,
        "SELECT LOWER(name) AS folded_name FROM items GROUP BY LOWER(name) HAVING COUNT(*) > $1"
    );
    assert_eq!(params, vec![Some(b"1".to_vec())]);
    assert_eq!(
        cmd.to_sql(),
        "SELECT LOWER(name) AS folded_name FROM items GROUP BY LOWER(name) HAVING COUNT(*) > 1"
    );
}
