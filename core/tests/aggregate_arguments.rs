//! `Expr::Aggregate` arguments, aggregate-local ORDER BY and WITHIN GROUP:
//! serde compatibility, both wire codecs, and every traversal that must see
//! the expressions inside them (sanitizer, access policy, RLS scoping).

use qail_core::access::{
    AccessContext, AccessErrorKind, AccessOperation, AccessPolicy, ColumnRule, TableAccessPolicy,
};
use qail_core::ast::builders::{aggregate, col};
use qail_core::ast::{
    AggregateFunc, BinaryOp, CageKind, Condition, Expr, Operator, Qail, SortOrder, Value,
};
use qail_core::rls::RlsContext;
use qail_core::wire::{decode_cmd_binary, decode_cmd_text, encode_cmd_binary, encode_cmd_text};

fn product(left: &str, right: &str) -> Expr {
    Expr::Binary {
        left: Box::new(col(left)),
        op: BinaryOp::Mul,
        right: Box::new(col(right)),
        alias: None,
    }
}

#[test]
fn payload_without_the_new_fields_decodes_to_the_column_form() {
    // JSON written before `args`/`order_by`/`within_group` existed.
    let json = r#"{"Aggregate":{"col":"amount","func":"Sum","distinct":false,"filter":null,"alias":"total"}}"#;
    let expr: Expr = serde_json::from_str(json).expect("decode");
    assert_eq!(
        expr,
        Expr::Aggregate {
            col: "amount".to_string(),
            func: AggregateFunc::Sum,
            distinct: false,
            filter: None,
            alias: Some("total".to_string()),
            args: vec![],
            order_by: vec![],
            within_group: vec![],
        }
    );
    // And the column form still serializes without them.
    let written = serde_json::to_string(&expr).expect("encode");
    assert!(!written.contains("args"), "{written}");
    assert!(!written.contains("order_by"), "{written}");
    assert!(!written.contains("within_group"), "{written}");
}

#[test]
fn parsed_aggregate_forms_survive_both_wire_codecs() {
    for query in [
        "get orders fields sum(price * quantity) as revenue",
        "get orders fields string_agg(status, ', ' order by created_at desc) as trail",
        "get orders fields array_agg(distinct status order by status desc nulls last) as s",
        "get orders fields percentile_cont(0.5) within group (order by amount) as median",
        "get orders fields mode() within group (order by status) as common",
        "get orders fields array_agg(id order by price desc) filter (where active = true) as ids",
    ] {
        let cmd = qail_core::parse(query).unwrap_or_else(|err| panic!("{query}: {err}"));
        let via_text = decode_cmd_text(&encode_cmd_text(&cmd))
            .unwrap_or_else(|err| panic!("{query}: text {err}"));
        assert_eq!(via_text, cmd, "{query}");
        let via_binary = decode_cmd_binary(&encode_cmd_binary(&cmd).expect("binary encode"))
            .unwrap_or_else(|err| panic!("{query}: binary {err}"));
        assert_eq!(via_binary, cmd, "{query}");
    }
}

#[test]
fn plain_column_argument_keeps_the_col_form() {
    let cmd = qail_core::parse("get orders fields sum(amount), count(*)").expect("parse");
    for column in &cmd.columns {
        let Expr::Aggregate { col, args, .. } = column else {
            panic!("{column:?}");
        };
        assert!(args.is_empty(), "{column:?}");
        assert!(col == "amount" || col == "*", "{column:?}");
    }
}

#[test]
fn sanitizer_walks_arguments_and_sort_keys() {
    let bad = || col("price;DROP");
    let in_arg = Qail::get("orders").column_expr(aggregate(AggregateFunc::Sum, [bad()]).build());
    let err = qail_core::sanitize::validate_ast(&in_arg).expect_err("arg");
    assert_eq!(err.field, "columns[0].arg");

    let in_order = Qail::get("orders").column_expr(
        aggregate(AggregateFunc::ArrayAgg, [col("id")])
            .order_by(bad(), SortOrder::Asc)
            .build(),
    );
    let err = qail_core::sanitize::validate_ast(&in_order).expect_err("order key");
    assert_eq!(err.field, "columns[0].aggregate_order");
}

#[test]
fn access_policy_sees_columns_inside_arguments_and_sort_keys() {
    let policy = AccessPolicy::new().with_table(
        "orders",
        TableAccessPolicy::new()
            .allow_operations([AccessOperation::Read])
            .read_columns(ColumnRule::only(["id", "price", "quantity"])),
    );
    let ctx = AccessContext::anonymous();
    let having = |left: Expr| Condition {
        left,
        op: Operator::Gt,
        value: Value::Int(0),
        is_array_unnest: false,
    };

    // A restricted projection names its output columns; an expression
    // aggregate has none, so it fails closed like any computed column.
    let projected = Qail::get("orders").column_expr(
        aggregate(AggregateFunc::Sum, [product("price", "quantity")]).alias("revenue"),
    );
    assert_eq!(
        policy
            .check_command(&ctx, &projected)
            .expect_err("computed")
            .kind,
        AccessErrorKind::UnsupportedColumnExpression {
            context: "read projection"
        }
    );

    let mut allowed = Qail::get("orders").columns(["id"]);
    allowed.having.push(having(
        aggregate(AggregateFunc::Sum, [product("price", "quantity")]).build(),
    ));
    policy
        .check_command(&ctx, &allowed)
        .expect("allowed columns");

    let mut hidden_in_arg = Qail::get("orders").columns(["id"]);
    hidden_in_arg.having.push(having(
        aggregate(AggregateFunc::Sum, [product("price", "cost")]).build(),
    ));
    assert_eq!(
        policy
            .check_command(&ctx, &hidden_in_arg)
            .expect_err("cost is not readable")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "cost".to_string()
        }
    );

    let mut hidden_in_order = Qail::get("orders").columns(["id"]);
    hidden_in_order.having.push(having(
        aggregate(AggregateFunc::Max, [col("price")])
            .order_by("cost", SortOrder::Desc)
            .build(),
    ));
    assert_eq!(
        policy
            .check_command(&ctx, &hidden_in_order)
            .expect_err("ordering by cost reads it")
            .kind,
        AccessErrorKind::ColumnDenied {
            column: "cost".to_string()
        }
    );
}

#[test]
fn rls_scopes_a_subquery_inside_an_aggregate_argument() {
    qail_core::rls::init_scope_registries_from_tables(
        &[
            ("_agg_rls_orders", "tenant_id"),
            ("_agg_rls_rates", "tenant_id"),
        ],
        &[],
    )
    .expect("boundary registration");

    let rate = Expr::Subquery {
        query: Box::new(Qail::get("_agg_rls_rates").columns(["rate"]).limit(1)),
        alias: None,
    };
    let query = Qail::get("_agg_rls_orders").column_expr(
        aggregate(
            AggregateFunc::Sum,
            [Expr::Binary {
                left: Box::new(col("amount")),
                op: BinaryOp::Mul,
                right: Box::new(rate),
                alias: None,
            }],
        )
        .alias("converted"),
    );
    let scoped = query
        .with_rls(&RlsContext::tenant("tenant-agg"))
        .expect("rls should apply");

    let Expr::Aggregate { args, .. } = &scoped.columns[0] else {
        panic!("{:?}", scoped.columns[0]);
    };
    let Expr::Binary { right, .. } = &args[0] else {
        panic!("{:?}", args[0]);
    };
    let Expr::Subquery { query, .. } = right.as_ref() else {
        panic!("{right:?}");
    };
    assert!(
        query.cages.iter().any(|cage| {
            matches!(cage.kind, CageKind::Filter)
                && cage.conditions.iter().any(|condition: &Condition| {
                    matches!(&condition.left, Expr::Named(name) if name.ends_with("tenant_id"))
                        && condition.op == Operator::Eq
                        && matches!(&condition.value, Value::String(v) if v == "tenant-agg")
                })
        }),
        "nested subquery was not tenant-scoped: {query:?}"
    );
}
