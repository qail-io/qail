//! Aggregate arguments are expressions, and aggregate-local ORDER BY and
//! WITHIN GROUP reach the SQL, in both the transpiler preview and the native
//! encoder. Before this, `sum(price * quantity)` became the identifier
//! `"(price * quantity)"` and STRING_AGG had no delimiter slot.

use qail_core::ast::builders::{aggregate, col, mode, percentile_cont};
use qail_core::ast::{Action, AggregateFunc, Condition, Expr, Operator, Qail, SortOrder, Value};
use qail_core::transpiler::ToSql;
use qail_pg::protocol::AstEncoder;

type Encoded = (String, String, Vec<Option<Vec<u8>>>);

fn both_paths(query: &str) -> Encoded {
    let cmd = qail_core::parse(query).unwrap_or_else(|err| panic!("{query}: {err}"));
    let (native_sql, params) =
        AstEncoder::encode_cmd_sql(&cmd).unwrap_or_else(|err| panic!("{query}: {err}"));
    (cmd.to_sql(), native_sql, params)
}

#[test]
fn a3_sum_of_product_is_an_expression_not_an_identifier() {
    let (transpiled, native, params) = both_paths("get orders fields sum(price * quantity)");
    assert_eq!(transpiled, "SELECT SUM((price * quantity)) FROM orders");
    assert_eq!(native, "SELECT SUM((price * quantity)) FROM orders");
    assert!(params.is_empty(), "{params:?}");
}

#[test]
fn a3_constant_cast_and_qualified_arguments() {
    let (transpiled, native, _) = both_paths(
        "get orders fields count(1) as n, sum(amount::numeric) as total, max(orders.id) as top",
    );
    println!("{transpiled}\n{native}");
    assert_eq!(
        native,
        "SELECT COUNT(1) AS n, SUM(amount::numeric) AS total, MAX(orders.id) AS top FROM orders"
    );
    assert_eq!(transpiled, native);
}

#[test]
fn a3_count_distinct_of_expression_keeps_distinct() {
    let (transpiled, native, _) =
        both_paths("get orders fields count(distinct lower(email)) as buyers");
    assert_eq!(
        transpiled,
        "SELECT COUNT(DISTINCT LOWER(email)) AS buyers FROM orders"
    );
    assert_eq!(
        native,
        "SELECT COUNT(DISTINCT LOWER(email)) AS buyers FROM orders"
    );
}

#[test]
fn a6_string_agg_without_delimiter_is_rejected() {
    let mut cmd = Qail::get("orders");
    cmd.columns.push(Expr::Aggregate {
        col: "status".to_string(),
        func: AggregateFunc::StringAgg,
        distinct: false,
        filter: None,
        alias: None,
        args: vec![],
        order_by: vec![],
        within_group: vec![],
    });
    let native = AstEncoder::encode_cmd_sql(&cmd);
    assert!(native.is_err(), "native encoded {:?}", native.map(|r| r.0));
    let transpiled = cmd.to_sql();
    assert!(transpiled.contains("ERROR"), "{transpiled}");
}

#[test]
fn a6_b4_typed_builder_string_agg_with_delimiter_and_order() {
    let expr = aggregate(
        AggregateFunc::StringAgg,
        [
            col("status"),
            Expr::Literal(Value::String("; ".to_string())),
        ],
    )
    .distinct()
    .order_by("status", SortOrder::DescNullsLast)
    .alias("statuses");
    let cmd = Qail::get("orders").column_expr(expr);
    let (native, params) = AstEncoder::encode_cmd_sql(&cmd).expect("native");
    assert_eq!(
        native,
        "SELECT STRING_AGG(DISTINCT status, '; ' ORDER BY status DESC NULLS LAST) AS statuses FROM orders"
    );
    assert!(params.is_empty(), "{params:?}");
    assert_eq!(
        cmd.to_sql(),
        "SELECT STRING_AGG(DISTINCT status, '; ' ORDER BY status DESC NULLS LAST) AS statuses FROM orders"
    );
}

#[test]
fn a3_placeholders_follow_text_order_across_args_filter_and_where() {
    // Projection binds come first; inside the call, FILTER follows the args.
    let cmd = Qail::get("orders")
        .column_expr(
            aggregate(
                AggregateFunc::Sum,
                [Expr::Case {
                    when_clauses: vec![(
                        Condition {
                            left: col("kind"),
                            op: Operator::Eq,
                            value: Value::String("retail".to_string()),
                            is_array_unnest: false,
                        },
                        Box::new(col("amount")),
                    )],
                    else_value: Some(Box::new(Expr::Literal(Value::Int(0)))),
                    alias: None,
                }],
            )
            .filter(vec![Condition {
                left: col("status"),
                op: Operator::Eq,
                value: Value::String("paid".to_string()),
                is_array_unnest: false,
            }])
            .alias("retail_paid"),
        )
        .filter("region", Operator::Eq, "bali");
    let (native, params) = AstEncoder::encode_cmd_sql(&cmd).expect("native");
    println!("{native} {params:?}");
    let filter_at = native.find("FILTER").expect("FILTER");
    let where_at = native.find("WHERE region").expect("WHERE");
    let filter_param = native[filter_at..where_at]
        .split('$')
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .expect("filter placeholder");
    let where_param = native[where_at..]
        .split('$')
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
        .expect("where placeholder");
    let value = |n: &str| params[n.parse::<usize>().unwrap() - 1].clone();
    assert_eq!(value(filter_param), Some(b"paid".to_vec()));
    assert_eq!(value(where_param), Some(b"bali".to_vec()));
}

#[test]
fn a3_col_and_args_together_are_rejected() {
    let mut cmd = Qail::get("orders");
    cmd.columns.push(Expr::Aggregate {
        col: "amount".to_string(),
        func: AggregateFunc::Sum,
        distinct: false,
        filter: None,
        alias: None,
        args: vec![col("price")],
        order_by: vec![],
        within_group: vec![],
    });
    let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("col + args");
    assert!(err.to_string().contains("both `col` and `args`"), "{err}");
    assert!(cmd.to_sql().contains("/* ERROR"), "{}", cmd.to_sql());
}

#[test]
fn b4_within_group_on_a_plain_aggregate_is_rejected() {
    let cmd = Qail::get("orders").column_expr(
        aggregate(AggregateFunc::Sum, [col("amount")])
            .within_group("amount", SortOrder::Asc)
            .build(),
    );
    assert!(AstEncoder::encode_cmd_sql(&cmd).is_err());
    let cmd = Qail::get("orders").column_expr(percentile_cont(0.9).build());
    let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("percentile without WITHIN GROUP");
    assert!(err.to_string().contains("WITHIN GROUP"), "{err}");
}

#[test]
fn b4_mode_has_no_direct_argument() {
    let cmd = Qail::get("orders").column_expr(
        mode()
            .within_group("status", SortOrder::Asc)
            .alias("common"),
    );
    let (native, _) = AstEncoder::encode_cmd_sql(&cmd).expect("native");
    assert_eq!(
        native,
        "SELECT MODE() WITHIN GROUP (ORDER BY status ASC) AS common FROM orders"
    );
    assert_eq!(cmd.to_sql(), native);
}

#[test]
fn b4_unmodelled_modifiers_fail_to_parse() {
    for query in [
        // Hypothetical-set aggregate: not modelled.
        "get orders fields rank(10) within group (order by amount)",
        // Ordered-set aggregate without WITHIN GROUP, with a modifier.
        "get orders fields percentile_cont(0.5) filter (where active = true)",
        // ORDER BY on a function Expr::Aggregate does not model.
        "get orders fields concat(a, b order by c)",
        // WITHIN GROUP on a plain aggregate.
        "get orders fields sum(amount) within group (order by amount)",
        // STRING_AGG with a local ORDER BY but no delimiter.
        "get orders fields string_agg(status order by id)",
        // No built-in arity matches.
        "get orders fields count(distinct status, region)",
        "get orders fields max(amount, fee)",
        "get orders fields array_agg(distinct status, region)",
    ] {
        assert!(qail_core::parse(query).is_err(), "{query} parsed");
    }
}

#[test]
fn a6_b4_string_agg_keeps_delimiter_and_local_order() {
    let (transpiled, native, params) =
        both_paths("get orders fields string_agg(status, ', ' order by created_at desc) as s");
    assert_eq!(
        transpiled,
        "SELECT STRING_AGG(status, ', ' ORDER BY created_at DESC) AS s FROM orders"
    );
    assert_eq!(
        native,
        "SELECT STRING_AGG(status, ', ' ORDER BY created_at DESC) AS s FROM orders"
    );
    assert!(params.is_empty(), "{params:?}");
}

#[test]
fn b4_array_agg_local_order_by() {
    let (transpiled, native, _) =
        both_paths("get orders fields array_agg(status order by created_at) as statuses");
    assert_eq!(
        transpiled,
        "SELECT ARRAY_AGG(status ORDER BY created_at ASC) AS statuses FROM orders"
    );
    assert_eq!(
        native,
        "SELECT ARRAY_AGG(status ORDER BY created_at ASC) AS statuses FROM orders"
    );
}

#[test]
fn b4_percentile_cont_within_group() {
    let (transpiled, native, _) = both_paths(
        "get orders fields percentile_cont(0.5) within group (order by amount) as median",
    );
    assert_eq!(
        transpiled,
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount ASC) AS median FROM orders"
    );
    assert_eq!(
        native,
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount ASC) AS median FROM orders"
    );
}

#[test]
fn b4_order_by_inside_a_window_call_is_rejected() {
    // Expr::Window has no aggregate ORDER BY slot; dropping it would change
    // the result order silently.
    let err = qail_core::parse("get orders fields array_agg(status order by id) over ()");
    assert!(err.is_err(), "{err:?}");
}

#[test]
fn a4_json_table_native_error_names_the_preview_only_path() {
    let mut cmd = Qail::get("orders.items");
    cmd.action = Action::JsonTable;
    cmd.columns = vec![Expr::Named("name=$.product".to_string())];
    assert!(cmd.to_sql().contains("JSON_TABLE("));
    let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("native JSON_TABLE");
    let message = err.to_string();
    assert!(
        message.contains("JSON_TABLE") && message.contains("preview"),
        "{message}"
    );
}
