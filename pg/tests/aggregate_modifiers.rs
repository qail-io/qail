//! Aggregate DISTINCT and FILTER written in the text DSL must reach the SQL
//! in both the transpiler and the native encoder, with FILTER values bound
//! as parameters natively. Dropping either modifier returns extra rows.

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
fn array_agg_distinct_reaches_both_paths() {
    let (transpiled, native, params) = both_paths("get orders fields array_agg(distinct status)");
    assert_eq!(transpiled, "SELECT ARRAY_AGG(DISTINCT status) FROM orders");
    assert_eq!(native, "SELECT ARRAY_AGG(DISTINCT status) FROM orders");
    assert!(params.is_empty(), "{params:?}");
}

#[test]
fn string_agg_distinct_keeps_its_delimiter_in_both_paths() {
    let (transpiled, native, params) =
        both_paths("get orders fields string_agg(distinct status, ',')");
    assert_eq!(
        transpiled,
        "SELECT STRING_AGG(DISTINCT status, ',') FROM orders"
    );
    assert_eq!(
        native,
        "SELECT STRING_AGG(DISTINCT status, ',') FROM orders"
    );
    assert!(params.is_empty(), "{params:?}");
}

#[test]
fn jsonb_agg_filter_binds_its_value() {
    let (transpiled, native, params) =
        both_paths("get orders fields jsonb_agg(payload) filter (where active = true)");
    assert_eq!(
        transpiled,
        "SELECT JSONB_AGG(payload) FILTER (WHERE active = true) FROM orders"
    );
    assert_eq!(
        native,
        "SELECT JSONB_AGG(payload) FILTER (WHERE active = $1) FROM orders"
    );
    assert_eq!(params, vec![Some(b"t".to_vec())]);
}

#[test]
fn windowed_sum_filter_binds_its_value() {
    let (transpiled, native, params) =
        both_paths("get orders fields sum(amount) filter (where active = true) over ()");
    assert_eq!(
        transpiled,
        "SELECT SUM(amount) FILTER (WHERE active = true) OVER () AS sum FROM orders"
    );
    assert_eq!(
        native,
        "SELECT SUM(amount) FILTER (WHERE active = $1) OVER () AS sum FROM orders"
    );
    assert_eq!(params, vec![Some(b"t".to_vec())]);
}

#[test]
fn filter_placeholders_follow_params_order() {
    // Projection placeholders come before WHERE placeholders; each $N must
    // index the value written for it.
    let (_, native, params) = both_paths(
        "get orders fields region, array_agg(distinct status) filter (where kind = 'retail') \
         where region = 'bali'",
    );
    assert_eq!(
        native,
        "SELECT region, ARRAY_AGG(DISTINCT status) FILTER (WHERE kind = $1) \
         FROM orders WHERE region = $2 GROUP BY region"
    );
    assert_eq!(
        params,
        vec![Some(b"retail".to_vec()), Some(b"bali".to_vec())]
    );

    let (_, native, params) = both_paths(
        "get orders fields id, sum(amount) filter (where status = 'paid') \
         over (partition by region order by id) as paid where region = 'bali'",
    );
    assert_eq!(
        native,
        "SELECT id, SUM(amount) FILTER (WHERE status = $1) \
         OVER (PARTITION BY region ORDER BY id ASC) AS paid FROM orders WHERE region = $2"
    );
    assert_eq!(params, vec![Some(b"paid".to_vec()), Some(b"bali".to_vec())]);
}

#[test]
fn window_filter_identifiers_are_validated_natively() {
    use qail_core::ast::{Condition, Expr, Operator, Qail, Value};

    let mut cmd = Qail::get("orders");
    cmd.columns.push(Expr::Window {
        name: "total".to_string(),
        func: "sum".to_string(),
        params: vec![Expr::Named("amount".to_string())],
        filter: Some(vec![Condition {
            left: Expr::Named("active; DROP TABLE orders".to_string()),
            op: Operator::Eq,
            value: Value::Bool(true),
            is_array_unnest: false,
        }]),
        partition: vec![],
        order: vec![],
        frame: None,
    });

    let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("unsafe FILTER column must not encode");
    assert!(err.to_string().contains("filter"), "{err}");
}

#[test]
fn modifiers_without_a_node_are_rejected_before_encoding() {
    for query in [
        "get orders fields count(distinct status) over ()",
        "get orders fields array_agg(distinct status, region)",
        "get orders fields bit_or(flags) filter (where active = true)",
    ] {
        assert!(
            qail_core::parse(query).is_err(),
            "aggregate modifiers would be dropped: {query}"
        );
    }
}
