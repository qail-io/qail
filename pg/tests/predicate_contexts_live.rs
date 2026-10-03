//! Live PostgreSQL checks that one predicate means the same thing in WHERE,
//! CASE, JOIN and projections, that bound subqueries outside the projection
//! share the statement's parameters, and that the null-safe / boolean
//! predicates return PostgreSQL's results, not `=` / `<>` results.
//!
//! Uses TEMP tables only.
//!
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test predicate_contexts_live -- --ignored --nocapture

use qail_core::ast::builders::{between, case_when, col, is_in};
use qail_core::ast::{BinaryOp, Condition, Expr, Operator, Qail, SortOrder, Value};
use qail_pg::protocol::AstEncoder;
use qail_pg::{PgDriver, PgResult};

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

fn condition(left: &str, op: Operator, value: Value) -> Condition {
    Condition {
        left: col(left),
        op,
        value,
        is_array_unnest: false,
    }
}

fn items_with_status(status: &str) -> Expr {
    Expr::Subquery {
        query: Box::new(
            Qail::get("qp_items")
                .columns(["id"])
                .filter("status", Operator::Eq, status)
                .order_by("id", SortOrder::Asc)
                .limit(1),
        ),
        alias: None,
    }
}

/// Run `cmd`, print its SQL and rows, return rows as text (`NULL` for NULL).
async fn rows(driver: &mut PgDriver, label: &str, cmd: &Qail) -> PgResult<Vec<Vec<String>>> {
    let (sql, params) = AstEncoder::encode_cmd_sql(cmd).expect("encode");
    let rows = driver.fetch_all_uncached(cmd).await?;
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| row.get_string(i).unwrap_or_else(|| "NULL".to_string()))
                .collect()
        })
        .collect();
    println!("{label}: {sql} [binds={}] -> {rows:?}", params.len());
    Ok(rows)
}

fn ids(rows: &[Vec<String>]) -> Vec<&str> {
    rows.iter().map(|row| row[0].as_str()).collect()
}

async fn setup(driver: &mut PgDriver) -> PgResult<()> {
    driver
        .execute_simple(
            "CREATE TEMP TABLE qp_items (id int PRIMARY KEY, status text);
             CREATE TEMP TABLE qp_orders (
                 id int PRIMARY KEY, item_id int, status text, prev_status text,
                 flag boolean, amount int, coupon text, payload jsonb);
             INSERT INTO qp_items VALUES (1, 'pending'), (2, 'done'), (3, 'pending');
             INSERT INTO qp_orders VALUES
                 (1, 1, 'paid', 'paid', true, 2, NULL, '{\"a\": 1}'),
                 (2, 2, 'open', 'paid', false, 5, 'X', '{\"b\": 1}'),
                 (3, NULL, NULL, NULL, NULL, 9, NULL, NULL);",
        )
        .await
}

#[tokio::test]
#[ignore = "Requires a local PostgreSQL; set QAIL_TEST_DB_URL"]
async fn predicates_agree_across_where_case_join_and_projection() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    setup(&mut driver).await?;

    // D1: the same BETWEEN in WHERE, CASE and JOIN.
    let where_rows = rows(
        &mut driver,
        "where between",
        &Qail::get("qp_orders")
            .columns(["id"])
            .filter_cond(between("amount", 1, 3))
            .order_by("id", SortOrder::Asc),
    )
    .await?;
    assert_eq!(ids(&where_rows), ["1"]);

    let case_rows = rows(
        &mut driver,
        "case between",
        &Qail::get("qp_orders")
            .columns(["id"])
            .columns_expr([
                case_when(between("amount", 1, 3), Expr::Literal(Value::Int(1)))
                    .otherwise(Expr::Literal(Value::Int(0)))
                    .alias("in_range"),
            ])
            .order_by("id", SortOrder::Asc),
    )
    .await?;
    assert_eq!(case_rows, [["1", "1"], ["2", "0"], ["3", "0"]]);

    let join_rows = rows(
        &mut driver,
        "join between/in/is null",
        &Qail::get("qp_items i")
            .columns(["i.id", "o.id"])
            .left_join_conds(
                "qp_orders o",
                vec![
                    Condition {
                        left: col("o.item_id"),
                        op: Operator::Eq,
                        value: Value::Column("i.id".to_string()),
                        is_array_unnest: false,
                    },
                    between("o.amount", 1, 5),
                    is_in("o.amount", [2, 5]),
                    condition("o.coupon", Operator::IsNull, Value::Null),
                ],
            )
            .order_by("i.id", SortOrder::Asc),
    )
    .await?;
    assert_eq!(join_rows, [["1", "1"], ["2", "NULL"], ["3", "NULL"]]);

    // D1: CASE JSON_EXISTS uses the SQL/JSON function form.
    let json_rows = rows(
        &mut driver,
        "case json_exists",
        &qail_core::parser::parse(
            "get qp_orders fields id, case when payload json_exists '$.a' then 1 else 0 end",
        )
        .expect("parse")
        .order_by("id", SortOrder::Asc),
    )
    .await?;
    assert_eq!(json_rows, [["1", "1"], ["2", "0"], ["3", "0"]]);

    // D3: projected null check is unary.
    let null_rows = rows(
        &mut driver,
        "projected is null",
        &Qail::get("qp_orders")
            .columns(["id"])
            .columns_expr([Expr::Binary {
                left: Box::new(col("coupon")),
                op: BinaryOp::IsNull,
                right: Box::new(Expr::Literal(Value::Null)),
                alias: Some("no_coupon".to_string()),
            }])
            .order_by("id", SortOrder::Asc),
    )
    .await?;
    assert_eq!(null_rows, [["1", "t"], ["2", "f"], ["3", "t"]]);

    driver
        .execute_simple("DROP TABLE qp_orders; DROP TABLE qp_items")
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a local PostgreSQL; set QAIL_TEST_DB_URL"]
async fn bound_subqueries_share_parameters_outside_the_projection() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    setup(&mut driver).await?;

    // E4: WHERE left side and ORDER BY.
    let where_rows = rows(
        &mut driver,
        "where-left + order-by subquery",
        &Qail::get("qp_orders")
            .columns(["id"])
            .filter("status", Operator::Eq, "paid")
            .filter_cond(Condition {
                left: items_with_status("pending"),
                op: Operator::Eq,
                value: Value::Column("item_id".to_string()),
                is_array_unnest: false,
            })
            .order_by_expr(items_with_status("done"), SortOrder::Asc),
    )
    .await?;
    assert_eq!(ids(&where_rows), ["1"]);

    // E4: UPDATE RETURNING.
    let mut update = Qail::set("qp_orders")
        .set_value("status", "shipped")
        .filter("id", Operator::Eq, 2);
    update.returning = Some(vec![col("id"), items_with_status("done")]);
    let update_rows = rows(&mut driver, "update returning subquery", &update).await?;
    assert_eq!(update_rows, [["2", "2"]]);

    // E4: ON CONFLICT assignment.
    let upsert = Qail::add("qp_orders")
        .set_value("id", 1)
        .set_value("item_id", 3)
        .on_conflict_update(&["id"], &[("item_id", items_with_status("done"))])
        .returning(["id", "item_id"]);
    let upsert_rows = rows(&mut driver, "on conflict subquery", &upsert).await?;
    assert_eq!(upsert_rows, [["1", "2"]]);

    // E7: an empty RETURNING list returns no rows; the write still happens.
    let mut silent = Qail::set("qp_orders")
        .set_value("coupon", "Y")
        .filter("id", Operator::Eq, 3);
    silent.returning = Some(Vec::new());
    let silent_rows = rows(&mut driver, "empty returning", &silent).await?;
    assert!(silent_rows.is_empty());
    let check = driver
        .simple_query("SELECT coupon FROM qp_orders WHERE id = 3")
        .await?;
    assert_eq!(check[0].get_string(0).as_deref(), Some("Y"));

    driver
        .execute_simple("DROP TABLE qp_orders; DROP TABLE qp_items")
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a local PostgreSQL; set QAIL_TEST_DB_URL"]
async fn null_safe_and_boolean_predicates_follow_postgres() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    setup(&mut driver).await?;

    let by = |op: Operator, column: &str, value: Value| {
        Qail::get("qp_orders")
            .columns(["id"])
            .filter_cond(condition(column, op, value))
            .order_by("id", SortOrder::Asc)
    };
    let paid = || Value::String("paid".to_string());

    let ne = rows(
        &mut driver,
        "status != 'paid'",
        &by(Operator::Ne, "status", paid()),
    )
    .await?;
    assert_eq!(ids(&ne), ["2"], "<> drops the NULL row");
    let distinct = rows(
        &mut driver,
        "status IS DISTINCT FROM 'paid'",
        &by(Operator::IsDistinctFrom, "status", paid()),
    )
    .await?;
    assert_eq!(ids(&distinct), ["2", "3"], "NULL is distinct from 'paid'");

    let same_null = rows(
        &mut driver,
        "coupon IS NOT DISTINCT FROM NULL",
        &by(Operator::IsNotDistinctFrom, "coupon", Value::Null),
    )
    .await?;
    assert_eq!(ids(&same_null), ["1", "3"]);

    for (op, expected) in [
        (Operator::IsTrue, vec!["1"]),
        (Operator::IsNotTrue, vec!["2", "3"]),
        (Operator::IsFalse, vec!["2"]),
        (Operator::IsNotFalse, vec!["1", "3"]),
        (Operator::IsUnknown, vec!["3"]),
        (Operator::IsNotUnknown, vec!["1", "2"]),
    ] {
        let got = rows(
            &mut driver,
            &format!("flag {op:?}"),
            &by(op, "flag", Value::Null),
        )
        .await?;
        assert_eq!(ids(&got), expected, "{op:?}");
    }

    let reversed = || Value::Array(vec![Value::Int(9), Value::Int(1)]);
    let plain = rows(
        &mut driver,
        "amount BETWEEN 9 AND 1",
        &by(Operator::Between, "amount", reversed()),
    )
    .await?;
    assert!(plain.is_empty());
    let symmetric = rows(
        &mut driver,
        "amount BETWEEN SYMMETRIC 9 AND 1",
        &by(Operator::BetweenSymmetric, "amount", reversed()),
    )
    .await?;
    assert_eq!(ids(&symmetric), ["1", "2", "3"]);
    let not_symmetric = rows(
        &mut driver,
        "amount NOT BETWEEN SYMMETRIC 9 AND 3",
        &by(
            Operator::NotBetweenSymmetric,
            "amount",
            Value::Array(vec![Value::Int(9), Value::Int(3)]),
        ),
    )
    .await?;
    assert_eq!(ids(&not_symmetric), ["1"]);

    // Projection, CASE and JOIN forms.
    let projected = rows(
        &mut driver,
        "projected distinct / case",
        &Qail::get("qp_orders")
            .columns(["id"])
            .columns_expr([
                Expr::Binary {
                    left: Box::new(col("status")),
                    op: BinaryOp::IsDistinctFrom,
                    right: Box::new(col("prev_status")),
                    alias: Some("changed".to_string()),
                },
                case_when(
                    condition("flag", Operator::IsUnknown, Value::Null),
                    Expr::Literal(Value::Int(0)),
                )
                .when(
                    condition("status", Operator::IsNotDistinctFrom, paid()),
                    Expr::Literal(Value::Int(1)),
                )
                .otherwise(Expr::Literal(Value::Int(2)))
                .alias("bucket"),
            ])
            .order_by("id", SortOrder::Asc),
    )
    .await?;
    assert_eq!(
        projected,
        [["1", "f", "1"], ["2", "t", "2"], ["3", "f", "0"]]
    );

    let joined = rows(
        &mut driver,
        "join is not distinct from",
        &Qail::get("qp_items i")
            .columns(["i.id", "o.id"])
            .left_join_conds(
                "qp_orders o",
                vec![Condition {
                    left: col("o.item_id"),
                    op: Operator::IsNotDistinctFrom,
                    value: Value::Column("i.id".to_string()),
                    is_array_unnest: false,
                }],
            )
            .order_by("i.id", SortOrder::Asc),
    )
    .await?;
    assert_eq!(joined, [["1", "1"], ["2", "2"], ["3", "NULL"]]);

    driver
        .execute_simple("DROP TABLE qp_orders; DROP TABLE qp_items")
        .await?;
    Ok(())
}
