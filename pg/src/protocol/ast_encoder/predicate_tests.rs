//! One predicate shape in every context: WHERE, CASE, JOIN, projection,
//! ORDER BY, RETURNING and ON CONFLICT must agree on arity, lists and
//! placeholder numbering.

use super::*;
use qail_core::ast::builders::{between, case_when, col, is_in};
use qail_core::ast::{BinaryOp, Condition, Expr, Operator, SortOrder, Value};

fn encode(cmd: &Qail) -> (String, Vec<Option<Vec<u8>>>) {
    AstEncoder::encode_cmd_sql(cmd).expect("encode")
}

fn text_params(params: &[Option<Vec<u8>>]) -> Vec<String> {
    params
        .iter()
        .map(|p| match p {
            Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            None => "NULL".to_string(),
        })
        .collect()
}

fn condition(left: &str, op: Operator, value: Value) -> Condition {
    Condition {
        left: col(left),
        op,
        value,
        is_array_unnest: false,
    }
}

fn pending_item_subquery() -> Expr {
    Expr::Subquery {
        query: Box::new(
            Qail::get("items")
                .columns(["id"])
                .filter("status", Operator::Eq, "pending")
                .limit(1),
        ),
        alias: None,
    }
}

#[test]
fn between_is_one_predicate_in_where_case_and_join() {
    let where_cmd = Qail::get("orders o")
        .columns(["o.id"])
        .filter_cond(between("o.amount", 1, 3));
    let (sql, params) = encode(&where_cmd);
    assert_eq!(
        sql,
        "SELECT o.id FROM orders o WHERE o.amount BETWEEN $1 AND $2"
    );
    assert_eq!(params.len(), 2);

    let case_cmd = Qail::get("orders o").columns_expr([case_when(
        between("o.amount", 1, 3),
        Expr::Literal(Value::Int(1)),
    )
    .otherwise(Expr::Literal(Value::Int(0)))
    .build()]);
    let (sql, params) = encode(&case_cmd);
    assert_eq!(
        sql,
        "SELECT CASE WHEN o.amount BETWEEN 1 AND 3 THEN 1 ELSE 0 END FROM orders o"
    );
    assert!(params.is_empty());

    let join_cmd = Qail::get("items i")
        .columns(["i.id"])
        .left_join_conds("orders o", vec![between("o.amount", 1, 3)]);
    let (sql, params) = encode(&join_cmd);
    assert_eq!(
        sql,
        "SELECT i.id FROM items i LEFT JOIN orders o ON o.amount BETWEEN 1 AND 3"
    );
    assert!(params.is_empty());
}

#[test]
fn join_in_and_null_checks_keep_their_arity() {
    let cmd = Qail::get("items i").columns(["i.id"]).left_join_conds(
        "orders o",
        vec![
            is_in("o.amount", [1, 2]),
            condition("o.deleted_at", Operator::IsNull, Value::Null),
            condition("o.paid_at", Operator::IsNotNull, Value::Null),
        ],
    );
    let (sql, params) = encode(&cmd);
    assert_eq!(
        sql,
        "SELECT i.id FROM items i LEFT JOIN orders o ON o.amount IN (1, 2) \
         AND o.deleted_at IS NULL AND o.paid_at IS NOT NULL"
    );
    assert!(params.is_empty());
}

#[test]
fn case_json_exists_uses_function_syntax() {
    let cmd = qail_core::parser::parse(
        "get events fields case when payload json_exists '$.a' then 1 else 0 end",
    )
    .expect("parse");
    let (sql, _) = encode(&cmd);
    assert!(
        sql.contains("CASE WHEN JSON_EXISTS(payload, '$.a') THEN 1 ELSE 0 END"),
        "{sql}"
    );
}

#[test]
fn case_exists_and_array_membership_match_where() {
    let exists = Condition {
        left: Expr::Named(String::new()),
        op: Operator::Exists,
        value: Value::Subquery(Box::new(Qail::get("items").columns(["id"]))),
        is_array_unnest: false,
    };
    let member = Condition {
        left: col("o.tag_ids"),
        op: Operator::Eq,
        value: Value::Column("t.id".to_string()),
        is_array_unnest: true,
    };
    let cmd = Qail::get("orders o").columns_expr([case_when(exists, Expr::Literal(Value::Int(1)))
        .when(member, Expr::Literal(Value::Int(2)))
        .otherwise(Expr::Literal(Value::Int(0)))
        .build()]);
    let (sql, _) = encode(&cmd);
    assert_eq!(
        sql,
        "SELECT CASE WHEN EXISTS (SELECT id FROM items) THEN 1 \
         WHEN EXISTS (SELECT 1 FROM unnest(o.tag_ids) _el WHERE _el = t.id) THEN 2 \
         ELSE 0 END FROM orders o"
    );
}

#[test]
fn projected_null_checks_are_unary() {
    let cmd = Qail::get("orders").columns_expr([
        Expr::Binary {
            left: Box::new(col("amount")),
            op: BinaryOp::IsNull,
            right: Box::new(Expr::Literal(Value::Null)),
            alias: None,
        },
        Expr::Binary {
            left: Box::new(col("amount")),
            op: BinaryOp::IsNotNull,
            right: Box::new(Expr::Literal(Value::Null)),
            alias: Some("has_amount".to_string()),
        },
    ]);
    let (sql, _) = encode(&cmd);
    assert_eq!(
        sql,
        "SELECT (amount IS NULL), (amount IS NOT NULL) AS has_amount FROM orders"
    );
}

#[test]
fn bound_subquery_in_order_by_shares_parameters() {
    let cmd = Qail::get("orders")
        .columns(["id"])
        .filter("status", Operator::Eq, "paid")
        .order_by_expr(pending_item_subquery(), SortOrder::Asc);
    let (sql, params) = encode(&cmd);
    assert_eq!(
        sql,
        "SELECT id FROM orders WHERE status = $1 ORDER BY \
         (SELECT id FROM items WHERE status = $2 LIMIT 1)"
    );
    assert_eq!(text_params(&params), ["paid", "pending"]);
}

#[test]
fn bound_subquery_on_where_left_shares_parameters() {
    let cmd = Qail::get("orders").columns(["id"]).filter_cond(Condition {
        left: pending_item_subquery(),
        op: Operator::Eq,
        value: Value::Column("item_id".to_string()),
        is_array_unnest: false,
    });
    let (sql, params) = encode(&cmd);
    assert_eq!(
        sql,
        "SELECT id FROM orders WHERE (SELECT id FROM items WHERE status = $1 LIMIT 1) = item_id"
    );
    assert_eq!(text_params(&params), ["pending"]);
}

#[test]
fn bound_subquery_in_update_and_delete_returning_shares_parameters() {
    let mut update = Qail::set("orders")
        .set_value("status", "paid")
        .filter("id", Operator::Eq, 7);
    update.returning = Some(vec![col("id"), pending_item_subquery()]);
    let (sql, params) = encode(&update);
    assert_eq!(
        sql,
        "UPDATE orders SET status = $1 WHERE id = $2 RETURNING id, \
         (SELECT id FROM items WHERE status = $3 LIMIT 1)"
    );
    assert_eq!(params.len(), 3);
    assert_eq!(text_params(&params)[2], "pending");

    let mut delete = Qail::del("orders").filter("id", Operator::Eq, 7);
    delete.returning = Some(vec![pending_item_subquery()]);
    let (sql, params) = encode(&delete);
    assert_eq!(
        sql,
        "DELETE FROM orders WHERE id = $1 RETURNING \
         (SELECT id FROM items WHERE status = $2 LIMIT 1)"
    );
    assert_eq!(params.len(), 2);
}

#[test]
fn bound_subquery_in_conflict_assignment_shares_parameters() {
    let cmd = Qail::add("orders")
        .set_value("id", 7)
        .set_value("item_id", 1)
        .on_conflict_update(&["id"], &[("item_id", pending_item_subquery())]);
    let (sql, params) = encode(&cmd);
    assert_eq!(
        sql,
        "INSERT INTO orders (id, item_id) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET \
         item_id = (SELECT id FROM items WHERE status = $3 LIMIT 1)"
    );
    assert_eq!(text_params(&params)[2], "pending");
}

#[test]
fn empty_returning_list_emits_no_returning_clause() {
    for mut cmd in [
        Qail::add("orders").set_value("status", "paid"),
        Qail::set("orders")
            .set_value("status", "paid")
            .filter("id", Operator::Eq, 1),
        Qail::del("orders").filter("id", Operator::Eq, 1),
    ] {
        cmd.returning = Some(Vec::new());
        let (sql, _) = encode(&cmd);
        assert!(!sql.contains("RETURNING"), "{sql}");
    }
}

#[test]
fn returning_preview_and_native_agree() {
    use qail_core::transpiler::ToSql;

    let returning = vec![
        col("id"),
        Expr::FunctionCall {
            name: "upper".to_string(),
            args: vec![col("status")],
            alias: Some("s".to_string()),
        },
        Expr::Aliased {
            name: "amount".to_string(),
            alias: "total".to_string(),
        },
    ];
    let tail = " RETURNING id, UPPER(status) AS s, amount AS total";
    let insert = Qail::add("orders").set_value("status", "paid");
    let update = Qail::set("orders")
        .set_value("status", "paid")
        .filter("id", Operator::Eq, 1);
    let delete = Qail::del("orders").filter("id", Operator::Eq, 1);
    for base in [insert, update, delete] {
        let mut cmd = base.clone();
        cmd.returning = Some(returning.clone());
        let (native, _) = encode(&cmd);
        let preview = cmd.to_sql();
        assert!(native.ends_with(tail), "{native}");
        assert!(preview.ends_with(tail), "{preview}");

        for absent in [None, Some(Vec::new())] {
            let mut cmd = base.clone();
            cmd.returning = absent;
            let (native, _) = encode(&cmd);
            assert!(!native.contains("RETURNING"), "{native}");
            assert!(!cmd.to_sql().contains("RETURNING"), "{}", cmd.to_sql());
        }

        let all = base.clone().returning_all();
        assert!(encode(&all).0.ends_with(" RETURNING *"));
        assert!(all.to_sql().ends_with(" RETURNING *"), "{}", all.to_sql());
    }

    let mut merge = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);
    merge.returning = Some(vec![Expr::FunctionCall {
        name: "merge_action".to_string(),
        args: Vec::new(),
        alias: Some("action_taken".to_string()),
    }]);
    let tail = " RETURNING MERGE_ACTION() AS action_taken";
    assert!(encode(&merge).0.ends_with(tail));
    assert!(merge.to_sql().ends_with(tail), "{}", merge.to_sql());
}

#[test]
fn join_left_expression_text_is_unchanged() {
    // Engine leg joins: `leg_ids[1] = legs.id` must keep its exact text.
    let cmd = Qail::get("odyssey_connections")
        .columns(["odyssey_connections.id"])
        .inner_join_conds(
            "odyssey_legs",
            vec![Condition {
                left: Expr::Subscript {
                    expr: Box::new(col("odyssey_connections.leg_ids")),
                    index: Box::new(Expr::Literal(Value::Int(1))),
                    alias: None,
                },
                op: Operator::Eq,
                value: Value::Column("odyssey_legs.id".to_string()),
                is_array_unnest: false,
            }],
        );
    let (sql, params) = encode(&cmd);
    assert_eq!(
        sql,
        "SELECT odyssey_connections.id FROM odyssey_connections INNER JOIN odyssey_legs \
         ON odyssey_connections.leg_ids[1] = odyssey_legs.id"
    );
    assert!(params.is_empty());
}

#[test]
fn null_safe_and_boolean_predicates_in_every_context() {
    let cmd = Qail::get("orders o")
        .columns(["o.id"])
        .filter_cond(condition(
            "o.status",
            Operator::IsDistinctFrom,
            Value::String("paid".to_string()),
        ))
        .filter_cond(condition(
            "o.coupon",
            Operator::IsNotDistinctFrom,
            Value::Null,
        ))
        .filter_cond(condition("o.flag", Operator::IsNotTrue, Value::Null))
        .filter_cond(condition(
            "o.amount",
            Operator::BetweenSymmetric,
            Value::Array(vec![Value::Int(9), Value::Int(1)]),
        ));
    let (sql, params) = encode(&cmd);
    assert_eq!(
        sql,
        "SELECT o.id FROM orders o WHERE o.status IS DISTINCT FROM $1 \
         AND o.coupon IS NOT DISTINCT FROM $2 AND o.flag IS NOT TRUE \
         AND o.amount BETWEEN SYMMETRIC $3 AND $4"
    );
    assert_eq!(text_params(&params), ["paid", "NULL", "9", "1"]);

    let case = case_when(
        condition("o.flag", Operator::IsUnknown, Value::Null),
        Expr::Literal(Value::Int(0)),
    )
    .when(
        condition(
            "o.status",
            Operator::IsNotDistinctFrom,
            Value::String("paid".to_string()),
        ),
        Expr::Literal(Value::Int(1)),
    )
    .otherwise(Expr::Literal(Value::Int(2)))
    .build();
    let (sql, params) = encode(&Qail::get("orders o").columns_expr([case]));
    assert_eq!(
        sql,
        "SELECT CASE WHEN o.flag IS UNKNOWN THEN 0 \
         WHEN o.status IS NOT DISTINCT FROM 'paid' THEN 1 ELSE 2 END FROM orders o"
    );
    assert!(params.is_empty());

    let join = Qail::get("items i").columns(["i.id"]).left_join_conds(
        "orders o",
        vec![
            Condition {
                left: col("o.item_id"),
                op: Operator::IsNotDistinctFrom,
                value: Value::Column("i.id".to_string()),
                is_array_unnest: false,
            },
            condition("o.archived", Operator::IsFalse, Value::Null),
            condition(
                "o.amount",
                Operator::NotBetweenSymmetric,
                Value::Array(vec![Value::Int(5), Value::Int(2)]),
            ),
        ],
    );
    let (sql, _) = encode(&join);
    assert_eq!(
        sql,
        "SELECT i.id FROM items i LEFT JOIN orders o ON o.item_id IS NOT DISTINCT FROM i.id \
         AND o.archived IS FALSE AND o.amount NOT BETWEEN SYMMETRIC 5 AND 2"
    );

    let projected = Qail::get("orders").columns_expr([
        Expr::Binary {
            left: Box::new(col("status")),
            op: BinaryOp::IsDistinctFrom,
            right: Box::new(col("previous_status")),
            alias: Some("changed".to_string()),
        },
        Expr::Binary {
            left: Box::new(col("flag")),
            op: BinaryOp::IsTrue,
            right: Box::new(Expr::Literal(Value::Null)),
            alias: None,
        },
    ]);
    let (sql, _) = encode(&projected);
    assert_eq!(
        sql,
        "SELECT (status IS DISTINCT FROM previous_status) AS changed, (flag IS TRUE) FROM orders"
    );
}

#[test]
fn join_fuzzy_wraps_like_where() {
    let join = Qail::get("items i").columns(["i.id"]).left_join_conds(
        "tags t",
        vec![condition(
            "t.name",
            Operator::Fuzzy,
            Value::String("red".to_string()),
        )],
    );
    let (sql, params) = encode(&join);
    assert_eq!(
        sql,
        "SELECT i.id FROM items i LEFT JOIN tags t ON t.name ILIKE '%' || $1 || '%'"
    );
    assert_eq!(text_params(&params), ["red"]);
}

#[test]
fn contains_any_token_without_unnest_fails_closed() {
    let cmd = Qail::get("faq").filter_cond(condition(
        "keywords",
        Operator::ArrayElemContainedInText,
        Value::String("ferry".to_string()),
    ));
    let err = AstEncoder::encode_cmd_sql(&cmd).expect_err("was silently `keywords = $1`");
    assert!(
        matches!(err, EncodeError::InvalidAst(ref m) if m.contains("is_array_unnest")),
        "{err}"
    );
}

#[test]
fn parsed_where_exists_encodes() {
    let cmd =
        qail_core::parser::parse("get users where exists (get orders fields id)").expect("parse");
    let (sql, _) = encode(&cmd);
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE EXISTS (SELECT id FROM orders)"
    );
}
