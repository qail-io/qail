//! Native encoder coverage for B2/B3 operators, D6 FROM sources, D7 call
//! arguments, RETURNING WITH aliases and temporal key DDL.

use qail_core::ast::*;
use qail_core::transpiler::ToSql;
use qail_pg::protocol::AstEncoder;

fn native(cmd: &Qail) -> (String, Vec<Option<Vec<u8>>>) {
    AstEncoder::encode_cmd_sql(cmd).unwrap_or_else(|err| panic!("{cmd:?}: {err}"))
}

fn native_err(cmd: &Qail) -> String {
    match AstEncoder::encode_cmd_sql(cmd) {
        Ok((sql, _)) => panic!("expected an encode error, got {sql}"),
        Err(err) => err.to_string(),
    }
}

fn param_text(params: &[Option<Vec<u8>>], i: usize) -> String {
    String::from_utf8(params[i].clone().expect("non-null param")).expect("utf8 param")
}

fn call(name: &str, args: Vec<Expr>) -> Expr {
    Expr::FunctionCall {
        name: name.to_string(),
        args,
        alias: None,
    }
}

fn named(name: &str, value: Expr) -> Expr {
    Expr::FunctionArg {
        name: Some(name.to_string()),
        variadic: false,
        value: Box::new(value),
    }
}

fn variadic(value: Expr) -> Expr {
    Expr::FunctionArg {
        name: None,
        variadic: true,
        value: Box::new(value),
    }
}

// ── B2 ──────────────────────────────────────────────────────────────────

#[test]
fn b2_native_jsonpath_exists_from_dsl() {
    let cmd = qail_core::parse("get docs fields id where payload @? '$.a'").expect("parse");
    let (sql, params) = native(&cmd);
    println!("native: {sql}");
    assert_eq!(
        sql,
        "SELECT id FROM docs WHERE payload @? CAST($1 AS jsonpath)"
    );
    assert_eq!(param_text(&params, 0), "$.a");
}

#[test]
fn b2_native_jsonpath_match_is_not_text_search() {
    let cmd =
        Qail::get("docs")
            .columns(["id"])
            .filter("payload", Operator::JsonPathMatch, "$.a == 1");
    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "SELECT id FROM docs WHERE payload @@ CAST($1 AS jsonpath)"
    );
    assert_eq!(param_text(&params, 0), "$.a == 1");

    let text_search =
        Qail::get("docs")
            .columns(["id"])
            .filter("body", Operator::TextSearch, "ferry");
    let (sql, _) = native(&text_search);
    assert!(sql.contains("to_tsvector('english'"), "{sql}");
}

#[test]
fn b2_native_projected_jsonpath_binary_casts_right_operand() {
    let cmd = Qail::get("docs").columns_expr(vec![Expr::Binary {
        left: Box::new(Expr::Named("payload".into())),
        op: BinaryOp::JsonPathMatch,
        right: Box::new(Expr::Literal(Value::String("$.a > 1".into()))),
        alias: Some("hit".into()),
    }]);
    let (sql, _) = native(&cmd);
    assert_eq!(
        sql,
        "SELECT (payload @@ CAST('$.a > 1' AS jsonpath)) AS hit FROM docs"
    );
    assert_eq!(
        cmd.to_sql(),
        "SELECT (payload @@ CAST('$.a > 1' AS jsonpath)) AS hit FROM docs"
    );
}

// ── B3 ──────────────────────────────────────────────────────────────────

#[test]
fn b3_native_range_and_network_predicates_bind_values() {
    for (op, symbol) in [
        (Operator::Adjacent, "-|-"),
        (Operator::StrictlyLeft, "<<"),
        (Operator::StrictlyRight, ">>"),
        (Operator::NotExtendsRight, "&<"),
        (Operator::NotExtendsLeft, "&>"),
        (Operator::SubnetOrEqual, "<<="),
        (Operator::SupernetOrEqual, ">>="),
    ] {
        let cmd = Qail::get("t").columns(["id"]).filter("v", op, "x");
        let (sql, params) = native(&cmd);
        assert_eq!(sql, format!("SELECT id FROM t WHERE v {symbol} $1"));
        assert_eq!(params.len(), 1);
        assert_eq!(
            cmd.to_sql(),
            format!("SELECT id FROM t WHERE v {symbol} 'x'")
        );
    }
}

#[test]
fn b3_native_bitwise_binary_ops() {
    for (op, symbol) in [
        (BinaryOp::BitAnd, "&"),
        (BinaryOp::BitOr, "|"),
        (BinaryOp::BitXor, "#"),
        (BinaryOp::ShiftLeft, "<<"),
        (BinaryOp::ShiftRight, ">>"),
    ] {
        let cmd = Qail::get("t").columns_expr(vec![Expr::Binary {
            left: Box::new(Expr::Named("flags".into())),
            op,
            right: Box::new(Expr::Literal(Value::Int(4))),
            alias: Some("v".into()),
        }]);
        let (sql, _) = native(&cmd);
        assert_eq!(sql, format!("SELECT (flags {symbol} 4) AS v FROM t"));
        assert_eq!(
            cmd.to_sql(),
            format!("SELECT (flags {symbol} 4) AS v FROM t")
        );
    }
}

// ── D6 ──────────────────────────────────────────────────────────────────

#[test]
fn d6_string_from_source_is_still_rejected() {
    let cmd = Qail::get("generate_series(1,3) g").columns(["g"]);
    let err = native_err(&cmd);
    assert!(err.contains("unsafe identifier in table"), "{err}");
}

#[test]
fn d6_native_from_subquery_numbers_params_in_text_order() {
    let inner = Qail::get("orders")
        .columns(["id", "total"])
        .filter("status", Operator::Eq, "paid");
    let cmd = Qail::get("o")
        .from_source(FromSource::subquery(inner, "o").column_aliases(["order_id", "amount"]))
        .columns(["order_id", "amount"])
        .filter("amount", Operator::Gt, 10);
    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "SELECT order_id, amount FROM (SELECT id, total FROM orders WHERE status = $1) AS o (order_id, amount) WHERE amount > $2"
    );
    assert_eq!(param_text(&params, 0), "paid");
    assert_eq!(param_text(&params, 1), "10");
    assert_eq!(
        cmd.to_sql(),
        "SELECT order_id, amount FROM (SELECT id, total FROM orders WHERE status = 'paid') AS o (order_id, amount) WHERE amount > 10"
    );
}

#[test]
fn d6_native_from_function_with_ordinality() {
    let source = FromSource::function(
        "unnest",
        [Expr::ArrayConstructor {
            elements: vec![
                Expr::Literal(Value::String("a".into())),
                Expr::Literal(Value::String("b".into())),
            ],
            alias: None,
        }],
        "u",
    )
    .with_ordinality()
    .column_aliases(["tag", "n"]);
    let cmd = Qail::get("u").from_source(source).columns(["tag", "n"]);
    let (sql, _) = native(&cmd);
    assert_eq!(
        sql,
        "SELECT tag, n FROM UNNEST(ARRAY['a', 'b']) WITH ORDINALITY AS u (tag, n)"
    );

    let series = FromSource::function(
        "generate_series",
        [Expr::Literal(Value::Int(1)), Expr::Literal(Value::Int(3))],
        "g",
    )
    .column_aliases(["i"]);
    let cmd = Qail::get("g").from_source(series).columns(["i"]);
    assert_eq!(
        native(&cmd).0,
        "SELECT i FROM GENERATE_SERIES(1, 3) AS g (i)"
    );
    assert_eq!(cmd.to_sql(), "SELECT i FROM GENERATE_SERIES(1, 3) AS g (i)");
}

#[test]
fn d6_from_source_shape_errors_are_loud() {
    let inner = Qail::get("orders").columns(["id"]);
    // `table` must name the source alias: scoping keys on it.
    let mismatch = Qail::get("orders").from_source(FromSource::subquery(inner.clone(), "o"));
    let mut mismatch = mismatch;
    mismatch.table = "orders".into();
    let err = native_err(&mismatch);
    assert!(err.contains("must equal the command table"), "{err}");
    assert!(
        mismatch.to_sql().contains("/* ERROR:"),
        "{}",
        mismatch.to_sql()
    );

    let mut write = Qail::set("o").set_value("id", 1);
    write.from_source = Some(FromSource::subquery(inner.clone(), "o"));
    assert!(native_err(&write).contains("only for SELECT"));
    assert!(write.to_sql().contains("/* ERROR:"), "{}", write.to_sql());

    let mut ddl = Qail {
        action: Action::Make,
        table: "o".into(),
        ..Default::default()
    };
    ddl.from_source = Some(FromSource::subquery(inner.clone(), "o"));
    assert!(native_err(&ddl).contains("only for SELECT"));

    let writing_inner = Qail::get("o").from_source(FromSource::subquery(
        Qail::del("orders").filter("id", Operator::Eq, 1),
        "o",
    ));
    assert!(native_err(&writing_inner).contains("read-only"));

    let mut sampled = Qail::get("o").from_source(FromSource::subquery(inner, "o"));
    sampled.sample = Some((SampleMethod::Bernoulli, 10.0, None));
    assert!(native_err(&sampled).contains("TABLESAMPLE"));
}

// ── D7 ──────────────────────────────────────────────────────────────────

#[test]
fn d7_native_named_and_variadic_arguments() {
    let cmd = Qail::get("t").columns_expr(vec![
        Expr::FunctionCall {
            name: "make_interval".into(),
            args: vec![named("days", Expr::Literal(Value::Int(3)))],
            alias: Some("i".into()),
        },
        Expr::FunctionCall {
            name: "format".into(),
            args: vec![
                Expr::Literal(Value::String("%s-%s".into())),
                variadic(Expr::Named("parts".into())),
            ],
            alias: Some("f".into()),
        },
    ]);
    let (sql, _) = native(&cmd);
    assert_eq!(
        sql,
        "SELECT MAKE_INTERVAL(days => 3) AS i, FORMAT('%s-%s', VARIADIC parts) AS f FROM t"
    );
    assert_eq!(
        cmd.to_sql(),
        "SELECT MAKE_INTERVAL(days => 3) AS i, FORMAT('%s-%s', VARIADIC parts) AS f FROM t"
    );
}

#[test]
fn d7_argument_rules_fail_loudly() {
    let positional_after_named = Qail::get("t").columns_expr(vec![call(
        "f",
        vec![named("a", Expr::Named("x".into())), Expr::Named("y".into())],
    )]);
    assert!(native_err(&positional_after_named).contains("positional function argument"));
    assert!(positional_after_named.to_sql().contains("/* ERROR:"));

    let variadic_not_last = Qail::get("t").columns_expr(vec![call(
        "f",
        vec![variadic(Expr::Named("x".into())), Expr::Named("y".into())],
    )]);
    assert!(native_err(&variadic_not_last).contains("VARIADIC must mark the last"));

    let misplaced = Qail::get("t").columns_expr(vec![named("a", Expr::Named("x".into()))]);
    assert!(native_err(&misplaced).contains("outside a function call"));
    assert!(misplaced.to_sql().contains("outside a function call"));

    let bad_name = Qail::get("t").columns_expr(vec![call(
        "f",
        vec![named("a; drop", Expr::Named("x".into()))],
    )]);
    assert!(native_err(&bad_name).contains("unsafe identifier"));
}

// ── RETURNING WITH aliases ───────────────────────────────────────────────

#[test]
fn c_native_returning_with_aliases_on_writes() {
    let update = Qail::set("orders")
        .set_value("status", "paid")
        .filter("id", Operator::Eq, 7)
        .returning(["o.status", "n.status"])
        .returning_aliases(Some("o"), Some("n"));
    let (sql, params) = native(&update);
    assert_eq!(
        sql,
        "UPDATE orders SET status = $1 WHERE id = $2 RETURNING WITH (OLD AS o, NEW AS n) o.status, n.status"
    );
    assert_eq!(params.len(), 2);
    assert_eq!(
        update.to_sql(),
        "UPDATE orders SET status = 'paid' WHERE id = 7 RETURNING WITH (OLD AS o, NEW AS n) o.status, n.status"
    );

    let delete = Qail::del("orders")
        .filter("id", Operator::Eq, 7)
        .returning(["prior.status"])
        .returning_aliases(Some("prior"), None);
    assert_eq!(
        native(&delete).0,
        "DELETE FROM orders WHERE id = $1 RETURNING WITH (OLD AS prior) prior.status"
    );
}

#[test]
fn c_returning_aliases_misuse_is_loud() {
    let no_list = Qail::set("orders")
        .set_value("status", "paid")
        .returning_aliases(Some("o"), None);
    assert!(native_err(&no_list).contains("non-empty RETURNING list"));

    let same = Qail::set("orders")
        .set_value("status", "paid")
        .returning(["o.status"])
        .returning_aliases(Some("o"), Some("O"));
    assert!(native_err(&same).contains("must differ"));

    let mut read = Qail::get("orders").columns(["id"]);
    read.returning_aliases = Some(ReturningAliases {
        before: Some("o".into()),
        after: None,
    });
    assert!(native_err(&read).contains("write action"));

    // The DSL text form has no syntax for the aliases: never drop them silently.
    let text = qail_core::wire::encode_cmd_text(&same);
    assert!(qail_core::wire::decode_cmd_text(&text).is_err(), "{text}");
}

#[test]
fn c_returning_with_string_form_stays_an_identifier() {
    let cmd = Qail::set("orders")
        .set_value("status", "paid")
        .returning(["WITH (OLD AS o, NEW AS n) o.status"]);
    assert!(native_err(&cmd).contains("unsafe identifier in returning"));
}

// ── Temporal keys ───────────────────────────────────────────────────────

#[test]
fn c_native_temporal_key_ddl() {
    let make = Qail {
        action: Action::Make,
        table: "room_bookings".into(),
        columns: vec![
            Expr::Def {
                name: "room_id".into(),
                data_type: "INT".into(),
                constraints: vec![],
            },
            Expr::Def {
                name: "during".into(),
                data_type: "TSTZRANGE".into(),
                constraints: vec![],
            },
        ],
        table_constraints: vec![TableConstraint::TemporalKey {
            name: Some("room_bookings_pkey".into()),
            primary: true,
            columns: vec!["room_id".into()],
            period: "during".into(),
        }],
        ..Default::default()
    };
    let (sql, _) = native(&make);
    println!("native: {sql}");
    assert!(
        sql.contains(
            "CONSTRAINT room_bookings_pkey PRIMARY KEY (room_id, during WITHOUT OVERLAPS)"
        ),
        "{sql}"
    );
    let preview = make.to_sql();
    assert!(
        preview.contains(
            "CONSTRAINT room_bookings_pkey PRIMARY KEY (room_id, during WITHOUT OVERLAPS)"
        ),
        "{preview}"
    );

    let fk = Qail {
        action: Action::Alter,
        table: "room_cleanings".into(),
        table_constraints: vec![TableConstraint::ForeignKey {
            name: Some("room_cleanings_fk".into()),
            columns: vec!["room_id".into(), "during".into()],
            ref_table: "room_bookings".into(),
            ref_columns: vec!["room_id".into(), "during".into()],
            period: true,
            on_delete: None,
            on_update: None,
            deferrable: None,
        }],
        ..Default::default()
    };
    let (sql, _) = native(&fk);
    println!("native: {sql}");
    assert!(
        sql.contains(
            "FOREIGN KEY (room_id, PERIOD during) REFERENCES room_bookings(room_id, PERIOD during)"
        ),
        "{sql}"
    );
    assert!(
        fk.to_sql().contains(
            "FOREIGN KEY (room_id, PERIOD during) REFERENCES room_bookings(room_id, PERIOD during)"
        ),
        "{}",
        fk.to_sql()
    );
}
