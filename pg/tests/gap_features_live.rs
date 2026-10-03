//! Live PostgreSQL checks for JSONPath and range/network/bitwise operators,
//! typed FROM sources, named/VARIADIC call arguments (any PostgreSQL 16+,
//! TEMP tables only) and RETURNING WITH aliases (PostgreSQL 18).
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test gap_features_live -- --ignored --nocapture

use qail_core::ast::*;
use qail_pg::protocol::AstEncoder;
use qail_pg::{PgDriver, PgResult};

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

async fn seeded() -> PgResult<PgDriver> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    driver
        .execute_simple(
            r#"
            CREATE TEMP TABLE gap_docs (
                id int, payload jsonb, body text, during int4range, net inet, flags int
            );
            INSERT INTO gap_docs VALUES
                (1, '{"a": 1}', 'fast ferry', '[1,5)', '10.1.2.3', 5),
                (2, '{"a": 2}', 'slow boat', '[5,9)', '192.168.1.1', 2),
                (3, '{"b": 1}', 'fast boat', '[20,30)', '10.9.9.9', 12);
            "#,
        )
        .await?;
    Ok(driver)
}

async fn ids(driver: &mut PgDriver, cmd: &Qail) -> PgResult<Vec<i32>> {
    let (sql, _) = AstEncoder::encode_cmd_sql(cmd).expect("encode");
    println!("{sql}");
    let mut ids: Vec<i32> = driver
        .fetch_all(cmd)
        .await?
        .iter()
        .map(|row| row.get_i32(0).expect("id"))
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

fn docs() -> Qail {
    Qail::get("gap_docs").columns(["id"])
}

#[tokio::test]
#[ignore = "Requires PostgreSQL at QAIL_TEST_DB_URL"]
async fn live_jsonpath_operators_are_not_text_search() -> PgResult<()> {
    let mut driver = seeded().await?;

    let exists = docs().filter("payload", Operator::JsonPathExists, "$.a");
    assert_eq!(ids(&mut driver, &exists).await?, vec![1, 2]);

    let matched = docs().filter("payload", Operator::JsonPathMatch, "$.a == 1");
    assert_eq!(ids(&mut driver, &matched).await?, vec![1]);

    let from_dsl = qail_core::parse("get gap_docs fields id where payload @? '$.b'").unwrap();
    assert_eq!(ids(&mut driver, &from_dsl).await?, vec![3]);

    // Bare `@@` keeps full-text search.
    let text_search = docs().filter("body", Operator::TextSearch, "fast");
    assert_eq!(ids(&mut driver, &text_search).await?, vec![1, 3]);

    // JSONPath `@@` against text is a type error, not a silent text search.
    let wrong_type = docs().filter("body", Operator::JsonPathMatch, "$.a == 1");
    let Err(err) = driver.fetch_all(&wrong_type).await else {
        panic!("text @@ jsonpath has no operator");
    };
    println!("text @@ jsonpath -> {err}");
    assert!(err.to_string().contains("operator does not exist"), "{err}");
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL at QAIL_TEST_DB_URL"]
async fn live_range_network_and_bitwise_operators() -> PgResult<()> {
    let mut driver = seeded().await?;

    let adjacent = docs().filter("during", Operator::Adjacent, "[9,12)");
    assert_eq!(ids(&mut driver, &adjacent).await?, vec![2]);
    let left = docs().filter("during", Operator::StrictlyLeft, "[10,12)");
    assert_eq!(ids(&mut driver, &left).await?, vec![1, 2]);
    let right = docs().filter("during", Operator::StrictlyRight, "[10,12)");
    assert_eq!(ids(&mut driver, &right).await?, vec![3]);
    let not_right = docs().filter("during", Operator::NotExtendsRight, "[0,6)");
    assert_eq!(ids(&mut driver, &not_right).await?, vec![1]);
    let not_left = docs().filter("during", Operator::NotExtendsLeft, "[5,6)");
    assert_eq!(ids(&mut driver, &not_left).await?, vec![2, 3]);
    // The pre-existing `&&`, `@>`, `<@` tokens bind against ranges too.
    let overlaps = docs().filter("during", Operator::Overlaps, "[4,6)");
    assert_eq!(ids(&mut driver, &overlaps).await?, vec![1, 2]);
    let contains = docs().filter("during", Operator::Contains, "[2,3)");
    assert_eq!(ids(&mut driver, &contains).await?, vec![1]);
    let contained = docs().filter("during", Operator::ContainedBy, "[0,10)");
    assert_eq!(ids(&mut driver, &contained).await?, vec![1, 2]);
    let subnet = docs().filter("net", Operator::SubnetOrEqual, "10.0.0.0/8");
    assert_eq!(ids(&mut driver, &subnet).await?, vec![1, 3]);
    let supernet =
        Qail::get("gap_docs")
            .columns(["id"])
            .filter("net", Operator::SupernetOrEqual, "10.1.2.3");
    assert_eq!(ids(&mut driver, &supernet).await?, vec![1]);

    let bits = Qail::get("gap_docs")
        .columns_expr(vec![
            Expr::Named("id".into()),
            Expr::Binary {
                left: Box::new(Expr::Named("flags".into())),
                op: BinaryOp::BitAnd,
                right: Box::new(Expr::Literal(Value::Int(4))),
                alias: Some("masked".into()),
            },
            Expr::Binary {
                left: Box::new(Expr::Named("flags".into())),
                op: BinaryOp::ShiftLeft,
                right: Box::new(Expr::Literal(Value::Int(1))),
                alias: Some("shifted".into()),
            },
            Expr::Binary {
                left: Box::new(Expr::Named("flags".into())),
                op: BinaryOp::BitXor,
                right: Box::new(Expr::Literal(Value::Int(1))),
                alias: Some("flipped".into()),
            },
        ])
        .order_asc("id");
    let rows = driver.fetch_all(&bits).await?;
    let got: Vec<(i32, i32, i32)> = rows
        .iter()
        .map(|row| {
            (
                row.get_i32(1).unwrap(),
                row.get_i32(2).unwrap(),
                row.get_i32(3).unwrap(),
            )
        })
        .collect();
    println!("flags & 4, << 1, # 1 -> {got:?}");
    assert_eq!(got, vec![(4, 10, 4), (0, 4, 3), (4, 24, 13)]);
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL at QAIL_TEST_DB_URL"]
async fn live_from_subquery_and_set_returning_function() -> PgResult<()> {
    let mut driver = seeded().await?;

    let inner =
        Qail::get("gap_docs")
            .columns(["id", "flags"])
            .filter("body", Operator::Like, "%boat");
    let cmd = Qail::get("d")
        .from_source(FromSource::subquery(inner, "d").column_aliases(["doc_id", "f"]))
        .columns(["doc_id"])
        .filter("f", Operator::Gt, 3);
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).expect("encode");
    println!("{sql} {:?}", params.len());
    assert_eq!(ids(&mut driver, &cmd).await?, vec![3]);

    let series = Qail::get("g")
        .from_source(
            FromSource::function(
                "generate_series",
                [
                    Expr::Literal(Value::Int(10)),
                    Expr::Literal(Value::Int(30)),
                    Expr::Literal(Value::Int(10)),
                ],
                "g",
            )
            .with_ordinality()
            .column_aliases(["v", "n"]),
        )
        .columns(["v", "n"])
        .order_asc("n");
    let (sql, _) = AstEncoder::encode_cmd_sql(&series).expect("encode");
    println!("{sql}");
    let rows = driver.fetch_all(&series).await?;
    let got: Vec<(i32, i64)> = rows
        .iter()
        .map(|row| (row.get_i32(0).unwrap(), row.get_i64(1).unwrap()))
        .collect();
    assert_eq!(got, vec![(10, 1), (20, 2), (30, 3)]);
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL at QAIL_TEST_DB_URL"]
async fn live_named_and_variadic_call_arguments() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let cmd = Qail::get("g")
        .from_source(FromSource::function(
            "generate_series",
            [Expr::Literal(Value::Int(1)), Expr::Literal(Value::Int(1))],
            "g",
        ))
        .columns_expr(vec![
            Expr::FunctionCall {
                name: "make_interval".into(),
                args: vec![
                    Expr::FunctionArg {
                        name: Some("days".into()),
                        variadic: false,
                        value: Box::new(Expr::Literal(Value::Int(3))),
                    },
                    Expr::FunctionArg {
                        name: Some("hours".into()),
                        variadic: false,
                        value: Box::new(Expr::Literal(Value::Int(4))),
                    },
                ],
                alias: Some("span".into()),
            },
            Expr::FunctionCall {
                name: "format".into(),
                args: vec![
                    Expr::Literal(Value::String("%s-%s".into())),
                    Expr::FunctionArg {
                        name: None,
                        variadic: true,
                        value: Box::new(Expr::ArrayConstructor {
                            elements: vec![
                                Expr::Literal(Value::String("a".into())),
                                Expr::Literal(Value::String("b".into())),
                            ],
                            alias: None,
                        }),
                    },
                ],
                alias: Some("joined".into()),
            },
        ]);
    let (sql, _) = AstEncoder::encode_cmd_sql(&cmd).expect("encode");
    println!("{sql}");
    let row = driver.fetch_one(&cmd).await?;
    let span = row.get_string(0).unwrap();
    let joined = row.get_string(1).unwrap();
    println!("span={span} joined={joined}");
    assert_eq!(span, "3 days 04:00:00");
    assert_eq!(joined, "a-b");
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL 18 at QAIL_TEST_DB_URL"]
async fn live_pg18_returning_with_aliases() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE gap_orders (id int PRIMARY KEY, status text);
             INSERT INTO gap_orders VALUES (1, 'new'), (2, 'new');",
        )
        .await?;

    let update = Qail::set("gap_orders")
        .set_value("status", "paid")
        .filter("id", Operator::Eq, 1)
        .returning(["o.status", "n.status"])
        .returning_aliases(Some("o"), Some("n"));
    let (sql, _) = AstEncoder::encode_cmd_sql(&update).expect("encode");
    println!("{sql}");
    let row = driver.fetch_one(&update).await?;
    let got = (row.get_string(0), row.get_string(1));
    println!("UPDATE old/new -> {got:?}");
    assert_eq!(got, (Some("new".into()), Some("paid".into())));

    let insert = Qail::add("gap_orders")
        .set_value("id", 3)
        .set_value("status", "draft")
        .returning(["before.status", "after.status"])
        .returning_aliases(Some("before"), Some("after"));
    let row = driver.fetch_one(&insert).await?;
    let got = (row.get_string(0), row.get_string(1));
    println!("INSERT old/new -> {got:?}");
    assert_eq!(got, (None, Some("draft".into())));

    let delete = Qail::del("gap_orders")
        .filter("id", Operator::Eq, 2)
        .returning(["gone.status", "kept.status"])
        .returning_aliases(Some("gone"), Some("kept"));
    let (sql, _) = AstEncoder::encode_cmd_sql(&delete).expect("encode");
    println!("{sql}");
    let row = driver.fetch_one(&delete).await?;
    let got = (row.get_string(0), row.get_string(1));
    println!("DELETE old/new -> {got:?}");
    assert_eq!(got, (Some("new".into()), None));

    driver
        .execute_simple("CREATE TEMP TABLE gap_updates (id int, status text); INSERT INTO gap_updates VALUES (1, 'shipped');")
        .await?;
    let merge = Qail::merge_into("gap_orders")
        .target_alias("t")
        .using_table_as("gap_updates", "s")
        .merge_on_column("t.id", Operator::Eq, "s.id")
        .when_matched_update(&[("status", Expr::Named("s.status".into()))])
        .returning(["was.status", "now.status"])
        .returning_aliases(Some("was"), Some("now"));
    let (sql, _) = AstEncoder::encode_cmd_sql(&merge).expect("encode");
    println!("{sql}");
    let row = driver.fetch_one(&merge).await?;
    let got = (row.get_string(0), row.get_string(1));
    println!("MERGE old/new -> {got:?}");
    assert_eq!(got, (Some("paid".into()), Some("shipped".into())));
    Ok(())
}
