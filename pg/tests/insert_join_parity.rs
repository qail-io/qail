//! Transpiler preview and native encoder agree on INSERT targets, UPDATE
//! targets, and JOIN ON string values.
//!
//! The offline tests compare `to_sql()` with `AstEncoder::encode_cmd_sql`.
//! The ignored live test runs both forms against TEMP tables:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test insert_join_parity -- --ignored --nocapture

use qail_core::ast::{Condition, Expr, Operator, Qail, Value};
use qail_core::transpiler::ToSql;
use qail_pg::protocol::AstEncoder;
use qail_pg::{PgDriver, PgResult};

fn native(cmd: &Qail) -> (String, Vec<Option<Vec<u8>>>) {
    AstEncoder::encode_cmd_sql(cmd).expect("native encode")
}

fn bind(text: &str) -> Option<Vec<u8>> {
    Some(text.as_bytes().to_vec())
}

fn join_eq(left: &str, value: Value) -> Condition {
    Condition {
        left: Expr::Named(left.to_string()),
        op: Operator::Eq,
        value,
        is_array_unnest: false,
    }
}

fn named_payload_insert(table: &str) -> Qail {
    Qail::add(table).set_value("status", "paid")
}

fn status_join(table: &str, items: &str, value: Value) -> Qail {
    Qail::get(format!("{table} o"))
        .columns(["o.id", "i.id"])
        .left_join_conds(format!("{items} i"), vec![join_eq("i.status", value)])
}

#[test]
fn named_payload_insert_previews_its_target_columns() {
    let cmd = named_payload_insert("orders");

    let (sql, params) = native(&cmd);
    assert_eq!(sql, "INSERT INTO orders (status) VALUES ($1)");
    assert_eq!(params, vec![bind("paid")]);

    let preview = cmd.to_sql();
    assert!(
        preview.starts_with("INSERT INTO orders (status) VALUES ('paid')"),
        "{preview}"
    );
}

#[test]
fn insert_preview_reads_the_payload_cage_not_the_first_cage() {
    let cmd = Qail::add("orders")
        .columns(["status"])
        .filter("id", Operator::Eq, 99)
        .set_value("status", "paid");

    let (sql, params) = native(&cmd);
    assert_eq!(sql, "INSERT INTO orders (status) VALUES ($1)");
    assert_eq!(params, vec![bind("paid")]);

    let preview = cmd.to_sql();
    assert!(
        preview.starts_with("INSERT INTO orders (status) VALUES ('paid')"),
        "{preview}"
    );
    assert!(!preview.contains("99"), "{preview}");
}

#[test]
fn positional_insert_pairs_explicit_columns_in_both_paths() {
    let cmd = Qail::add("orders")
        .columns(["note", "status"])
        .values([Value::String("n1".into()), Value::String("paid".into())]);

    let (sql, params) = native(&cmd);
    assert_eq!(sql, "INSERT INTO orders (note, status) VALUES ($1, $2)");
    assert_eq!(params, vec![bind("n1"), bind("paid")]);

    let preview = cmd.to_sql();
    assert!(
        preview.starts_with("INSERT INTO orders (note, status) VALUES ('n1', 'paid')"),
        "{preview}"
    );
}

#[test]
fn insert_preview_rejects_shapes_the_native_encoder_rejects() {
    let mixed = Qail::add("orders")
        .values([Value::String("n1".into())])
        .set_value("status", "paid");
    assert!(AstEncoder::encode_cmd_sql(&mixed).is_err());
    assert!(mixed.to_sql().contains("/* ERROR:"), "{}", mixed.to_sql());

    let count_mismatch = Qail::add("orders")
        .columns(["note", "status"])
        .set_value("status", "paid");
    assert!(AstEncoder::encode_cmd_sql(&count_mismatch).is_err());
    assert!(
        count_mismatch.to_sql().contains("/* ERROR:"),
        "{}",
        count_mismatch.to_sql()
    );

    let duplicate = Qail::add("orders")
        .set_value("status", "paid")
        .set_value("STATUS", "open");
    assert!(AstEncoder::encode_cmd_sql(&duplicate).is_err());
    assert!(
        duplicate.to_sql().contains("/* ERROR:"),
        "{}",
        duplicate.to_sql()
    );
}

#[test]
fn named_payload_out_of_column_list_order_is_rejected() {
    let insert = Qail::add("orders")
        .columns(["a", "b"])
        .set_value("b", "for-b")
        .set_value("a", "for-a");
    let error = AstEncoder::encode_cmd_sql(&insert).unwrap_err().to_string();
    assert!(error.contains("does not match column list"), "{error}");
    assert!(insert.to_sql().contains("/* ERROR"), "{}", insert.to_sql());

    let update = Qail::set("orders")
        .columns(["a", "b"])
        .set_value("b", "for-b")
        .set_value("a", "for-a")
        .filter("id", Operator::Eq, 1);
    assert!(AstEncoder::encode_cmd_sql(&update).is_err());
    assert!(update.to_sql().contains("/* ERROR"), "{}", update.to_sql());

    let ordered = Qail::add("orders")
        .columns(["a", "b"])
        .set_value("a", "for-a")
        .set_value("b", "for-b");
    let (sql, params) = native(&ordered);
    assert_eq!(sql, "INSERT INTO orders (a, b) VALUES ($1, $2)");
    assert_eq!(params, vec![bind("for-a"), bind("for-b")]);
}

#[test]
fn positional_update_previews_the_explicit_target_column() {
    let cmd = Qail::set("orders")
        .columns(["status"])
        .values([Value::String("paid".into())])
        .filter("id", Operator::Eq, 7);

    let (sql, params) = native(&cmd);
    assert_eq!(sql, "UPDATE orders SET status = $1 WHERE id = $2");
    assert_eq!(params, vec![bind("paid"), bind("7")]);

    assert_eq!(
        cmd.to_sql(),
        "UPDATE orders SET status = 'paid' WHERE id = 7"
    );
}

#[test]
fn text_update_without_fields_clause_assigns_its_payload_columns() {
    // The parser leaves a lone `*` in `columns`; it is not a target list.
    let cmd =
        qail_core::parser::parse("set users values verified = true where id = 1").expect("parse");

    let (sql, params) = native(&cmd);
    assert_eq!(sql, "UPDATE users SET verified = $1 WHERE id = $2");
    assert_eq!(params.len(), 2);

    assert_eq!(
        cmd.to_sql(),
        "UPDATE users SET verified = true WHERE id = 1"
    );
}

#[test]
fn join_dotted_string_binds_as_a_literal() {
    let cmd = status_join("orders", "items", Value::String("red.blue".into()));

    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "SELECT o.id, i.id FROM orders o LEFT JOIN items i ON i.status = $1"
    );
    assert_eq!(params, vec![bind("red.blue")]);

    let preview = cmd.to_sql();
    assert!(preview.contains("ON i.status = 'red.blue'"), "{preview}");
}

#[test]
fn join_jsonpath_string_binds_as_a_literal() {
    let cmd = status_join("orders", "items", Value::String("$.a".into()));

    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "SELECT o.id, i.id FROM orders o LEFT JOIN items i ON i.status = $1"
    );
    assert_eq!(params, vec![bind("$.a")]);
}

#[test]
fn join_column_value_stays_an_identifier() {
    let cmd = status_join("orders", "items", Value::Column("o.status".into()));

    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "SELECT o.id, i.id FROM orders o LEFT JOIN items i ON i.status = o.status"
    );
    assert!(params.is_empty());
}

#[test]
fn join_and_where_parameters_number_in_sql_order() {
    let cmd = Qail::get("orders o")
        .columns(["o.id"])
        .left_join_conds(
            "items i",
            vec![
                join_eq("i.order_id", Value::Column("o.id".into())),
                join_eq("i.status", Value::String("red.blue".into())),
            ],
        )
        .inner_join_conds(
            "notes n",
            vec![
                join_eq("n.order_id", Value::Column("o.id".into())),
                join_eq("n.kind", Value::String("a.b".into())),
            ],
        )
        .filter("o.status", Operator::Eq, "open");

    let (sql, params) = native(&cmd);
    assert_eq!(
        sql,
        "SELECT o.id FROM orders o \
         LEFT JOIN items i ON i.order_id = o.id AND i.status = $1 \
         INNER JOIN notes n ON n.order_id = o.id AND n.kind = $2 \
         WHERE o.status = $3"
    );
    assert_eq!(params, vec![bind("red.blue"), bind("a.b"), bind("open")]);
}

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

#[tokio::test]
#[ignore = "Requires a local PostgreSQL lab; set QAIL_TEST_DB_URL"]
async fn live_join_literals_and_insert_targets() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE qail_parity_orders (id integer, status text);
             CREATE TEMP TABLE qail_parity_items (id integer, order_id integer, status text);
             CREATE TEMP TABLE qail_parity_writes (note text, status text);
             INSERT INTO qail_parity_orders VALUES (1, 'red.blue'), (2, 'open');
             INSERT INTO qail_parity_items VALUES (10, 1, 'red.blue'), (20, 2, 'other');",
        )
        .await?;

    // A dotted string is the literal text, so only item 10 matches.
    let literal = status_join(
        "qail_parity_orders",
        "qail_parity_items",
        Value::String("red.blue".into()),
    );
    println!("literal join native: {:?}", native(&literal).0);
    let rows = driver.fetch_all(&literal).await?;
    let matched: Vec<(i32, Option<i32>)> = rows
        .iter()
        .map(|row| (row.get_i32(0).unwrap_or_default(), row.get_i32(1)))
        .collect();
    println!("literal join rows (order id, item id): {matched:?}");
    let mut matched_items: Vec<i32> = matched.iter().filter_map(|(_, item)| *item).collect();
    matched_items.sort_unstable();
    matched_items.dedup();
    assert_eq!(matched_items, vec![10]);

    // The transpiled preview runs and agrees.
    let preview_rows = driver.simple_query(&literal.to_sql()).await?;
    let preview_items: Vec<Option<String>> =
        preview_rows.iter().map(|row| row.get_string(1)).collect();
    println!("literal join preview item ids: {preview_items:?}");
    assert!(preview_items.contains(&Some("10".to_string())));
    assert!(!preview_items.contains(&Some("20".to_string())));

    // A Value::Column still compares two columns: item 10 (status
    // 'red.blue') matches order 1 (status 'red.blue'); item 20 matches none.
    let column = status_join(
        "qail_parity_orders",
        "qail_parity_items",
        Value::Column("o.status".into()),
    );
    println!("column join native: {:?}", native(&column).0);
    let rows = driver.fetch_all(&column).await?;
    let pairs: Vec<(i32, Option<i32>)> = rows
        .iter()
        .map(|row| (row.get_i32(0).unwrap_or_default(), row.get_i32(1)))
        .collect();
    println!("column join rows (order id, item id): {pairs:?}");
    assert!(pairs.contains(&(1, Some(10))));
    assert!(pairs.contains(&(2, None)));
    assert_eq!(pairs.len(), 2);

    // Named payload INSERT lands in `status`, not the first table column.
    let insert = named_payload_insert("qail_parity_writes");
    println!("named insert native: {:?}", native(&insert).0);
    driver.execute(&insert).await?;
    let preview = insert.to_sql();
    println!("named insert preview: {preview}");
    driver.execute_simple(&preview).await?;
    let rows = driver
        .simple_query("SELECT note, status FROM qail_parity_writes")
        .await?;
    let written: Vec<(Option<String>, Option<String>)> = rows
        .iter()
        .map(|row| (row.get_string(0), row.get_string(1)))
        .collect();
    println!("named insert rows (note, status): {written:?}");
    assert_eq!(
        written,
        vec![
            (None, Some("paid".to_string())),
            (None, Some("paid".to_string()))
        ]
    );

    Ok(())
}
