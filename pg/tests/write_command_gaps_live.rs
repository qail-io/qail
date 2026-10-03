//! Live PostgreSQL checks for write-command gaps on TEMP tables: WITH on
//! writes and data-modifying CTEs (E3), ON CONFLICT ON CONSTRAINT (B6),
//! TRUNCATE / LOCK / EXPLAIN natively (A7), Unicode identifiers (D9), and
//! UPDATE subscript / field targets (D15).
//!
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test write_command_gaps_live -- --ignored --nocapture

use qail_core::ast::{Condition, Expr, Operator, Qail, Value};
use qail_pg::{PgDriver, PgResult};

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

/// One text cell from a simple-protocol query (text format).
async fn scalar(driver: &mut PgDriver, sql: &str) -> PgResult<String> {
    let rows = driver.simple_query(sql).await?;
    Ok(rows
        .first()
        .and_then(|row| row.get_string(0))
        .unwrap_or_default())
}

#[tokio::test]
#[ignore = "Requires a live PostgreSQL at QAIL_TEST_DB_URL"]
async fn write_ctes_run_against_the_server() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE gap_orders (id bigint PRIMARY KEY, status text NOT NULL); \
             CREATE TEMP TABLE gap_items (id bigint PRIMARY KEY, status text NOT NULL); \
             CREATE TEMP TABLE gap_log (id bigint NOT NULL); \
             INSERT INTO gap_items VALUES (1, 'pending'), (2, 'done'), (3, 'pending'); \
             INSERT INTO gap_orders VALUES (10, 'void'), (11, 'open'), (12, 'void')",
        )
        .await?;

    let chosen = || {
        Qail::get("gap_items")
            .columns(["id"])
            .eq("status", "pending")
    };

    // INSERT ... SELECT reading a WITH relation.
    let mut insert = Qail::add("gap_orders")
        .columns(["id", "status"])
        .with("chosen", chosen());
    insert.source_query = Some(Box::new(
        Qail::get("chosen")
            .column("id")
            .select_expr(Expr::Literal(Value::String("copied".into()))),
    ));
    let inserted = driver.execute(&insert).await?;
    println!("E3 INSERT ... SELECT via WITH: {inserted} rows");
    assert_eq!(inserted, 2);

    // UPDATE ... FROM a WITH relation: WITH binds $1, SET binds $2.
    let update = Qail::set("gap_orders")
        .set_value("status", "picked")
        .update_from(["chosen"])
        .filter_cond(Condition {
            left: Expr::Named("gap_orders.id".into()),
            op: Operator::Eq,
            value: Value::Column("chosen.id".into()),
            is_array_unnest: false,
        })
        .with("chosen", chosen());
    let updated = driver.execute(&update).await?;
    let picked = scalar(
        &mut driver,
        "SELECT string_agg(id::text, ',' ORDER BY id) FROM gap_orders WHERE status = 'picked'",
    )
    .await?;
    println!("E3 UPDATE ... FROM via WITH: {updated} rows; picked ids = {picked}");
    assert_eq!(picked, "1,3");

    // Data-modifying CTE read by the top-level SELECT.
    let deleted = Qail::get("gone").with(
        "gone",
        Qail::del("gap_orders")
            .eq("status", "void")
            .returning(["id"]),
    );
    let rows = driver.fetch_all(&deleted).await?;
    let mut ids: Vec<i64> = rows.iter().filter_map(|row| row.get_i64(0)).collect();
    ids.sort_unstable();
    let left = scalar(
        &mut driver,
        "SELECT count(*) FROM gap_orders WHERE status = 'void'",
    )
    .await?;
    println!(
        "E3 WITH gone AS (DELETE ... RETURNING id) SELECT: returned {ids:?}; void rows left = {left}"
    );
    assert_eq!(ids, [10, 12]);
    assert_eq!(left, "0");

    // Data-modifying CTE feeding a top-level INSERT.
    let mut moved = Qail::add("gap_log").columns(["id"]).with(
        "moved",
        Qail::set("gap_orders")
            .set_value("status", "archived")
            .eq("status", "open")
            .returning(["id"]),
    );
    moved.source_query = Some(Box::new(Qail::get("moved").columns(["id"])));
    let logged = driver.execute(&moved).await?;
    let log = scalar(&mut driver, "SELECT string_agg(id::text, ',') FROM gap_log").await?;
    println!(
        "E3 WITH moved AS (UPDATE ... RETURNING id) INSERT ... SELECT: {logged} rows; log = {log}"
    );
    assert_eq!(log, "11");

    // The server enforces the same nesting rule the encoder applies.
    let server = driver
        .simple_query(
            "SELECT * FROM gap_items WHERE id IN \
             (WITH d AS (DELETE FROM gap_log RETURNING id) SELECT id FROM d)",
        )
        .await
        .map(|rows| rows.len());
    println!("server, write CTE in a subquery: {server:?}");
    assert!(server.is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a live PostgreSQL at QAIL_TEST_DB_URL"]
async fn conflict_constraint_and_action_parity_run_against_the_server() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE gap_slots (id bigint, code text, status text, \
             CONSTRAINT gap_slots_code_key UNIQUE (code)); \
             INSERT INTO gap_slots VALUES (1, 'A', 'open')",
        )
        .await?;

    // B6: ON CONFLICT ON CONSTRAINT.
    let upsert = Qail::add("gap_slots")
        .set_value("id", 2)
        .set_value("code", "A")
        .set_value("status", "held")
        .on_conflict_constraint_update(
            "gap_slots_code_key",
            &[("status", Expr::Named("excluded.status".into()))],
        );
    let affected = driver.execute(&upsert).await?;
    let state = scalar(
        &mut driver,
        "SELECT id || ':' || status FROM gap_slots WHERE code = 'A'",
    )
    .await?;
    println!("B6 ON CONSTRAINT DO UPDATE: {affected} row; row = {state}");
    assert_eq!(state, "1:held");

    let skip = Qail::add("gap_slots")
        .set_value("id", 3)
        .set_value("code", "A")
        .set_value("status", "dup")
        .on_conflict_constraint_nothing("gap_slots_code_key");
    let affected = driver.execute(&skip).await?;
    println!("B6 ON CONSTRAINT DO NOTHING: {affected} rows");
    assert_eq!(affected, 0);

    // A7: EXPLAIN / EXPLAIN ANALYZE with a bound parameter.
    let plan = driver
        .fetch_all(&Qail::explain("gap_slots").eq("code", "A"))
        .await?;
    let first = plan
        .first()
        .and_then(|row| row.get_string(0))
        .unwrap_or_default();
    println!("A7 EXPLAIN: {} plan rows; first = {first}", plan.len());
    assert!(first.contains("gap_slots"), "{first}");
    let analyzed = driver
        .fetch_all(&Qail::explain_analyze("gap_slots").eq("code", "A"))
        .await?;
    let text: Vec<String> = analyzed
        .iter()
        .filter_map(|row| row.get_string(0))
        .collect();
    println!("A7 EXPLAIN ANALYZE: {}", text.join(" | "));
    assert!(text.iter().any(|line| line.contains("actual")), "{text:?}");

    // A7: LOCK needs a transaction block; inside one it succeeds.
    let outside = driver.execute(&Qail::lock("gap_slots")).await;
    println!("A7 LOCK outside a transaction: {outside:?}");
    assert!(outside.is_err());
    driver.begin().await?;
    driver.execute(&Qail::lock("gap_slots")).await?;
    let mode = scalar(
        &mut driver,
        "SELECT mode FROM pg_locks WHERE relation = 'gap_slots'::regclass \
         AND pid = pg_backend_pid() AND granted",
    )
    .await?;
    driver.commit().await?;
    println!("A7 LOCK inside a transaction: granted {mode}");
    assert_eq!(mode, "AccessExclusiveLock");

    // A7: TRUNCATE.
    driver.execute(&Qail::truncate("gap_slots")).await?;
    let count = scalar(&mut driver, "SELECT count(*) FROM gap_slots").await?;
    println!("A7 TRUNCATE: rows left = {count}");
    assert_eq!(count, "0");
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a live PostgreSQL at QAIL_TEST_DB_URL"]
async fn unicode_identifiers_and_update_targets_run_against_the_server() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE café (naïve text, größe bigint); \
             CREATE TEMP TABLE gap_addr (city text, zip text); \
             CREATE TEMP TABLE gap_people (id bigint, names text[], slot int, \
             address gap_addr, stops gap_addr[]); \
             INSERT INTO gap_people VALUES (1, ARRAY['a','b','c'], 2, \
             ROW('Kuta','80361')::gap_addr, ARRAY[ROW('X','1')::gap_addr, ROW('Y','2')::gap_addr])",
        )
        .await?;

    // D9: native INSERT and SELECT over Unicode identifiers.
    driver
        .execute(
            &Qail::add("café")
                .set_value("naïve", "oui")
                .set_value("größe", 7),
        )
        .await?;
    let rows = driver
        .fetch_all(&Qail::get("café").columns(["naïve"]).eq("größe", 7))
        .await?;
    let got = rows
        .first()
        .and_then(|row| row.get_string(0))
        .unwrap_or_default();
    println!("D9 SELECT naïve FROM café WHERE größe = $1: {got}");
    assert_eq!(got, "oui");

    // D15: array element, composite field, and element field targets.
    let element = Expr::Subscript {
        expr: Box::new(Expr::Named("names".into())),
        index: Box::new(Expr::Literal(Value::Int(2))),
        alias: None,
    };
    let field = Expr::FieldAccess {
        expr: Box::new(Expr::Named("address".into())),
        field: "city".into(),
        alias: None,
    };
    let element_field = Expr::FieldAccess {
        expr: Box::new(Expr::Subscript {
            expr: Box::new(Expr::Named("stops".into())),
            index: Box::new(Expr::Named("slot".into())),
            alias: None,
        }),
        field: "city".into(),
        alias: None,
    };
    let update = Qail::set("gap_people")
        .set_target(element, "B")
        .set_target(field, "Ubud")
        .set_target(element_field, "Gili")
        .eq("id", 1);
    let affected = driver.execute(&update).await?;
    let state = scalar(
        &mut driver,
        "SELECT names::text || ' ' || (address).city || ' ' || stops[2].city \
         || ' ' || (address).zip FROM gap_people WHERE id = 1",
    )
    .await?;
    println!("D15 UPDATE names[2], address.city, stops[slot].city: {affected} row; {state}");
    assert_eq!(state, "{a,B,c} Ubud Gili 80361");
    Ok(())
}
