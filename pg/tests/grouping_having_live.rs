//! Live PostgreSQL checks that native SELECT encoding applies HAVING and
//! grouping modes, using TEMP tables only.
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test grouping_having_live -- --ignored --nocapture

use qail_core::ast::{AggregateFunc, Condition, Expr, GroupByMode, Operator, Qail, Value};
use qail_pg::protocol::AstEncoder;
use qail_pg::{PgDriver, PgResult, PgRow};

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

const TABLE: &str = "qail_grouping_probe";

async fn seeded_driver() -> PgResult<PgDriver> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    // sales: 3 rows, ops: 2 rows, hr: 1 row.
    driver
        .execute_simple(&format!(
            "CREATE TEMP TABLE {TABLE} (department text NOT NULL, status text NOT NULL); \
             INSERT INTO {TABLE} VALUES \
             ('sales', 'paid'), ('sales', 'paid'), ('sales', 'void'), \
             ('ops', 'paid'), ('ops', 'paid'), ('hr', 'paid')"
        ))
        .await?;
    Ok(driver)
}

fn count_star() -> Expr {
    Expr::Aggregate {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: None,
        alias: None,
    }
}

fn cond(left: Expr, op: Operator, value: Value) -> Condition {
    Condition {
        left,
        op,
        value,
        is_array_unnest: false,
    }
}

fn department_counts(rows: &[PgRow]) -> Vec<(Option<String>, i64)> {
    let mut out: Vec<(Option<String>, i64)> = rows
        .iter()
        .map(|row| {
            let department = if row.is_null(0) {
                None
            } else {
                row.get_string(0)
            };
            let count = row.get_i64(1).expect("count decodes");
            (department, count)
        })
        .collect();
    out.sort();
    out
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL at QAIL_TEST_DB_URL"]
async fn having_filters_groups_on_the_server() -> PgResult<()> {
    let mut driver = seeded_driver().await?;

    let cmd = Qail::get(TABLE)
        .columns(["department"])
        .column_expr(count_star())
        .filter("status", Operator::Eq, "paid")
        .having_cond(cond(count_star(), Operator::Gte, Value::Int(2)))
        .having_cond(cond(
            Expr::Named("department".to_string()),
            Operator::Ne,
            Value::String("ops".to_string()),
        ));

    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd)?;
    println!("HAVING sql: {sql}");
    println!(
        "HAVING params: {:?}",
        params
            .iter()
            .map(|p| p.as_deref().map(String::from_utf8_lossy))
            .collect::<Vec<_>>()
    );

    let control = Qail::get(TABLE)
        .columns(["department"])
        .column_expr(count_star())
        .filter("status", Operator::Eq, "paid");
    let control_rows = department_counts(&driver.fetch_all_uncached(&control).await?);
    println!("without HAVING: {control_rows:?}");
    assert_eq!(
        control_rows,
        vec![
            (Some("hr".to_string()), 1),
            (Some("ops".to_string()), 2),
            (Some("sales".to_string()), 2),
        ]
    );

    let uncached = department_counts(&driver.fetch_all_uncached(&cmd).await?);
    let cached = department_counts(&driver.fetch_all(&cmd).await?);
    println!("with HAVING (uncached): {uncached:?}");
    println!("with HAVING (cached): {cached:?}");
    assert_eq!(uncached, vec![(Some("sales".to_string()), 2)]);
    assert_eq!(cached, uncached);

    Ok(())
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL at QAIL_TEST_DB_URL"]
async fn rollup_on_explicit_keys_returns_the_subtotal_row() -> PgResult<()> {
    let mut driver = seeded_driver().await?;

    let mut cmd = Qail::get(TABLE)
        .columns(["department"])
        .column_expr(count_star())
        .group_by_expr([Expr::Named("department".to_string())]);
    cmd.group_by_mode = GroupByMode::Rollup;

    let (sql, _) = AstEncoder::encode_cmd_sql(&cmd)?;
    println!("ROLLUP sql: {sql}");

    let rows = department_counts(&driver.fetch_all_uncached(&cmd).await?);
    println!("ROLLUP rows: {rows:?}");
    assert_eq!(
        rows,
        vec![
            (None, 6),
            (Some("hr".to_string()), 1),
            (Some("ops".to_string()), 2),
            (Some("sales".to_string()), 3),
        ]
    );

    Ok(())
}
