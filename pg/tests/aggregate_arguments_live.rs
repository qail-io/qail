//! Live PostgreSQL proof that expression arguments, STRING_AGG delimiters,
//! aggregate-local ORDER BY and WITHIN GROUP written in the text DSL reach
//! the server through the native driver. Uses one connection and TEMP tables.
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test aggregate_arguments_live -- --ignored --nocapture

use qail_pg::protocol::AstEncoder;
use qail_pg::{PgDriver, PgError, PgResult, PgRow};
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

async fn run(driver: &mut PgDriver, query: &str) -> PgResult<Vec<PgRow>> {
    let cmd = qail_core::parse(query).map_err(|err| PgError::Encode(err.to_string()))?;
    let (sql, params) =
        AstEncoder::encode_cmd_sql(&cmd).map_err(|err| PgError::Encode(err.to_string()))?;
    let shown: Vec<String> = params
        .iter()
        .map(|p| match p {
            Some(bytes) => String::from_utf8_lossy(bytes).into_owned(),
            None => "NULL".to_string(),
        })
        .collect();
    println!("{sql}  params={shown:?}");
    driver.fetch_all(&cmd).await
}

#[tokio::test]
#[ignore = "Requires a live PostgreSQL at QAIL_TEST_DB_URL"]
async fn dsl_aggregate_arguments_and_order_change_live_results() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = format!("qail_agg_args_{}", Uuid::new_v4().simple());
    driver
        .execute_simple(&format!(
            "CREATE TEMP TABLE {table} (id integer, region text, status text, \
             price integer, quantity integer, active boolean); \
             INSERT INTO {table} VALUES \
             (1, 'bali', 'paid', 10, 2, true), \
             (2, 'bali', 'open', 20, 1, false), \
             (3, 'bali', 'paid', 30, 3, true), \
             (4, 'lombok', 'void', 45, 1, true), \
             (5, 'lombok', 'open', 50, 2, false)"
        ))
        .await?;

    // A3: SUM over an expression, not a column named "(price * quantity)".
    let rows = run(
        &mut driver,
        &format!("get {table} fields sum(price * quantity) as revenue"),
    )
    .await?;
    let revenue = rows[0].get_i64(0).expect("revenue");
    println!("sum(price * quantity) = {revenue}");
    assert_eq!(revenue, 10 * 2 + 20 + 30 * 3 + 45 + 50 * 2);

    // A3 + A1: DISTINCT over an expression argument.
    let rows = run(
        &mut driver,
        &format!("get {table} fields count(distinct upper(region)) as regions"),
    )
    .await?;
    let regions = rows[0].get_i64(0).expect("regions");
    println!("count(distinct upper(region)) = {regions}");
    assert_eq!(regions, 2);

    // A6 + B4: STRING_AGG keeps its delimiter and its local ORDER BY.
    let rows = run(
        &mut driver,
        &format!("get {table} fields string_agg(status, '|' order by id desc) as trail"),
    )
    .await?;
    let trail = rows[0].get_string(0).expect("trail");
    println!("string_agg(status, '|' order by id desc) = {trail}");
    assert_eq!(trail, "open|void|paid|open|paid");

    // B4: ARRAY_AGG ordering differs from the scan order; FILTER binds a value.
    let rows = run(
        &mut driver,
        &format!(
            "get {table} fields array_agg(id order by price desc) \
             filter (where active = true) as ids"
        ),
    )
    .await?;
    let ids = rows[0].get_text_array(0).expect("int[] as text");
    println!("array_agg(id order by price desc) filter (active) = {ids:?}");
    assert_eq!(ids, vec!["4", "3", "1"]);

    // B4: DISTINCT with ORDER BY on the argument.
    let rows = run(
        &mut driver,
        &format!("get {table} fields array_agg(distinct status order by status desc) as s"),
    )
    .await?;
    let statuses = rows[0].get_text_array(0).expect("text[]");
    println!("array_agg(distinct status order by status desc) = {statuses:?}");
    assert_eq!(statuses, vec!["void", "paid", "open"]);

    // B4: ordered-set aggregates, grouped.
    let rows = run(
        &mut driver,
        &format!(
            "get {table} fields region, \
             percentile_cont(0.5) within group (order by price) as median, \
             percentile_disc(0.5) within group (order by price) as disc, \
             mode() within group (order by status) as common \
             order by region"
        ),
    )
    .await?;
    let observed: Vec<(String, f64, i64, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get_string(0).expect("region"),
                row.get_f64(1).expect("median"),
                row.get_i64(2).expect("disc"),
                row.get_string(3).expect("common"),
            )
        })
        .collect();
    println!("(region, median, disc, mode) = {observed:?}");
    assert_eq!(
        observed,
        vec![
            ("bali".to_string(), 20.0, 20, "paid".to_string()),
            ("lombok".to_string(), 47.5, 45, "open".to_string()),
        ]
    );

    driver
        .execute_simple(&format!("DROP TABLE {table}"))
        .await?;
    Ok(())
}
