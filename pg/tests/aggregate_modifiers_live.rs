//! Live PostgreSQL proof that aggregate DISTINCT and FILTER written in the
//! text DSL change the rows the native driver returns, including FILTER on a
//! window aggregate. Uses one connection and TEMP tables only.
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test aggregate_modifiers_live -- --ignored --nocapture

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
async fn dsl_aggregate_modifiers_change_live_results() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = format!("qail_agg_probe_{}", Uuid::new_v4().simple());
    driver
        .execute_simple(&format!(
            "CREATE TEMP TABLE {table} (id integer, region text, status text, \
             amount integer, active boolean, payload jsonb); \
             INSERT INTO {table} VALUES \
             (1, 'bali', 'paid', 10, true, '{{\"n\": 1}}'), \
             (2, 'bali', 'paid', 20, false, '{{\"n\": 2}}'), \
             (3, 'bali', 'open', 30, true, '{{\"n\": 3}}'), \
             (4, 'lombok', 'paid', 45, true, '{{\"n\": 4}}'), \
             (5, 'lombok', 'open', 50, false, '{{\"n\": 5}}')"
        ))
        .await?;

    // A1: DISTINCT inside a generic aggregate.
    let rows = run(
        &mut driver,
        &format!("get {table} fields array_agg(status) as statuses"),
    )
    .await?;
    let all = rows[0].get_text_array(0).expect("text[]");
    let rows = run(
        &mut driver,
        &format!("get {table} fields array_agg(distinct status) as statuses"),
    )
    .await?;
    let mut distinct = rows[0].get_text_array(0).expect("text[]");
    distinct.sort();
    println!("array_agg(status) = {all:?}; array_agg(distinct status) = {distinct:?}");
    assert_eq!(all.len(), 5);
    assert_eq!(distinct, vec!["open".to_string(), "paid".to_string()]);

    // A1: FILTER on a generic aggregate, with a bound value.
    let rows = run(
        &mut driver,
        &format!("get {table} fields jsonb_agg(payload) filter (where active = true) as kept"),
    )
    .await?;
    let kept = rows[0].get_string(0).expect("jsonb");
    let rows = run(
        &mut driver,
        &format!(
            "get {table} fields array_agg(distinct region) filter (where status = 'open') as open_regions"
        ),
    )
    .await?;
    let mut open_regions = rows[0].get_text_array(0).expect("text[]");
    open_regions.sort();
    println!("jsonb_agg(payload) filter (active) = {kept}; open regions = {open_regions:?}");
    assert_eq!(kept.matches("\"n\"").count(), 3, "{kept}");
    for excluded in ["\"n\": 2", "\"n\": 5"] {
        assert!(!kept.contains(excluded), "inactive row kept: {kept}");
    }
    assert_eq!(open_regions, vec!["bali".to_string(), "lombok".to_string()]);

    // A2: FILTER on a window aggregate, whole-table and partitioned.
    let rows = run(
        &mut driver,
        &format!(
            "get {table} fields id, \
             sum(amount) filter (where active = true) over () as active_total, \
             sum(amount) over () as total, \
             sum(amount) filter (where status = 'paid') over (partition by region) as region_paid \
             order by id"
        ),
    )
    .await?;
    let observed: Vec<(i64, i64, i64, i64)> = rows
        .iter()
        .map(|row| {
            (
                row.get_i64(0).expect("id"),
                row.get_i64(1).expect("active_total"),
                row.get_i64(2).expect("total"),
                row.get_i64(3).expect("region_paid"),
            )
        })
        .collect();
    println!("(id, active_total, total, region_paid) = {observed:?}");
    assert_eq!(
        observed,
        vec![
            (1, 85, 155, 30),
            (2, 85, 155, 30),
            (3, 85, 155, 30),
            (4, 85, 155, 45),
            (5, 85, 155, 45),
        ]
    );

    driver
        .execute_simple(&format!("DROP TABLE {table}"))
        .await?;
    Ok(())
}
