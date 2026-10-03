//! Live PostgreSQL checks for AST bytea parameters and JSON path operands.
//!
//! Every table is a TEMP table on the test session.
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test ast_value_live -- --ignored --nocapture

use qail_core::ast::{Operator, Qail, Value};
use qail_core::transpiler::ToSql;
use qail_pg::{PgDriver, PgResult};

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

fn hex(data: &[u8]) -> String {
    data.iter().map(|b| format!("{b:02x}")).collect()
}

const BYTEA_CASES: [&[u8]; 5] = [
    b"\\x4142",
    &[0, 255],
    b"a\0b\\c\\\\",
    &[0x5c, 0x00, 0x5c, 0x78],
    &[],
];

async fn assert_stored(driver: &mut PgDriver, row_id: usize, data: &[u8]) -> PgResult<()> {
    let rows = driver
        .simple_query(&format!(
            "SELECT octet_length(b), encode(b, 'hex') FROM qail_bytea_probe WHERE id = {row_id}"
        ))
        .await?;
    let len = rows[0].get_string(0).expect("octet_length");
    let stored = rows[0].get_string(1).unwrap_or_default();
    println!(
        "bytea id={row_id} sent={} octet_length={len} encode_hex={stored}",
        hex(data)
    );
    assert_eq!(len, data.len().to_string(), "row {row_id}");
    assert_eq!(stored, hex(data), "row {row_id}");
    Ok(())
}

#[tokio::test]
#[ignore = "Requires local Podman PostgreSQL qail-pg18-lab on 127.0.0.1:55432"]
async fn ast_bytea_values_store_exact_bytes() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE qail_bytea_probe (id integer PRIMARY KEY, b bytea NOT NULL)",
        )
        .await?;

    for (id, data) in BYTEA_CASES.iter().enumerate() {
        let insert = Qail::add("qail_bytea_probe")
            .set_value("id", id as i64)
            .set_value("b", Value::Bytes(data.to_vec()));
        assert_eq!(driver.execute(&insert).await?, 1);
        assert_stored(&mut driver, id, data).await?;
    }

    // Pipelined Binds take the same encoder path.
    let batch: Vec<Qail> = BYTEA_CASES
        .iter()
        .enumerate()
        .map(|(id, data)| {
            Qail::add("qail_bytea_probe")
                .set_value("id", 100 + id as i64)
                .set_value("b", Value::Bytes(data.to_vec()))
        })
        .collect();
    assert_eq!(
        driver.execute_batch(&batch).await?,
        vec![1; BYTEA_CASES.len()]
    );

    for (id, data) in BYTEA_CASES.iter().enumerate() {
        assert_stored(&mut driver, 100 + id, data).await?;

        // A bytea filter value matches the stored bytes through the cached path.
        let lookup = Qail::get("qail_bytea_probe")
            .column("id")
            .filter("b", Operator::Eq, Value::Bytes(data.to_vec()))
            .filter("id", Operator::Lt, Value::Int(100));
        let rows = driver.fetch_all(&lookup).await?;
        let ids: Vec<i32> = rows.iter().filter_map(|r| r.get_i32(0)).collect();
        println!("bytea filter sent={} matched ids={ids:?}", hex(data));
        assert_eq!(ids, vec![id as i32]);
    }
    Ok(())
}

#[tokio::test]
#[ignore = "Requires local Podman PostgreSQL qail-pg18-lab on 127.0.0.1:55432"]
async fn json_numeric_key_and_array_index_read_different_values() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE qail_json_probe (id integer PRIMARY KEY, doc jsonb NOT NULL);\
             INSERT INTO qail_json_probe VALUES \
             (1, '{\"0\":\"key\",\"123\":\"long key\",\"a\":[10,20]}'), \
             (2, '[\"first\",\"second\"]')",
        )
        .await?;

    let cmd = qail_core::parse(
        "get qail_json_probe fields doc->>'0', doc->>'123', doc->'a'->>0, doc->>0, doc->>-1, \
         doc->'0', doc->0 \
         order by id asc",
    )
    .expect("DSL must parse");

    let preview = cmd.to_sql();
    println!("preview: {preview}");
    let native = driver.fetch_all(&cmd).await?;
    let previewed = driver.simple_query(&preview).await?;

    for (label, rows) in [("native", &native), ("preview", &previewed)] {
        let values: Vec<Vec<Option<String>>> = rows
            .iter()
            .map(|r| (0..7).map(|i| r.get_string(i)).collect())
            .collect();
        println!("{label}: {values:?}");
        let s = |v: &str| Some(v.to_string());
        let object_row = vec![
            s("key"),
            s("long key"),
            s("10"),
            None,
            None,
            s("\"key\""),
            None,
        ];
        let array_row = vec![
            None,
            None,
            None,
            s("first"),
            s("second"),
            None,
            s("\"first\""),
        ];
        assert_eq!(values, vec![object_row, array_row], "{label}");
    }
    Ok(())
}
