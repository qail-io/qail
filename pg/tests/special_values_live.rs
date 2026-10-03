//! Live PostgreSQL checks that special values decode the same from the text
//! and binary result formats, under the session settings that change text
//! output (`DateStyle`, `TimeZone`, `bytea_output`).
//!
//! Only SELECT literals and ON COMMIT DROP temp tables; settings are SET LOCAL.
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --features chrono --test special_values_live -- --ignored --nocapture

use qail_core::ast::builders::{cast, text};
use qail_core::ast::{Operator, Qail, Value};
use qail_core::transpiler::ToSql;
use qail_pg::protocol::AstEncoder;
use qail_pg::{
    ArrayDimension, Date, FromPg, Numeric, PgArray, PgConnection, PgPool, PgResult, PgRow,
    PoolConfig, Time, Timestamp, TypeError,
};

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

async fn pool() -> PgResult<PgPool> {
    PgPool::connect(
        PoolConfig::from_url(&database_url())?
            .min_connections(0)
            .max_connections(1),
    )
    .await
}

/// The same single-row SELECT fetched in text (0) and binary (1) result format.
async fn text_and_binary(conn: &mut PgConnection, sql: &str) -> PgResult<(PgRow, PgRow)> {
    let mut text = conn.query_rows_with_result_format(sql, &[], 0).await?;
    let mut binary = conn.query_rows_with_result_format(sql, &[], 1).await?;
    assert_eq!(text.len(), 1, "{sql}");
    assert_eq!(binary.len(), 1, "{sql}");
    Ok((text.remove(0), binary.remove(0)))
}

fn raw_text(row: &PgRow, idx: usize) -> String {
    String::from_utf8_lossy(row.get_bytes(idx).unwrap_or_default()).into_owned()
}

/// Text decode must equal binary decode, or fail; it must never differ.
fn assert_text_matches_binary<T: FromPg + PartialEq + std::fmt::Debug>(
    label: &str,
    text: &PgRow,
    binary: &PgRow,
    idx: usize,
) -> Result<T, TypeError> {
    let from_binary = binary
        .try_get::<T>(idx)
        .unwrap_or_else(|err| panic!("{label}: binary decode failed: {err}"));
    let from_text = text.try_get::<T>(idx);
    println!(
        "{label}: text {:?} -> {:?}; binary -> {from_binary:?}",
        raw_text(text, idx),
        from_text
    );
    match from_text {
        Ok(value) => {
            assert_eq!(value, from_binary, "{label}: text and binary disagree");
            Ok(value)
        }
        Err(err) => Err(err),
    }
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL via QAIL_TEST_DB_URL"]
async fn bytea_escape_output_matches_binary() -> PgResult<()> {
    let pool = pool().await?;
    let mut conn = pool.acquire_system().await?;
    let pg = conn.get_mut()?;
    pg.execute_simple("SET LOCAL bytea_output = 'escape'")
        .await?;
    let (text, binary) = text_and_binary(
        pg,
        r"SELECT '\x015c41ff'::bytea, ''::bytea, 'plain'::bytea, '\x5c783030'::bytea",
    )
    .await?;
    for idx in 0..4 {
        assert_text_matches_binary::<Vec<u8>>(&format!("bytea col {idx}"), &text, &binary, idx)
            .unwrap_or_else(|err| panic!("bytea col {idx}: {err}"));
    }
    conn.release_checked().await
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL via QAIL_TEST_DB_URL"]
async fn numeric_and_float_special_values_match_binary() -> PgResult<()> {
    let pool = pool().await?;
    let mut conn = pool.acquire_system().await?;
    let pg = conn.get_mut()?;
    let (text, binary) = text_and_binary(
        pg,
        "SELECT 'Infinity'::numeric, '-Infinity'::numeric, 'NaN'::numeric, 42.50::numeric",
    )
    .await?;
    for idx in 0..4 {
        assert_text_matches_binary::<Numeric>(&format!("numeric col {idx}"), &text, &binary, idx)
            .unwrap_or_else(|err| panic!("numeric col {idx}: {err}"));
    }

    let (text, binary) = text_and_binary(
        pg,
        "SELECT 'Infinity'::float8, '-Infinity'::float8, 'NaN'::float8",
    )
    .await?;
    for idx in 0..3 {
        let t = text.try_get::<f64>(idx).expect("text float");
        let b = binary.try_get::<f64>(idx).expect("binary float");
        println!("float8 col {idx}: text {t} binary {b}");
        assert!(t == b || (t.is_nan() && b.is_nan()));
    }
    conn.release_checked().await
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL via QAIL_TEST_DB_URL"]
async fn time_end_of_day_matches_binary() -> PgResult<()> {
    let pool = pool().await?;
    let mut conn = pool.acquire_system().await?;
    let pg = conn.get_mut()?;
    let (text, binary) =
        text_and_binary(pg, "SELECT '24:00'::time, '23:59:59.999999'::time").await?;
    for idx in 0..2 {
        assert_text_matches_binary::<Time>(&format!("time col {idx}"), &text, &binary, idx)
            .unwrap_or_else(|err| panic!("time col {idx}: {err}"));
    }
    conn.release_checked().await
}

const DATE_EXPRS: &[&str] = &[
    "'2026-09-30'::date",
    "'0044-03-15 BC'::date",
    "'infinity'::date",
    "'-infinity'::date",
];

const TIMESTAMP_EXPRS: &[&str] = &[
    "'2026-09-30 14:05:06.5'::timestamp",
    "'0044-03-15 12:00 BC'::timestamp",
    "'infinity'::timestamp",
    "'2026-09-30 14:05:06.5+02'::timestamptz",
    "'1900-01-01 00:00:00+00'::timestamptz",
    "'0044-03-15 12:00:00+00 BC'::timestamptz",
    "'-infinity'::timestamptz",
];

const STYLES: &[&str] = &[
    "ISO, MDY",
    "German",
    "SQL, MDY",
    "SQL, DMY",
    "Postgres, MDY",
    "Postgres, DMY",
];

#[tokio::test]
#[ignore = "Requires local PostgreSQL via QAIL_TEST_DB_URL"]
async fn temporal_text_matches_binary_under_every_datestyle() -> PgResult<()> {
    let pool = pool().await?;
    let mut conn = pool.acquire_system().await?;
    let pg = conn.get_mut()?;
    let date_sql = format!("SELECT {}", DATE_EXPRS.join(", "));
    let ts_sql = format!("SELECT {}", TIMESTAMP_EXPRS.join(", "));
    let mut decoded = 0usize;
    let mut refused = 0usize;

    for zone in ["UTC", "Asia/Kolkata"] {
        for style in STYLES {
            pg.execute_simple(&format!(
                "SET LOCAL TimeZone = '{zone}'; SET LOCAL DateStyle = '{style}'"
            ))
            .await?;
            let iso = style.starts_with("ISO");
            let (text, binary) = text_and_binary(pg, &date_sql).await?;
            for (idx, expr) in DATE_EXPRS.iter().enumerate() {
                let label = format!("[{zone} / {style}] {expr}");
                match assert_text_matches_binary::<Date>(&label, &text, &binary, idx) {
                    Ok(_) => decoded += 1,
                    Err(err) => {
                        assert!(!iso && !style.starts_with("German"), "{label}: {err}");
                        assert!(err.to_string().contains("DateStyle"), "{label}: {err}");
                        refused += 1;
                    }
                }
            }
            let (text, binary) = text_and_binary(pg, &ts_sql).await?;
            for (idx, expr) in TIMESTAMP_EXPRS.iter().enumerate() {
                let label = format!("[{zone} / {style}] {expr}");
                match assert_text_matches_binary::<Timestamp>(&label, &text, &binary, idx) {
                    Ok(_) => decoded += 1,
                    Err(err) => {
                        assert!(!iso, "{label}: {err}");
                        let msg = err.to_string();
                        assert!(
                            msg.contains("DateStyle") || msg.contains("zone"),
                            "{label}: {msg}"
                        );
                        refused += 1;
                    }
                }
            }
        }
    }
    println!("temporal cells decoded identically: {decoded}; refused with an error: {refused}");
    conn.release_checked().await
}

#[cfg(feature = "chrono")]
#[tokio::test]
#[ignore = "Requires local PostgreSQL via QAIL_TEST_DB_URL"]
async fn chrono_text_matches_binary_and_refuses_infinity() -> PgResult<()> {
    use chrono::{DateTime, Utc};

    let pool = pool().await?;
    let mut conn = pool.acquire_system().await?;
    let pg = conn.get_mut()?;
    pg.execute_simple("SET LOCAL TimeZone = 'Asia/Kolkata'")
        .await?;
    let (text, binary) = text_and_binary(
        pg,
        "SELECT '2026-09-30 14:05:06.5+02'::timestamptz, \
         '1900-01-01 00:00:00+00'::timestamptz, \
         '0044-03-15 12:00:00+00 BC'::timestamptz, \
         'infinity'::timestamptz, '-infinity'::timestamp",
    )
    .await?;
    for idx in 0..3 {
        assert_text_matches_binary::<DateTime<Utc>>(
            &format!("chrono col {idx}"),
            &text,
            &binary,
            idx,
        )
        .unwrap_or_else(|err| panic!("chrono col {idx}: {err}"));
    }
    for idx in 3..5 {
        let text_err = text.try_get::<DateTime<Utc>>(idx).unwrap_err();
        let binary_err = binary.try_get::<DateTime<Utc>>(idx).unwrap_err();
        println!("chrono infinity col {idx}: text {text_err}; binary {binary_err}");
        assert!(text_err.to_string().contains("infinity"), "{text_err}");
        assert!(binary_err.to_string().contains("infinity"), "{binary_err}");
    }
    conn.release_checked().await
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL via QAIL_TEST_DB_URL"]
async fn vec_array_decoders_match_binary_and_refuse_shapes_they_cannot_hold() -> PgResult<()> {
    let pool = pool().await?;
    let mut conn = pool.acquire_system().await?;
    let pg = conn.get_mut()?;
    let (text, binary) = text_and_binary(
        pg,
        r#"SELECT '{1,2,-3}'::int4[], '{a,"b c","{x}"}'::text[]"#,
    )
    .await?;
    assert_text_matches_binary::<Vec<i64>>("int4[] as Vec<i64>", &text, &binary, 0)
        .expect("int4[] text");
    assert_text_matches_binary::<Vec<String>>("text[] as Vec<String>", &text, &binary, 1)
        .expect("text[] text");

    let (text, binary) = text_and_binary(
        pg,
        "SELECT '{{a,b},{c,d}}'::text[], '[0:1]={a,b}'::text[], '{a,NULL}'::text[]",
    )
    .await?;
    for idx in 0..3 {
        let t = text.try_get::<Vec<String>>(idx);
        let b = binary.try_get::<Vec<String>>(idx);
        println!(
            "Vec<String> col {idx}: text {:?} -> {t:?}; binary -> {b:?}",
            raw_text(&text, idx)
        );
        assert!(t.is_err(), "text col {idx} must refuse");
        assert!(b.is_err(), "binary col {idx} must refuse");
    }
    conn.release_checked().await
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL via QAIL_TEST_DB_URL"]
async fn pg_array_keeps_shape_nulls_and_bounds_in_both_formats() -> PgResult<()> {
    let pool = pool().await?;
    let mut conn = pool.acquire_system().await?;
    let pg = conn.get_mut()?;

    let (text, binary) = text_and_binary(
        pg,
        "SELECT '{{1,2,3},{4,NULL,6}}'::int4[], \
                '[0:1][-2:-1]={{a,b},{c,NULL}}'::text[], \
                '{}'::int8[], \
                ARRAY[[[1]],[[2]]]::int8[]",
    )
    .await?;
    let ints = assert_text_matches_binary::<PgArray<i64>>("int4[][]", &text, &binary, 0)
        .expect("int4[][] text");
    assert_eq!(
        ints.dimensions(),
        &[
            ArrayDimension {
                len: 2,
                lower_bound: 1
            },
            ArrayDimension {
                len: 3,
                lower_bound: 1
            }
        ]
    );
    assert_eq!(
        ints.elements(),
        &[Some(1), Some(2), Some(3), Some(4), None, Some(6)]
    );
    let texts = assert_text_matches_binary::<PgArray<String>>("text[][] bounds", &text, &binary, 1)
        .expect("text[][] text");
    assert_eq!(
        texts.dimensions(),
        &[
            ArrayDimension {
                len: 2,
                lower_bound: 0
            },
            ArrayDimension {
                len: 2,
                lower_bound: -2
            }
        ]
    );
    assert_eq!(texts.elements()[3], None);
    let empty = assert_text_matches_binary::<PgArray<i64>>("empty int8[]", &text, &binary, 2)
        .expect("empty text");
    assert_eq!(empty.ndim(), 0);
    let cube = assert_text_matches_binary::<PgArray<i64>>("int8[][][]", &text, &binary, 3)
        .expect("3-D text");
    assert_eq!(cube.ndim(), 3);

    let (text, binary) = text_and_binary(
        pg,
        "SELECT '{1.5,NaN,Infinity,-Infinity,NULL}'::numeric[], \
                '{2026-09-30,infinity,\"0044-03-15 BC\"}'::date[], \
                '{24:00,12:30:00.5}'::time[], \
                ARRAY['2026-09-30 14:05:06.5+02'::timestamptz, 'infinity', NULL]",
    )
    .await?;
    assert_text_matches_binary::<PgArray<Numeric>>("numeric[]", &text, &binary, 0)
        .expect("numeric[] text");
    assert_text_matches_binary::<PgArray<Date>>("date[]", &text, &binary, 1).expect("date[] text");
    assert_text_matches_binary::<PgArray<Time>>("time[]", &text, &binary, 2).expect("time[] text");
    assert_text_matches_binary::<PgArray<Timestamp>>("timestamptz[]", &text, &binary, 3)
        .expect("timestamptz[] text");

    for output in ["hex", "escape"] {
        pg.execute_simple(&format!("SET LOCAL bytea_output = '{output}'"))
            .await?;
        let (text, binary) = text_and_binary(
            pg,
            r"SELECT ARRAY['\x015c22'::bytea, NULL, '\x'::bytea, 'a,b{}'::bytea]",
        )
        .await?;
        assert_text_matches_binary::<PgArray<Vec<u8>>>(
            &format!("bytea[] ({output})"),
            &text,
            &binary,
            0,
        )
        .expect("bytea[] text");
    }
    conn.release_checked().await
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL via QAIL_TEST_DB_URL"]
async fn non_finite_float_preview_does_not_compare_against_a_column() -> PgResult<()> {
    let pool = pool().await?;
    let mut conn = pool.acquire_system().await?;
    let pg = conn.get_mut()?;
    pg.execute_simple(
        "CREATE TEMP TABLE qail_float_preview (x float8, inf float8, nan float8) ON COMMIT DROP; \
         INSERT INTO qail_float_preview VALUES (2, 2, 2), ('Infinity', 0, 0)",
    )
    .await?;

    // The documented spelling for a special value: a cast text literal.
    let special = Qail::get("qail_float_preview").filter(
        "x",
        Operator::Eq,
        Value::Expr(Box::new(cast(text("Infinity"), "float8").build())),
    );
    let (special_sql, special_params) = AstEncoder::encode_cmd_sql(&special)?;
    let native_rows = pg.query_rows(&special_sql, &special_params).await?;
    let preview_sql = special.to_sql();
    let preview_rows = pg.query_rows(&preview_sql, &[]).await?;
    println!(
        "cast literal: native {special_sql:?} -> {} row(s); preview {preview_sql:?} -> {} row(s)",
        native_rows.len(),
        preview_rows.len()
    );
    assert_eq!(native_rows.len(), 1);
    assert_eq!(preview_rows.len(), 1);
    let finite = Qail::get("qail_float_preview").filter("x", Operator::Eq, Value::Float(2.0));
    let (finite_sql, finite_params) = AstEncoder::encode_cmd_sql(&finite)?;
    let finite_rows = pg.query_rows(&finite_sql, &finite_params).await?;
    println!(
        "native finite {finite_sql:?} -> {} row(s)",
        finite_rows.len()
    );
    assert_eq!(finite_rows.len(), 1);

    let cmd =
        Qail::get("qail_float_preview").filter("x", Operator::Eq, Value::Float(f64::INFINITY));
    let native = AstEncoder::encode_cmd_sql(&cmd);
    println!(
        "native non-finite -> {:?}",
        native.as_ref().map(|(sql, _)| sql)
    );
    assert!(native.is_err(), "native encoder must reject +infinity");

    let sql = cmd.to_sql();
    let outcome = pg.query_rows_with_result_format(&sql, &[], 0).await;
    println!(
        "preview {sql:?} -> {:?}",
        outcome.as_ref().map(|rows| rows.len())
    );
    assert!(
        outcome.is_err(),
        "x = +infinity matched a row through column `inf`"
    );
    // The failed statement aborted the transaction; drop the checkout.
    drop(conn);
    Ok(())
}
