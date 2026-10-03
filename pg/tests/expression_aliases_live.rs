//! Live alias regression using synthetic TEMP rows only.

use qail_core::ast::{AggregateFunc, Condition, Expr, GroupByMode, Operator, Qail, Value};
use qail_core::transpiler::ToSql;
use qail_pg::{PgDriver, PgResult, PgRow};

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

fn grouped_rows(rows: &[PgRow]) -> Vec<(Option<String>, i64)> {
    let mut values = rows
        .iter()
        .map(|row| (row.get_string(0), row.get_i64(1).expect("count decodes")))
        .collect::<Vec<_>>();
    values.sort();
    values
}

#[tokio::test]
#[ignore = "Requires local PostgreSQL at QAIL_TEST_DB_URL"]
async fn grouped_aliases_execute_in_preview_cached_and_uncached_paths() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE qail_alias_probe (name text NOT NULL, active boolean NOT NULL); \
         INSERT INTO qail_alias_probe VALUES ('A', true), ('a', true), ('B', true), ('A', false)",
        )
        .await?;
    let key = Expr::FunctionCall {
        name: "lower".into(),
        args: vec![Expr::Aliased {
            name: "name".into(),
            alias: "input_name".into(),
        }],
        alias: Some("folded_name".into()),
    };
    let count = Expr::Aggregate {
        col: "*".into(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: None,
        alias: Some("row_count".into()),
    };
    for mode in [GroupByMode::Simple, GroupByMode::Rollup, GroupByMode::Cube] {
        let mut expected = vec![(Some("a".to_string()), 2)];
        if mode != GroupByMode::Simple {
            expected.insert(0, (None, 3));
        }
        let mut cmd = Qail::get("qail_alias_probe")
            .column_expr(key.clone())
            .column_expr(count.clone())
            .group_by_expr([key.clone()])
            .eq("active", true)
            .having_cond(Condition {
                left: count.clone(),
                op: Operator::Gt,
                value: Value::Int(1),
                is_array_unnest: false,
            });
        cmd.group_by_mode = mode;
        let preview = cmd.to_sql();
        let preview_rows = grouped_rows(&driver.simple_query(&preview).await?);
        let uncached = grouped_rows(&driver.fetch_all_uncached(&cmd).await?);
        let cached = grouped_rows(&driver.fetch_all(&cmd).await?);
        let cache_hit = grouped_rows(&driver.fetch_all(&cmd).await?);
        println!(
            "{:?}: preview={preview}; rows={uncached:?}; preview rows={preview_rows:?}",
            cmd.group_by_mode
        );
        assert_eq!(uncached, expected);
        assert_eq!(cached, expected);
        assert_eq!(cache_hit, expected);
        assert_eq!(preview_rows, expected);

        let mut wrapped = Qail::get("qail_alias_probe")
            .column_expr(key.clone())
            .column_expr(count.clone())
            .group_by_expr([Expr::Literal(Value::Expr(Box::new(key.clone())))])
            .eq("active", true)
            .having_cond(Condition {
                left: Expr::Literal(Value::Expr(Box::new(count.clone()))),
                op: Operator::Gt,
                value: Value::Int(1),
                is_array_unnest: false,
            });
        wrapped.group_by_mode = cmd.group_by_mode.clone();
        let wrapped_preview = grouped_rows(&driver.simple_query(&wrapped.to_sql()).await?);
        let wrapped_uncached = grouped_rows(&driver.fetch_all_uncached(&wrapped).await?);
        let wrapped_cached = grouped_rows(&driver.fetch_all(&wrapped).await?);
        let wrapped_cache_hit = grouped_rows(&driver.fetch_all(&wrapped).await?);
        println!(
            "wrapped {:?}: rows={wrapped_uncached:?}",
            wrapped.group_by_mode
        );
        assert_eq!(wrapped_preview, expected);
        assert_eq!(wrapped_uncached, expected);
        assert_eq!(wrapped_cached, expected);
        assert_eq!(wrapped_cache_hit, expected);
    }
    Ok(())
}
