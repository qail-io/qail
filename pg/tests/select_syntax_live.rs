//! Live PostgreSQL checks for SELECT syntax emission: qualified wildcards,
//! subscript grouping, set-operation ALL forms, CTE materialization and
//! SEARCH/CYCLE, window frame modes, array slices, DSL expression parity,
//! and row-lock options. Every query goes through the native encoder.
//!
//! All tests except `row_lock_options_across_sessions` use TEMP tables.
//! That one needs a database where it may create and drop a scratch table,
//! because a lock held by one session is only visible to another on a real
//! table.
//!
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test select_syntax_live -- --ignored --nocapture

use qail_core::ast::{
    Condition, CteCycle, CteSearch, CteSearchOrder, Expr, FrameBound, FrameExclusion, Operator,
    Qail, SetOp, SortOrder, Value, WindowFrame, values::IntervalUnit,
};
use qail_pg::protocol::AstEncoder;
use qail_pg::{PgDriver, PgResult, PgRow};
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

async fn connect() -> PgResult<PgDriver> {
    PgDriver::connect_url(&database_url()).await
}

async fn fetch(driver: &mut PgDriver, cmd: &Qail) -> PgResult<Vec<PgRow>> {
    let (sql, _) = AstEncoder::encode_cmd_sql(cmd).expect("native encode");
    println!("SQL: {sql}");
    driver.fetch_all_uncached(cmd).await
}

fn must_fail(result: PgResult<Vec<PgRow>>, why: &str) -> qail_pg::PgError {
    match result {
        Ok(rows) => panic!("{why}: got {} rows", rows.len()),
        Err(err) => err,
    }
}

fn text(row: &PgRow, idx: usize) -> String {
    row.get_string(idx).unwrap_or_else(|| "<null>".to_string())
}

fn cast_text(expr: Expr, alias: &str) -> Expr {
    Expr::Cast {
        expr: Box::new(expr),
        target_type: "text".to_string(),
        alias: Some(alias.to_string()),
    }
}

fn int(n: i64) -> Expr {
    Expr::Literal(Value::Int(n))
}

#[tokio::test]
#[ignore = "Requires PostgreSQL via QAIL_TEST_DB_URL"]
async fn qualified_wildcard_returns_the_relation_columns() -> PgResult<()> {
    let mut driver = connect().await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE qail_sx_people (id integer, name text);
             INSERT INTO qail_sx_people VALUES (1, 'ada');",
        )
        .await?;

    // The text the encoder used to send: a column literally named `*`.
    let before = must_fail(
        driver
            .simple_query("SELECT qail_sx_people.\"*\" FROM qail_sx_people")
            .await,
        "quoted star names a column that does not exist",
    );
    println!("before: {before}");
    assert_eq!(before.sqlstate(), Some("42703"));

    let rows = fetch(
        &mut driver,
        &Qail::get("qail_sx_people").columns(["qail_sx_people.*"]),
    )
    .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
    assert_eq!(rows[0].get_i32(0), Some(1));
    assert_eq!(text(&rows[0], 1), "ada");

    let rows = fetch(
        &mut driver,
        &Qail::get("qail_sx_people p").columns(["p.*", "p.id"]),
    )
    .await?;
    assert_eq!(rows[0].len(), 3);
    assert_eq!(rows[0].get_i32(2), Some(1));
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL via QAIL_TEST_DB_URL"]
async fn function_result_subscript_executes() -> PgResult<()> {
    let mut driver = connect().await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE qail_sx_one (id integer); INSERT INTO qail_sx_one VALUES (1);",
        )
        .await?;

    let before = must_fail(
        driver
            .simple_query("SELECT ARRAY_APPEND(ARRAY[1], 2)[2] FROM qail_sx_one")
            .await,
        "PostgreSQL does not subscript a bare function call",
    );
    println!("before: {before}");
    assert_eq!(before.sqlstate(), Some("42601"));

    let appended = Expr::FunctionCall {
        name: "array_append".to_string(),
        args: vec![
            Expr::ArrayConstructor {
                elements: vec![int(1)],
                alias: None,
            },
            int(2),
        ],
        alias: None,
    };
    let cmd = Qail::get("qail_sx_one").columns_expr([
        cast_text(
            Expr::Subscript {
                expr: Box::new(appended.clone()),
                index: Box::new(int(2)),
                alias: None,
            },
            "second",
        ),
        cast_text(
            Expr::ArraySlice {
                expr: Box::new(appended),
                lower: Some(Box::new(int(1))),
                upper: Some(Box::new(int(2))),
                alias: None,
            },
            "both",
        ),
    ]);
    let rows = fetch(&mut driver, &cmd).await?;
    assert_eq!(text(&rows[0], 0), "2");
    assert_eq!(text(&rows[0], 1), "{1,2}");
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL via QAIL_TEST_DB_URL"]
async fn array_slices_execute() -> PgResult<()> {
    let mut driver = connect().await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE qail_sx_arrays (arr integer[]);
             INSERT INTO qail_sx_arrays VALUES (ARRAY[10, 20, 30, 40]);",
        )
        .await?;

    let slice = |lower: Option<i64>, upper: Option<i64>, alias: &str| {
        cast_text(
            Expr::ArraySlice {
                expr: Box::new(Expr::Named("arr".to_string())),
                lower: lower.map(|n| Box::new(int(n))),
                upper: upper.map(|n| Box::new(int(n))),
                alias: None,
            },
            alias,
        )
    };
    let cmd = Qail::get("qail_sx_arrays").columns_expr([
        slice(Some(2), Some(3), "mid"),
        slice(None, Some(2), "head"),
        slice(Some(3), None, "tail"),
        slice(None, None, "all_of_it"),
    ]);
    let rows = fetch(&mut driver, &cmd).await?;
    assert_eq!(text(&rows[0], 0), "{20,30}");
    assert_eq!(text(&rows[0], 1), "{10,20}");
    assert_eq!(text(&rows[0], 2), "{30,40}");
    assert_eq!(text(&rows[0], 3), "{10,20,30,40}");

    let dsl = qail_core::parse("get qail_sx_arrays fields arr[2:3]::text as mid, arr[4] as last")
        .expect("DSL slice parses");
    let rows = fetch(&mut driver, &dsl).await?;
    assert_eq!(text(&rows[0], 0), "{20,30}");
    assert_eq!(rows[0].get_i32(1), Some(40));
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL via QAIL_TEST_DB_URL"]
async fn intersect_all_and_except_all_keep_duplicates() -> PgResult<()> {
    let mut driver = connect().await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE qail_sx_left (id integer);
             CREATE TEMP TABLE qail_sx_right (id integer);
             INSERT INTO qail_sx_left VALUES (1), (1), (1), (2);
             INSERT INTO qail_sx_right VALUES (1), (1), (3);",
        )
        .await?;

    let ids = |rows: &[PgRow]| {
        let mut ids: Vec<i32> = rows.iter().filter_map(|row| row.get_i32(0)).collect();
        ids.sort_unstable();
        ids
    };
    let combine = |op: SetOp| {
        let mut cmd = Qail::get("qail_sx_left").columns(["id"]);
        cmd.set_ops
            .push((op, Box::new(Qail::get("qail_sx_right").columns(["id"]))));
        cmd
    };

    let intersect_all = fetch(&mut driver, &combine(SetOp::IntersectAll)).await?;
    let intersect = fetch(&mut driver, &combine(SetOp::Intersect)).await?;
    let except_all = fetch(&mut driver, &combine(SetOp::ExceptAll)).await?;
    let except = fetch(&mut driver, &combine(SetOp::Except)).await?;
    println!(
        "INTERSECT ALL {:?} INTERSECT {:?} EXCEPT ALL {:?} EXCEPT {:?}",
        ids(&intersect_all),
        ids(&intersect),
        ids(&except_all),
        ids(&except)
    );
    assert_eq!(ids(&intersect_all), vec![1, 1]);
    assert_eq!(ids(&intersect), vec![1]);
    assert_eq!(ids(&except_all), vec![1, 2]);
    assert_eq!(ids(&except), vec![2]);
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL via QAIL_TEST_DB_URL"]
async fn cte_materialization_search_and_cycle() -> PgResult<()> {
    let mut driver = connect().await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE qail_sx_edges (src integer, dst integer);
             INSERT INTO qail_sx_edges VALUES (1, 2), (2, 3), (3, 1), (2, 4);",
        )
        .await?;

    for cmd in [
        Qail::get("picked")
            .columns(["src"])
            .with(
                "picked",
                Qail::get("qail_sx_edges").columns(["src"]).eq("src", 2),
            )
            .cte_materialized(),
        Qail::get("picked")
            .columns(["src"])
            .with(
                "picked",
                Qail::get("qail_sx_edges").columns(["src"]).eq("src", 2),
            )
            .cte_not_materialized(),
    ] {
        let rows = fetch(&mut driver, &cmd).await?;
        assert_eq!(rows.len(), 2);
    }

    // A 1 -> 2 -> 3 -> 1 loop: UNION ALL without CYCLE would never end.
    let base = Qail::get("qail_sx_edges")
        .columns(["src", "dst"])
        .eq("src", 1);
    let step = Qail::get("qail_sx_edges e")
        .columns(["e.src", "e.dst"])
        .inner_join_conds(
            "walk w",
            vec![Condition {
                left: Expr::Named("e.src".to_string()),
                op: Operator::Eq,
                value: Value::Column("w.dst".to_string()),
                is_array_unnest: false,
            }],
        );
    let cmd = Qail::get("walk")
        .columns(["src", "dst", "is_cycle", "ord"])
        .with("walk", base)
        .recursive(step)
        .cte_search(CteSearch {
            order: CteSearchOrder::BreadthFirst,
            by: vec!["dst".to_string()],
            set_column: "ord".to_string(),
        })
        .cte_cycle(CteCycle {
            columns: vec!["dst".to_string()],
            set_column: "is_cycle".to_string(),
            using_column: "path".to_string(),
        })
        .order_asc("ord");
    let rows = fetch(&mut driver, &cmd).await?;
    let walked: Vec<(i32, i32, bool)> = rows
        .iter()
        .map(|row| {
            (
                row.get_i32(0).unwrap(),
                row.get_i32(1).unwrap(),
                row.get_bool(2).unwrap(),
            )
        })
        .collect();
    println!("walk (src, dst, is_cycle) in BREADTH FIRST order: {walked:?}");
    assert_eq!(
        walked,
        vec![
            (1, 2, false),
            (2, 3, false),
            (2, 4, false),
            (3, 1, false),
            (1, 2, true),
        ]
    );
    Ok(())
}

fn summed(frame: WindowFrame) -> Qail {
    Qail::get("qail_sx_sales")
        .columns(["day"])
        .column_expr(Expr::Window {
            name: "running".to_string(),
            func: "sum".to_string(),
            params: vec![Expr::Named("amount".to_string())],
            filter: None,
            partition: vec![],
            order: vec![qail_core::ast::Cage {
                kind: qail_core::ast::CageKind::Sort(SortOrder::Asc),
                conditions: vec![Condition {
                    left: Expr::Named("day".to_string()),
                    op: Operator::Eq,
                    value: Value::Null,
                    is_array_unnest: false,
                }],
                logical_op: qail_core::ast::LogicalOp::And,
            }],
            frame: Some(frame),
        })
        .order_asc("day")
        .order_asc("amount")
}

#[tokio::test]
#[ignore = "Requires PostgreSQL via QAIL_TEST_DB_URL"]
async fn window_groups_exclusion_and_interval_range() -> PgResult<()> {
    let mut driver = connect().await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE qail_sx_sales (day date, amount integer);
             INSERT INTO qail_sx_sales VALUES
               ('2026-10-01', 10), ('2026-10-01', 20), ('2026-10-02', 5), ('2026-10-04', 1);",
        )
        .await?;

    let sums = |rows: &[PgRow]| -> Vec<i64> {
        rows.iter()
            .map(|row| row.get_i64(1).unwrap_or(-1))
            .collect()
    };

    // GROUPS counts peer groups: 10-04 still reaches back to 10-02.
    let groups = fetch(
        &mut driver,
        &summed(WindowFrame::Groups {
            start: FrameBound::Preceding(1),
            end: FrameBound::CurrentRow,
            exclude: FrameExclusion::NoOthers,
        }),
    )
    .await?;
    // RANGE '1 day' measures distance: 10-04 has no row within one day before it.
    let range = fetch(
        &mut driver,
        &summed(WindowFrame::Range {
            start: FrameBound::IntervalPreceding {
                amount: 1,
                unit: IntervalUnit::Day,
            },
            end: FrameBound::CurrentRow,
            exclude: FrameExclusion::NoOthers,
        }),
    )
    .await?;
    // EXCLUDE TIES drops the other 10-01 row from each 10-01 frame.
    let ties = fetch(
        &mut driver,
        &summed(WindowFrame::Rows {
            start: FrameBound::UnboundedPreceding,
            end: FrameBound::UnboundedFollowing,
            exclude: FrameExclusion::Ties,
        }),
    )
    .await?;
    println!(
        "GROUPS {:?} RANGE {:?} EXCLUDE TIES {:?}",
        sums(&groups),
        sums(&range),
        sums(&ties)
    );
    assert_eq!(sums(&groups), vec![30, 30, 35, 6]);
    assert_eq!(sums(&range), vec![30, 30, 35, 1]);
    assert_eq!(sums(&ties), vec![16, 26, 36, 36]);

    let dsl = qail_core::parse(
        "get qail_sx_sales fields day, sum(amount) over (order by day range between 1d preceding and current row exclude current row) as s",
    )
    .expect("DSL frame parses");
    let rows = fetch(&mut driver, &dsl).await?;
    let mut excluded = sums(&rows);
    excluded.sort_unstable();
    // Each row's own amount is excluded from its frame; 10-04 has nothing left.
    assert_eq!(excluded, vec![-1, 10, 20, 30]);
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL via QAIL_TEST_DB_URL"]
async fn dsl_expression_parity_executes() -> PgResult<()> {
    let mut driver = connect().await?;
    driver
        .execute_simple(
            "CREATE TEMP TABLE qail_sx_orders (amount numeric, active boolean, status text, placed timestamptz);
             INSERT INTO qail_sx_orders VALUES (12.345, true, 'paid', '2026-10-03 10:00:00+00');",
        )
        .await?;
    let cmd = qail_core::parse(
        "get qail_sx_orders fields amount::numeric(12,2)::text as amt, \
         (amount > 10 and active = true) as big, not active as inactive, \
         case status when 'paid' then 1 when 'void' then 2 else 0 end as code, \
         amount::double precision as dbl, placed::timestamp with time zone as at, \
         (amount + 1) as bumped",
    )
    .expect("DSL parses");
    let rows = fetch(&mut driver, &cmd).await?;
    let row = &rows[0];
    println!(
        "amt={} big={:?} inactive={:?} code={:?} dbl={:?}",
        text(row, 0),
        row.get_bool(1),
        row.get_bool(2),
        row.get_i32(3),
        row.get_f64(4)
    );
    assert_eq!(text(row, 0), "12.35");
    assert_eq!(row.get_bool(1), Some(true));
    assert_eq!(row.get_bool(2), Some(false));
    assert_eq!(row.get_i32(3), Some(1));
    assert_eq!(row.get_f64(4), Some(12.345));
    assert_eq!(text(row, 6), "13.345");
    Ok(())
}

#[tokio::test]
#[ignore = "Requires a database where the test may create a scratch table (QAIL_TEST_DB_URL)"]
async fn row_lock_options_across_sessions() -> PgResult<()> {
    let table = format!("qail_sx_lock_{}", Uuid::new_v4().simple());
    let mut holder = connect().await?;
    let mut waiter = connect().await?;
    holder
        .execute_simple(&format!(
            "CREATE TABLE {table} (id integer PRIMARY KEY, state text);
             INSERT INTO {table} VALUES (1, 'queued'), (2, 'queued');"
        ))
        .await?;

    let outcome = async {
        holder.execute_simple("BEGIN").await?;
        let held = fetch(
            &mut holder,
            &Qail::get(table.as_str())
                .columns(["id"])
                .eq("id", 1)
                .for_update(),
        )
        .await?;
        assert_eq!(held.len(), 1);

        waiter.execute_simple("BEGIN").await?;
        let nowait = must_fail(
            fetch(
                &mut waiter,
                &Qail::get(table.as_str())
                    .columns(["id"])
                    .eq("id", 1)
                    .for_update()
                    .nowait(),
            )
            .await,
            "NOWAIT on a locked row errors instead of waiting",
        );
        println!("NOWAIT: {nowait}");
        assert_eq!(nowait.sqlstate(), Some("55P03"));
        waiter.execute_simple("ROLLBACK").await?;

        waiter.execute_simple("BEGIN").await?;
        let skipped = fetch(
            &mut waiter,
            &Qail::get(table.as_str())
                .columns(["id"])
                .for_no_key_update()
                .lock_of([table.as_str()])
                .skip_locked(),
        )
        .await?;
        let ids: Vec<i32> = skipped.iter().filter_map(|row| row.get_i32(0)).collect();
        println!("FOR NO KEY UPDATE OF ... SKIP LOCKED: {ids:?}");
        assert_eq!(ids, vec![2]);
        waiter.execute_simple("ROLLBACK").await?;

        let dsl = qail_core::parse(&format!(
            "get {table} fields id where id = 1 for update nowait"
        ))
        .expect("DSL lock parses");
        waiter.execute_simple("BEGIN").await?;
        let dsl_err = must_fail(
            fetch(&mut waiter, &dsl).await,
            "DSL NOWAIT errors on the held row",
        );
        assert_eq!(dsl_err.sqlstate(), Some("55P03"));
        waiter.execute_simple("ROLLBACK").await?;

        holder.execute_simple("ROLLBACK").await?;
        PgResult::Ok(())
    }
    .await;

    let mut cleanup = connect().await?;
    cleanup
        .execute_simple(&format!("DROP TABLE IF EXISTS {table}"))
        .await?;
    outcome
}
