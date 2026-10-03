//! Typed AST additions for SELECT syntax: set-operation ALL forms, row-lock
//! options, CTE materialization and SEARCH/CYCLE, window frame modes,
//! exclusion and interval offsets, and array slices. Each is checked in the
//! preview transpiler and the native encoder, and invalid shapes must fail.

use qail_core::ast::{
    CteCycle, CteMaterialization, CteSearch, CteSearchOrder, Expr, FrameBound, FrameExclusion,
    Qail, SetOp, SortOrder, Value, WindowFrame, values::IntervalUnit,
};
use qail_core::transpiler::ToSql;
use qail_pg::protocol::AstEncoder;

fn native(cmd: &Qail) -> String {
    AstEncoder::encode_cmd_sql(cmd)
        .map(|(sql, _)| sql)
        .unwrap_or_else(|err| panic!("native encode failed: {err}"))
}

fn native_err(cmd: &Qail) -> String {
    match AstEncoder::encode_cmd_sql(cmd) {
        Ok((sql, _)) => panic!("expected an encode error, got `{sql}`"),
        Err(err) => err.to_string(),
    }
}

fn assert_both(cmd: &Qail, expected: &str) {
    assert_eq!(cmd.to_sql(), expected, "transpiler");
    assert_eq!(native(cmd), expected, "native encoder");
}

fn int(n: i64) -> Expr {
    Expr::Literal(Value::Int(n))
}

// ---------------------------------------------------------------- B9

#[test]
fn intersect_all_and_except_all() {
    let mut cmd = Qail::get("a").columns(["id"]);
    cmd.set_ops.push((
        SetOp::IntersectAll,
        Box::new(Qail::get("b").columns(["id"])),
    ));
    cmd.set_ops
        .push((SetOp::ExceptAll, Box::new(Qail::get("c").columns(["id"]))));
    assert_both(
        &cmd,
        "SELECT id FROM a INTERSECT ALL SELECT id FROM b EXCEPT ALL SELECT id FROM c",
    );
}

// ---------------------------------------------------------------- B7

#[test]
fn lock_nowait_and_of_list() {
    let cmd = Qail::get("orders o")
        .columns(["o.id"])
        .for_no_key_update()
        .lock_of(["o"])
        .nowait();
    assert_both(
        &cmd,
        "SELECT o.id FROM orders o FOR NO KEY UPDATE OF o NOWAIT",
    );

    let cmd = Qail::get("jobs").columns(["id"]).for_share().skip_locked();
    assert_both(&cmd, "SELECT id FROM jobs FOR SHARE SKIP LOCKED");
}

#[test]
fn lock_options_without_a_lock_mode_fail() {
    let cmd = Qail::get("jobs").columns(["id"]).nowait();
    assert!(native_err(&cmd).contains("NOWAIT requires a row lock mode"));
    assert!(cmd.to_sql().contains("/* ERROR"), "{}", cmd.to_sql());

    let cmd = Qail::get("jobs").columns(["id"]).lock_of(["jobs"]);
    assert!(native_err(&cmd).contains("OF requires a row lock mode"));

    let cmd = Qail::get("jobs").columns(["id"]).skip_locked();
    assert!(native_err(&cmd).contains("SKIP LOCKED requires a row lock mode"));
}

#[test]
fn nowait_and_skip_locked_are_exclusive() {
    let cmd = Qail::get("jobs")
        .columns(["id"])
        .for_update_skip_locked()
        .nowait();
    assert!(native_err(&cmd).contains("NOWAIT and SKIP LOCKED"));
    assert!(cmd.to_sql().contains("/* ERROR"), "{}", cmd.to_sql());
}

#[test]
fn lock_of_rejects_qualified_names() {
    let cmd = Qail::get("public.jobs")
        .columns(["id"])
        .for_update()
        .lock_of(["public.jobs"]);
    assert!(native_err(&cmd).contains("lock_of"));
}

// ---------------------------------------------------------------- B8

fn tree_cte() -> Qail {
    let base = Qail::get("nodes").columns(["id", "parent_id"]).filter(
        "parent_id",
        qail_core::ast::Operator::IsNull,
        Value::Null,
    );
    let step = Qail::get("nodes n")
        .columns(["n.id", "n.parent_id"])
        .inner_join_conds(
            "tree t",
            vec![qail_core::ast::Condition {
                left: Expr::Named("n.parent_id".to_string()),
                op: qail_core::ast::Operator::Eq,
                value: Value::Column("t.id".to_string()),
                is_array_unnest: false,
            }],
        );
    Qail::get("tree")
        .columns(["id"])
        .with("tree", base)
        .recursive(step)
}

#[test]
fn cte_materialization_both_paths() {
    let cmd = Qail::get("recent")
        .columns(["id"])
        .with("recent", Qail::get("orders").columns(["id"]))
        .cte_materialized();
    assert_both(
        &cmd,
        "WITH recent(id) AS MATERIALIZED (SELECT id FROM orders) SELECT id FROM recent",
    );
    let cmd = Qail::get("recent")
        .columns(["id"])
        .with("recent", Qail::get("orders").columns(["id"]))
        .cte_not_materialized();
    assert_both(
        &cmd,
        "WITH recent(id) AS NOT MATERIALIZED (SELECT id FROM orders) SELECT id FROM recent",
    );
    assert_eq!(
        cmd.ctes[0].materialization,
        Some(CteMaterialization::NotMaterialized)
    );
}

#[test]
fn recursive_cte_search_and_cycle() {
    let cmd = tree_cte()
        .cte_search(CteSearch {
            order: CteSearchOrder::DepthFirst,
            by: vec!["id".to_string()],
            set_column: "ord".to_string(),
        })
        .cte_cycle(CteCycle {
            columns: vec!["id".to_string()],
            set_column: "is_cycle".to_string(),
            using_column: "path".to_string(),
        });
    let expected = "WITH RECURSIVE tree(id, parent_id) AS (SELECT id, parent_id FROM nodes WHERE parent_id IS NULL UNION ALL SELECT n.id, n.parent_id FROM nodes n INNER JOIN tree t ON n.parent_id = t.id) SEARCH DEPTH FIRST BY id SET ord CYCLE id SET is_cycle USING path SELECT id FROM tree";
    assert_eq!(cmd.to_sql(), expected, "transpiler");
    assert_eq!(native(&cmd), expected, "native");
}

#[test]
fn search_on_non_recursive_cte_fails() {
    let cmd = Qail::get("recent")
        .columns(["id"])
        .with("recent", Qail::get("orders").columns(["id"]))
        .cte_search(CteSearch {
            order: CteSearchOrder::BreadthFirst,
            by: vec!["id".to_string()],
            set_column: "ord".to_string(),
        });
    assert!(native_err(&cmd).contains("SEARCH/CYCLE requires a recursive CTE"));
    assert!(cmd.to_sql().contains("/* ERROR"), "{}", cmd.to_sql());
}

// ---------------------------------------------------------------- B5

fn window(frame: WindowFrame) -> Qail {
    Qail::get("orders").column_expr(Expr::Window {
        name: "s".to_string(),
        func: "sum".to_string(),
        params: vec![Expr::Named("amount".to_string())],
        partition: vec![],
        order: vec![qail_core::ast::Cage {
            kind: qail_core::ast::CageKind::Sort(SortOrder::Asc),
            conditions: vec![qail_core::ast::Condition {
                left: Expr::Named("placed_at".to_string()),
                op: qail_core::ast::Operator::Eq,
                value: Value::Null,
                is_array_unnest: false,
            }],
            logical_op: qail_core::ast::LogicalOp::And,
        }],
        frame: Some(frame),
    })
}

#[test]
fn range_interval_offset_and_exclusion() {
    let cmd = window(WindowFrame::Range {
        start: FrameBound::IntervalPreceding {
            amount: 7,
            unit: IntervalUnit::Day,
        },
        end: FrameBound::CurrentRow,
        exclude: FrameExclusion::CurrentRow,
    });
    assert_both(
        &cmd,
        "SELECT SUM(amount) OVER (ORDER BY placed_at ASC RANGE BETWEEN INTERVAL '7 days' PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) AS s FROM orders",
    );
}

#[test]
fn groups_frame() {
    let cmd = window(WindowFrame::Groups {
        start: FrameBound::Preceding(1),
        end: FrameBound::Following(1),
        exclude: FrameExclusion::Group,
    });
    assert_both(
        &cmd,
        "SELECT SUM(amount) OVER (ORDER BY placed_at ASC GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) AS s FROM orders",
    );
}

#[test]
fn interval_offset_outside_range_fails() {
    let cmd = window(WindowFrame::Rows {
        start: FrameBound::IntervalPreceding {
            amount: 1,
            unit: IntervalUnit::Hour,
        },
        end: FrameBound::CurrentRow,
        exclude: FrameExclusion::NoOthers,
    });
    assert!(native_err(&cmd).contains("interval frame offsets require RANGE"));
    assert!(cmd.to_sql().contains("/* ERROR"), "{}", cmd.to_sql());
}

#[test]
fn negative_frame_offset_fails() {
    let cmd = window(WindowFrame::Rows {
        start: FrameBound::Preceding(-1),
        end: FrameBound::CurrentRow,
        exclude: FrameExclusion::NoOthers,
    });
    assert!(native_err(&cmd).contains("frame offset must not be negative"));
    assert!(cmd.to_sql().contains("/* ERROR"), "{}", cmd.to_sql());
}

// ---------------------------------------------------------------- B11

#[test]
fn array_slice_bounds() {
    let slice = |lower: Option<Expr>, upper: Option<Expr>| Expr::ArraySlice {
        expr: Box::new(Expr::Named("arr".to_string())),
        lower: lower.map(Box::new),
        upper: upper.map(Box::new),
        alias: None,
    };
    let cmd = Qail::get("orders").columns_expr([
        slice(Some(int(1)), Some(int(3))),
        slice(None, Some(int(2))),
        slice(Some(int(2)), None),
        slice(None, None),
    ]);
    assert_both(
        &cmd,
        "SELECT arr[1:3], arr[:2], arr[2:], arr[:] FROM orders",
    );
}

#[test]
fn array_slice_of_function_result_is_parenthesized() {
    let cmd = Qail::get("orders").column_expr(Expr::ArraySlice {
        expr: Box::new(Expr::FunctionCall {
            name: "string_to_array".to_string(),
            args: vec![
                Expr::Named("tags".to_string()),
                Expr::Literal(Value::String(",".to_string())),
            ],
            alias: None,
        }),
        lower: Some(Box::new(int(1))),
        upper: Some(Box::new(int(2))),
        alias: Some("head".to_string()),
    });
    assert_both(
        &cmd,
        "SELECT (STRING_TO_ARRAY(tags, ','))[1:2] AS head FROM orders",
    );
}
