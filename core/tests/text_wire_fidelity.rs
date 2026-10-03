//! Text wire must decode to the exact AST it encoded, single and batch.

use proptest::prelude::*;
use qail_core::ast::builders::*;
use qail_core::ast::{
    Action, BinaryOp, CTEDef, Cage, CageKind, Constraint, Expr, FrameBound, IndexDef, JoinKind,
    LogicalOp, Operator, Qail, SetOp, SortOrder, Value, WindowFrame,
};
use qail_core::wire::*;

fn window(func: &str, frame: Option<WindowFrame>) -> Expr {
    Expr::Window {
        name: "w".to_string(),
        func: func.to_string(),
        params: vec![col("amount")],
        partition: vec!["tenant_id".to_string()],
        order: vec![Cage {
            kind: CageKind::Sort(SortOrder::Desc),
            conditions: vec![eq("created_at", Value::Null)],
            logical_op: LogicalOp::And,
        }],
        frame,
    }
}

fn union_of(mut lhs: Qail, op: SetOp, rhs: Qail) -> Qail {
    lhs.set_ops.push((op, Box::new(rhs)));
    lhs
}

fn corpus() -> Vec<(&'static str, Qail)> {
    let base = || Qail::get("orders").columns(["id", "status"]);
    vec![
        ("plain select", base().where_eq("status", "paid").limit(10)),
        (
            "insert with values",
            Qail::add("rows")
                .set_value("id", 1)
                .set_value("status", "a"),
        ),
        (
            "insert returning",
            Qail::add("rows")
                .set_value("id", 1)
                .set_value("note", "it's")
                .returning(["id"]),
        ),
        (
            "upsert do update",
            Qail::add("rows")
                .set_value("id", 1)
                .set_value("status", "a")
                .on_conflict_update(&["id"], &[("status", col("excluded.status"))]),
        ),
        (
            "upsert do nothing",
            Qail::add("rows")
                .set_value("id", 1)
                .on_conflict_nothing(&["id"]),
        ),
        (
            "update",
            Qail::set("rows")
                .set_value("status", "b")
                .set_value("seen", true)
                .where_eq("id", 7),
        ),
        ("delete", Qail::del("rows").where_eq("id", 7)),
        (
            "union",
            union_of(
                base(),
                SetOp::Union,
                Qail::get("archived").columns(["id", "status"]),
            ),
        ),
        (
            "union all + intersect",
            union_of(
                union_of(base(), SetOp::UnionAll, base()),
                SetOp::Intersect,
                base(),
            ),
        ),
        (
            "except",
            union_of(base(), SetOp::Except, base().where_eq("status", "void")),
        ),
        ("distinct", {
            let mut q = base();
            q.distinct = true;
            q
        }),
        ("distinct on", base().distinct_on(["status"])),
        (
            "group by having",
            Qail::get("orders")
                .column("status")
                .column_expr(count().alias("n"))
                .group_by(["status"])
                .having_cond(cond(count().build(), Operator::Gt, 1)),
        ),
        (
            "window without frame",
            Qail::get("orders").column_expr(window("row_number", None)),
        ),
        (
            "window rows frame",
            Qail::get("orders").column_expr(window(
                "sum",
                Some(WindowFrame::Rows {
                    start: FrameBound::UnboundedPreceding,
                    end: FrameBound::CurrentRow,
                }),
            )),
        ),
        (
            "window range frame",
            Qail::get("orders").column_expr(window(
                "avg",
                Some(WindowFrame::Range {
                    start: FrameBound::Preceding(3),
                    end: FrameBound::Following(2),
                }),
            )),
        ),
        ("for update", base().where_eq("id", 1).for_update()),
        ("skip locked", base().limit(5).for_update_skip_locked()),
        ("for no key update", base().for_no_key_update()),
        ("for share", base().for_share()),
        ("for key share", base().for_key_share()),
        (
            "cte",
            Qail::get("recent")
                .with("recent", base().order_desc("id").limit(3))
                .columns(["id"]),
        ),
        ("recursive cte", {
            let cte = CTEDef {
                name: "tree".to_string(),
                recursive: true,
                columns: vec!["id".to_string(), "parent_id".to_string()],
                base_query: Box::new(Qail::get("nodes").columns(["id", "parent_id"])),
                recursive_query: Some(Box::new(
                    Qail::get("nodes")
                        .columns(["nodes.id", "nodes.parent_id"])
                        .inner_join("tree", "nodes.parent_id", "tree.id"),
                )),
                source_table: None,
            };
            Qail::get("tree").with_cte(cte).columns(["id"])
        }),
        (
            "joins",
            base()
                .left_join("customers", "orders.customer_id", "customers.id")
                .join(JoinKind::Inner, "items", "items.order_id", "orders.id"),
        ),
        (
            "aliased join",
            base().left_join_as("customers", "c", "orders.customer_id", "c.id"),
        ),
        (
            "scalar subquery",
            base().column_expr(Expr::Subquery {
                query: Box::new(
                    Qail::get("items")
                        .column("price")
                        .where_eq("id", 1)
                        .limit(1),
                ),
                alias: Some("price".to_string()),
            }),
        ),
        (
            "exists",
            base().filter_cond(cond(
                exists(Qail::get("items").where_eq("order_id", 1)),
                Operator::Eq,
                true,
            )),
        ),
        (
            "in subquery",
            base().filter(
                "id",
                Operator::In,
                Value::Subquery(Box::new(Qail::get("items").column("order_id"))),
            ),
        ),
        (
            "json paths",
            base()
                .column_expr(json("meta", "phone").alias("phone"))
                .column_expr(json_path("meta", ["a", "0", "b"]).build())
                .column_expr(json_obj("meta", "tags").get("x").get_text("y").build())
                .filter_cond(cond(json("meta", "status").build(), Operator::Eq, "ok")),
        ),
        (
            "or filter + offset + order",
            base()
                .where_eq("a", 1)
                .or_filter("b", Operator::Eq, 2)
                .order_by("id", SortOrder::Asc)
                .offset(20)
                .limit(10),
        ),
        (
            "expressions",
            base()
                .column_expr(coalesce([col("name"), text("n/a")]).alias("name"))
                .column_expr(cast(col("amount"), "text").alias("amount_text"))
                .column_expr(binary(col("a"), BinaryOp::Add, int(1)).alias("a1"))
                .column_expr(
                    case_when(eq("status", "paid"), text("y"))
                        .otherwise(text("n"))
                        .alias("p"),
                ),
        ),
        (
            "aggregate filter + distinct",
            Qail::get("orders")
                .column_expr(count_distinct("customer_id").alias("c"))
                .column_expr(sum("amount").filter(vec![eq("status", "paid")]).alias("s")),
        ),
        (
            "fetch with ties",
            base().order_desc("id").fetch_with_ties(3),
        ),
        ("tablesample", base().tablesample_bernoulli(12.5)),
        ("only", base().only()),
        (
            "returning update",
            Qail::set("rows")
                .set_value("status", "c")
                .where_eq("id", 1)
                .returning_all(),
        ),
        (
            "make table",
            Qail::make("things").columns_expr([
                Expr::Def {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    constraints: vec![Constraint::PrimaryKey],
                },
                Expr::Def {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![Constraint::Nullable, Constraint::Unique],
                },
            ]),
        ),
        (
            "index",
            Qail {
                action: Action::Index,
                table: "things".to_string(),
                index_def: Some(IndexDef {
                    name: "things_name_idx".to_string(),
                    table: "things".to_string(),
                    columns: vec!["name".to_string()],
                    unique: true,
                    index_type: None,
                    include: vec![],
                    concurrently: false,
                    where_clause: None,
                }),
                ..Default::default()
            },
        ),
    ]
}

fn lossy(cases: &[(&'static str, Qail)]) -> Vec<String> {
    let mut failures = Vec::new();
    for (name, cmd) in cases {
        let wire = encode_cmd_text(cmd);
        match decode_cmd_text(&wire) {
            Ok(decoded) if decoded == *cmd => {}
            Ok(_) => failures.push(format!("single `{name}` decoded differently: {wire:?}")),
            Err(err) => failures.push(format!("single `{name}` failed: {err}")),
        }
        match decode_cmds_text(&encode_cmds_text(std::slice::from_ref(cmd))) {
            Ok(decoded) if decoded == std::slice::from_ref(cmd) => {}
            Ok(_) => failures.push(format!("batch `{name}` decoded differently")),
            Err(err) => failures.push(format!("batch `{name}` failed: {err}")),
        }
    }
    failures
}

#[test]
fn text_wire_round_trips_builder_corpus_exactly() {
    let failures = lossy(&corpus());
    assert!(
        failures.is_empty(),
        "{} lossy text-wire round trips:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn text_wire_batch_round_trips_whole_corpus_exactly() {
    let cmds: Vec<Qail> = corpus().into_iter().map(|(_, cmd)| cmd).collect();
    assert_eq!(decode_cmds_text(&encode_cmds_text(&cmds)).unwrap(), cmds);
}

#[test]
fn text_wire_keeps_v1_for_exact_commands() {
    let cmd = Qail::get("orders")
        .columns(["id"])
        .where_eq("id", 1)
        .limit(1);
    assert!(encode_cmd_text(&cmd).starts_with("QAIL-CMD/1\n"));
    assert!(encode_cmds_text(&[cmd.clone(), cmd]).starts_with("QAIL-CMDS/1\n"));

    let lossy = Qail::add("rows")
        .set_value("id", 1)
        .set_value("status", "a");
    assert!(encode_cmd_text(&lossy).starts_with("QAIL-CMD/2\n"));
    let batch = [Qail::get("orders").limit(1), lossy];
    assert!(encode_cmds_text(&batch).starts_with("QAIL-CMDS/2\n"));
}

#[test]
fn text_wire_rejects_inexact_commands_v2_sanitization_refuses() {
    // Canonical text of a DO block does not reparse to it, and the v2 decoder
    // refuses procedural actions: decoding fails instead of running another command.
    let cmd = Qail::do_block("BEGIN NULL; END", "plpgsql");
    let wire = encode_cmd_text(&cmd);
    assert!(wire.starts_with("QAIL-CMD/2\n"));
    assert!(
        decode_cmd_text(&wire)
            .unwrap_err()
            .contains("procedural/session")
    );
    assert!(decode_cmds_text(&encode_cmds_text(&[cmd])).is_err());
}

proptest! {
    #[test]
    fn text_wire_round_trips_composed_selects(
        picks in proptest::collection::vec(0usize..12, 0..6),
        limit in proptest::option::of(0i64..1_000),
        word in "[a-z][a-z0-9_ ']{0,12}",
    ) {
        let mut cmd = Qail::get("orders").columns(["id", "status"]);
        for pick in picks {
            cmd = match pick {
                0 => cmd.where_eq("status", word.as_str()),
                1 => { cmd.distinct = true; cmd }
                2 => cmd.distinct_on(["status"]),
                3 => cmd.group_by(["status"]).having_cond(cond(count().build(), Operator::Gt, 1)),
                4 => cmd.column_expr(window("sum", Some(WindowFrame::Rows {
                    start: FrameBound::Preceding(1),
                    end: FrameBound::CurrentRow,
                }))),
                5 => cmd.for_update(),
                6 => cmd.for_update_skip_locked(),
                7 => cmd.left_join("customers", "orders.customer_id", "customers.id"),
                8 => cmd.column_expr(json_path("meta", ["a", "0", "b"]).build()),
                9 => union_of(cmd, SetOp::UnionAll, Qail::get("archived").columns(["id", "status"])),
                10 => cmd.with("recent", Qail::get("orders").where_eq("note", word.as_str())),
                _ => cmd.order_desc("id").offset(5),
            };
        }
        if let Some(limit) = limit {
            cmd = cmd.limit(limit);
        }
        prop_assert_eq!(decode_cmd_text(&encode_cmd_text(&cmd)), Ok(cmd.clone()));
        let batch = vec![cmd.clone(), Qail::add("rows").set_value("note", word.as_str())];
        prop_assert_eq!(decode_cmds_text(&encode_cmds_text(&batch)), Ok(batch));
    }
}
