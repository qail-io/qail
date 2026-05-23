//! Live PostgreSQL regression tests for recursive CTE set-operation grouping.
//!
//! Default local target:
//!   podman start qail-pg18-lab
//!   cargo test -p qail-pg --test recursive_cte_live -- --ignored --nocapture
//!
//! Override with QAIL_RECURSIVE_CTE_HOST, QAIL_RECURSIVE_CTE_PORT,
//! QAIL_RECURSIVE_CTE_USER, QAIL_RECURSIVE_CTE_PASSWORD, and QAIL_RECURSIVE_CTE_DB.

use qail_core::ast::{
    Action, BinaryOp, CTEDef, Cage, CageKind, Condition, Expr, Join, JoinKind, LogicalOp, Operator,
    Qail, SetOp, Value,
};
use qail_pg::protocol::AstEncoder;
use qail_pg::{PgConnection, PgResult};

async fn connect_lab() -> PgResult<PgConnection> {
    let host = std::env::var("QAIL_RECURSIVE_CTE_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("QAIL_RECURSIVE_CTE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(55432);
    let user = std::env::var("QAIL_RECURSIVE_CTE_USER").unwrap_or_else(|_| "qail_lab".into());
    let database =
        std::env::var("QAIL_RECURSIVE_CTE_DB").unwrap_or_else(|_| "qail_engine_lab".into());
    let password =
        std::env::var("QAIL_RECURSIVE_CTE_PASSWORD").unwrap_or_else(|_| "qail_lab".into());

    PgConnection::connect_with_password(&host, port, &user, &database, Some(&password)).await
}

fn seed_select(table: &str) -> Qail {
    Qail::get(table).columns(["id", "depth", "path"])
}

fn depth_plus_one() -> Expr {
    Expr::Binary {
        left: Box::new(Expr::Named("monster_tree.depth".to_string())),
        op: BinaryOp::Add,
        right: Box::new(Expr::Literal(Value::Int(1))),
        alias: Some("depth".to_string()),
    }
}

fn extended_path() -> Expr {
    let path_with_separator = Expr::Binary {
        left: Box::new(Expr::Named("monster_tree.path".to_string())),
        op: BinaryOp::Concat,
        right: Box::new(Expr::Literal(Value::String(">".to_string()))),
        alias: None,
    };

    let child_as_text = Expr::Cast {
        expr: Box::new(Expr::Named("qail_monster_edges.child_id".to_string())),
        target_type: "text".to_string(),
        alias: None,
    };

    Expr::Binary {
        left: Box::new(path_with_separator),
        op: BinaryOp::Concat,
        right: Box::new(child_as_text),
        alias: Some("path".to_string()),
    }
}

fn monster_recursive_cte() -> Qail {
    let mut base = seed_select("qail_monster_seed_a");
    base.set_ops.push((
        SetOp::UnionAll,
        Box::new(seed_select("qail_monster_seed_b")),
    ));
    base.set_ops.push((
        SetOp::UnionAll,
        Box::new(seed_select("qail_monster_seed_c")),
    ));

    let mut recursive = Qail::get("qail_monster_edges");
    recursive.columns = vec![
        Expr::Aliased {
            name: "qail_monster_edges.child_id".to_string(),
            alias: "id".to_string(),
        },
        depth_plus_one(),
        extended_path(),
    ];
    recursive.joins.push(Join {
        table: "monster_tree".to_string(),
        kind: JoinKind::Inner,
        on: Some(vec![Condition {
            left: Expr::Named("qail_monster_edges.parent_id".to_string()),
            op: Operator::Eq,
            value: Value::Column("monster_tree.id".to_string()),
            is_array_unnest: false,
        }]),
        on_true: false,
    });
    recursive.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("monster_tree.depth".to_string()),
            op: Operator::Lt,
            value: Value::Int(3),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    recursive
        .set_ops
        .push((SetOp::UnionAll, Box::new(seed_select("qail_monster_empty"))));

    let mut cmd = Qail::get("monster_tree");
    cmd.action = Action::With;
    cmd.ctes = vec![CTEDef {
        name: "monster_tree".to_string(),
        recursive: true,
        columns: vec!["id".to_string(), "depth".to_string(), "path".to_string()],
        base_query: Box::new(base),
        recursive_query: Some(Box::new(recursive)),
        source_table: None,
    }];
    cmd
}

async fn seed_monster_tables(conn: &mut PgConnection) -> PgResult<()> {
    let sql = r#"
        CREATE TEMP TABLE qail_monster_seed_a (
            id integer,
            depth integer,
            path text
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE qail_monster_seed_b (
            id integer,
            depth integer,
            path text
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE qail_monster_seed_c (
            id integer,
            depth integer,
            path text
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE qail_monster_empty (
            id integer,
            depth integer,
            path text
        ) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE qail_monster_edges (
            parent_id integer,
            child_id integer
        ) ON COMMIT PRESERVE ROWS;

        INSERT INTO qail_monster_seed_a
        SELECT gs, 0, 'a:' || gs::text
        FROM generate_series(1, 10) AS seed(gs);

        INSERT INTO qail_monster_seed_b
        SELECT gs, 0, 'b:' || gs::text
        FROM generate_series(11, 20) AS seed(gs);

        INSERT INTO qail_monster_seed_c
        SELECT gs, 0, 'c:' || gs::text
        FROM generate_series(21, 30) AS seed(gs);

        INSERT INTO qail_monster_edges
        SELECT parent_id, parent_id * 10 + branch_id
        FROM generate_series(1, 9999) AS parent(parent_id)
        CROSS JOIN generate_series(1, 3) AS branch(branch_id);
        "#;

    conn.execute_simple(sql).await
}

#[tokio::test]
#[ignore = "Requires local Podman PostgreSQL qail-pg18-lab on 127.0.0.1:55432"]
async fn monster_recursive_cte_executes_grouped_multi_set_ops() -> PgResult<()> {
    let mut conn = connect_lab().await?;
    seed_monster_tables(&mut conn).await?;

    let cmd = monster_recursive_cte();
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd)?;

    assert_eq!(params.len(), 1);
    assert!(
        sql.contains(
            "AS ((SELECT id, depth, path FROM qail_monster_seed_a UNION ALL SELECT id, depth, path FROM qail_monster_seed_b UNION ALL SELECT id, depth, path FROM qail_monster_seed_c) UNION ALL (SELECT"
        ),
        "recursive CTE arms must be explicitly grouped: {sql}"
    );
    assert!(
        sql.contains(
            "WHERE monster_tree.depth < $1 UNION ALL SELECT id, depth, path FROM qail_monster_empty)"
        ),
        "recursive arm set_ops must stay inside the grouped recursive term: {sql}"
    );

    let rows = conn.query_rows(&sql, &params).await?;
    let mut depth_counts = [0usize; 4];
    let mut saw_deep_path = false;

    for row in &rows {
        let depth = row.get_i32(1).expect("depth column must decode") as usize;
        depth_counts[depth] += 1;

        if row.get_string(2).as_deref() == Some("a:1>11>111>1111") {
            saw_deep_path = true;
        }
    }

    assert_eq!(rows.len(), 1_200);
    assert_eq!(depth_counts, [30, 90, 270, 810]);
    assert!(
        saw_deep_path,
        "expected depth-3 path from the recursive fanout"
    );

    Ok(())
}
