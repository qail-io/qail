//! Live PostgreSQL regression tests for set-operation operand grouping.
//!
//! Default local target:
//!   podman start qail-pg18-lab
//!   cargo test -p qail-pg --test set_ops_live -- --ignored --nocapture

use qail_core::ast::{Qail, SetOp};
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

async fn seed_tables(conn: &mut PgConnection) -> PgResult<()> {
    conn.execute_simple(
        r#"
        CREATE TEMP TABLE qail_setop_l (id integer) ON COMMIT PRESERVE ROWS;
        CREATE TEMP TABLE qail_setop_r (id integer) ON COMMIT PRESERVE ROWS;
        INSERT INTO qail_setop_l VALUES (1), (2);
        INSERT INTO qail_setop_r VALUES (3), (4);
        "#,
    )
    .await
}

fn sorted_ids(rows: &[qail_pg::PgRow]) -> Vec<i32> {
    let mut ids: Vec<i32> = rows
        .iter()
        .map(|row| row.get_i32(0).expect("id must decode"))
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
#[ignore = "Requires local Podman PostgreSQL qail-pg18-lab on 127.0.0.1:55432"]
async fn set_op_executes_with_limited_left_operand() -> PgResult<()> {
    let mut conn = connect_lab().await?;
    seed_tables(&mut conn).await?;

    let mut q1 = Qail::get("qail_setop_l").columns(["id"]).limit(1);
    let q2 = Qail::get("qail_setop_r").columns(["id"]);
    q1.set_ops.push((SetOp::Union, Box::new(q2)));

    let (sql, params) = AstEncoder::encode_cmd_sql(&q1)?;
    assert_eq!(
        sql,
        "(SELECT id FROM qail_setop_l LIMIT 1) UNION SELECT id FROM qail_setop_r"
    );

    let rows = conn.query_rows(&sql, &params).await?;
    assert_eq!(sorted_ids(&rows), vec![1, 3, 4]);

    Ok(())
}

#[tokio::test]
#[ignore = "Requires local Podman PostgreSQL qail-pg18-lab on 127.0.0.1:55432"]
async fn set_op_executes_with_sorted_limited_right_operand() -> PgResult<()> {
    let mut conn = connect_lab().await?;
    seed_tables(&mut conn).await?;

    let mut q1 = Qail::get("qail_setop_l").columns(["id"]);
    let q2 = Qail::get("qail_setop_r")
        .columns(["id"])
        .order_desc("id")
        .limit(1);
    q1.set_ops.push((SetOp::Union, Box::new(q2)));

    let (sql, params) = AstEncoder::encode_cmd_sql(&q1)?;
    assert_eq!(
        sql,
        "SELECT id FROM qail_setop_l UNION (SELECT id FROM qail_setop_r ORDER BY id DESC LIMIT 1)"
    );

    let rows = conn.query_rows(&sql, &params).await?;
    assert_eq!(sorted_ids(&rows), vec![1, 2, 4]);

    Ok(())
}

#[tokio::test]
#[ignore = "Requires local Podman PostgreSQL qail-pg18-lab on 127.0.0.1:55432"]
async fn set_op_executes_with_fetch_left_operand() -> PgResult<()> {
    let mut conn = connect_lab().await?;
    seed_tables(&mut conn).await?;

    let mut q1 = Qail::get("qail_setop_l")
        .columns(["id"])
        .order_asc("id")
        .fetch_first(1);
    let q2 = Qail::get("qail_setop_r").columns(["id"]);
    q1.set_ops.push((SetOp::Union, Box::new(q2)));

    let (sql, params) = AstEncoder::encode_cmd_sql(&q1)?;
    assert_eq!(
        sql,
        "(SELECT id FROM qail_setop_l ORDER BY id FETCH FIRST 1 ROWS ONLY) UNION SELECT id FROM qail_setop_r"
    );

    let rows = conn.query_rows(&sql, &params).await?;
    assert_eq!(sorted_ids(&rows), vec![1, 3, 4]);

    Ok(())
}
