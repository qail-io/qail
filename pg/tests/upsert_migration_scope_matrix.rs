//! Guarded native writes after every transport and conflict-builder order.
//!
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test upsert_migration_scope_matrix -- --ignored --nocapture
use qail_core::ast::{Expr, Qail};
use qail_core::rls::{RlsContext, init_scope_registries_from_tables};
use qail_core::wire::*;
use qail_pg::{PgDriver, PgError, PgResult};

const TABLE: &str = "qail_e1_migration_matrix";
const FORMATS: [&str; 6] = ["json", "binary", "text", "batch", "clone", "nested"];

fn transport(cmd: &Qail, format: &str) -> Qail {
    let decoded = match format {
        "json" => serde_json::from_slice(&serde_json::to_vec(cmd).unwrap()).unwrap(),
        "binary" => decode_cmd_binary(&encode_cmd_binary(cmd).unwrap()).unwrap(),
        "text" => decode_cmd_text(&encode_cmd_text(cmd)).unwrap(),
        "batch" => decode_cmds_text(&encode_cmds_text(std::slice::from_ref(cmd)))
            .unwrap()
            .remove(0),
        "clone" => cmd.clone(),
        "nested" => {
            let outer = Qail::get(TABLE).with("pending", cmd.clone());
            let decoded = decode_cmd_binary(&encode_cmd_binary(&outer).unwrap()).unwrap();
            assert_eq!(outer, decoded);
            *decoded.ctes.into_iter().next().unwrap().base_query
        }
        _ => panic!("unknown audit format"),
    };
    assert_eq!(*cmd, decoded);
    decoded
}

fn update(cmd: Qail, key: &str) -> Qail {
    cmd.on_conflict_update(&[key], &[("status", Expr::Named("EXCLUDED.status".into()))])
}

fn command(order: usize, format: &str, ctx: &RlsContext, id: i32) -> Qail {
    let base = Qail::add(TABLE)
        .set_value("id", id)
        .set_value("alt", id + 100)
        .set_value("status", "changed'🌍");
    let scoped = match order {
        0 => base.with_rls(ctx).unwrap(),
        1 => update(base, "id").with_rls(ctx).unwrap(),
        2 => base.on_conflict_nothing(&["id"]).with_rls(ctx).unwrap(),
        _ => panic!("unknown audit order"),
    };
    let decoded = transport(&scoped, format);
    let configured = update(decoded.on_conflict_nothing(&["id"]), "id");
    update(configured, if order == 2 { "alt" } else { "id" })
}

#[tokio::test]
#[ignore = "requires a live PostgreSQL (QAIL_TEST_DB_URL)"]
async fn audited_scoped_transport_matrix() -> PgResult<()> {
    init_scope_registries_from_tables(&[(TABLE, "tenant_id")], &[(TABLE, "owner_id")]).unwrap();
    let url = std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    });
    let mut driver = PgDriver::connect_url(&url).await?;
    driver.execute_simple(&format!("CREATE TEMP TABLE {TABLE} (id integer PRIMARY KEY, alt integer UNIQUE, tenant_id text, owner_id text, status text)")).await?;
    driver.execute_simple(&format!("INSERT INTO {TABLE} VALUES (1,101,'a','u1','initial'), (2,102,'a','u2','initial'), (3,103,'b','u1','initial'), (4,104,NULL,'u1','initial')")).await?;
    let contexts = [
        RlsContext::tenant("a").with_user("u1"),
        RlsContext::global().with_user("u1"),
    ];
    let (mut conflicts, mut inserts, mut restrictions, mut rejected) = (0, 0, 0, 0);
    for (context_index, ctx) in contexts.iter().enumerate() {
        for order in 0..3 {
            for format in FORMATS {
                let own_id = if context_index == 0 { 1 } else { 4 };
                for id in 1..=4 {
                    driver.begin().await?;
                    let cmd = command(order, format, ctx, id);
                    assert_eq!(cmd.conflict_update_scope.len(), 2);
                    assert_eq!(driver.execute(&cmd).await?, u64::from(id == own_id));
                    let rows = driver.fetch_all_cached(&cmd.returning(["status"])).await?;
                    assert_eq!(rows.len(), usize::from(id == own_id));
                    if id == own_id {
                        assert_eq!(rows[0].get_string(0).as_deref(), Some("changed'🌍"));
                    }
                    let rows = driver
                        .simple_query(&format!(
                            "SELECT count(*)::text FROM {TABLE} WHERE status <> 'initial'"
                        ))
                        .await?;
                    assert_eq!(
                        rows[0].get_string(0).as_deref(),
                        Some(if id == own_id { "1" } else { "0" })
                    );
                    driver.rollback().await?;
                    conflicts += 1;
                }
                driver.begin().await?;
                let inserted = driver
                    .fetch_all_cached(&command(order, format, ctx, 20).returning([
                        "id",
                        "owner_id",
                        "tenant_id",
                    ]))
                    .await?;
                assert_eq!(inserted.len(), 1);
                assert_eq!(inserted[0].get_i32(0), Some(20));
                assert_eq!(inserted[0].get_string(1).as_deref(), Some("u1"));
                assert_eq!(
                    inserted[0].get_string(2).as_deref(),
                    if context_index == 0 { Some("a") } else { None }
                );
                driver.rollback().await?;
                inserts += 1;

                driver.begin().await?;
                let mut restricted = command(order, format, ctx, own_id);
                restricted
                    .on_conflict
                    .as_mut()
                    .unwrap()
                    .where_conditions
                    .push(qail_core::ast::builders::eq(
                        &format!("{TABLE}.owner_id"),
                        "u2",
                    ));
                let restricted = update(
                    transport(&restricted, format).on_conflict_nothing(&["id"]),
                    "id",
                );
                assert_eq!(driver.execute(&restricted).await?, 0);
                assert!(
                    driver
                        .fetch_all_cached(&restricted.returning(["status"]))
                        .await?
                        .is_empty()
                );
                driver.rollback().await?;
                restrictions += 1;

                for column in ["tenant_id", "owner_id"] {
                    let invalid = command(order, format, ctx, own_id).on_conflict_update(
                        &["id"],
                        &[(column, Expr::Named(format!("EXCLUDED.{column}")))],
                    );
                    assert!(matches!(
                        driver.execute(&invalid).await,
                        Err(PgError::Encode(_))
                    ));
                    assert!(matches!(
                        driver
                            .fetch_all_cached(&invalid.returning(["status"]))
                            .await,
                        Err(PgError::Encode(_))
                    ));
                    rejected += 2;
                }
                let mut missing = command(order, format, ctx, own_id);
                missing
                    .on_conflict
                    .as_mut()
                    .unwrap()
                    .where_conditions
                    .clear();
                let missing = transport(&missing, format);
                assert!(matches!(
                    driver.execute(&missing).await,
                    Err(PgError::Encode(_))
                ));
                assert!(matches!(
                    driver
                        .fetch_all_cached(&missing.returning(["status"]))
                        .await,
                    Err(PgError::Encode(_))
                ));
                rejected += 2;
            }
        }
    }
    let rows = driver
        .simple_query(&format!(
            "SELECT count(*)::text FROM {TABLE} WHERE status <> 'initial' OR id > 4"
        ))
        .await?;
    assert_eq!(rows[0].get_string(0).as_deref(), Some("0"));
    driver
        .execute_simple(&format!("DROP TABLE {TABLE}"))
        .await?;
    println!(
        "final migration audit: {conflicts} tenant/owner/global conflicts; {inserts} inserts; {restrictions} retained restrictive guards; {rejected} native Encode rejections; changed/extra rows=0; fixture dropped"
    );
    Ok(())
}
