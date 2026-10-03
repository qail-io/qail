//! Explicitly selected native-driver check; only session-local TEMP data.
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test upsert_update_guard_live -- --ignored --nocapture

use qail_core::ast::{Condition, Expr, Operator, Qail, Value};
use qail_core::prelude::eq;
use qail_core::rls::{RlsContext, init_scope_registries_from_tables};
use qail_pg::{PgDriver, PgResult};

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

fn upsert(table: &str, id: i32, tenant: &str, status: &str, guards: Vec<Condition>) -> Qail {
    let mut cmd = Qail::add(table)
        .set_value("id", id)
        .set_value("tenant_id", tenant)
        .set_value("status", status)
        .on_conflict_update(
            &["id"],
            &[("status", Expr::Named("EXCLUDED.status".into()))],
        );
    cmd.on_conflict.as_mut().unwrap().where_conditions = guards;
    cmd
}

async fn status(driver: &mut PgDriver, table: &str, id: i32) -> PgResult<String> {
    let rows = driver
        .simple_query(&format!("SELECT status FROM {table} WHERE id = {id}"))
        .await?;
    Ok(rows[0].get_string(0).unwrap())
}

#[tokio::test]
#[ignore = "requires a live PostgreSQL (QAIL_TEST_DB_URL)"]
async fn synthetic_lab_native_upsert_update_guard() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;

    let table = format!("qail_e1_{}", uuid::Uuid::new_v4().simple());
    driver.execute_simple(&format!(
        "CREATE TEMP TABLE {table} (id integer PRIMARY KEY, tenant_id text NOT NULL, status text NOT NULL, marker text)"
    )).await?;
    let tenant_a = "tenant.a'bound";
    let tenant_b = "tenant-b";
    assert_eq!(
        driver
            .execute(&upsert(&table, 1, tenant_a, "initial", vec![]))
            .await?,
        1
    );

    // A different connection must not see the temporary fixture.
    let mut observer = PgDriver::connect_url(&database_url()).await?;
    let visible = observer
        .simple_query(&format!("SELECT to_regclass('{table}') IS NULL"))
        .await?;
    assert_eq!(visible[0].get_bool(0), Some(true));

    driver.begin().await?;
    let false_cmd = upsert(
        &table,
        1,
        tenant_b,
        "must-not-land",
        vec![eq(&format!("{table}.tenant_id"), tenant_b)],
    );
    let affected = driver.execute(&false_cmd).await?;
    let returned = driver
        .fetch_all(&false_cmd.clone().returning(["id", "status"]))
        .await?;
    let observed = status(&mut driver, &table, 1).await?;
    driver.rollback().await?;
    println!(
        "false guard: affected={affected}, returning={}, status={observed}; transaction rolled back",
        returned.len()
    );
    assert_eq!(
        affected, 0,
        "false guard must leave the conflicting row unchanged"
    );
    assert!(returned.is_empty());
    assert_eq!(observed, "initial");

    driver.begin().await?;
    let true_cmd = upsert(
        &table,
        1,
        tenant_a,
        "updated",
        vec![eq(&format!("{table}.tenant_id"), tenant_a)],
    );
    assert_eq!(driver.execute(&true_cmd).await?, 1);
    let returned = driver
        .fetch_all(&true_cmd.clone().returning(["id", "status"]))
        .await?;
    assert_eq!(returned.len(), 1);
    assert_eq!(returned[0].get_string(1).as_deref(), Some("updated"));
    // Reuse the same prepared SQL with a different guard value.
    let returned = driver
        .fetch_all(&false_cmd.returning(["id", "status"]))
        .await?;
    assert!(returned.is_empty());
    assert_eq!(status(&mut driver, &table, 1).await?, "updated");
    driver.rollback().await?;
    assert_eq!(status(&mut driver, &table, 1).await?, "initial");
    println!(
        "true guard: affected=1, returning=1; cached false guard returning=0; rollback restored initial"
    );

    driver.begin().await?;
    let inserted = upsert(
        &table,
        2,
        tenant_b,
        "inserted",
        vec![eq(&format!("{table}.tenant_id"), "no-match")],
    );
    assert_eq!(driver.execute(&inserted).await?, 1);
    let returned = driver
        .fetch_all(
            &upsert(
                &table,
                3,
                tenant_b,
                "inserted-returning",
                vec![eq(&format!("{table}.tenant_id"), "no-match")],
            )
            .returning(["id", "status"]),
        )
        .await?;
    assert_eq!(returned.len(), 1);
    assert_eq!(
        returned[0].get_string(1).as_deref(),
        Some("inserted-returning")
    );
    driver.commit().await?;
    assert_eq!(status(&mut driver, &table, 2).await?, "inserted");
    println!(
        "nonconflicting insert with false update guard: affected=1, returning=1; checked commit retained inserts"
    );

    driver.begin().await?;
    let null_guard = Condition {
        left: Expr::Named(format!("{table}.marker")),
        op: Operator::IsNull,
        value: Value::Null,
        is_array_unnest: false,
    };
    let cmd = upsert(
        &table,
        1,
        tenant_a,
        "null-ok",
        vec![
            eq(
                &format!("{table}.tenant_id"),
                Value::Column("EXCLUDED.tenant_id".into()),
            ),
            null_guard,
        ],
    );
    assert_eq!(driver.execute(&cmd).await?, 1);
    let unknown = upsert(
        &table,
        1,
        tenant_a,
        "unknown-must-not-land",
        vec![eq(&format!("{table}.marker"), "present")],
    );
    assert_eq!(driver.execute(&unknown).await?, 0);
    let bound_null = upsert(
        &table,
        1,
        tenant_a,
        "bound-null-must-not-land",
        vec![
            eq(&format!("{table}.marker"), Value::Null),
            eq(&format!("{table}.tenant_id"), tenant_a),
        ],
    );
    assert_eq!(driver.execute(&bound_null).await?, 0);
    assert_eq!(status(&mut driver, &table, 1).await?, "null-ok");

    // Both the existing filter cages and explicit conflict predicates restrict updates.
    let false_conflict = upsert(
        &table,
        1,
        tenant_a,
        "bad-conflict",
        vec![eq(&format!("{table}.tenant_id"), tenant_b)],
    )
    .or_filter(format!("{table}.status"), Operator::Eq, "null-ok")
    .or_filter(format!("{table}.status"), Operator::Eq, "initial");
    assert_eq!(driver.execute(&false_conflict).await?, 0);
    let false_filter = true_cmd.eq(format!("{table}.status"), "no-match");
    assert_eq!(driver.execute(&false_filter).await?, 0);
    assert_eq!(status(&mut driver, &table, 1).await?, "null-ok");
    driver.rollback().await?;
    println!(
        "qualified EXCLUDED comparison + IS NULL: affected=1; NULL/UNKNOWN and either false guard group: affected=0"
    );

    // Exercise the real scope builder, which stores tenant guards on OnConflict.
    init_scope_registries_from_tables(&[(&table, "tenant_id")], &[]).unwrap();
    driver.begin().await?;
    let other_tenant = upsert(&table, 1, tenant_b, "cross-tenant", vec![])
        .with_rls(&RlsContext::tenant(tenant_b))
        .unwrap();
    assert_eq!(driver.execute(&other_tenant).await?, 0);
    assert!(
        driver
            .fetch_all(&other_tenant.returning(["id"]))
            .await?
            .is_empty()
    );
    let same_tenant = upsert(&table, 1, tenant_a, "own-tenant", vec![])
        .with_rls(&RlsContext::tenant(tenant_a))
        .unwrap();
    assert_eq!(driver.execute(&same_tenant).await?, 1);
    assert_eq!(status(&mut driver, &table, 1).await?, "own-tenant");
    assert_eq!(status(&mut driver, &table, 2).await?, "inserted");
    driver.rollback().await?;
    assert_eq!(status(&mut driver, &table, 1).await?, "initial");
    println!(
        "with_rls tenant guard: other tenant affected=0/returning=0, own tenant affected=1; other row unchanged; rollback verified"
    );

    driver
        .execute_simple(&format!("DROP TABLE {table}"))
        .await?;
    let remaining = driver
        .simple_query(&format!("SELECT to_regclass('{table}') IS NULL"))
        .await?;
    assert_eq!(remaining[0].get_bool(0), Some(true));
    println!("cleanup: temporary fixture removed; no permanent rows, roles or policies changed");
    Ok(())
}
