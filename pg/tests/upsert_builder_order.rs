//! RLS scope must survive conflict configuration before and after with_rls().
use qail_core::ast::{Expr, Operator, Qail, Value};
use qail_core::prelude::eq;
use qail_core::rls::{RlsContext, init_scope_registries_from_tables};
use qail_pg::protocol::AstEncoder;
use qail_pg::{PgDriver, PgResult};
use std::sync::Once;

const TABLE: &str = "qail_e1_builder_rows";
const MODES: [&str; 5] = [
    "scope-last",
    "scope-first",
    "replace-update",
    "nothing-then-update",
    "update-nothing-update",
];

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

fn register() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        init_scope_registries_from_tables(&[(TABLE, "tenant_id")], &[]).unwrap();
    });
}

fn update(cmd: Qail) -> Qail {
    cmd.on_conflict_update(
        &["id"],
        &[("status", Expr::Named("EXCLUDED.status".into()))],
    )
}

fn command(mode: &str, id: i32, tenant: &str, status: &str) -> Qail {
    let base = Qail::add(TABLE)
        .set_value("id", id)
        .set_value("status", status);
    let ctx = RlsContext::tenant(tenant);
    match mode {
        "scope-last" => update(base).with_rls(&ctx).unwrap(),
        "scope-first" => update(base.with_rls(&ctx).unwrap()),
        "replace-update" => update(update(base).with_rls(&ctx).unwrap()),
        "nothing-then-update" => update(base.on_conflict_nothing(&["id"]).with_rls(&ctx).unwrap()),
        "update-nothing-update" => update(
            update(base)
                .with_rls(&ctx)
                .unwrap()
                .on_conflict_nothing(&["id"]),
        ),
        _ => panic!("unknown test mode"),
    }
}

fn constraint_update(cmd: Qail) -> Qail {
    cmd.on_conflict_constraint_update(
        "qail_e1_builder_rows_pkey",
        &[("status", Expr::Named("EXCLUDED.status".into()))],
    )
}

/// `command` with an `ON CONFLICT ON CONSTRAINT` target instead of columns.
fn constraint_command(mode: &str, tenant: &str) -> Qail {
    let base = Qail::add(TABLE)
        .set_value("id", 1)
        .set_value("status", "changed");
    let ctx = RlsContext::tenant(tenant);
    match mode {
        "scope-last" => constraint_update(base).with_rls(&ctx).unwrap(),
        "scope-first" => constraint_update(base.with_rls(&ctx).unwrap()),
        "replace-update" => constraint_update(constraint_update(base).with_rls(&ctx).unwrap()),
        "nothing-then-update" => constraint_update(
            base.on_conflict_constraint_nothing("qail_e1_builder_rows_pkey")
                .with_rls(&ctx)
                .unwrap(),
        ),
        "update-nothing-update" => constraint_update(
            constraint_update(base)
                .with_rls(&ctx)
                .unwrap()
                .on_conflict_constraint_nothing("qail_e1_builder_rows_pkey"),
        ),
        _ => panic!("unknown test mode"),
    }
}

#[test]
fn constraint_target_keeps_scope_in_every_builder_order() {
    use qail_core::transpiler::ToSql;

    register();
    for mode in MODES {
        let cmd = constraint_command(mode, "a'bound");
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert!(
            sql.contains(" ON CONFLICT ON CONSTRAINT qail_e1_builder_rows_pkey DO UPDATE SET "),
            "{mode}: {sql}"
        );
        assert!(
            sql.ends_with("WHERE qail_e1_builder_rows.tenant_id = $4"),
            "{mode}: {sql}"
        );
        assert_eq!(params.len(), 4, "{mode}");
        assert_eq!(params[2], params[3], "{mode}");

        let preview = cmd.to_sql();
        let update_part = preview
            .split(" DO UPDATE SET ")
            .nth(1)
            .unwrap_or_else(|| panic!("{mode}: {preview}"));
        assert!(
            update_part.contains(" WHERE ") && update_part.contains("tenant_id"),
            "{mode}: {preview}"
        );
    }
}

#[test]
fn native_scope_survives_every_conflict_builder_order() {
    register();
    for mode in MODES {
        let (sql, params) =
            AstEncoder::encode_cmd_sql(&command(mode, 1, "a'bound", "changed")).unwrap();
        assert!(
            sql.ends_with("WHERE qail_e1_builder_rows.tenant_id = $4"),
            "{mode}: {sql}"
        );
        assert_eq!(params.len(), 4, "{mode}");
        assert_eq!(params[2], params[3], "{mode}");
    }
}

#[test]
fn replacing_conflict_keeps_predicates_and_rescoping_replaces_tenant() {
    register();
    let mut cmd = command("scope-last", 1, "a", "changed");
    cmd.on_conflict
        .as_mut()
        .unwrap()
        .where_conditions
        .push(eq(&format!("{TABLE}.status"), "ready"));
    let cmd = update(cmd.on_conflict_nothing(&["id"]))
        .with_rls(&RlsContext::tenant("b"))
        .unwrap();
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert!(sql.contains("status = $4"), "{sql}");
    assert!(sql.ends_with("tenant_id = $5"), "{sql}");
    assert_eq!(params[2].as_deref(), Some(b"b".as_slice()));
    assert_eq!(params[3].as_deref(), Some(b"ready".as_slice()));
    assert_eq!(params[4].as_deref(), Some(b"b".as_slice()));
}

#[test]
fn late_scope_assignment_or_removed_guard_fails_native_validation() {
    register();
    let scoped = command("scope-first", 1, "a", "changed");
    let forbidden = scoped.clone().on_conflict_update(
        &["id"],
        &[("tenant_id", Expr::Named("EXCLUDED.tenant_id".into()))],
    );
    assert!(AstEncoder::encode_cmd(&forbidden).is_err());
    let mut missing = scoped;
    missing
        .on_conflict
        .as_mut()
        .unwrap()
        .where_conditions
        .clear();
    assert!(AstEncoder::encode_cmd(&missing).is_err());
}

#[test]
fn positional_scope_and_filter_parameters_keep_their_order() {
    register();
    let base = Qail::add(TABLE)
        .columns(["id", "status"])
        .values(vec![Value::Int(1), Value::String("changed".into())]);
    let cmd = update(base.with_rls(&RlsContext::tenant("a'bound")).unwrap())
        .eq(format!("{TABLE}.id"), 1)
        .or_filter(format!("{TABLE}.status"), Operator::Eq, "initial")
        .or_filter(format!("{TABLE}.status"), Operator::Eq, "ready");
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
    assert!(sql.ends_with("WHERE qail_e1_builder_rows.id = $4 AND (qail_e1_builder_rows.status = $5 OR qail_e1_builder_rows.status = $6) AND qail_e1_builder_rows.tenant_id = $7"), "{sql}");
    assert_eq!(params.len(), 7);
    assert_eq!(params[2], params[6]);
    assert_eq!(params[6].as_deref(), Some(b"a'bound".as_slice()));
}

#[test]
fn replacing_conflict_preserves_additional_scope_column_predicates() {
    register();
    for mode in MODES {
        let mut cmd = command(mode, 1, "a", "changed");
        let restriction = eq(&format!("{TABLE}.tenant_id"), "b");
        cmd.on_conflict
            .as_mut()
            .unwrap()
            .where_conditions
            .push(restriction.clone());
        let cmd = update(update(cmd.on_conflict_nothing(&["id"])));
        let guards = &cmd.on_conflict.as_ref().unwrap().where_conditions;
        assert!(guards.contains(&restriction), "{mode}: {guards:?}");
        assert_eq!(guards.len(), 2, "{mode}");
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert!(
            sql.ends_with("tenant_id = $4 AND qail_e1_builder_rows.tenant_id = $5"),
            "{sql}"
        );
        assert_eq!(params[3].as_deref(), Some(b"a".as_slice()));
        assert_eq!(params[4].as_deref(), Some(b"b".as_slice()));
    }
}

#[tokio::test]
#[ignore = "requires a live PostgreSQL (QAIL_TEST_DB_URL)"]
async fn synthetic_lab_both_scope_orders_and_replacements() -> PgResult<()> {
    register();
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    // Fixed name is session-local: parallel tests cannot see or change this table.
    driver.execute_simple(&format!("CREATE TEMP TABLE {TABLE} (id integer PRIMARY KEY, tenant_id text NOT NULL, status text NOT NULL)")).await?;
    driver
        .execute_simple(&format!("INSERT INTO {TABLE} VALUES (1, 'a', 'initial')"))
        .await?;
    let mut failures = Vec::new();
    for mode in MODES {
        driver.begin().await?;
        let cmd = command(mode, 1, "b", "must-not-land");
        let affected = driver.execute(&cmd).await?;
        let returned = driver.fetch_all(&cmd.returning(["status"])).await?.len();
        let rows = driver
            .simple_query(&format!("SELECT status FROM {TABLE} WHERE id=1"))
            .await?;
        let status = rows[0].get_string(0).unwrap();
        driver.rollback().await?;
        println!(
            "{mode}: unauthorized affected={affected}, returning={returned}, status={status}; rolled back"
        );
        if (affected, returned, status.as_str()) != (0, 0, "initial") {
            failures.push(mode);
        }
        driver.begin().await?;
        assert_eq!(
            driver.execute(&command(mode, 1, "a", "permitted")).await?,
            1
        );
        assert_eq!(
            driver
                .fetch_all(&command(mode, 1, "a", "permitted").returning(["status"]))
                .await?
                .len(),
            1
        );
        assert_eq!(driver.execute(&command(mode, 2, "b", "inserted")).await?, 1);
        let rows = driver
            .simple_query(&format!("SELECT tenant_id, status FROM {TABLE} WHERE id=2"))
            .await?;
        assert_eq!(rows[0].get_string(0).as_deref(), Some("b"));
        assert_eq!(rows[0].get_string(1).as_deref(), Some("inserted"));
        driver.rollback().await?;
        println!("{mode}: permitted affected=1/returning=1, nonconflicting insert=1; rolled back");
        driver.begin().await?;
        let mut composed = command(mode, 1, "a", "composed")
            .eq(format!("{TABLE}.id"), 1)
            .or_filter(format!("{TABLE}.status"), Operator::Eq, "initial")
            .or_filter(format!("{TABLE}.status"), Operator::Eq, "ready");
        composed
            .on_conflict
            .as_mut()
            .unwrap()
            .where_conditions
            .push(eq(&format!("{TABLE}.status"), "blocked"));
        composed = update(composed.on_conflict_nothing(&["id"]));
        assert_eq!(driver.execute(&composed).await?, 0);
        assert!(
            driver
                .fetch_all(&composed.clone().returning(["status"]))
                .await?
                .is_empty()
        );
        for condition in &mut composed.on_conflict.as_mut().unwrap().where_conditions {
            if condition.left == Expr::Named(format!("{TABLE}.status")) {
                condition.value = Value::String("initial".into());
            }
        }
        let returned = driver.fetch_all(&composed.returning(["status"])).await?;
        assert_eq!(returned.len(), 1);
        assert_eq!(returned[0].get_string(0).as_deref(), Some("composed"));
        driver.rollback().await?;
        println!(
            "{mode}: retained explicit predicate + AND/OR filters + scope: false=0/0, true returning=1; rolled back"
        );
        driver.begin().await?;
        let mut restricted = command(mode, 1, "a", "must-not-land");
        restricted
            .on_conflict
            .as_mut()
            .unwrap()
            .where_conditions
            .push(eq(&format!("{TABLE}.tenant_id"), "b"));
        let restricted = update(restricted.on_conflict_nothing(&["id"]));
        assert_eq!(driver.execute(&restricted).await?, 0);
        assert!(
            driver
                .fetch_all(&restricted.returning(["status"]))
                .await?
                .is_empty()
        );
        let rows = driver
            .simple_query(&format!("SELECT status FROM {TABLE} WHERE id=1"))
            .await?;
        assert_eq!(rows[0].get_string(0).as_deref(), Some("initial"));
        driver.rollback().await?;
        println!(
            "{mode}: additional scope-column restriction retained, affected=0/returning=0/status=initial; rolled back"
        );
    }
    driver
        .execute_simple(&format!("DROP TABLE {TABLE}"))
        .await?;
    assert!(
        failures.is_empty(),
        "unguarded builder sequences: {failures:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires a live PostgreSQL (QAIL_TEST_DB_URL)"]
async fn synthetic_lab_scope_transport_migration() -> PgResult<()> {
    use qail_core::wire::*;
    register();
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    driver.execute_simple(&format!("CREATE TEMP TABLE {TABLE} (id integer PRIMARY KEY, tenant_id text NOT NULL, status text NOT NULL)")).await?;
    driver
        .execute_simple(&format!("INSERT INTO {TABLE} VALUES (1, 'a', 'initial')"))
        .await?;
    for scope_first in [false, true] {
        for transport in ["json", "binary", "text", "batch"] {
            let migrate = |id, tenant, status| {
                let cmd = Qail::add(TABLE)
                    .set_value("id", id)
                    .set_value("status", status);
                let cmd = if scope_first { cmd } else { update(cmd) };
                let cmd = cmd.with_rls(&RlsContext::tenant(tenant)).unwrap();
                let decoded = match transport {
                    "json" => serde_json::from_str(&serde_json::to_string(&cmd).unwrap()).unwrap(),
                    "binary" => decode_cmd_binary(&encode_cmd_binary(&cmd).unwrap()).unwrap(),
                    "text" => decode_cmd_text(&encode_cmd_text(&cmd)).unwrap(),
                    "batch" => decode_cmds_text(&encode_cmds_text(&[cmd]))
                        .unwrap()
                        .remove(0),
                    _ => unreachable!(),
                };
                update(decoded.on_conflict_nothing(&["id"]))
            };
            driver.begin().await?;
            let denied = migrate(1, "b", "must-not-land");
            assert_eq!(driver.execute(&denied).await?, 0);
            assert!(
                driver
                    .fetch_all_cached(&denied.returning(["status"]))
                    .await?
                    .is_empty()
            );
            let rows = driver
                .simple_query(&format!("SELECT status FROM {TABLE} WHERE id=1"))
                .await?;
            assert_eq!(rows[0].get_string(0).as_deref(), Some("initial"));
            let permitted = migrate(1, "a", "permitted");
            assert_eq!(driver.execute(&permitted).await?, 1);
            let returned = driver
                .fetch_all_cached(&permitted.returning(["status"]))
                .await?;
            assert_eq!(returned.len(), 1);
            assert_eq!(returned[0].get_string(0).as_deref(), Some("permitted"));
            assert_eq!(driver.execute(&migrate(2, "b", "inserted")).await?, 1);
            let rows = driver
                .simple_query(&format!("SELECT tenant_id, status FROM {TABLE} WHERE id=2"))
                .await?;
            assert_eq!(rows[0].get_string(0).as_deref(), Some("b"));
            assert_eq!(rows[0].get_string(1).as_deref(), Some("inserted"));
            driver.rollback().await?;
            println!(
                "scope_first={scope_first}, {transport}: denied=0/0/unchanged, permitted=1/1, insert=1; rollback checked"
            );
        }
    }
    let rows = driver
        .simple_query(&format!(
            "SELECT count(*)::text FROM {TABLE} WHERE id<>1 OR status<>'initial'"
        ))
        .await?;
    assert_eq!(rows[0].get_string(0).as_deref(), Some("0"));
    driver
        .execute_simple(&format!("DROP TABLE {TABLE}"))
        .await?;
    println!(
        "scope transport migration: 8 native order/format cases passed; changed/extra rows=0; temp fixture dropped"
    );
    Ok(())
}
