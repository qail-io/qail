//! INSERT SELECT scope stamping, using synthetic TEMP tables for live checks.

use qail_core::ast::{CageKind, Expr, Qail, Value};
use qail_core::rls::RlsContext;
use qail_core::transpiler::ToSql;
use qail_pg::protocol::AstEncoder;
use qail_pg::{PgDriver, PgResult};
use std::sync::Once;

const TARGET: &str = "qail_scope_select_target";
const SOURCE: &str = "qail_scope_select_source";
const OWNED: &str = "qail_scope_select_owned";
const OWNER_SOURCE: &str = "qail_scope_select_owner_source";
const IDENTITY: &str = "qail_scope_select_identity";

fn setup() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        qail_core::rls::init_scope_registries_from_tables(
            &[
                (TARGET, "tenant_id"),
                (SOURCE, "tenant_id"),
                (OWNED, "tenant_id"),
                (OWNER_SOURCE, "tenant_id"),
                (IDENTITY, "tenant_id"),
                ("qail_scope_identity_input", "tenant_id"),
                ("qail_scope_org_source", "org_id"),
                ("qail_scope_ambiguous", "scope_id"),
            ],
            &[
                (OWNED, "owner_id"),
                (OWNER_SOURCE, "owner_id"),
                ("qail_scope_owner_only", "owner_id"),
                ("qail_scope_ambiguous", "scope_id"),
            ],
        )
        .unwrap();
    });
}

fn insert(target: &str, columns: &[&str], projection: &[&str]) -> Qail {
    let mut cmd = Qail::add(target).columns(columns.iter().copied());
    cmd.source_query = Some(Box::new(
        Qail::get(SOURCE)
            .columns(projection.iter().copied())
            .eq("enabled", true),
    ));
    cmd
}

#[test]
fn insert_select_scope_stamps_projection_without_payload() {
    setup();
    for explicit in [false, true] {
        let columns = if explicit {
            vec!["id", "tenant_id"]
        } else {
            vec!["id"]
        };
        let cmd = insert(TARGET, &columns, &columns)
            .returning(["id", "tenant_id"])
            .with_rls(&RlsContext::tenant("tenant'a"))
            .unwrap();
        println!(
            "baseline shape: native={:?} preview={}",
            AstEncoder::encode_cmd_sql(&cmd),
            cmd.to_sql()
        );
        assert!(
            !cmd.cages
                .iter()
                .any(|c| matches!(c.kind, CageKind::Payload))
        );
        assert_eq!(
            cmd.columns,
            vec![Expr::Named("id".into()), Expr::Named("tenant_id".into())]
        );
        assert_eq!(
            cmd.source_query.as_ref().unwrap().columns[1],
            Expr::Literal(Value::String("tenant'a".into()))
        );
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert_eq!(
            params,
            vec![Some(b"t".to_vec()), Some(b"tenant'a".to_vec())]
        );
        assert!(sql.contains("enabled = $1"));
        assert!(sql.contains("tenant_id = $2"));
        println!("native={sql} params={params:?} preview={}", cmd.to_sql());
        assert!(sql.contains("WHERE"));
        assert!(cmd.to_sql().contains("tenant''a"));
        let repeated = cmd
            .clone()
            .with_rls(&RlsContext::tenant("tenant'a"))
            .unwrap();
        assert_eq!(cmd, repeated);
    }
}

#[test]
fn insert_select_scope_handles_owner_and_global() {
    setup();
    let owned = insert(OWNED, &["id"], &["id"])
        .with_rls(&RlsContext::tenant("tenant-a").with_user("user-a"))
        .unwrap();
    assert_eq!(owned.source_query.as_ref().unwrap().columns.len(), 3);
    assert!(AstEncoder::encode_cmd_sql(&owned).is_ok());
    let global = insert(TARGET, &["id", "tenant_id"], &["id", "tenant_id"])
        .with_rls(&RlsContext::global())
        .unwrap();
    assert_eq!(
        global.source_query.as_ref().unwrap().columns[1],
        Expr::Literal(Value::Null)
    );
    assert!(AstEncoder::encode_cmd_sql(&global).is_ok());
}

#[test]
fn insert_select_scope_rejects_ambiguous_shapes() {
    setup();
    let mut cases = vec![
        insert(TARGET, &[], &["id"]),
        insert(TARGET, &["id"], &["*"]),
        insert(TARGET, &["id", "tenant_id"], &["id"]),
        insert(TARGET, &["id", "id"], &["id", "id"]),
        insert(TARGET, &["id", "Tenant_ID"], &["id", "tenant_id"]),
        insert(TARGET, &["id"], &["id"]).set_value("tenant_id", "forged"),
        insert(TARGET, &["id"], &["id"]).overriding_user_value(),
    ];
    let mut distinct = insert(TARGET, &["id"], &["id"]);
    distinct.source_query.as_mut().unwrap().distinct = true;
    cases.push(distinct);
    let mut sets = insert(TARGET, &["id"], &["id"]);
    sets.source_query.as_mut().unwrap().set_ops.push((
        qail_core::ast::SetOp::Union,
        Box::new(Qail::get(SOURCE).columns(["id"])),
    ));
    cases.push(sets);
    let mut default = insert(TARGET, &["id"], &["id"]);
    default.default_values = true;
    cases.push(default);
    let mut ordinal = insert(TARGET, &["id", "tenant_id"], &["id", "tenant_id"]);
    ordinal.source_query = Some(Box::new(
        Qail::get(SOURCE)
            .columns(["id", "tenant_id"])
            .order_by("2", qail_core::ast::SortOrder::Asc),
    ));
    cases.push(ordinal);
    let mut aggregate = insert(TARGET, &["id"], &["id"]);
    aggregate.source_query.as_mut().unwrap().columns = vec![Expr::Named("count(*)".into())];
    cases.push(aggregate);
    let mut failed = Vec::new();
    for (index, cmd) in cases.into_iter().enumerate() {
        if cmd.with_rls(&RlsContext::tenant("tenant-a")).is_ok() {
            failed.push(index);
        }
    }
    assert!(failed.is_empty(), "ambiguous cases accepted: {failed:?}");
    assert!(
        insert(TARGET, &["id"], &["id"])
            .with_rls(&RlsContext::tenant("tenant-a\0"))
            .is_err()
    );
    assert!(
        insert(OWNED, &["id"], &["id"])
            .with_rls(&RlsContext::tenant("tenant-a"))
            .is_err()
    );
}

#[test]
fn insert_select_scope_retains_conflict_guards_in_both_builder_orders() {
    setup();
    let ctx = RlsContext::tenant("tenant-a");
    for before in [false, true] {
        let mut cmd = insert(TARGET, &["id"], &["id"]);
        if before {
            cmd = cmd.with_rls(&ctx).unwrap();
        }
        cmd = cmd.on_conflict_update(&["id"], &[("id", Expr::Named("EXCLUDED.id".into()))]);
        if !before {
            cmd = cmd.with_rls(&ctx).unwrap();
        }
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).unwrap();
        assert!(
            sql.contains(&format!(
                "DO UPDATE SET id = EXCLUDED.id WHERE {TARGET}.tenant_id = $3"
            )),
            "{sql}"
        );
        assert_eq!(
            params,
            vec![
                Some(b"t".to_vec()),
                Some(b"tenant-a".to_vec()),
                Some(b"tenant-a".to_vec())
            ]
        );
        let decoded: Qail = serde_json::from_str(&serde_json::to_string(&cmd).unwrap()).unwrap();
        assert_eq!(AstEncoder::encode_cmd_sql(&decoded).unwrap(), (sql, params));
    }
}

#[test]
fn scoped_values_inserts_keep_rendering() {
    setup();
    let tenant = RlsContext::tenant("tenant-a");
    let update = [("status", Expr::Named("EXCLUDED.status".into()))];
    let values = || {
        Qail::add(TARGET)
            .set_value("id", 1)
            .set_value("status", "ok")
    };
    let cases: Vec<(&str, Qail, &str)> = vec![
        (
            "named + returning",
            values().with_rls(&tenant).unwrap().returning(["id"]),
            "tenant-a",
        ),
        (
            "forged tenant restamped",
            values()
                .set_value("tenant_id", "tenant-b")
                .with_rls(&tenant)
                .unwrap(),
            "tenant-a",
        ),
        (
            "conflict update then scope",
            values()
                .on_conflict_update(&["id"], &update)
                .with_rls(&tenant)
                .unwrap(),
            "tenant-a",
        ),
        (
            "scope then conflict update",
            values()
                .with_rls(&tenant)
                .unwrap()
                .on_conflict_update(&["id"], &update),
            "tenant-a",
        ),
        (
            "scope then conflict nothing",
            values()
                .with_rls(&tenant)
                .unwrap()
                .on_conflict_nothing(&["id"]),
            "tenant-a",
        ),
        (
            "column added after scope",
            values().with_rls(&tenant).unwrap().set_value("note", "x"),
            "tenant-a",
        ),
        (
            "tenant and owner",
            Qail::add(OWNED)
                .set_value("id", 1)
                .with_rls(&RlsContext::tenant("tenant-a").with_user("user-1"))
                .unwrap(),
            "user-1",
        ),
        (
            "positional columns",
            Qail::add(TARGET)
                .columns(["id", "tenant_id"])
                .values([Value::Int(1), Value::String("tenant-b".into())])
                .with_rls(&tenant)
                .unwrap(),
            "tenant-a",
        ),
    ];
    for (name, cmd, expected) in cases {
        let (sql, params) = AstEncoder::encode_cmd_sql(&cmd)
            .unwrap_or_else(|error| panic!("{name}: native rejected: {error}"));
        assert!(
            params
                .iter()
                .any(|p| p.as_deref() == Some(expected.as_bytes())),
            "{name}: {sql} {params:?}"
        );
        assert!(
            params
                .iter()
                .all(|p| p.as_deref() != Some(b"tenant-b".as_slice())),
            "{name}: {sql}"
        );
        let preview = cmd.to_sql();
        assert!(preview.starts_with("INSERT INTO"), "{name}: {preview}");
    }

    let global = values().with_rls(&RlsContext::global()).unwrap();
    AstEncoder::encode_cmd_sql(&global).expect("global scope renders");
    assert!(global.to_sql().starts_with("INSERT INTO"));
}

fn mutated_inserts() -> Vec<(&'static str, Qail)> {
    let scoped = insert(TARGET, &["id"], &["id"])
        .returning(["id", "tenant_id"])
        .with_rls(&RlsContext::tenant("tenant-a"))
        .unwrap();
    let mut cases = Vec::new();
    let mut cmd = scoped.clone();
    cmd.source_query.as_mut().unwrap().cages.clear();
    cases.push(("removed source filters", cmd));
    let mut cmd = scoped.clone();
    cmd.source_query.as_mut().unwrap().columns[1] = Expr::Named("tenant_id".into());
    cases.push(("replaced stamp with source column", cmd));
    let mut cmd = scoped.clone();
    cmd.source_query.as_mut().unwrap().columns[1] = Expr::Literal(Value::String("tenant-b".into()));
    cases.push(("forged projection", cmd));
    let mut cmd = scoped.clone();
    cmd.columns.swap(0, 1);
    cases.push(("swapped target columns", cmd));
    let mut cmd = scoped.clone();
    cmd.source_query = Some(Box::new(Qail::get(SOURCE).columns(["id", "tenant_id"])));
    cases.push(("replaced source", cmd));
    let mut cmd = scoped.clone();
    cmd.source_query.as_mut().unwrap().set_ops.push((
        qail_core::ast::SetOp::Union,
        Box::new(Qail::get(SOURCE).columns(["id", "tenant_id"])),
    ));
    cases.push(("added set branch", cmd));
    let mut cmd = scoped.clone();
    cmd.source_query = None;
    cmd.columns.clear();
    cmd.default_values = true;
    cases.push(("removed source for defaults", cmd));
    let mut cmd = scoped.clone();
    cmd.source_query = None;
    cmd.columns.clear();
    cmd = cmd
        .columns(["tenant_id", "id"])
        .set_value("id", "tenant-b")
        .set_value("tenant_id", "tenant-a");
    cases.push(("converted to misaligned named values", cmd));
    cases.push(("late USER override", scoped.clone().overriding_user_value()));
    cases.push((
        "late tenant conflict assignment",
        scoped.clone().on_conflict_update(
            &["id"],
            &[("tenant_id", Expr::Literal(Value::String("tenant-b".into())))],
        ),
    ));
    let mut cmd = scoped
        .clone()
        .on_conflict_update(&["id"], &[("id", Expr::Named("EXCLUDED.id".into()))]);
    cmd.on_conflict.as_mut().unwrap().where_conditions.clear();
    cases.push(("removed conflict guard", cmd));
    let mut cmd = scoped.clone();
    cmd.source_query.as_mut().unwrap().table = format!("pg_temp.{SOURCE}");
    cmd.source_query.as_mut().unwrap().cages.clear();
    cases.push(("qualified source bypass", cmd));
    let mut cmd = scoped.clone();
    cmd.source_query.as_mut().unwrap().table = "unregistered_source".into();
    cases.push(("unregistered source", cmd));
    let subquery = Expr::Subquery {
        query: Box::new(Qail::get(SOURCE).columns(["id"]).limit(1)),
        alias: None,
    };
    let mut cmd = scoped.clone();
    cmd.returning = Some(vec![subquery.clone()]);
    cases.push(("late RETURNING subquery", cmd));
    cases.push((
        "late conflict subquery",
        scoped
            .clone()
            .on_conflict_update(&["id"], &[("id", subquery)]),
    ));
    let mut cmd = scoped.clone();
    cmd.action = qail_core::ast::Action::Get;
    cases.push(("changed statement action", cmd));
    let mut cmd = scoped;
    cmd.table = format!("pg_temp.{TARGET}");
    cases.push(("retargeted scope", cmd));
    cases
}

#[test]
fn insert_select_scope_rejects_mutation_at_both_renderers() {
    setup();
    let mut accepted = Vec::new();
    for (name, cmd) in mutated_inserts() {
        if AstEncoder::encode_cmd_sql(&cmd).is_ok() {
            accepted.push(format!("native: {name}"));
        }
        if !cmd.to_sql().contains("INVALID APPLIED INSERT SCOPE") {
            accepted.push(format!("preview: {name}"));
        }
    }
    assert!(
        accepted.is_empty(),
        "scope mutations accepted: {accepted:?}"
    );
}

#[test]
fn insert_select_scope_checks_registry_dimensions_and_nested_commands() {
    setup();
    let ctx = RlsContext::tenant("tenant-a").with_user("user-a");
    for target in [
        "unregistered_target",
        "qail_scope_owner_only",
        "qail_scope_ambiguous",
    ] {
        assert!(
            insert(target, &["id"], &["id"]).with_rls(&ctx).is_err(),
            "{target}"
        );
    }
    let mut incomplete = insert(TARGET, &["id"], &["id"]);
    incomplete.source_query.as_mut().unwrap().table = OWNER_SOURCE.into();
    assert!(incomplete.with_rls(&ctx).is_err());
    let mut different_column = insert(TARGET, &["id"], &["id"]);
    different_column.source_query.as_mut().unwrap().table = "qail_scope_org_source".into();
    let cmd = different_column.with_rls(&ctx).unwrap();
    assert!(
        cmd.to_sql()
            .contains("qail_scope_org_source.org_id = 'tenant-a'")
    );
    assert!(AstEncoder::encode_cmd_sql(&cmd).is_ok());

    let mut early = Qail::add(TARGET)
        .with_rls(&ctx)
        .unwrap()
        .columns(["id", "tenant_id"]);
    early.source_query = Some(Box::new(Qail::get(SOURCE).columns(["id", "tenant_id"])));
    assert!(AstEncoder::encode_cmd_sql(&early).is_err());
    assert_eq!(early.to_sql(), "INVALID APPLIED INSERT SCOPE");

    let owned = insert(OWNED, &["id"], &["id"])
        .with_rls(&RlsContext::global().with_user("user-a"))
        .unwrap();
    assert!(AstEncoder::encode_cmd_sql(&owned).is_ok());
    assert_eq!(
        owned.source_query.as_ref().unwrap().columns[1],
        Expr::Literal(Value::Null)
    );
    let forbidden = owned.on_conflict_update(
        &["id"],
        &[("owner_id", Expr::Literal(Value::String("user-b".into())))],
    );
    assert!(AstEncoder::encode_cmd_sql(&forbidden).is_err());
    assert_eq!(forbidden.to_sql(), "INVALID APPLIED INSERT SCOPE");

    let invalid = mutated_inserts().remove(0).1;
    let wrapped = Qail::get("scoped_cte").with("scoped_cte", invalid.clone());
    assert!(AstEncoder::encode_cmd_sql(&wrapped).is_err());
    assert!(wrapped.to_sql().contains("INVALID APPLIED INSERT SCOPE"));
    let mut set = Qail::get(SOURCE).columns(["id", "tenant_id"]);
    set.set_ops
        .push((qail_core::ast::SetOp::Union, Box::new(invalid.clone())));
    assert!(AstEncoder::encode_cmd_sql(&set).is_err());
    assert!(set.to_sql().contains("INVALID APPLIED INSERT SCOPE"));
    for cmd in [invalid, wrapped, set] {
        let decoded =
            qail_core::wire::decode_cmd_binary(&qail_core::wire::encode_cmd_binary(&cmd).unwrap())
                .unwrap();
        assert!(AstEncoder::encode_cmd_sql(&decoded).is_err());
        assert!(decoded.to_sql().contains("INVALID APPLIED INSERT SCOPE"));
    }
    // Harmless edits and explicit rescoping remain usable.
    let cmd = insert(TARGET, &["id"], &["id"])
        .with_rls(&ctx)
        .unwrap()
        .returning(["id"]);
    assert!(AstEncoder::encode_cmd_sql(&cmd).is_ok());
    assert!(!cmd.to_sql().contains("INVALID"));
    let corrected = mutated_inserts().remove(0).1.with_rls(&ctx).unwrap();
    assert!(AstEncoder::encode_cmd_sql(&corrected).is_ok());
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL; synthetic TEMP tables only"]
async fn insert_select_scope_live_rejects_mutation_without_writes() -> PgResult<()> {
    setup();
    let url = std::env::var("QAIL_TEST_DB_URL").expect("explicit scratch database required");
    let mut driver = PgDriver::connect_url(&url).await?;
    driver.execute_simple(&format!(
        "CREATE TEMP TABLE {SOURCE} (id text, tenant_id text, enabled boolean);
         CREATE TEMP TABLE {TARGET} (id text PRIMARY KEY DEFAULT 'default', tenant_id text DEFAULT 'tenant-b');
         INSERT INTO {SOURCE} VALUES ('1', 'tenant-a', true), ('2', 'tenant-b', true)"
    )).await?;
    let mut accepted = Vec::new();
    for native in [false, true] {
        for (name, cmd) in mutated_inserts() {
            driver.execute_simple(&format!("TRUNCATE {TARGET}")).await?;
            if name == "late tenant conflict assignment" {
                driver
                    .execute_simple(&format!("INSERT INTO {TARGET} VALUES ('1', 'tenant-a')"))
                    .await?;
            }
            let before = driver
                .simple_query(&format!("SELECT id, tenant_id FROM {TARGET} ORDER BY id"))
                .await?;
            let before: Vec<_> = before.iter().map(|r| (r.text(0), r.text(1))).collect();
            let result = if native {
                driver.fetch_all_uncached(&cmd).await
            } else {
                driver.simple_query(&cmd.to_sql()).await
            };
            let after = driver
                .simple_query(&format!("SELECT id, tenant_id FROM {TARGET} ORDER BY id"))
                .await?;
            let after: Vec<_> = after.iter().map(|r| (r.text(0), r.text(1))).collect();
            println!(
                "native={native} mutation={name}: rejected={} before={before:?} after={after:?}",
                result.is_err()
            );
            if result.is_ok() || before != after {
                accepted.push(format!("native={native}: {name}"));
            }
        }
    }
    assert!(
        accepted.is_empty(),
        "scope mutations executed: {accepted:?}"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "Requires QAIL_TEST_DB_URL; synthetic TEMP tables only"]
async fn insert_select_scope_live_isolates_preview_and_native() -> PgResult<()> {
    setup();
    let url = std::env::var("QAIL_TEST_DB_URL")
        .unwrap_or_else(|_| "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".into());
    let mut driver = PgDriver::connect_url(&url).await?;
    driver
        .execute_simple(&format!(
            "CREATE TEMP TABLE {SOURCE} (id integer, tenant_id text, enabled boolean);
         CREATE TEMP TABLE {TARGET} (id integer PRIMARY KEY, tenant_id text);
         CREATE TEMP TABLE {OWNED} (id integer, tenant_id text, owner_id text);
         CREATE TEMP TABLE {OWNER_SOURCE} (id integer, tenant_id text, owner_id text);
         INSERT INTO {OWNER_SOURCE} VALUES (1, 'tenant-a', 'user-a'),
         (2, 'tenant-a', 'user-b'), (3, 'tenant-b', 'user-a');
         INSERT INTO {SOURCE} VALUES (1, 'tenant-a', true), (2, 'tenant-b', true),
         (3, NULL, true), (4, 'tenant-a', false)"
        ))
        .await?;
    for native in [false, true] {
        for explicit in [false, true] {
            driver.execute_simple(&format!("TRUNCATE {TARGET}")).await?;
            let columns = if explicit {
                vec!["id", "tenant_id"]
            } else {
                vec!["id"]
            };
            let cmd = insert(TARGET, &columns, &columns)
                .returning(["id", "tenant_id"])
                .with_rls(&RlsContext::tenant("tenant-a"))
                .unwrap();
            let rows = if native {
                driver.fetch_all_uncached(&cmd).await?
            } else {
                driver.simple_query(&cmd.to_sql()).await?
            };
            let cells: Vec<_> = rows.iter().map(|r| (r.text(0), r.text(1))).collect();
            println!("native={native} explicit={explicit}: {cells:?}");
            assert_eq!(cells, vec![("1".into(), "tenant-a".into())]);
        }
        driver.execute_simple(&format!("TRUNCATE {TARGET}")).await?;
        let mut forged = insert(TARGET, &["id", "tenant_id"], &["id", "tenant_id"]);
        forged.source_query.as_mut().unwrap().columns[1] =
            Expr::Literal(Value::String("tenant-b".into()));
        let cmd = forged
            .returning(["id", "tenant_id"])
            .with_rls(&RlsContext::tenant("tenant-a"))
            .unwrap();
        let rows = if native {
            driver.fetch_all_uncached(&cmd).await?
        } else {
            driver.simple_query(&cmd.to_sql()).await?
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].text(1), "tenant-a");

        driver.execute_simple(&format!("TRUNCATE {TARGET}")).await?;
        let cmd = insert(TARGET, &["id"], &["id"])
            .returning(["id", "tenant_id"])
            .with_rls(&RlsContext::global())
            .unwrap();
        let rows = if native {
            driver.fetch_all_uncached(&cmd).await?
        } else {
            driver.simple_query(&cmd.to_sql()).await?
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].text(0), "3");
        assert!(rows[0].columns[1].is_none());

        driver.execute_simple(&format!("TRUNCATE {OWNED}")).await?;
        let mut cmd = insert(
            OWNED,
            &["id", "tenant_id", "owner_id"],
            &["id", "tenant_id", "id"],
        );
        cmd.source_query.as_mut().unwrap().columns[2] =
            Expr::Literal(Value::String("user-b".into()));
        let cmd = cmd
            .returning(["id", "tenant_id", "owner_id"])
            .with_rls(&RlsContext::tenant("tenant-a").with_user("user-a"))
            .unwrap();
        let rows = if native {
            driver.fetch_all_uncached(&cmd).await?
        } else {
            driver.simple_query(&cmd.to_sql()).await?
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].text(1), "tenant-a");
        assert_eq!(rows[0].text(2), "user-a");
        println!("native={native}: forged tenant/owner replaced; global NULL isolated");
        driver.execute_simple(&format!("TRUNCATE {OWNED}")).await?;
        let mut cmd = Qail::add(OWNED).columns(["id"]);
        cmd.source_query = Some(Box::new(Qail::get(OWNER_SOURCE).columns(["id"])));
        let cmd = cmd
            .returning(["id", "tenant_id", "owner_id"])
            .with_rls(&RlsContext::tenant("tenant-a").with_user("user-a"))
            .unwrap();
        let rows = if native {
            driver.fetch_all_uncached(&cmd).await?
        } else {
            driver.simple_query(&cmd.to_sql()).await?
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].text(0), "1");
        println!("native={native}: source tenant and owner predicates preserved");

        driver.execute_simple(&format!("TRUNCATE {TARGET}")).await?;
        let cmd = Qail::add(TARGET)
            .set_value("id", 8)
            .with_rls(&RlsContext::tenant("tenant-a"))
            .unwrap()
            .returning(["id", "tenant_id"]);
        let rows = if native {
            driver.fetch_all_uncached(&cmd).await?
        } else {
            driver.simple_query(&cmd.to_sql()).await?
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].text(1), "tenant-a");
    }
    driver
        .execute_simple(&format!(
            "INSERT INTO {SOURCE} VALUES (2, 'tenant-a', true)"
        ))
        .await?;
    for native in [false, true] {
        for before in [false, true] {
            driver.execute_simple(&format!("TRUNCATE {TARGET}; INSERT INTO {TARGET} VALUES (1, 'tenant-a'), (2, 'tenant-b')")).await?;
            let ctx = RlsContext::tenant("tenant-a");
            let mut cmd = insert(TARGET, &["id"], &["id"]).returning(["id", "tenant_id"]);
            if before {
                cmd = cmd.with_rls(&ctx).unwrap();
            }
            cmd = cmd.on_conflict_update(&["id"], &[("id", Expr::Named("EXCLUDED.id".into()))]);
            if !before {
                cmd = cmd.with_rls(&ctx).unwrap();
            }
            let rows = if native {
                driver.fetch_all_uncached(&cmd).await?
            } else {
                driver.simple_query(&cmd.to_sql()).await?
            };
            assert_eq!(rows.len(), 1, "foreign-tenant conflict must not update");
            assert_eq!(rows[0].text(0), "1");
            let rows = driver
                .simple_query(&format!("SELECT tenant_id FROM {TARGET} WHERE id = 2"))
                .await?;
            assert_eq!(rows[0].text(0), "tenant-b");
            println!(
                "native={native} scope-before-conflict={before}: own update returned, foreign update skipped"
            );
        }
    }
    driver.execute_simple(&format!(
        "CREATE TEMP TABLE {IDENTITY} (id integer, tenant_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 900));
         CREATE TEMP TABLE qail_scope_identity_input (id integer, tenant_id bigint);
         INSERT INTO qail_scope_identity_input VALUES (1, 42)"
    )).await?;
    for native in [false, true] {
        let mut cmd = Qail::add(IDENTITY).columns(["id", "tenant_id"]);
        cmd.source_query = Some(Box::new(
            Qail::get("qail_scope_identity_input").columns(["id", "tenant_id"]),
        ));
        assert!(
            cmd.clone()
                .overriding_user_value()
                .with_rls(&RlsContext::tenant("42"))
                .is_err()
        );
        let cmd = cmd
            .overriding_system_value()
            .returning(["id", "tenant_id"])
            .with_rls(&RlsContext::tenant("42"))
            .unwrap();
        let rows = if native {
            driver.fetch_all_uncached(&cmd).await?
        } else {
            driver.simple_query(&cmd.to_sql()).await?
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get_i64(1), Some(42));
        let before = driver
            .simple_query(&format!("SELECT count(*) FROM {IDENTITY}"))
            .await?[0]
            .text(0);
        let late_user = cmd.overriding_user_value();
        let result = if native {
            driver.fetch_all_uncached(&late_user).await
        } else {
            driver.simple_query(&late_user.to_sql()).await
        };
        assert!(result.is_err());
        let after = driver
            .simple_query(&format!("SELECT count(*) FROM {IDENTITY}"))
            .await?[0]
            .text(0);
        assert_eq!(
            before, after,
            "late USER override must not insert sequence-generated scope"
        );
        println!("native={native}: USER VALUE refused; SYSTEM VALUE stored tenant 42");
    }
    Ok(())
}
