//! Live PostgreSQL checks for MERGE assignment DEFAULT, ONLY inheritance
//! selection, and INSERT-arm DEFAULT VALUES / OVERRIDING, including the
//! tenant stamp `with_rls` adds to those arms. TEMP tables only.
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test merge_shapes_live -- --ignored --nocapture

use qail_core::ast::{Expr, Operator, OverridingKind, Qail};
use qail_core::error::QailBuildError;
use qail_core::parser::parse;
use qail_core::rls::RlsContext;
use qail_pg::{PgDriver, PgResult};
use uuid::Uuid;

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

fn temp_name(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

async fn rows(driver: &mut PgDriver, select: &str) -> PgResult<Vec<String>> {
    Ok(driver
        .simple_query(select)
        .await?
        .into_iter()
        .map(|row| row.get_string(0).unwrap_or_default())
        .collect())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL 17+; set QAIL_TEST_DB_URL"]
async fn merge_assignment_and_insert_default_use_column_defaults() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let target = temp_name("qail_merge_dflt_t");
    let source = temp_name("qail_merge_dflt_s");
    driver
        .execute_simple(&format!(
            "CREATE TEMP TABLE {target} (id int PRIMARY KEY, name text DEFAULT 'dflt-name', \
             status text DEFAULT 'dflt-status');
             CREATE TEMP TABLE {source} (id int, name text);
             INSERT INTO {target} VALUES (1, 'old', 'old');
             INSERT INTO {source} VALUES (1, 'src-1'), (2, 'src-2');"
        ))
        .await?;

    let cmd = parse(&format!(
        "merge {target} as t using {source} as s on t.id = s.id \
         when matched then update set name = default \
         when not matched then insert (id, name, status) values (s.id, s.name, default)"
    ))
    .expect("parse");
    let affected = driver.execute(&cmd).await?;
    let got = rows(
        &mut driver,
        &format!("SELECT concat_ws('|', id, name, status) FROM {target} ORDER BY id"),
    )
    .await?;
    println!("F7 affected={affected} rows={got:?}");

    assert_eq!(affected, 2);
    assert_eq!(got, ["1|dflt-name|old", "2|src-2|dflt-status"]);
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL 17+; set QAIL_TEST_DB_URL"]
async fn merge_only_target_and_source_skip_inheritance_children() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let parent = temp_name("qail_merge_only_p");
    let child = temp_name("qail_merge_only_c");
    let source = temp_name("qail_merge_only_s");
    let source_child = temp_name("qail_merge_only_sc");
    driver
        .execute_simple(&format!(
            "CREATE TEMP TABLE {parent} (id int, name text);
             CREATE TEMP TABLE {child} () INHERITS ({parent});
             CREATE TEMP TABLE {source} (id int, name text);
             CREATE TEMP TABLE {source_child} () INHERITS ({source});
             INSERT INTO {parent} VALUES (1, 'parent'), (2, 'parent');
             INSERT INTO {child} VALUES (1, 'child'), (2, 'child');
             INSERT INTO {source} VALUES (1, 'from-source');
             INSERT INTO {source_child} VALUES (2, 'from-source-child');"
        ))
        .await?;

    let cmd = Qail::merge_into(&parent)
        .only()
        .target_alias("t")
        .using_only_table_as(&source, "s")
        .merge_on_column("t.id", Operator::Eq, "s.id")
        .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);
    let affected = driver.execute(&cmd).await?;
    let got = rows(
        &mut driver,
        &format!(
            "SELECT concat_ws('|', tableoid::regclass::text = '{child}', id, name) \
             FROM {parent} ORDER BY 1"
        ),
    )
    .await?;
    println!("F8 ONLY affected={affected} rows={got:?}");

    assert_eq!(
        affected, 1,
        "only parent row 1 matches the parent-only source"
    );
    assert_eq!(
        got,
        ["f|1|from-source", "f|2|parent", "t|1|child", "t|2|child"]
    );

    let control = Qail::merge_into(&parent)
        .target_alias("t")
        .using_table_as(&source, "s")
        .merge_on_column("t.id", Operator::Eq, "s.id")
        .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);
    let control_affected = driver.execute(&control).await?;
    println!("F8 control (no ONLY) affected={control_affected}");
    assert_eq!(
        control_affected, 4,
        "without ONLY both sides include children"
    );
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL 17+; set QAIL_TEST_DB_URL"]
async fn merge_insert_overriding_and_default_values_shapes() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let always = temp_name("qail_merge_ovr_a");
    let by_default = temp_name("qail_merge_ovr_d");
    let source = temp_name("qail_merge_ovr_s");
    driver
        .execute_simple(&format!(
            "CREATE TEMP TABLE {always} (id int GENERATED ALWAYS AS IDENTITY (START 100) \
             PRIMARY KEY, name text DEFAULT 'dflt');
             CREATE TEMP TABLE {by_default} (id int GENERATED BY DEFAULT AS IDENTITY \
             (START 500) PRIMARY KEY, name text);
             CREATE TEMP TABLE {source} (id int, name text);
             INSERT INTO {source} VALUES (1, 'a'), (2, 'b');"
        ))
        .await?;

    let defaults = Qail::merge_into(&always)
        .target_alias("t")
        .using_table_as(&source, "s")
        .merge_on_column("t.id", Operator::Eq, "s.id")
        .when_not_matched_insert_default_values();
    let defaults_affected = driver.execute(&defaults).await?;
    let system = parse(&format!(
        "merge {always} as t using {source} as s on t.id = s.id \
         when not matched then insert (id, name) overriding system value values (s.id, s.name)"
    ))
    .expect("parse");
    let system_affected = driver.execute(&system).await?;
    let always_rows = rows(
        &mut driver,
        &format!("SELECT concat_ws('|', id, name) FROM {always} ORDER BY id"),
    )
    .await?;
    println!(
        "F9 SYSTEM affected={system_affected} DEFAULT VALUES affected={defaults_affected} \
         rows={always_rows:?}"
    );
    assert_eq!(system_affected, 2);
    assert_eq!(defaults_affected, 2);
    assert_eq!(always_rows, ["1|a", "2|b", "100|dflt", "101|dflt"]);

    let user = Qail::merge_into(&by_default)
        .target_alias("t")
        .using_table_as(&source, "s")
        .merge_on_column("t.id", Operator::Eq, "s.id")
        .when_not_matched_insert_overriding(
            OverridingKind::UserValue,
            &["id", "name"],
            &[
                Expr::Named("s.id".to_string()),
                Expr::Named("s.name".to_string()),
            ],
        );
    let user_affected = driver.execute(&user).await?;
    let user_rows = rows(
        &mut driver,
        &format!("SELECT concat_ws('|', id, name) FROM {by_default} ORDER BY id"),
    )
    .await?;
    println!("F9 USER affected={user_affected} rows={user_rows:?}");
    assert_eq!(user_affected, 2);
    assert_eq!(user_rows, ["500|a", "501|b"]);
    Ok(())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL 17+; set QAIL_TEST_DB_URL"]
async fn merge_with_rls_stamps_tenant_on_default_values_and_overriding_arms() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let target = temp_name("qail_merge_rls_t");
    let source = temp_name("qail_merge_rls_s");
    driver
        .execute_simple(&format!(
            "CREATE TEMP TABLE {target} (id int GENERATED ALWAYS AS IDENTITY (START 900) \
             PRIMARY KEY, name text DEFAULT 'dflt', tenant_id text DEFAULT 'unstamped');
             CREATE TEMP TABLE {source} (id int, name text, tenant_id text);
             INSERT INTO {source} VALUES (1, 'a1', 'tenant-a'), (2, 'b2', 'tenant-b');"
        ))
        .await?;
    // Boundary API: registers the tables AND seals the process `Initialized`.
    qail_core::rls::init_scope_registries_from_tables(
        &[(&target, "tenant_id"), (&source, "tenant_id")],
        &[],
    )
    .expect("scope registries seal");
    let tenant_a = RlsContext::tenant("tenant-a");

    let defaults = Qail::merge_into(&target)
        .target_alias("t")
        .using_table_as(&source, "s")
        .merge_on_column("t.name", Operator::Eq, "s.name")
        .when_not_matched_insert_default_values()
        .with_rls(&tenant_a)
        .expect("DEFAULT VALUES arm is stamped");
    let defaults_affected = driver.execute(&defaults).await?;

    let system = Qail::merge_into(&target)
        .target_alias("t")
        .using_table_as(&source, "s")
        .merge_on_column("t.id", Operator::Eq, "s.id")
        .when_not_matched_insert_overriding(
            OverridingKind::SystemValue,
            &["id", "name", "tenant_id"],
            &[
                Expr::Named("s.id".to_string()),
                Expr::Named("s.name".to_string()),
                Expr::Default,
            ],
        )
        .with_rls(&tenant_a)
        .expect("OVERRIDING SYSTEM VALUE arm is stamped");
    let system_affected = driver.execute(&system).await?;

    let user = Qail::merge_into(&target)
        .using_table_as(&source, "s")
        .merge_on_column("id", Operator::Eq, "s.id")
        .when_not_matched_insert_overriding(
            OverridingKind::UserValue,
            &["id"],
            &[Expr::Named("s.id".to_string())],
        )
        .with_rls(&tenant_a)
        .expect_err("OVERRIDING USER VALUE is refused for a tenant table");

    let got = rows(
        &mut driver,
        &format!("SELECT concat_ws('|', id, name, tenant_id) FROM {target} ORDER BY id"),
    )
    .await?;
    println!(
        "RLS DEFAULT VALUES affected={defaults_affected} SYSTEM affected={system_affected} \
         USER err={user} rows={got:?}"
    );

    assert_eq!(defaults_affected, 1, "only the tenant-a source row inserts");
    assert_eq!(system_affected, 1, "only the tenant-a source row inserts");
    assert!(matches!(
        user,
        QailBuildError::RlsMergeOverridingUserValueDenied { .. }
    ));
    assert_eq!(got, ["1|a1|tenant-a", "900|dflt|tenant-a"]);
    Ok(())
}
