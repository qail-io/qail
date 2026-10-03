//! DDL properties read by `qail pull` encode identically in the native encoder
//! and the SQL preview, and the server keeps them (gaps.md A5, D12, D13, D14,
//! E12, F12).
//!
//! Live check (temp objects only):
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test ddl_pull_identity -- --ignored --nocapture

use qail_core::ast::{
    Action, ColumnGeneration, Constraint, Expr, FunctionDef, FunctionOptions, IdentityOptions,
    IndexDef, Qail, TriggerDef, TriggerEvent, TriggerTiming,
};
use qail_core::transpiler::ToSql;
use qail_pg::protocol::AstEncoder;
use qail_pg::{PgDriver, PgResult};

fn native(cmd: &Qail) -> Result<String, String> {
    AstEncoder::encode_cmd_sql(cmd)
        .map(|(sql, params)| {
            assert!(params.is_empty(), "DDL carries no bind params");
            sql
        })
        .map_err(|e| e.to_string())
}

fn table_cmd(table: &str) -> Qail {
    Qail {
        action: Action::Make,
        table: table.to_string(),
        columns: vec![
            Expr::Def {
                name: "id".into(),
                data_type: "BIGINT".into(),
                constraints: vec![
                    Constraint::PrimaryKey,
                    Constraint::Generated(ColumnGeneration::Identity {
                        by_default: true,
                        options: IdentityOptions {
                            start: Some(100),
                            increment: Some(5),
                            min_value: Some(50),
                            max_value: Some(100_000),
                            cache: Some(10),
                            cycle: true,
                        },
                    }),
                ],
            },
            Expr::Def {
                name: "name".into(),
                data_type: "TEXT".into(),
                constraints: vec![Constraint::Nullable, Constraint::Collate("C".into())],
            },
            Expr::Def {
                name: "doc".into(),
                data_type: "JSON".into(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "label".into(),
                data_type: "VARCHAR".into(),
                constraints: vec![Constraint::Nullable],
            },
            Expr::Def {
                name: "seq_val".into(),
                data_type: "INT".into(),
                constraints: vec![Constraint::Nullable],
            },
        ],
        ..Default::default()
    }
}

fn index_cmd(table: &str) -> Qail {
    Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "qail_probe_name_idx".into(),
            table: table.to_string(),
            columns: vec!["name".into()],
            storage_params: vec!["fillfactor=70".into()],
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn function_cmd(name: &str) -> Qail {
    Qail {
        action: Action::CreateFunction,
        function_def: Some(FunctionDef {
            name: name.to_string(),
            args: vec!["x int".into()],
            returns: "int".into(),
            body: "SELECT x + 1".into(),
            language: Some("sql".into()),
            volatility: Some("immutable".into()),
            options: FunctionOptions {
                strict: true,
                security_definer: true,
                leakproof: false,
                parallel: Some("safe".into()),
                cost: Some("50".into()),
                rows: None,
                config: vec!["search_path TO 'pg_catalog', 'pg_temp'".into()],
            },
        }),
        ..Default::default()
    }
}

fn trigger_cmd(table: &str, name: &str, when: bool) -> Qail {
    Qail {
        action: Action::CreateTrigger,
        trigger_def: Some(TriggerDef {
            name: name.to_string(),
            table: table.to_string(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Update],
            update_columns: vec![],
            for_each_row: when,
            execute_function: "qail_probe_trg_fn".into(),
            condition: when.then(|| "old.name IS DISTINCT FROM new.name".to_string()),
            old_table: (!when).then(|| "old_rows".to_string()),
            new_table: (!when).then(|| "new_rows".to_string()),
        }),
        ..Default::default()
    }
}

fn alter_sequence_cmd(seq: &str, owner: &str) -> Qail {
    Qail {
        action: Action::AlterSequence,
        table: seq.to_string(),
        columns: vec![Expr::Named(format!("OWNED BY {owner}"))],
        ..Default::default()
    }
}

#[test]
fn native_encoder_matches_sql_preview_for_pulled_properties() {
    let cmds = [
        table_cmd("t"),
        index_cmd("t"),
        function_cmd("f"),
        trigger_cmd("t", "trg_when", true),
        trigger_cmd("t", "trg_transition", false),
        alter_sequence_cmd("s", "t.seq_val"),
    ];
    // The preview pretty-prints CREATE TABLE across lines; compare token text.
    let squash = |sql: &str| sql.split_whitespace().collect::<String>();
    for cmd in &cmds {
        let native = native(cmd).expect("native encodes");
        assert_eq!(squash(&native), squash(&cmd.to_sql()), "{:?}", cmd.action);
    }
    let table = native(&cmds[0]).unwrap();
    assert!(table.contains("GENERATED BY DEFAULT AS IDENTITY (START WITH 100 INCREMENT BY 5 MINVALUE 50 MAXVALUE 100000 CACHE 10 CYCLE)"), "{table}");
    assert!(table.contains("name TEXT COLLATE \"C\""), "{table}");
    // Uppercase JSON / VARCHAR are exact schema types, not DSL shorthands.
    assert!(table.contains("doc JSON"), "{table}");
    assert!(table.contains("label VARCHAR"), "{table}");
    assert!(
        !table.contains("JSONB") && !table.contains("VARCHAR(255)"),
        "{table}"
    );
    assert!(
        native(&cmds[1])
            .unwrap()
            .ends_with("WITH (fillfactor = '70')")
    );
    assert!(
        native(&cmds[2])
            .unwrap()
            .contains("IMMUTABLE STRICT SECURITY DEFINER PARALLEL SAFE COST 50 SET search_path TO 'pg_catalog', 'pg_temp' AS")
    );
    assert!(
        native(&cmds[3])
            .unwrap()
            .contains("FOR EACH ROW WHEN (old.name IS DISTINCT FROM new.name) EXECUTE")
    );
    assert!(
        native(&cmds[4])
            .unwrap()
            .contains("REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT")
    );
    assert_eq!(
        native(&cmds[5]).unwrap(),
        "ALTER SEQUENCE s OWNED BY t.seq_val"
    );
}

#[test]
fn native_encoder_rejects_unsafe_pulled_properties() {
    let mut bad_collate = table_cmd("t");
    if let Expr::Def { constraints, .. } = &mut bad_collate.columns[1] {
        constraints.push(Constraint::Collate("POSIX".into()));
    }
    assert!(native(&bad_collate).is_err(), "two collations");

    let mut bad_set = function_cmd("f");
    bad_set.function_def.as_mut().unwrap().options.config =
        vec!["search_path TO x; DROP TABLE t".into()];
    assert!(native(&bad_set).is_err());

    let mut bad_when = trigger_cmd("t", "x", true);
    bad_when.trigger_def.as_mut().unwrap().condition =
        Some("true) EXECUTE FUNCTION evil(); --".into());
    assert!(native(&bad_when).is_err());

    let mut bad_param = index_cmd("t");
    bad_param.index_def.as_mut().unwrap().storage_params = vec!["fill;factor=1".into()];
    assert!(native(&bad_param).is_err());

    let empty_alter = Qail {
        action: Action::AlterSequence,
        table: "s".into(),
        ..Default::default()
    };
    assert!(native(&empty_alter).is_err());
}

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

async fn one_row(driver: &mut PgDriver, sql: &str) -> PgResult<Vec<String>> {
    let rows = driver.simple_query(sql).await?;
    let row = rows.first().expect("one row");
    Ok((0..row.len()).map(|i| row.text(i)).collect())
}

#[tokio::test]
#[ignore = "Requires a local PostgreSQL in QAIL_TEST_DB_URL (temp objects only)"]
async fn server_keeps_encoded_pull_properties() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let table = "pg_temp.qail_probe_pull";
    driver
        .execute_simple("CREATE SEQUENCE pg_temp.qail_probe_seq START 7 CACHE 3")
        .await?;
    driver
        .execute_simple(
            "CREATE FUNCTION pg_temp.qail_probe_trg_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$",
        )
        .await?;
    for cmd in [
        table_cmd(table),
        index_cmd(table),
        function_cmd("pg_temp.qail_probe_fn"),
        alter_sequence_cmd("pg_temp.qail_probe_seq", "pg_temp.qail_probe_pull.seq_val"),
    ] {
        let sql = native(&cmd).expect("encodes");
        println!("{sql}");
        driver.execute(&cmd).await?;
    }
    for (name, when) in [("qail_probe_when", true), ("qail_probe_transition", false)] {
        let mut cmd = trigger_cmd(table, name, when);
        cmd.trigger_def.as_mut().unwrap().execute_function = "pg_temp.qail_probe_trg_fn".into();
        println!("{}", native(&cmd).expect("encodes"));
        driver.execute(&cmd).await?;
    }

    let identity = one_row(
        &mut driver,
        "SELECT a.attidentity, q.seqstart, q.seqincrement, q.seqmin, q.seqmax, q.seqcache, q.seqcycle \
         FROM pg_attribute a JOIN pg_depend d ON d.refobjid = a.attrelid AND d.refobjsubid = a.attnum AND d.deptype = 'i' \
         JOIN pg_sequence q ON q.seqrelid = d.objid \
         WHERE a.attrelid = 'pg_temp.qail_probe_pull'::regclass AND a.attname = 'id'",
    )
    .await?;
    println!("identity: {identity:?}");
    assert_eq!(identity, ["d", "100", "5", "50", "100000", "10", "t"]);

    let columns = one_row(
        &mut driver,
        "SELECT (SELECT collname FROM pg_collation WHERE oid = (SELECT attcollation FROM pg_attribute WHERE attrelid = 'pg_temp.qail_probe_pull'::regclass AND attname = 'name')), \
                format_type((SELECT atttypid FROM pg_attribute WHERE attrelid = 'pg_temp.qail_probe_pull'::regclass AND attname = 'doc'), -1), \
                (SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'pg_temp.qail_probe_pull'::regclass AND attname = 'label')",
    )
    .await?;
    println!("columns: {columns:?}");
    assert_eq!(columns, ["C", "json", "character varying"]);

    let index = one_row(
        &mut driver,
        "SELECT array_to_string(reloptions, ',') FROM pg_class WHERE oid = 'pg_temp.qail_probe_name_idx'::regclass",
    )
    .await?;
    println!("index reloptions: {index:?}");
    assert_eq!(index, ["fillfactor=70"]);

    let function = one_row(
        &mut driver,
        "SELECT provolatile, proisstrict, prosecdef, proparallel, procost, array_to_string(proconfig, ';') \
         FROM pg_proc WHERE oid = 'pg_temp.qail_probe_fn(int)'::regprocedure",
    )
    .await?;
    println!("function: {function:?}");
    assert_eq!(
        function,
        ["i", "t", "t", "s", "50", "search_path=pg_catalog, pg_temp"]
    );

    let owner = one_row(
        &mut driver,
        "SELECT d.deptype, a.attname FROM pg_depend d \
         JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid \
         WHERE d.objid = 'pg_temp.qail_probe_seq'::regclass AND d.classid = 'pg_class'::regclass AND d.refclassid = 'pg_class'::regclass",
    )
    .await?;
    println!("sequence owner: {owner:?}");
    assert_eq!(owner, ["a", "seq_val"]);

    let triggers = driver
        .simple_query(
            "SELECT pg_get_triggerdef(oid) FROM pg_trigger \
             WHERE tgrelid = 'pg_temp.qail_probe_pull'::regclass ORDER BY tgname",
        )
        .await?;
    let triggers: Vec<String> = triggers.iter().map(|row| row.text(0)).collect();
    println!("triggers: {triggers:#?}");
    assert!(
        triggers[0]
            .contains("REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT")
    );
    assert!(triggers[1].contains("FOR EACH ROW WHEN ((old.name IS DISTINCT FROM new.name))"));
    Ok(())
}
