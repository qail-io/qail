//! Schema metadata that `qail pull` reads must survive schema text, the
//! migration model, and both DDL renderers (SQL preview and native encoder
//! share the fragment helpers tested here).
//!
//! Covers gaps.md A5, D11, D12, D13, D14, E12, F12.

use qail_core::ast::{
    Action, ColumnGeneration, Constraint, Expr, FunctionDef, FunctionOptions, IdentityOptions,
    IndexDef, Qail, TriggerDef, TriggerEvent, TriggerTiming,
};
use qail_core::migrate::{
    Column, ColumnType, Generated, Index, Schema, Sequence, Table, diff_schemas_checked,
    parse_qail, schema_to_commands, to_qail_string,
};
use qail_core::transpiler::ToSql;

const PULLED: &str = r#"# QAIL Schema

sequence owner_seq { start 500 increment 1 minvalue 1 maxvalue 9223372036854775807 cache 5 owned_by owner_t.id }
sequence seq_custom { as integer start 10 increment 3 minvalue 1 maxvalue 1000 cache 20 cycle }

table gen_t {
  id INT primary_key
  a INT not_null
  v INT generated_virtual((a * 2))
  s INT generated_stored((a + 1))
}

table ident_t {
  id BIGINT primary_key generated_by_default_identity(start 100 increment 5 minvalue 50 maxvalue 100000 cache 10 cycle)
  n INT not_null generated_identity
  name TEXT collate "C"
  label VARCHAR(20) collate "POSIX"
}

table owner_t {
  id INT primary_key default nextval('owner_seq'::regclass)
}

table types_t {
  id INT primary_key
  j JSON
  jb JSONB
  r REAL
  d DOUBLE PRECISION
  c CHARACTER(3)
  ts3 TIMESTAMP(3)
  tstz0 TIMESTAMPTZ(0)
  t2 TIME(2)
  ttz TIMETZ
  b BIT(8)
  vb VARBIT(16)
  arr VARCHAR(20)[]
  iv INTERVAL DAY TO SECOND(3)
}

table trig_t {
  id INT primary_key
  val INT
}

index idx_fill on types_t (id) with (fillfactor=70)
function f_over(x int4) returns integer language SQL immutable strict parallel safe $$
SELECT x + 1
$$

function f_secure() returns integer language PLPGSQL stable security_definer leakproof parallel restricted cost 50 set "search_path TO 'public', 'pg_temp'" $$
BEGIN RETURN 1; END
$$

function f_rows() returns SETOF integer language SQL rows 42 $$
SELECT 1
$$

trigger trig_stmt on trig_t after insert for_each_statement execute trg_fn
trigger trig_transition on trig_t after update for_each_statement old_table old_rows new_table new_rows execute trg_fn
trigger trig_when on trig_t before update of val execute trg_fn when (old.val IS DISTINCT FROM new.val)
"#;

fn column<'a>(schema: &'a Schema, table: &str, name: &str) -> &'a Column {
    schema.tables[table]
        .columns
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("{table}.{name}"))
}

#[test]
fn pulled_schema_text_parses_into_the_full_model() {
    let schema = parse_qail(PULLED).expect("pulled text parses");

    // A5: virtual stays virtual.
    assert!(matches!(
        &column(&schema, "gen_t", "v").generated,
        Some(Generated::AlwaysVirtual(expr)) if expr == "(a * 2)"
    ));
    assert!(matches!(
        &column(&schema, "gen_t", "s").generated,
        Some(Generated::AlwaysStored(expr)) if expr == "(a + 1)"
    ));

    // D11: distinct types keep their identity.
    let ty = |name: &str| column(&schema, "types_t", name).data_type.to_pg_type();
    assert_eq!(ty("j"), "JSON");
    assert_eq!(ty("jb"), "JSONB");
    assert_eq!(ty("r"), "REAL");
    assert_eq!(ty("d"), "DOUBLE PRECISION");
    assert_eq!(ty("c"), "CHARACTER(3)");
    assert_eq!(ty("ts3"), "TIMESTAMP(3)");
    assert_eq!(ty("tstz0"), "TIMESTAMPTZ(0)");
    assert_eq!(ty("t2"), "TIME(2)");
    assert_eq!(ty("ttz"), "TIMETZ");
    assert_eq!(ty("b"), "BIT(8)");
    assert_eq!(ty("vb"), "VARBIT(16)");
    assert_eq!(ty("arr"), "VARCHAR(20)[]");
    assert_eq!(ty("iv"), "INTERVAL DAY TO SECOND(3)");

    // D14: identity options and collation.
    let id = column(&schema, "ident_t", "id");
    assert!(matches!(id.generated, Some(Generated::ByDefaultIdentity)));
    assert_eq!(
        id.identity_options,
        IdentityOptions {
            start: Some(100),
            increment: Some(5),
            min_value: Some(50),
            max_value: Some(100_000),
            cache: Some(10),
            cycle: true,
        }
    );
    assert!(column(&schema, "ident_t", "n").identity_options.is_empty());
    assert_eq!(
        column(&schema, "ident_t", "name").collation.as_deref(),
        Some("C")
    );
    assert_eq!(
        column(&schema, "ident_t", "label").collation.as_deref(),
        Some("POSIX")
    );

    // D13: index storage parameters.
    assert_eq!(schema.indexes[0].storage_params, vec!["fillfactor=70"]);

    // F12: sequence type, cache, cycle, ownership.
    let seq = schema
        .sequences
        .iter()
        .find(|s| s.name == "seq_custom")
        .unwrap();
    assert_eq!(seq.data_type.as_deref(), Some("integer"));
    assert_eq!(seq.cache, Some(20));
    assert!(seq.cycle);
    let owned = schema
        .sequences
        .iter()
        .find(|s| s.name == "owner_seq")
        .unwrap();
    assert_eq!(owned.owned_by.as_deref(), Some("owner_t.id"));

    // D12: execution properties.
    let f_over = &schema.functions[0];
    assert_eq!(f_over.volatility.as_deref(), Some("immutable"));
    assert!(f_over.options.strict);
    assert_eq!(f_over.options.parallel.as_deref(), Some("safe"));
    let f_secure = &schema.functions[1];
    assert_eq!(f_secure.returns, "integer");
    assert!(f_secure.options.security_definer && f_secure.options.leakproof);
    assert_eq!(f_secure.options.parallel.as_deref(), Some("restricted"));
    assert_eq!(f_secure.options.cost.as_deref(), Some("50"));
    assert_eq!(
        f_secure.options.config,
        vec!["search_path TO 'public', 'pg_temp'"]
    );
    let f_rows = &schema.functions[2];
    assert_eq!(f_rows.returns, "SETOF integer");
    assert_eq!(f_rows.options.rows.as_deref(), Some("42"));

    // E12: orientation, transition tables, WHEN.
    let stmt = &schema.triggers[0];
    assert!(!stmt.for_each_row);
    let transition = &schema.triggers[1];
    assert!(!transition.for_each_row);
    assert_eq!(transition.old_table.as_deref(), Some("old_rows"));
    assert_eq!(transition.new_table.as_deref(), Some("new_rows"));
    let when = &schema.triggers[2];
    assert!(when.for_each_row);
    assert_eq!(
        when.condition.as_deref(),
        Some("old.val IS DISTINCT FROM new.val")
    );
}

#[test]
fn pulled_schema_text_round_trips_byte_for_byte() {
    let schema = parse_qail(PULLED).expect("pulled text parses");
    let emitted = to_qail_string(&schema);
    let reparsed = parse_qail(&emitted).expect("emitted text parses");
    assert_eq!(to_qail_string(&reparsed), emitted);
    for line in PULLED.lines().filter(|l| {
        l.starts_with("  ")
            || l.starts_with("sequence ")
            || l.starts_with("index ")
            || l.starts_with("function ")
            || l.starts_with("trigger ")
    }) {
        assert!(emitted.lines().any(|e| e == line), "lost line: {line}");
    }
}

#[test]
fn schema_commands_carry_every_property_into_ddl() {
    let schema = parse_qail(PULLED).expect("pulled text parses");
    let sql = schema_to_commands(&schema)
        .iter()
        .map(|cmd| cmd.to_sql())
        .collect::<Vec<_>>()
        .join(";\n");
    for expected in [
        "    v INT GENERATED ALWAYS AS ((a * 2)),",
        "GENERATED ALWAYS AS ((a + 1)) STORED",
        "GENERATED BY DEFAULT AS IDENTITY (START WITH 100 INCREMENT BY 5 MINVALUE 50 MAXVALUE 100000 CACHE 10 CYCLE)",
        "GENERATED ALWAYS AS IDENTITY",
        "name TEXT COLLATE \"C\"",
        "label VARCHAR(20) COLLATE \"POSIX\"",
        "j JSON",
        "r REAL",
        "c CHARACTER(3)",
        "iv INTERVAL DAY TO SECOND(3)",
        "WITH (fillfactor = '70')",
    ] {
        assert!(sql.contains(expected), "missing {expected:?} in:\n{sql}");
    }
}

#[test]
fn json_real_and_char_are_not_normalized_by_the_type_parser() {
    assert_eq!(
        "json".parse::<ColumnType>(),
        Ok(ColumnType::Range("JSON".into()))
    );
    assert_eq!("jsonb".parse::<ColumnType>(), Ok(ColumnType::Jsonb));
    assert_eq!(
        "real".parse::<ColumnType>(),
        Ok(ColumnType::Range("REAL".into()))
    );
    assert_eq!(
        "float4".parse::<ColumnType>(),
        Ok(ColumnType::Range("REAL".into()))
    );
    assert_eq!("float8".parse::<ColumnType>(), Ok(ColumnType::Float));
    assert_eq!(
        "char".parse::<ColumnType>(),
        Ok(ColumnType::Range("CHARACTER(1)".into()))
    );
    assert_eq!(
        "timestamp(3) with time zone".parse::<ColumnType>(),
        Ok(ColumnType::Range("TIMESTAMPTZ(3)".into()))
    );
    assert_eq!(
        "bit varying(4)".parse::<ColumnType>(),
        Ok(ColumnType::Range("VARBIT(4)".into()))
    );
    assert!("timestamp(9)".parse::<ColumnType>().is_err());
    assert!("interval day to fortnight".parse::<ColumnType>().is_err());
    assert!("interval year(3)".parse::<ColumnType>().is_err());
    // Value mapping (codegen) keeps the closest native family.
    assert_eq!(
        ColumnType::Range("JSON".into()).native_family(),
        ColumnType::Jsonb
    );
    assert_eq!(
        ColumnType::Range("TIMESTAMP(3)".into()).native_family(),
        ColumnType::Timestamp
    );
}

#[test]
fn malformed_options_are_rejected_not_dropped() {
    for (bad, needle) in [
        (
            "table t {\n  id INT generated_identity(start x)\n}\n",
            "requires an integer",
        ),
        (
            "table t {\n  id INT generated_identity(bogus 1)\n}\n",
            "unknown identity option",
        ),
        ("table t {\n  id TEXT collate\n}\n", "requires a collation"),
        ("table t {\n  id TEXT collate C;x\n}\n", "invalid collation"),
        (
            "table t {\n  id INT\n}\nindex i on t (id) with (fillfactor)\n",
            "invalid index storage parameter",
        ),
        (
            "function f() returns int language sql parallel maybe $$ select 1 $$\n",
            "parallel mode",
        ),
        (
            "function f() returns int language sql cost -1 $$ select 1 $$\n",
            "invalid function cost",
        ),
        (
            "function f() returns int language sql set search_path $$ select 1 $$\n",
            "quoted setting",
        ),
        (
            "trigger t on x after insert execute f when old.a > 1\n",
            "wrapped in parentheses",
        ),
        (
            "trigger t on x after insert old_table execute f\n",
            "trigger old_table requires a name",
        ),
        (
            "trigger t on x after insert old_table a-b execute f\n",
            "invalid trigger old_table name",
        ),
    ] {
        let err = parse_qail(bad).expect_err(bad);
        assert!(err.contains(needle), "{bad:?} -> {err}");
    }
}

fn users_with(col: Column) -> Schema {
    let mut schema = Schema::new();
    schema.add_table(
        Table::new("users")
            .column(Column::new("id", ColumnType::Int).primary_key())
            .column(col),
    );
    schema
}

#[test]
fn checked_diff_fails_closed_on_changed_pull_properties() {
    // Virtual vs stored generation.
    let stored = users_with(Column::new("v", ColumnType::Int).generated_stored("id * 2"));
    let virt = users_with(Column::new("v", ColumnType::Int).generated_virtual("id * 2"));
    let err = diff_schemas_checked(&stored, &virt).expect_err("kind change");
    assert!(err.contains("GENERATED/IDENTITY"), "{err}");

    // Identity options.
    let plain = users_with(Column::new("n", ColumnType::Int).generated_identity());
    let mut tuned_col = Column::new("n", ColumnType::Int).generated_identity();
    tuned_col.identity_options.start = Some(100);
    let tuned = users_with(tuned_col);
    let err = diff_schemas_checked(&plain, &tuned).expect_err("identity option change");
    assert!(err.contains("GENERATED/IDENTITY"), "{err}");

    // Collation.
    let c = users_with(Column::new("name", ColumnType::Text).collate("C"));
    let posix = users_with(Column::new("name", ColumnType::Text).collate("POSIX"));
    let err = diff_schemas_checked(&c, &posix).expect_err("collation change");
    assert!(err.contains("collations"), "{err}");

    // json -> jsonb is a type change, not equal.
    let json = users_with(Column::new("doc", ColumnType::Range("JSON".into())));
    let jsonb = users_with(Column::new("doc", ColumnType::Jsonb));
    let err = diff_schemas_checked(&json, &jsonb).expect_err("json -> jsonb");
    assert!(err.contains("JSON -> JSONB"), "{err}");

    // Index storage parameters.
    let mut a = users_with(Column::new("email", ColumnType::Text));
    let mut b = a.clone();
    let mut idx = Index::new("users_email_idx", "users", vec!["email".into()]);
    idx.storage_params = vec!["fillfactor=70".into()];
    a.add_index(idx.clone());
    idx.storage_params = vec!["fillfactor=90".into()];
    b.add_index(idx);
    let err = diff_schemas_checked(&a, &b).expect_err("storage change");
    assert!(err.contains("storage_params"), "{err}");

    // Same key list classified as column vs expression is not a change.
    let mut col_form = users_with(Column::new("email", ColumnType::Text));
    let mut expr_form = col_form.clone();
    col_form.add_index(Index::new(
        "users_email_ops",
        "users",
        vec!["email text_pattern_ops".into()],
    ));
    expr_form.add_index(Index::expression(
        "users_email_ops",
        "users",
        vec!["email text_pattern_ops".into()],
    ));
    assert!(
        diff_schemas_checked(&col_form, &expr_form)
            .expect("same DDL")
            .is_empty()
    );
}

#[test]
fn new_columns_and_indexes_carry_properties_through_state_diff() {
    let old = users_with(Column::new("name", ColumnType::Text));
    let mut new = users_with(Column::new("name", ColumnType::Text));
    let table = new.tables.get_mut("users").unwrap();
    table
        .columns
        .push(Column::new("label", ColumnType::Text).collate("C"));
    table
        .columns
        .push(Column::new("twice", ColumnType::Int).generated_virtual("id * 2"));
    let mut idx = Index::new("users_name_idx", "users", vec!["name".into()]);
    idx.storage_params = vec!["fillfactor=70".into()];
    new.add_index(idx);

    let sql = diff_schemas_checked(&old, &new)
        .expect("additive diff")
        .iter()
        .map(|cmd| cmd.to_sql())
        .collect::<Vec<_>>()
        .join(";\n");
    assert!(sql.contains("label TEXT COLLATE \"C\""), "{sql}");
    assert!(sql.contains("GENERATED ALWAYS AS (id * 2)"), "{sql}");
    assert!(
        !sql.contains("GENERATED ALWAYS AS (id * 2) STORED"),
        "{sql}"
    );
    assert!(sql.contains("WITH (fillfactor = '70')"), "{sql}");
}

#[test]
fn sequence_type_is_serialized() {
    let mut schema = Schema::new();
    let mut seq = Sequence::new("s").start(1);
    seq.data_type = Some("smallint".into());
    schema.add_sequence(seq);
    let text = to_qail_string(&schema);
    assert!(
        text.contains("sequence s { as smallint start 1 }"),
        "{text}"
    );
    let reparsed = parse_qail(&text).expect("parses");
    assert_eq!(reparsed.sequences[0].data_type.as_deref(), Some("smallint"));
}

#[test]
fn schema_validation_rejects_identity_options_without_identity() {
    let mut col = Column::new("n", ColumnType::Int);
    col.identity_options.start = Some(5);
    let errors = users_with(col).validate().expect_err("invalid");
    assert!(
        errors.iter().any(|e| e.contains("identity options")),
        "{errors:?}"
    );
}

fn function_cmd(options: FunctionOptions) -> Qail {
    Qail {
        action: Action::CreateFunction,
        function_def: Some(FunctionDef {
            name: "f".into(),
            args: vec![],
            returns: "int".into(),
            body: "SELECT 1".into(),
            language: Some("sql".into()),
            volatility: Some("stable".into()),
            options,
        }),
        ..Default::default()
    }
}

#[test]
fn function_options_render_and_reject_unsafe_settings() {
    let sql = function_cmd(FunctionOptions {
        strict: true,
        security_definer: true,
        leakproof: true,
        parallel: Some("safe".into()),
        cost: Some("5".into()),
        rows: None,
        config: vec!["search_path TO 'public', 'pg_temp'".into()],
    })
    .to_sql();
    assert_eq!(
        sql,
        "CREATE OR REPLACE FUNCTION f() RETURNS int LANGUAGE sql STABLE STRICT SECURITY DEFINER LEAKPROOF PARALLEL SAFE COST 5 SET search_path TO 'public', 'pg_temp' AS $$ SELECT 1 $$"
    );
    for bad in [
        FunctionOptions {
            config: vec!["search_path TO public; DROP TABLE x".into()],
            ..Default::default()
        },
        FunctionOptions {
            config: vec!["bad name TO 1".into()],
            ..Default::default()
        },
        FunctionOptions {
            parallel: Some("sometimes".into()),
            ..Default::default()
        },
        FunctionOptions {
            cost: Some("NaN".into()),
            ..Default::default()
        },
    ] {
        assert_eq!(
            function_cmd(bad).to_sql(),
            "/* ERROR: Invalid function options */"
        );
    }
}

fn trigger_cmd(condition: Option<&str>, old_table: Option<&str>) -> Qail {
    Qail {
        action: Action::CreateTrigger,
        trigger_def: Some(TriggerDef {
            name: "t".into(),
            table: "x".into(),
            timing: TriggerTiming::After,
            events: vec![TriggerEvent::Update],
            update_columns: vec![],
            for_each_row: old_table.is_none(),
            execute_function: "f".into(),
            condition: condition.map(str::to_string),
            old_table: old_table.map(str::to_string),
            new_table: None,
        }),
        ..Default::default()
    }
}

#[test]
fn trigger_when_and_transition_tables_render() {
    assert_eq!(
        trigger_cmd(Some("old.a IS DISTINCT FROM new.a"), None).to_sql(),
        "CREATE TRIGGER t AFTER UPDATE ON x FOR EACH ROW WHEN (old.a IS DISTINCT FROM new.a) EXECUTE FUNCTION f()"
    );
    assert_eq!(
        trigger_cmd(None, Some("old_rows")).to_sql(),
        "CREATE TRIGGER t AFTER UPDATE ON x REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION f()"
    );
    assert_eq!(
        trigger_cmd(Some("true); DROP TABLE x; --"), None).to_sql(),
        "/* ERROR: Invalid trigger WHEN condition */"
    );
    assert_eq!(
        trigger_cmd(None, Some("a b")).to_sql(),
        "/* ERROR: Invalid trigger transition tables */"
    );
}

#[test]
fn collate_identity_and_index_options_render_or_reject() {
    let make = |constraints: Vec<Constraint>| Qail {
        action: Action::Make,
        table: "t".into(),
        columns: vec![Expr::Def {
            name: "c".into(),
            data_type: "TEXT".into(),
            constraints,
        }],
        ..Default::default()
    };
    assert!(
        make(vec![Constraint::Collate("C".into())])
            .to_sql()
            .contains("c TEXT COLLATE \"C\" NOT NULL")
    );
    assert!(
        make(vec![Constraint::Collate("C\"; DROP".into())])
            .to_sql()
            .contains("ERROR")
    );
    assert!(
        make(vec![
            Constraint::Collate("C".into()),
            Constraint::Collate("POSIX".into())
        ])
        .to_sql()
        .contains("ERROR")
    );
    let identity = make(vec![Constraint::Generated(ColumnGeneration::Identity {
        by_default: false,
        options: IdentityOptions {
            increment: Some(-1),
            ..Default::default()
        },
    })]);
    assert!(
        identity
            .to_sql()
            .contains("GENERATED ALWAYS AS IDENTITY (INCREMENT BY -1)")
    );

    let index = |params: Vec<String>| Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "i".into(),
            table: "t".into(),
            columns: vec!["c".into()],
            storage_params: params,
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(
        index(vec!["fillfactor=70".into(), "deduplicate_items=off".into()]).to_sql(),
        "CREATE INDEX i ON t (c) WITH (fillfactor = '70', deduplicate_items = 'off')"
    );
    assert_eq!(
        index(vec!["fillfactor=70'); DROP TABLE t; --".into()]).to_sql(),
        "CREATE INDEX i ON t (c) WITH (fillfactor = '70''); DROP TABLE t; --')"
    );
    assert_eq!(
        index(vec!["fill factor=70".into()]).to_sql(),
        "/* ERROR: Invalid index storage parameter */"
    );
}

#[test]
fn alter_sequence_renders_owned_by() {
    let cmd = Qail {
        action: Action::AlterSequence,
        table: "owner_seq".into(),
        columns: vec![Expr::Named("OWNED BY owner_t.id".into())],
        ..Default::default()
    };
    assert_eq!(cmd.to_sql(), "ALTER SEQUENCE owner_seq OWNED BY owner_t.id");
}

#[test]
fn wire_payloads_without_new_fields_still_decode() {
    let mut cmd = trigger_cmd(Some("new.a > 0"), None);
    let encoded = qail_core::wire::encode_cmd_binary(&cmd).expect("encode");
    assert_eq!(
        qail_core::wire::decode_cmd_binary(&encoded).expect("decode"),
        cmd
    );

    // A payload produced before these fields existed has no keys for them.
    cmd = trigger_cmd(None, None);
    let mut json: serde_json::Value = serde_json::to_value(&cmd).unwrap();
    let def = json["trigger_def"].as_object_mut().unwrap();
    assert!(!def.contains_key("condition"), "unset fields are skipped");
    def.remove("old_table");
    let decoded: Qail = serde_json::from_value(json).expect("decodes without new keys");
    assert_eq!(decoded, cmd);

    let func = function_cmd(FunctionOptions::default());
    let json = serde_json::to_value(&func).unwrap();
    assert!(json["function_def"].get("options").is_none());
    let decoded: Qail = serde_json::from_value(json).unwrap();
    assert_eq!(decoded, func);
}

#[test]
fn build_schema_parser_accepts_pulled_column_options() {
    let schema = qail_core::build::Schema::parse(
        r#"table ident_t {
  id BIGINT primary_key generated_by_default_identity(start 100 increment 5 cycle) references users(id)
  name TEXT collate "C" not_null
  v INT generated_virtual((id * 2))
  j JSON
}
"#,
    )
    .expect("build parser accepts pulled options");
    let table = schema.table("ident_t").expect("table");
    assert_eq!(
        table.columns.get("j"),
        Some(&ColumnType::Range("JSON".into()))
    );
    assert_eq!(
        table.foreign_keys.len(),
        1,
        "FK after identity options is kept"
    );
}
