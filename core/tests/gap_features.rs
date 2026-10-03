//! B2/B3/D7/C: DSL, preview and schema-text coverage for JSONPath operators,
//! range/network/bitwise operators, named/VARIADIC call arguments and
//! PostgreSQL 18 temporal keys.

use qail_core::ast::*;
use qail_core::migrate::{parse_qail, to_qail_string};
use qail_core::parse;
use qail_core::transpiler::{ToSql, ToSqlParameterized};

fn first_op(dsl: &str) -> Operator {
    let cmd = parse(dsl).unwrap_or_else(|err| panic!("{dsl}: {err}"));
    cmd.cages
        .iter()
        .find(|cage| matches!(cage.kind, CageKind::Filter))
        .and_then(|cage| cage.conditions.first())
        .map(|cond| cond.op)
        .unwrap_or_else(|| panic!("{dsl}: no filter condition"))
}

#[test]
fn b2_dsl_jsonpath_exists_is_distinct_from_text_search() {
    let dsl = "get docs fields id where payload @? '$.a'";
    let cmd = parse(dsl).unwrap_or_else(|err| panic!("{dsl}: {err}"));
    let sql = cmd.to_sql();
    println!("{dsl} -> {sql}");
    assert!(sql.contains("@?"), "{sql}");
    assert!(!sql.contains("to_tsvector"), "{sql}");
}

#[test]
fn b2_dsl_jsonpath_match_is_distinct_from_text_search() {
    let dsl = "get docs fields id where payload jsonpath_match '$.a == 1'";
    let cmd = parse(dsl).unwrap_or_else(|err| panic!("{dsl}: {err}"));
    let sql = cmd.to_sql();
    println!("{dsl} -> {sql}");
    assert!(sql.contains("@@"), "{sql}");
    assert!(!sql.contains("to_tsvector"), "{sql}");
}

#[test]
fn b2_at_at_keeps_text_search_meaning() {
    assert_eq!(
        first_op("get docs fields id where tsv @@ \"rust\""),
        Operator::TextSearch
    );
    let sql = parse("get docs fields id where body @@ 'fast ferry'")
        .unwrap()
        .to_sql();
    assert!(sql.contains("to_tsvector('english'"), "{sql}");
}

#[test]
fn b3_dsl_parses_range_and_network_operators() {
    for (dsl, symbol) in [
        ("get t fields id where during -|- '[1,2)'", "-|-"),
        ("get t fields id where during << '[5,6)'", "<<"),
        ("get t fields id where during >> '[5,6)'", ">>"),
        ("get t fields id where during &< '[5,6)'", "&<"),
        ("get t fields id where during &> '[5,6)'", "&>"),
        ("get t fields id where ip <<= '10.0.0.0/8'", "<<="),
        ("get t fields id where net >>= '10.1.2.3'", ">>="),
    ] {
        let cmd = parse(dsl).unwrap_or_else(|err| panic!("{dsl}: {err}"));
        let sql = cmd.to_sql();
        println!("{dsl} -> {sql}");
        assert!(sql.contains(&format!(" {symbol} ")), "{dsl}: {sql}");
    }
}

#[test]
fn d7_dsl_parses_named_and_variadic_arguments() {
    let dsl = "get t fields calculate(amount, digits => 2) as v";
    let cmd = parse(dsl).unwrap_or_else(|err| panic!("{dsl}: {err}"));
    let sql = cmd.to_sql();
    println!("{dsl} -> {sql}");
    assert!(sql.contains("digits => 2"), "{sql}");

    let dsl = "get t fields concat_ws(',', variadic names) as v";
    let cmd = parse(dsl).unwrap_or_else(|err| panic!("{dsl}: {err}"));
    let sql = cmd.to_sql();
    println!("{dsl} -> {sql}");
    assert!(sql.contains("VARIADIC"), "{sql}");
}

#[test]
fn c_schema_text_round_trips_temporal_keys() {
    let text = "# QAIL Schema\n\n\
table room_bookings {\n  room_id INT not_null\n  during TSTZRANGE not_null\n  primary_key (room_id, during without_overlaps) constraint room_bookings_pkey\n}\n\n\
table room_cleanings {\n  room_id INT not_null\n  during TSTZRANGE not_null\n  foreign_key (room_id, period during) references room_bookings(room_id, period during) constraint room_cleanings_fk\n}\n\n";
    let schema = parse_qail(text).unwrap_or_else(|err| panic!("temporal schema text: {err}"));
    assert_eq!(to_qail_string(&schema), text);
}

#[test]
fn c_returning_with_aliases_is_typed() {
    let cmd = Qail::set("orders")
        .set_value("status", "paid")
        .returning(["o.status", "n.status"])
        .returning_aliases(Some("o"), Some("n"));
    let sql = cmd.to_sql_parameterized().sql;
    println!("typed RETURNING WITH -> {sql}");
    assert_eq!(
        sql,
        "UPDATE orders SET status = 'paid' RETURNING WITH (OLD AS o, NEW AS n) o.status, n.status"
    );

    let insert = Qail::add("orders")
        .set_value("status", "new")
        .returning(["n.status"])
        .returning_aliases(None, Some("n"));
    assert!(
        insert
            .to_sql()
            .ends_with("RETURNING WITH (NEW AS n) n.status"),
        "{}",
        insert.to_sql()
    );
    assert_eq!(
        Qail::set("t")
            .returning_aliases(None, None)
            .returning_aliases,
        None
    );
}

// ── Serialization, sanitizer and normalizer coverage ────────────────────

#[test]
fn ast_payloads_without_the_added_fields_still_decode() {
    let json = serde_json::to_value(Qail::get("orders").columns(["id"])).unwrap();
    let object = json.as_object().unwrap();
    assert!(!object.contains_key("from_source"));
    assert!(!object.contains_key("returning_aliases"));
    let decoded: Qail = serde_json::from_value(json).unwrap();
    assert_eq!(decoded.from_source, None);

    let fk: TableConstraint = serde_json::from_str(
        r#"{"ForeignKey":{"name":null,"columns":["a"],"ref_table":"t","ref_columns":["b"]}}"#,
    )
    .unwrap();
    assert!(matches!(
        fk,
        TableConstraint::ForeignKey { period: false, .. }
    ));
}

#[test]
fn binary_wire_keeps_from_source_and_text_wire_refuses_it() {
    let cmd = Qail::get("o")
        .from_source(
            FromSource::subquery(Qail::get("orders").columns(["id"]), "o").column_aliases(["k"]),
        )
        .columns(["k"]);
    let bytes = qail_core::wire::encode_cmd_binary(&cmd).unwrap();
    assert_eq!(qail_core::wire::decode_cmd_binary(&bytes).unwrap(), cmd);

    // `get o` would read a table named o: the text form must not round-trip.
    let text = qail_core::wire::encode_cmd_text(&cmd);
    assert!(qail_core::wire::decode_cmd_text(&text).is_err(), "{text}");
}

#[test]
fn sanitizer_walks_from_source_and_function_args() {
    let ok = Qail::get("g")
        .from_source(FromSource::function(
            "generate_series",
            [Expr::Literal(Value::Int(1)), Expr::Literal(Value::Int(3))],
            "g",
        ))
        .columns(["g"]);
    qail_core::sanitize::validate_ast(&ok).unwrap();

    let bad_inner = Qail::get("o").from_source(FromSource::subquery(
        Qail::get("orders; drop table x").columns(["id"]),
        "o",
    ));
    assert!(qail_core::sanitize::validate_ast(&bad_inner).is_err());

    let mismatch = {
        let mut cmd = Qail::get("o").from_source(FromSource::subquery(Qail::get("orders"), "o"));
        cmd.table = "orders".into();
        cmd
    };
    assert!(qail_core::sanitize::validate_ast(&mismatch).is_err());

    let misplaced = Qail::get("t").columns_expr(vec![Expr::FunctionArg {
        name: Some("a".into()),
        variadic: false,
        value: Box::new(Expr::Named("x".into())),
    }]);
    assert!(qail_core::sanitize::validate_ast(&misplaced).is_err());

    let bad_order = Qail::get("t").columns_expr(vec![Expr::FunctionCall {
        name: "f".into(),
        args: vec![
            Expr::FunctionArg {
                name: None,
                variadic: true,
                value: Box::new(Expr::Named("x".into())),
            },
            Expr::Named("y".into()),
        ],
        alias: None,
    }]);
    assert!(qail_core::sanitize::validate_ast(&bad_order).is_err());
}

#[test]
fn normalizer_keeps_text_search_and_jsonpath_match_apart() {
    let cmd = Qail::get("docs")
        .columns(["id"])
        .filter("body", Operator::TextSearch, "x")
        .filter("body", Operator::JsonPathMatch, "x");
    let normalized = qail_core::optimizer::normalize_select(&cmd).unwrap();
    let cleaned = qail_core::optimizer::cleanup_select(&normalized).to_qail();
    let ops: Vec<Operator> = cleaned
        .cages
        .iter()
        .flat_map(|cage| cage.conditions.iter().map(|c| c.op))
        .collect();
    assert!(ops.contains(&Operator::TextSearch), "{ops:?}");
    assert!(ops.contains(&Operator::JsonPathMatch), "{ops:?}");

    // Typed FROM sources and RETURNING aliases skip the normalizers instead of being dropped.
    let from = Qail::get("o").from_source(FromSource::subquery(Qail::get("orders"), "o"));
    assert!(qail_core::optimizer::normalize_select(&from).is_err());
    let aliased = Qail::set("orders")
        .set_value("status", "paid")
        .returning(["o.status"])
        .returning_aliases(Some("o"), None);
    assert!(qail_core::optimizer::normalize_mutation(&aliased).is_err());
}

#[test]
fn access_policy_checks_the_from_subquery_tables() {
    use qail_core::access::{AccessContext, AccessDecision, AccessPolicy};
    let policy = AccessPolicy {
        default_decision: AccessDecision::Deny,
        tables: Default::default(),
    };
    let cmd = Qail::get("o").from_source(FromSource::subquery(
        Qail::get("orders").columns(["id"]),
        "o",
    ));
    let err = policy
        .check_command(&AccessContext::default(), &cmd)
        .expect_err("the inner table has no policy");
    assert!(err.to_string().contains("orders"), "{err}");
}

// ── Temporal keys: schema model, DDL and diff ───────────────────────────

const TEMPORAL: &str = "table room_bookings {\n  room_id INT not_null\n  during TSTZRANGE not_null\n  primary_key (room_id, during without_overlaps) constraint room_bookings_pkey\n}\n\n\
table room_cleanings {\n  room_id INT not_null\n  during TSTZRANGE not_null\n  foreign_key (room_id, period during) references room_bookings(room_id, period during) constraint room_cleanings_fk\n}\n";

#[test]
fn c_schema_commands_emit_temporal_ddl() {
    let schema = parse_qail(TEMPORAL).unwrap();
    schema.validate().unwrap();
    let sql: Vec<String> = qail_core::migrate::schema_to_commands(&schema)
        .iter()
        .map(|cmd| cmd.to_sql())
        .collect();
    let all = sql.join(";\n");
    println!("{all}");
    assert!(
        all.contains(
            "CONSTRAINT room_bookings_pkey PRIMARY KEY (room_id, during WITHOUT OVERLAPS)"
        ),
        "{all}"
    );
    assert!(
        all.contains(
            "FOREIGN KEY (room_id, PERIOD during) REFERENCES room_bookings(room_id, PERIOD during)"
        ),
        "{all}"
    );
    assert!(
        !all.contains("during TSTZRANGE NOT NULL PRIMARY KEY")
            && !all.contains("PRIMARY KEY (room_id, during)"),
        "temporal key must not degrade to an equality key: {all}"
    );
}

#[test]
fn c_schema_rejects_malformed_temporal_keys() {
    for (text, needle) in [
        (
            "table t {\n  a INT\n  r TSTZRANGE\n  primary_key (a, r)\n}\n",
            "without_overlaps",
        ),
        (
            "table t {\n  r TSTZRANGE\n  primary_key (r without_overlaps)\n}\n",
            "at least one column",
        ),
        (
            "table p {\n  a INT\n  r TSTZRANGE\n  primary_key (a, r without_overlaps)\n}\n\ntable t {\n  a INT\n  r TSTZRANGE\n  foreign_key (period r, a) references p(period r, a)\n}\n",
            "last key column",
        ),
        (
            "table p {\n  a INT\n  r TSTZRANGE\n  primary_key (a, r without_overlaps)\n}\n\ntable t {\n  a INT\n  r TSTZRANGE\n  foreign_key (a, period r) references p(a, r)\n}\n",
            "both sides",
        ),
    ] {
        let err = parse_qail(text).expect_err(text);
        assert!(err.contains(needle), "{text}: {err}");
    }

    let two_primary = parse_qail(
        "table t {\n  a INT primary_key\n  r TSTZRANGE\n  primary_key (a, r without_overlaps)\n}\n",
    )
    .unwrap();
    let errors = two_primary.validate().expect_err("two primary keys");
    assert!(
        errors
            .iter()
            .any(|e| e.contains("more than one PRIMARY KEY")),
        "{errors:?}"
    );
}

#[test]
fn c_checked_diff_refuses_temporal_key_changes_on_existing_tables() {
    let old = parse_qail("table t {\n  a INT not_null\n  r TSTZRANGE not_null\n}\n").unwrap();
    let new = parse_qail(
        "table t {\n  a INT not_null\n  r TSTZRANGE not_null\n  unique (a, r without_overlaps)\n}\n",
    )
    .unwrap();
    let err = qail_core::migrate::diff_schemas_checked(&old, &new).expect_err("must refuse");
    assert!(err.contains("temporal (WITHOUT OVERLAPS) keys"), "{err}");

    // A brand-new table carries its temporal key in CREATE TABLE.
    let empty = parse_qail("").unwrap();
    let cmds = qail_core::migrate::diff_schemas_checked(&empty, &new).unwrap();
    let sql = cmds
        .iter()
        .map(|c| c.to_sql())
        .collect::<Vec<_>>()
        .join(";\n");
    assert!(sql.contains("UNIQUE (a, r WITHOUT OVERLAPS)"), "{sql}");
}
