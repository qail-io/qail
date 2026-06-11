use crate::ast::*;
use crate::parser::parse;

#[test]
fn test_parse_call_command() {
    let cmd = parse("call refresh_materialized_views()").unwrap();
    assert_eq!(cmd.action, Action::Call);
    assert_eq!(cmd.table, "refresh_materialized_views()");
}

#[test]
fn test_parse_call_command_rejects_statement_delimiters() {
    assert!(parse("call refresh_materialized_views(); drop table users").is_err());
    assert!(parse("call 1refresh_materialized_views()").is_err());
}

#[test]
fn test_parse_do_command_with_language() {
    let cmd = parse("do $$ BEGIN RAISE NOTICE 'ok'; END; $$ language plpgsql").unwrap();
    assert_eq!(cmd.action, Action::Do);
    assert_eq!(cmd.table, "plpgsql");
    assert_eq!(
        cmd.payload.as_deref(),
        Some(" BEGIN RAISE NOTICE 'ok'; END; ")
    );
}

#[test]
fn test_parse_preserves_comment_markers_inside_quoted_literals() {
    let cmd = parse(r#"get docs fields id where body = "alpha -- beta /* gamma */""#).unwrap();
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "docs");
    assert_eq!(
        cmd.cages[0].conditions[0].value,
        Value::String("alpha -- beta /* gamma */".to_string())
    );
}

#[test]
fn test_parse_preserves_comment_markers_inside_triple_quoted_literals() {
    let cmd = parse("get docs fields id where body = '''alpha -- beta /* gamma */'''").unwrap();
    assert_eq!(
        cmd.cages[0].conditions[0].value,
        Value::String("alpha -- beta /* gamma */".to_string())
    );
}

#[test]
fn test_parse_strips_comments_outside_literals() {
    let cmd = parse(
        "get docs -- outside line comment\n\
         fields id /* outside block comment */ where active = true",
    )
    .unwrap();

    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "docs");
    assert_eq!(cmd.columns, vec![Expr::Named("id".to_string())]);
    assert_eq!(
        cmd.cages[0].conditions[0].left,
        Expr::Named("active".to_string())
    );
}

#[test]
fn test_parse_do_preserves_comment_markers_inside_dollar_body() {
    let cmd = parse(
        "do $$ BEGIN RAISE NOTICE '-- not a comment /* still body */'; END; $$ language plpgsql",
    )
    .unwrap();

    assert_eq!(cmd.action, Action::Do);
    assert_eq!(
        cmd.payload.as_deref(),
        Some(" BEGIN RAISE NOTICE '-- not a comment /* still body */'; END; ")
    );
}

#[test]
fn test_parse_session_commands() {
    let set_cmd = parse("session set statement_timeout = '5000'").unwrap();
    assert_eq!(set_cmd.action, Action::SessionSet);
    assert_eq!(set_cmd.table, "statement_timeout");
    assert_eq!(set_cmd.payload.as_deref(), Some("5000"));

    let set_guc_cmd = parse("session set app.current_tenant_id = 'tenant-1'").unwrap();
    assert_eq!(set_guc_cmd.action, Action::SessionSet);
    assert_eq!(set_guc_cmd.table, "app.current_tenant_id");
    assert_eq!(set_guc_cmd.payload.as_deref(), Some("tenant-1"));

    let show_cmd = parse("session show statement_timeout").unwrap();
    assert_eq!(show_cmd.action, Action::SessionShow);
    assert_eq!(show_cmd.table, "statement_timeout");

    let reset_cmd = parse("session reset statement_timeout").unwrap();
    assert_eq!(reset_cmd.action, Action::SessionReset);
    assert_eq!(reset_cmd.table, "statement_timeout");
}

#[test]
fn test_session_setting_keys_reject_malformed_names() {
    assert!(parse("session set app..current_tenant_id = tenant-1").is_err());
    assert!(parse("session set 1app.current_tenant_id = tenant-1").is_err());
    assert!(parse("session set app-current_tenant_id = tenant-1").is_err());
    assert!(parse("session show app..current_tenant_id").is_err());
    assert!(parse("session reset app-current_tenant_id").is_err());
}

fn first_op(query: &str) -> Operator {
    let cmd = parse(query).unwrap();
    cmd.cages[0].conditions[0].op
}

#[test]
fn test_parse_extended_symbol_operators() {
    assert_eq!(
        first_op("get users fields id where name ~* \"^a\""),
        Operator::RegexI
    );
    assert_eq!(
        first_op("get users fields id where metadata @> '{\"role\":\"admin\"}'"),
        Operator::Contains
    );
    assert_eq!(
        first_op("get users fields id where tags <@ '[\"a\",\"b\"]'"),
        Operator::ContainedBy
    );
    assert_eq!(
        first_op("get users fields id where tags && '[\"a\"]'"),
        Operator::Overlaps
    );
    assert_eq!(
        first_op("get docs fields id where tsv @@ \"rust\""),
        Operator::TextSearch
    );
    assert_eq!(
        first_op("get docs fields id where data #>> \"{a,b}\""),
        Operator::JsonPathText
    );
    assert_eq!(
        first_op("get docs fields id where data #> \"{a,b}\""),
        Operator::JsonPath
    );
    assert_eq!(
        first_op("get docs fields id where data ? \"key\""),
        Operator::KeyExists
    );
    assert_eq!(
        first_op("get docs fields id where data ?| \"{a,b}\""),
        Operator::KeyExistsAny
    );
    assert_eq!(
        first_op("get docs fields id where data ?& \"{a,b}\""),
        Operator::KeyExistsAll
    );
}

#[test]
fn test_parse_extended_keyword_operators() {
    assert_eq!(
        first_op("get users fields id where name similar to \"(A|B)%\""),
        Operator::SimilarTo
    );
    assert_eq!(
        first_op("get docs fields id where payload json_exists \"$.a\""),
        Operator::JsonExists
    );
    assert_eq!(
        first_op("get docs fields id where payload json_query \"$.a\""),
        Operator::JsonQuery
    );
    assert_eq!(
        first_op("get docs fields id where payload json_value \"$.a\""),
        Operator::JsonValue
    );
    assert_eq!(
        first_op("get users fields id where name regex \"^a\""),
        Operator::Regex
    );
}

#[test]
fn test_interval_month_suffix_is_not_parsed_as_minutes() {
    let cmd = parse("get subscriptions fields id where age = 6mo").unwrap();
    assert_eq!(
        cmd.cages[0].conditions[0].value,
        Value::Interval {
            amount: 6,
            unit: crate::ast::values::IntervalUnit::Month
        }
    );
}

#[test]
fn test_bracket_literal_does_not_trigger_table_filter_desugar() {
    let cmd = parse("get users fields id where tags && '[\"a\",\"b\"]'").unwrap();
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.cages[0].conditions[0].op, Operator::Overlaps);
}

#[test]
fn test_json_literals_are_validated_as_json() {
    let cmd = parse("get docs fields id where metadata @> {\"tags\":[\"a\",{\"b\":true}],\"n\":1}")
        .unwrap();
    assert_eq!(
        cmd.cages[0].conditions[0].value,
        Value::Json("{\"tags\":[\"a\",{\"b\":true}],\"n\":1}".to_string())
    );

    for query in [
        "get docs fields id where metadata @> {]}",
        "get docs fields id where metadata @> {\"a\": [1, }",
        "get docs fields id where metadata @> [1, } ]",
        "get docs fields id where metadata @> {\"a\": \"unterminated}",
        "get docs fields id where metadata @> {\"a\": tru}",
        "get docs fields id where metadata @> {\"a\", 1}",
        "get docs fields id where metadata @> [1,,2]",
    ] {
        assert!(
            parse(query).is_err(),
            "invalid JSON literal parsed: {query}"
        );
    }
}
