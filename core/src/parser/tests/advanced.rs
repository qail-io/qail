use crate::ast::*;
use crate::parser::parse;

#[test]
fn test_parse_call_command() {
    let cmd = parse("call refresh_materialized_views()").unwrap();
    assert_eq!(cmd.action, Action::Call);
    assert_eq!(cmd.table, "refresh_materialized_views()");
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
fn test_parse_session_commands() {
    let set_cmd = parse("session set statement_timeout = '5000'").unwrap();
    assert_eq!(set_cmd.action, Action::SessionSet);
    assert_eq!(set_cmd.table, "statement_timeout");
    assert_eq!(set_cmd.payload.as_deref(), Some("5000"));

    let show_cmd = parse("session show statement_timeout").unwrap();
    assert_eq!(show_cmd.action, Action::SessionShow);
    assert_eq!(show_cmd.table, "statement_timeout");

    let reset_cmd = parse("session reset statement_timeout").unwrap();
    assert_eq!(reset_cmd.action, Action::SessionReset);
    assert_eq!(reset_cmd.table, "statement_timeout");
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
fn test_bracket_literal_does_not_trigger_table_filter_desugar() {
    let cmd = parse("get users fields id where tags && '[\"a\",\"b\"]'").unwrap();
    assert_eq!(cmd.action, Action::Get);
    assert_eq!(cmd.table, "users");
    assert_eq!(cmd.cages[0].conditions[0].op, Operator::Overlaps);
}
