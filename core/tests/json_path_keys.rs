//! JSON path operands keep their PostgreSQL type through parse, preview,
//! and canonical text.
//!
//! PostgreSQL overloads `->`/`->>`: a text operand selects an object key and
//! an integer operand selects an array position, so `payload->'0'` and
//! `payload->0` read different values.

use qail_core::ast::builders::{ExprExt, col, json, json_path};
use qail_core::ast::{Expr, JsonPathSegment, Qail};
use qail_core::parse;
use qail_core::transpiler::ToSql;

fn json_column(segments: Vec<(JsonPathSegment, bool)>) -> Qail {
    let mut cmd = Qail::get("events");
    cmd.columns = vec![Expr::JsonAccess {
        column: "payload".to_string(),
        path_segments: segments,
        alias: None,
    }];
    cmd
}

fn segments_of(cmd: &Qail) -> &[(JsonPathSegment, bool)] {
    match &cmd.columns[0] {
        Expr::JsonAccess { path_segments, .. } => path_segments,
        other => panic!("expected JSON access, got {other:?}"),
    }
}

fn preview(dsl: &str) -> String {
    parse(dsl).expect("DSL must parse").to_sql()
}

#[test]
fn quoted_numeric_key_stays_text_in_preview() {
    let sql = preview("get events fields payload->'0'");
    assert!(sql.contains("payload->'0'"), "{sql}");
}

#[test]
fn quoted_numeric_key_stays_text_with_text_extraction() {
    let sql = preview("get events fields payload->>'123'");
    assert!(sql.contains("payload->>'123'"), "{sql}");
}

#[test]
fn unquoted_integer_stays_array_index_in_preview() {
    let sql = preview("get events fields payload->0");
    assert!(sql.contains("payload->0"), "{sql}");
    assert!(!sql.contains("payload->'0'"), "{sql}");
}

#[test]
fn nonnumeric_key_stays_text_in_preview() {
    let sql = preview("get events fields payload->'name'");
    assert!(sql.contains("payload->'name'"), "{sql}");
}

#[test]
fn canonical_text_round_trip_keeps_key_and_index_apart() {
    let cmd = parse("get events fields payload->'0'->1->>'7'").expect("DSL must parse");
    let text = cmd.to_string();
    assert!(text.contains("payload->'0'->1->>'7'"), "{text}");
    let reparsed = parse(&text).expect("canonical text must parse");
    assert_eq!(reparsed, cmd);
    assert!(
        reparsed.to_sql().contains("payload->'0'->1->>'7'"),
        "{}",
        reparsed.to_sql()
    );
}

#[test]
fn parser_records_key_or_index_explicitly() {
    let cmd = parse("get events fields payload->'0'->-1->>\"12\"->>'it''s'").expect("DSL");
    assert_eq!(
        segments_of(&cmd),
        &[
            (JsonPathSegment::Key("0".into()), false),
            (JsonPathSegment::Index(-1), false),
            (JsonPathSegment::Key("12".into()), true),
            (JsonPathSegment::Key("it's".into()), true),
        ]
    );
    let text = cmd.to_string();
    assert_eq!(
        parse(&text).expect("canonical text must parse"),
        cmd,
        "{text}"
    );
}

#[test]
fn typed_numeric_key_renders_as_text_operand() {
    let cmd = json_column(vec![
        (JsonPathSegment::Key("0".into()), false),
        (JsonPathSegment::Index(0), true),
    ]);
    let sql = cmd.to_sql();
    assert!(sql.contains("payload->'0'->>0"), "{sql}");
}

#[test]
fn key_builders_take_text_and_path_builders_read_integers_as_positions() {
    let key: Expr = json("payload", "0").into();
    let path: Expr = col("payload").path("items.0.name").into();
    let listed: Expr = json_path("payload", ["items", "-1", "name"]).into();
    let chained: Expr = json("payload", "a").get("0").index(1).index_text(-1).into();
    assert_eq!(key.to_string(), "payload->>'0'");
    assert_eq!(path.to_string(), "payload->'items'->0->>'name'");
    assert_eq!(listed.to_string(), "payload->'items'->-1->>'name'");
    assert_eq!(chained.to_string(), "payload->>'a'->'0'->1->>-1");
}

#[test]
fn serde_keeps_plain_string_meaning_and_tags_numeric_keys() {
    let cmd = json_column(vec![
        (JsonPathSegment::Key("0".into()), false),
        (JsonPathSegment::Index(0), false),
        (JsonPathSegment::Key("name".into()), true),
    ]);
    let encoded = serde_json::to_value(&cmd).expect("serialize");
    let segments = &encoded["columns"][0]["JsonAccess"]["path_segments"];
    assert_eq!(
        segments,
        &serde_json::json!([[{"key": "0"}, false], ["0", false], ["name", true]])
    );
    let decoded: Qail = serde_json::from_value(encoded).expect("deserialize");
    assert_eq!(decoded, cmd);

    // Segments serialized as bare strings keep the meaning they rendered with.
    let plain: Vec<(JsonPathSegment, bool)> =
        serde_json::from_str(r#"[["0", false], ["-2", false], ["name", true], [3, true]]"#)
            .expect("plain segments");
    assert_eq!(
        plain,
        vec![
            (JsonPathSegment::Index(0), false),
            (JsonPathSegment::Index(-2), false),
            (JsonPathSegment::Key("name".into()), true),
            (JsonPathSegment::Index(3), true),
        ]
    );

    for bad in [
        r#"{}"#,
        r#"{"key": "0", "index": 0}"#,
        r#"{"name": "0"}"#,
        r#"18446744073709551615"#,
        r#"true"#,
    ] {
        assert!(
            serde_json::from_str::<JsonPathSegment>(bad).is_err(),
            "{bad} must not decode"
        );
    }
}

#[test]
fn binary_and_text_wire_codecs_keep_key_and_index_apart() {
    let cmd = json_column(vec![
        (JsonPathSegment::Key("0".into()), false),
        (JsonPathSegment::Index(0), true),
    ]);
    let binary = qail_core::wire::encode_cmd_binary(&cmd).expect("binary encode");
    let from_binary = qail_core::wire::decode_cmd_binary(&binary).expect("binary decode");
    assert_eq!(segments_of(&from_binary), segments_of(&cmd));

    let text = qail_core::wire::encode_cmd_text(&cmd);
    let from_text = qail_core::wire::decode_cmd_text(&text).expect("text decode");
    assert_eq!(segments_of(&from_text), segments_of(&cmd));
}

#[test]
fn sanitizer_accepts_numeric_keys_and_indexes_but_not_unsafe_keys() {
    let ok = json_column(vec![
        (JsonPathSegment::Key("0".into()), false),
        (JsonPathSegment::Index(-1), true),
    ]);
    assert!(qail_core::sanitize::validate_ast(&ok).is_ok());

    let bad = json_column(vec![(JsonPathSegment::Key("a'b".into()), true)]);
    assert!(qail_core::sanitize::validate_ast(&bad).is_err());
}
