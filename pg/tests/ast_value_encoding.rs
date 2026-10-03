//! Native AST encoding of bytea values and JSON path operands.
//!
//! AST parameters travel in text format (Bind format count 0), so a bytea
//! value must arrive as bytea hex input text, never as its raw data bytes.

use qail_core::ast::{Operator, Qail, Value};
use qail_pg::protocol::AstEncoder;

fn bytea_params(data: &[u8]) -> Vec<Option<Vec<u8>>> {
    let cmd = Qail::add("blobs").set_value("b", Value::Bytes(data.to_vec()));
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).expect("insert must encode");
    assert_eq!(sql, "INSERT INTO blobs (b) VALUES ($1)");
    params
}

#[test]
fn bytea_param_is_hex_text_for_backslash_x_data() {
    assert_eq!(
        bytea_params(b"\\x4142"),
        vec![Some(b"\\x5c7834313432".to_vec())]
    );
}

#[test]
fn bytea_param_is_hex_text_for_non_utf8_data() {
    assert_eq!(bytea_params(&[0, 255]), vec![Some(b"\\x00ff".to_vec())]);
}

#[test]
fn bytea_param_is_hex_text_for_empty_data() {
    assert_eq!(bytea_params(&[]), vec![Some(b"\\x".to_vec())]);
}

#[test]
fn bytea_filter_param_is_hex_text() {
    let cmd = Qail::get("blobs").filter("b", Operator::Eq, Value::Bytes(vec![0, b'\\', 10]));
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).expect("select must encode");
    assert_eq!(sql, "SELECT * FROM blobs WHERE b = $1");
    assert_eq!(params, vec![Some(b"\\x005c0a".to_vec())]);
}

#[test]
fn bytea_bind_payload_is_hex_text_in_wire_and_batch_frames() {
    let cmd = Qail::add("blobs").set_value("b", Value::Bytes(vec![0, 255]));
    let (wire, params) = AstEncoder::encode_cmd(&cmd).expect("insert must encode");
    assert_eq!(params, vec![Some(b"\\x00ff".to_vec())]);
    // Bind parameter: Int32 length then the value bytes.
    let needle = [&6i32.to_be_bytes()[..], b"\\x00ff"].concat();
    assert!(
        wire.windows(needle.len()).any(|w| w == needle.as_slice()),
        "single-command Bind must carry the hex text"
    );

    let batch = AstEncoder::encode_batch(&[cmd.clone(), cmd]).expect("batch must encode");
    let hits = batch
        .windows(needle.len())
        .filter(|w| *w == needle.as_slice())
        .count();
    assert_eq!(hits, 2, "every pipelined Bind must carry the hex text");

    let raw = [&2i32.to_be_bytes()[..], &[0u8, 255u8][..]].concat();
    assert!(!wire.windows(raw.len()).any(|w| w == raw.as_slice()));
    assert!(!batch.windows(raw.len()).any(|w| w == raw.as_slice()));
}

#[test]
fn bytea_params_only_path_is_hex_text() {
    let cmd = Qail::add("blobs").set_value("b", Value::Bytes(vec![0x41, 0x42]));
    let params = AstEncoder::encode_cmd_params_only(&cmd).expect("params must encode");
    assert_eq!(params, vec![Some(b"\\x4142".to_vec())]);
}

fn native_sql(dsl: &str) -> String {
    let cmd = qail_core::parse(dsl).expect("DSL must parse");
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).expect("query must encode");
    assert!(
        params.is_empty(),
        "JSON path operands are inline: {params:?}"
    );
    sql
}

#[test]
fn native_quoted_numeric_key_stays_text() {
    assert_eq!(
        native_sql("get events fields payload->'0'"),
        "SELECT (payload->'0') FROM events"
    );
}

#[test]
fn native_quoted_numeric_key_stays_text_with_text_extraction() {
    assert_eq!(
        native_sql("get events fields payload->>'123'"),
        "SELECT (payload->>'123') FROM events"
    );
}

#[test]
fn native_unquoted_integer_stays_array_index() {
    assert_eq!(
        native_sql("get events fields payload->0, payload->>-1"),
        "SELECT (payload->0), (payload->>-1) FROM events"
    );
}

#[test]
fn native_nonnumeric_key_stays_text() {
    assert_eq!(
        native_sql("get events fields payload->'name'"),
        "SELECT (payload->'name') FROM events"
    );
}

#[test]
fn native_typed_numeric_key_stays_text() {
    use qail_core::ast::{Expr, JsonPathSegment};

    let mut cmd = Qail::get("events");
    cmd.columns = vec![Expr::JsonAccess {
        column: "payload".to_string(),
        path_segments: vec![
            (JsonPathSegment::Key("0".into()), false),
            (JsonPathSegment::Key("it's".into()), false),
            (JsonPathSegment::Index(-1), true),
        ],
        alias: None,
    }];
    let (sql, params) = AstEncoder::encode_cmd_sql(&cmd).expect("query must encode");
    assert!(params.is_empty());
    assert_eq!(sql, "SELECT (payload->'0'->'it''s'->>-1) FROM events");
}
