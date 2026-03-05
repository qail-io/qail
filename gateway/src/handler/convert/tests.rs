use super::*;

#[test]
fn typed_bool_oid_16() {
    assert_eq!(text_to_json_typed("t", 16), serde_json::Value::Bool(true));
    assert_eq!(
        text_to_json_typed("true", 16),
        serde_json::Value::Bool(true)
    );
    assert_eq!(text_to_json_typed("f", 16), serde_json::Value::Bool(false));
    assert_eq!(
        text_to_json_typed("false", 16),
        serde_json::Value::Bool(false)
    );
}

#[test]
fn typed_int_oid_23() {
    assert_eq!(text_to_json_typed("42", 23), serde_json::json!(42));
    assert_eq!(text_to_json_typed("-1", 23), serde_json::json!(-1));
    assert_eq!(text_to_json_typed("0", 23), serde_json::json!(0));
    assert_eq!(
        text_to_json_typed("not_a_number", 23),
        serde_json::json!("not_a_number")
    );
}

#[test]
fn typed_bigint_oid_20() {
    assert_eq!(
        text_to_json_typed("9223372036854775807", 20),
        serde_json::json!(9223372036854775807_i64)
    );
}

#[test]
fn typed_float_oid_701() {
    assert_eq!(text_to_json_typed("2.72", 701), serde_json::json!(2.72));
    assert_eq!(text_to_json_typed("0.0", 701), serde_json::json!(0.0));
    assert_eq!(text_to_json_typed("NaN", 701), serde_json::json!("NaN"));
}

#[test]
fn typed_numeric_oid_1700() {
    assert_eq!(text_to_json_typed("100", 1700), serde_json::json!(100));
    assert_eq!(text_to_json_typed("99.95", 1700), serde_json::json!(99.95));
    assert_eq!(
        text_to_json_typed("1e999", 1700),
        serde_json::json!("1e999")
    );
}

#[test]
fn typed_json_oid_114() {
    assert_eq!(
        text_to_json_typed(r#"{"key":"val"}"#, 114),
        serde_json::json!({"key": "val"})
    );
    assert_eq!(
        text_to_json_typed("not json", 114),
        serde_json::json!("not json")
    );
}

#[test]
fn typed_jsonb_oid_3802() {
    assert_eq!(
        text_to_json_typed("[1,2,3]", 3802),
        serde_json::json!([1, 2, 3])
    );
}

#[test]
fn typed_text_oids_return_string() {
    for oid in [25_u32, 1042, 1043, 2950, 1082, 1114, 1184] {
        assert_eq!(
            text_to_json_typed("hello", oid),
            serde_json::json!("hello"),
            "OID {} should return string",
            oid
        );
    }
}

#[test]
fn typed_array_oid_1007() {
    assert_eq!(
        text_to_json_typed("{1,2,3}", 1007),
        serde_json::json!([1, 2, 3])
    );
}

#[test]
fn typed_unknown_oid_falls_back_to_guess() {
    assert_eq!(text_to_json_typed("42", 0), serde_json::json!(42));
    assert_eq!(text_to_json_typed("hello", 0), serde_json::json!("hello"));
}

#[test]
fn guess_integer() {
    assert_eq!(text_to_json_guess("42"), serde_json::json!(42));
}

#[test]
fn guess_float() {
    assert_eq!(text_to_json_guess("2.72"), serde_json::json!(2.72));
}

#[test]
fn guess_bool() {
    assert_eq!(text_to_json_guess("true"), serde_json::json!(true));
    assert_eq!(text_to_json_guess("t"), serde_json::json!(true));
    assert_eq!(text_to_json_guess("false"), serde_json::json!(false));
    assert_eq!(text_to_json_guess("f"), serde_json::json!(false));
}

#[test]
fn guess_json_object() {
    assert_eq!(
        text_to_json_guess(r#"{"a":1}"#),
        serde_json::json!({"a": 1})
    );
}

#[test]
fn guess_json_array() {
    assert_eq!(text_to_json_guess("[1,2]"), serde_json::json!([1, 2]));
}

#[test]
fn guess_string_fallback() {
    assert_eq!(
        text_to_json_guess("hello world"),
        serde_json::json!("hello world")
    );
}

#[test]
fn pg_array_empty() {
    assert_eq!(pg_array_to_json("{}"), serde_json::json!([]));
}

#[test]
fn pg_array_ints() {
    assert_eq!(pg_array_to_json("{1,2,3}"), serde_json::json!([1, 2, 3]));
}

#[test]
fn pg_array_with_null() {
    assert_eq!(
        pg_array_to_json("{1,NULL,3}"),
        serde_json::json!([1, serde_json::Value::Null, 3])
    );
}

#[test]
fn pg_array_quoted_strings() {
    assert_eq!(
        pg_array_to_json(r#"{"hello","world"}"#),
        serde_json::json!(["hello", "world"])
    );
}

#[test]
fn pg_array_non_array_passthrough() {
    assert_eq!(
        pg_array_to_json("not an array"),
        serde_json::json!("not an array")
    );
}

#[test]
fn pg_array_floats() {
    assert_eq!(pg_array_to_json("{1.5,2.7}"), serde_json::json!([1.5, 2.7]));
}
