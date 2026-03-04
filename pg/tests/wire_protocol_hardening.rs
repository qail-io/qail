//! Wire Protocol Hardening Tests
//!
//! Validates byte-level correctness and security of the PostgreSQL
//! wire protocol encoder against adversarial inputs.

use bytes::BytesMut;
use qail_pg::protocol::EncodeError;
use qail_pg::protocol::encoder::{Param, PgEncoder};

// ============================================================================
// MESSAGE TYPE & LENGTH INTEGRITY
// ============================================================================

#[test]
fn query_message_type_is_q() {
    let bytes = PgEncoder::try_encode_query_string("SELECT 1").unwrap();
    assert_eq!(bytes[0], b'Q', "Query message type must be 'Q'");
}

#[test]
fn query_length_accuracy() {
    let sql = "SELECT * FROM users WHERE id = 42";
    let bytes = PgEncoder::try_encode_query_string(sql).unwrap();
    let declared_len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    // declared_len = 4 (itself) + sql.len() + 1 (null terminator)
    assert_eq!(
        declared_len as usize,
        4 + sql.len() + 1,
        "Declared length must match actual content"
    );
    // Total message = 1 (type) + declared_len
    assert_eq!(
        bytes.len(),
        1 + declared_len as usize,
        "Total message size must match"
    );
}

#[test]
fn query_null_terminator_present() {
    let bytes = PgEncoder::try_encode_query_string("SELECT 1").unwrap();
    assert_eq!(
        *bytes.last().unwrap(),
        0,
        "Query string must be null-terminated"
    );
}

#[test]
fn query_empty_string() {
    let bytes = PgEncoder::try_encode_query_string("").unwrap();
    assert_eq!(bytes[0], b'Q');
    let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    assert_eq!(len, 5, "Empty query: len = 4 (self) + 0 (sql) + 1 (null)");
    assert_eq!(bytes[5], 0, "Null terminator for empty query");
    assert_eq!(bytes.len(), 6);
}

#[test]
fn terminate_message_exact_bytes() {
    let bytes = PgEncoder::encode_terminate();
    assert_eq!(
        bytes.as_ref(),
        &[b'X', 0, 0, 0, 4],
        "Terminate must be exactly 'X' + len(4)"
    );
}

#[test]
fn sync_message_exact_bytes() {
    let bytes = PgEncoder::encode_sync();
    assert_eq!(
        bytes.as_ref(),
        &[b'S', 0, 0, 0, 4],
        "Sync must be exactly 'S' + len(4)"
    );
}

// ============================================================================
// PARSE MESSAGE
// ============================================================================

#[test]
fn parse_message_structure() {
    let bytes = PgEncoder::try_encode_parse("", "SELECT $1", &[]).unwrap();
    assert_eq!(bytes[0], b'P', "Parse message type must be 'P'");
    // After length, first byte should be 0 (unnamed statement null terminator)
    assert_eq!(
        bytes[5], 0,
        "Unnamed statement must be empty null-terminated string"
    );
    // SQL follows: "SELECT $1\0"
    let sql_start = 6;
    let sql_end = sql_start + 9; // "SELECT $1"
    assert_eq!(&bytes[sql_start..sql_end], b"SELECT $1");
    assert_eq!(bytes[sql_end], 0, "SQL must be null-terminated");
}

#[test]
fn parse_with_named_statement() {
    let bytes = PgEncoder::try_encode_parse("my_stmt", "SELECT 1", &[]).unwrap();
    assert_eq!(bytes[0], b'P');
    // Statement name "my_stmt" + null
    assert_eq!(&bytes[5..12], b"my_stmt");
    assert_eq!(bytes[12], 0);
}

#[test]
fn parse_with_param_types() {
    let bytes = PgEncoder::try_encode_parse("", "SELECT $1", &[23]).unwrap(); // 23 = int4
    assert_eq!(bytes[0], b'P');
    // Should set param count to 1 and OID to 23
    let content = &bytes[5..];
    // Find the null after SQL
    let sql_null_pos = content.iter().position(|&b| b == 0).unwrap(); // stmt name
    let sql_start = sql_null_pos + 1;
    let sql_data = &content[sql_start..];
    let sql_null = sql_data.iter().position(|&b| b == 0).unwrap();
    let after_sql = &sql_data[sql_null + 1..];
    // param count (2 bytes)
    let count = i16::from_be_bytes([after_sql[0], after_sql[1]]);
    assert_eq!(count, 1, "Must have 1 param type");
    // OID (4 bytes)
    let oid = u32::from_be_bytes([after_sql[2], after_sql[3], after_sql[4], after_sql[5]]);
    assert_eq!(oid, 23, "OID must be 23 (int4)");
}

// ============================================================================
// BIND MESSAGE
// ============================================================================

#[test]
fn bind_with_text_params() {
    let params = vec![Some(b"hello".to_vec()), Some(b"42".to_vec())];
    let bytes = PgEncoder::encode_bind("", "", &params).unwrap();
    assert_eq!(bytes[0], b'B', "Bind message type must be 'B'");
    let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    assert_eq!(
        bytes.len(),
        1 + len as usize,
        "Total size must match declared length"
    );
}

#[test]
fn bind_with_null_param() {
    let params = vec![None];
    let bytes = PgEncoder::encode_bind("", "", &params).unwrap();
    // NULL param is encoded as -1 (4 bytes)
    let content = &bytes[5..]; // skip type + length
    // portal null + stmt null + format_count(2) + param_count(2)
    let param_data_start = 1 + 1 + 2 + 2; // = 6
    let null_marker = i32::from_be_bytes([
        content[param_data_start],
        content[param_data_start + 1],
        content[param_data_start + 2],
        content[param_data_start + 3],
    ]);
    assert_eq!(null_marker, -1, "NULL param must be encoded as -1");
}

#[test]
fn bind_too_many_params_rejected() {
    let params: Vec<Option<Vec<u8>>> = (0..32768).map(|_| Some(vec![0])).collect();
    let result = PgEncoder::encode_bind("", "", &params);
    assert!(result.is_err(), "Must reject > i16::MAX params");
    match result.unwrap_err() {
        EncodeError::TooManyParameters(n) => assert_eq!(n, 32768),
        e => panic!("Wrong error type: {:?}", e),
    }
}

#[test]
fn bind_exactly_max_params() {
    // i16::MAX = 32767 — this should succeed
    let params: Vec<Option<Vec<u8>>> = (0..32767).map(|_| Some(vec![0])).collect();
    let result = PgEncoder::encode_bind("", "", &params);
    assert!(result.is_ok(), "Exactly i16::MAX params must succeed");
}

#[test]
fn bind_empty_params() {
    let params: Vec<Option<Vec<u8>>> = vec![];
    let bytes = PgEncoder::encode_bind("", "", &params).unwrap();
    assert_eq!(bytes[0], b'B');
    let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    assert!(len > 4, "Even empty bind has header overhead");
}

#[test]
fn bind_binary_data_param() {
    // Param containing raw bytes including null bytes
    let data = vec![0u8, 1, 2, 255, 0, 128];
    let params = vec![Some(data.clone())];
    let bytes = PgEncoder::encode_bind("", "", &params).unwrap();
    // Verify the data is in the buffer
    let content = &bytes[5..];
    // Find the param data: portal(1) + stmt(1) + format(2) + count(2) + len(4) = 10
    let data_start = 1 + 1 + 2 + 2 + 4;
    assert_eq!(
        &content[data_start..data_start + 6],
        &data,
        "Binary data must be preserved exactly"
    );
}

// ============================================================================
// EXECUTE MESSAGE
// ============================================================================

#[test]
fn execute_message_structure() {
    let bytes = PgEncoder::try_encode_execute("", 0).unwrap();
    assert_eq!(bytes[0], b'E', "Execute type");
    let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    assert_eq!(len, 9, "Execute len = 4 + 1(portal null) + 4(max_rows)");
    assert_eq!(bytes[5], 0, "Unnamed portal");
    let max_rows = i32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]);
    assert_eq!(max_rows, 0, "Unlimited rows");
}

#[test]
fn execute_with_row_limit() {
    let bytes = PgEncoder::try_encode_execute("", 100).unwrap();
    let max_rows = i32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]);
    assert_eq!(max_rows, 100, "Row limit must be 100");
}

// ============================================================================
// DESCRIBE MESSAGE
// ============================================================================

#[test]
fn describe_statement() {
    let bytes = PgEncoder::try_encode_describe(false, "").unwrap();
    assert_eq!(bytes[0], b'D', "Describe type");
    assert_eq!(bytes[5], b'S', "Describe Statement");
}

#[test]
fn describe_portal() {
    let bytes = PgEncoder::try_encode_describe(true, "").unwrap();
    assert_eq!(bytes[0], b'D');
    assert_eq!(bytes[5], b'P', "Describe Portal");
}

// ============================================================================
// EXTENDED QUERY PIPELINE
// ============================================================================

#[test]
fn extended_query_pipeline_ordering() {
    let params = vec![Some(b"test".to_vec())];
    let bytes = PgEncoder::encode_extended_query("SELECT $1", &params).unwrap();

    // Find all message type bytes and verify ordering: P, B, E, S
    let mut types = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let msg_type = bytes[pos];
        types.push(msg_type);
        let len = i32::from_be_bytes([
            bytes[pos + 1],
            bytes[pos + 2],
            bytes[pos + 3],
            bytes[pos + 4],
        ]);
        pos += 1 + len as usize;
    }
    assert_eq!(
        types,
        vec![b'P', b'B', b'E', b'S'],
        "Pipeline must be Parse→Bind→Execute→Sync, got: {:?}",
        types
    );
}

#[test]
fn extended_query_too_many_params() {
    let params: Vec<Option<Vec<u8>>> = (0..32768).map(|_| Some(vec![0])).collect();
    let result = PgEncoder::encode_extended_query("SELECT $1", &params);
    assert!(result.is_err(), "Must reject > i16::MAX params");
}

#[test]
fn extended_query_no_params() {
    let bytes = PgEncoder::encode_extended_query("SELECT 1", &[]).unwrap();
    // Should still have all 4 message types
    assert_eq!(bytes[0], b'P', "First message must be Parse");
}

// ============================================================================
// ULTRA-OPTIMIZED ENCODERS
// ============================================================================

#[test]
fn ultra_bind_produces_same_as_standard() {
    let params_standard = vec![Some(b"hello".to_vec()), None, Some(b"42".to_vec())];
    let standard = PgEncoder::encode_bind("", "my_stmt", &params_standard).unwrap();

    let mut ultra_buf = BytesMut::new();
    let params_ultra = vec![Param::Bytes(b"hello"), Param::Null, Param::Bytes(b"42")];
    PgEncoder::encode_bind_ultra(&mut ultra_buf, "my_stmt", &params_ultra).unwrap();

    // Both buffers should have the same message type and payload structure
    assert_eq!(standard[0], ultra_buf[0], "Both must be 'B'");
    let std_len = i32::from_be_bytes([standard[1], standard[2], standard[3], standard[4]]);
    let ult_len = i32::from_be_bytes([ultra_buf[1], ultra_buf[2], ultra_buf[3], ultra_buf[4]]);
    assert_eq!(std_len, ult_len, "Lengths must match");
    assert_eq!(
        standard, ultra_buf,
        "Standard and ultra bind must produce identical bytes"
    );
}

#[test]
fn ultra_execute_exact_bytes() {
    let mut buf = BytesMut::new();
    PgEncoder::encode_execute_ultra(&mut buf);
    assert_eq!(
        buf.as_ref(),
        &[b'E', 0, 0, 0, 9, 0, 0, 0, 0, 0],
        "Ultra execute must match hardcoded bytes"
    );
}

#[test]
fn ultra_sync_exact_bytes() {
    let mut buf = BytesMut::new();
    PgEncoder::encode_sync_ultra(&mut buf);
    assert_eq!(buf.as_ref(), &[b'S', 0, 0, 0, 4]);
}

#[test]
fn ultra_bind_too_many_params() {
    let mut buf = BytesMut::new();
    let params: Vec<Param> = (0..32768).map(|_| Param::Null).collect();
    let result = PgEncoder::encode_bind_ultra(&mut buf, "", &params);
    assert!(result.is_err());
}

// ============================================================================
// SQL WITH EMBEDDED NULL BYTES
// ============================================================================

#[test]
fn query_string_with_null_bytes() {
    // SQL containing a null byte must be rejected fail-closed to prevent
    // query truncation at the wire protocol null terminator.
    let sql = "SELECT * FROM users\0; DROP TABLE orders";
    let bytes = PgEncoder::try_encode_query_string(sql).unwrap_err();
    assert_eq!(bytes, EncodeError::NullByte);
}

// ============================================================================
// LENGTH/SIZE EDGE CASES
// ============================================================================

#[test]
fn bind_to_produces_correct_length() {
    let mut buf = BytesMut::new();
    let params = vec![Some(b"test_value".to_vec()), None];
    PgEncoder::encode_bind_to(&mut buf, "stmt1", &params).unwrap();

    assert_eq!(buf[0], b'B');
    let declared_len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
    assert_eq!(
        buf.len(),
        1 + declared_len as usize,
        "bind_to length must be accurate"
    );
}

#[test]
fn execute_to_produces_correct_bytes() {
    let mut buf = BytesMut::new();
    PgEncoder::encode_execute_to(&mut buf);
    assert_eq!(buf.as_ref(), &[b'E', 0, 0, 0, 9, 0, 0, 0, 0, 0]);
}

#[test]
fn sync_to_produces_correct_bytes() {
    let mut buf = BytesMut::new();
    PgEncoder::encode_sync_to(&mut buf);
    assert_eq!(buf.as_ref(), &[b'S', 0, 0, 0, 4]);
}
