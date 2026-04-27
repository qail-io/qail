//! Wire protocol tests.

use super::backend::MAX_BACKEND_FRAME_LEN;
use super::*;

/// Helper: build a raw wire message from type byte + payload.
fn wire_msg(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let len = (payload.len() + 4) as u32;
    let mut buf = vec![msg_type];
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

// ========== Buffer boundary tests ==========

#[test]
fn decode_empty_buffer_returns_error() {
    assert!(BackendMessage::decode(&[]).is_err());
}

#[test]
fn decode_too_short_buffer_returns_error() {
    // 1-4 bytes are all too short for the 5-byte header
    for len in 1..5 {
        let buf = vec![b'Z'; len];
        let result = BackendMessage::decode(&buf);
        assert!(result.is_err(), "Expected error for {}-byte buffer", len);
    }
}

#[test]
fn decode_incomplete_message_returns_error() {
    // Header says length=100 but only 10 bytes present
    let mut buf = vec![b'Z'];
    buf.extend_from_slice(&100u32.to_be_bytes());
    buf.extend_from_slice(&[0u8; 5]); // only 5 payload bytes, need 96
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("Incomplete")
    );
}

#[test]
fn decode_oversized_message_returns_error() {
    let mut buf = vec![b'D'];
    buf.extend_from_slice(&((MAX_BACKEND_FRAME_LEN as u32) + 1).to_be_bytes());
    let err = BackendMessage::decode(&buf).unwrap_err();
    assert!(err.contains("Message too large"));
}

#[test]
fn decode_unknown_message_type_returns_error() {
    let buf = wire_msg(b'@', &[0]);
    let result = BackendMessage::decode(&buf);
    assert!(result.unwrap_err().contains("Unknown message type"));
}

// ========== Auth decode tests ==========

#[test]
fn decode_auth_ok() {
    let payload = 0i32.to_be_bytes();
    let buf = wire_msg(b'R', &payload);
    let (msg, consumed) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(msg, BackendMessage::AuthenticationOk));
    assert_eq!(consumed, buf.len());
}

#[test]
fn decode_auth_ok_with_trailing_bytes_returns_error() {
    let mut payload = 0i32.to_be_bytes().to_vec();
    payload.push(0xAA);
    let buf = wire_msg(b'R', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("invalid payload length")
    );
}

#[test]
fn decode_auth_payload_too_short() {
    // Auth needs at least 4 bytes for type field
    let buf = wire_msg(b'R', &[0, 0]);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("too short")
    );
}

#[test]
fn decode_auth_cleartext_password() {
    let payload = 3i32.to_be_bytes();
    let buf = wire_msg(b'R', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(
        msg,
        BackendMessage::AuthenticationCleartextPassword
    ));
}

#[test]
fn decode_auth_kerberos_v5() {
    let payload = 2i32.to_be_bytes();
    let buf = wire_msg(b'R', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(msg, BackendMessage::AuthenticationKerberosV5));
}

#[test]
fn decode_auth_gss() {
    let payload = 7i32.to_be_bytes();
    let buf = wire_msg(b'R', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(msg, BackendMessage::AuthenticationGSS));
}

#[test]
fn decode_auth_scm_credential() {
    let payload = 6i32.to_be_bytes();
    let buf = wire_msg(b'R', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(msg, BackendMessage::AuthenticationSCMCredential));
}

#[test]
fn decode_auth_sspi() {
    let payload = 9i32.to_be_bytes();
    let buf = wire_msg(b'R', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(msg, BackendMessage::AuthenticationSSPI));
}

#[test]
fn decode_auth_gss_continue() {
    let mut payload = 8i32.to_be_bytes().to_vec();
    payload.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
    let buf = wire_msg(b'R', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::AuthenticationGSSContinue(token) => {
            assert_eq!(token, vec![0xde, 0xad, 0xbe, 0xef]);
        }
        _ => panic!("Expected AuthenticationGSSContinue"),
    }
}

#[test]
fn decode_auth_md5_missing_salt() {
    // Auth type 5 (MD5) needs 8 bytes total (4 type + 4 salt)
    let mut payload = 5i32.to_be_bytes().to_vec();
    payload.extend_from_slice(&[0, 0, 0]); // only 3 salt bytes, need 4
    let buf = wire_msg(b'R', &payload);
    assert!(BackendMessage::decode(&buf).unwrap_err().contains("MD5"));
}

#[test]
fn decode_auth_md5_valid_salt() {
    let mut payload = 5i32.to_be_bytes().to_vec();
    payload.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    let buf = wire_msg(b'R', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::AuthenticationMD5Password(salt) => {
            assert_eq!(salt, [0xDE, 0xAD, 0xBE, 0xEF]);
        }
        _ => panic!("Expected MD5 auth"),
    }
}

#[test]
fn decode_auth_unknown_type_returns_error() {
    let payload = 99i32.to_be_bytes();
    let buf = wire_msg(b'R', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("Unknown auth type")
    );
}

#[test]
fn decode_auth_sasl_mechanisms() {
    let mut payload = 10i32.to_be_bytes().to_vec();
    payload.extend_from_slice(b"SCRAM-SHA-256\0\0"); // one mechanism + double null
    let buf = wire_msg(b'R', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::AuthenticationSASL(mechs) => {
            assert_eq!(mechs, vec!["SCRAM-SHA-256"]);
        }
        _ => panic!("Expected SASL auth"),
    }
}

#[test]
fn decode_auth_sasl_truncated_mechanism_list_returns_error() {
    let mut payload = 10i32.to_be_bytes().to_vec();
    payload.extend_from_slice(b"SCRAM-SHA-256"); // missing null terminator(s)
    let buf = wire_msg(b'R', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("terminator")
    );
}

#[test]
fn decode_auth_sasl_trailing_bytes_after_terminator_returns_error() {
    let mut payload = 10i32.to_be_bytes().to_vec();
    payload.extend_from_slice(b"SCRAM-SHA-256\0\0X");
    let buf = wire_msg(b'R', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("trailing bytes")
    );
}

// ========== ReadyForQuery tests ==========

#[test]
fn decode_ready_for_query_idle() {
    let buf = wire_msg(b'Z', b"I");
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(
        msg,
        BackendMessage::ReadyForQuery(TransactionStatus::Idle)
    ));
}

#[test]
fn decode_ready_for_query_in_transaction() {
    let buf = wire_msg(b'Z', b"T");
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(
        msg,
        BackendMessage::ReadyForQuery(TransactionStatus::InBlock)
    ));
}

#[test]
fn decode_ready_for_query_failed() {
    let buf = wire_msg(b'Z', b"E");
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(
        msg,
        BackendMessage::ReadyForQuery(TransactionStatus::Failed)
    ));
}

#[test]
fn decode_ready_for_query_empty_payload() {
    let buf = wire_msg(b'Z', &[]);
    assert!(BackendMessage::decode(&buf).unwrap_err().contains("empty"));
}

#[test]
fn decode_ready_for_query_unknown_status() {
    let buf = wire_msg(b'Z', b"X");
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("Unknown transaction")
    );
}

#[test]
fn decode_ready_for_query_with_trailing_bytes_returns_error() {
    let buf = wire_msg(b'Z', b"II");
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("payload")
    );
}

// ========== DataRow tests ==========

#[test]
fn decode_data_row_empty_columns() {
    let payload = 0i16.to_be_bytes();
    let buf = wire_msg(b'D', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::DataRow(cols) => assert!(cols.is_empty()),
        _ => panic!("Expected DataRow"),
    }
}

#[test]
fn decode_data_row_with_null() {
    let mut payload = 1i16.to_be_bytes().to_vec();
    payload.extend_from_slice(&(-1i32).to_be_bytes()); // NULL
    let buf = wire_msg(b'D', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::DataRow(cols) => {
            assert_eq!(cols.len(), 1);
            assert!(cols[0].is_none());
        }
        _ => panic!("Expected DataRow"),
    }
}

#[test]
fn decode_data_row_with_value() {
    let mut payload = 1i16.to_be_bytes().to_vec();
    let data = b"hello";
    payload.extend_from_slice(&(data.len() as i32).to_be_bytes());
    payload.extend_from_slice(data);
    let buf = wire_msg(b'D', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::DataRow(cols) => {
            assert_eq!(cols.len(), 1);
            assert_eq!(cols[0].as_deref(), Some(b"hello".as_slice()));
        }
        _ => panic!("Expected DataRow"),
    }
}

#[test]
fn decode_data_row_negative_count_returns_error() {
    let payload = (-1i16).to_be_bytes();
    let buf = wire_msg(b'D', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("invalid column count")
    );
}

#[test]
fn decode_data_row_invalid_negative_length_returns_error() {
    let mut payload = 1i16.to_be_bytes().to_vec();
    payload.extend_from_slice(&(-2i32).to_be_bytes());
    let buf = wire_msg(b'D', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("invalid column length")
    );
}

#[test]
fn decode_data_row_truncated_column_data() {
    let mut payload = 1i16.to_be_bytes().to_vec();
    // Claims 100 bytes of data but payload ends immediately
    payload.extend_from_slice(&100i32.to_be_bytes());
    let buf = wire_msg(b'D', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("truncated")
    );
}

#[test]
fn decode_data_row_trailing_bytes_returns_error() {
    let mut payload = 1i16.to_be_bytes().to_vec();
    payload.extend_from_slice(&1i32.to_be_bytes());
    payload.push(b'x');
    payload.push(0xAA); // trailing garbage
    let buf = wire_msg(b'D', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("trailing")
    );
}

#[test]
fn decode_data_row_payload_too_short() {
    let buf = wire_msg(b'D', &[0]); // only 1 byte, need 2
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("too short")
    );
}

#[test]
fn decode_data_row_claims_too_many_columns() {
    // Claims 1000 columns but only a few bytes of payload
    let payload = 1000i16.to_be_bytes();
    let buf = wire_msg(b'D', &payload);
    assert!(BackendMessage::decode(&buf).unwrap_err().contains("claims"));
}

// ========== RowDescription tests ==========

#[test]
fn decode_row_description_zero_fields() {
    let payload = 0i16.to_be_bytes();
    let buf = wire_msg(b'T', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::RowDescription(fields) => assert!(fields.is_empty()),
        _ => panic!("Expected RowDescription"),
    }
}

#[test]
fn decode_row_description_negative_count() {
    let payload = (-1i16).to_be_bytes();
    let buf = wire_msg(b'T', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("invalid field count")
    );
}

#[test]
fn decode_row_description_truncated_field() {
    let mut payload = 1i16.to_be_bytes().to_vec();
    payload.extend_from_slice(b"id\0"); // field name
    // Missing 18 bytes of fixed field data
    let buf = wire_msg(b'T', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("truncated")
    );
}

#[test]
fn decode_row_description_single_field() {
    let mut payload = 1i16.to_be_bytes().to_vec();
    payload.extend_from_slice(b"id\0"); // name
    payload.extend_from_slice(&0u32.to_be_bytes()); // table_oid
    payload.extend_from_slice(&0i16.to_be_bytes()); // column_attr
    payload.extend_from_slice(&23u32.to_be_bytes()); // type_oid (int4)
    payload.extend_from_slice(&4i16.to_be_bytes()); // type_size
    payload.extend_from_slice(&(-1i32).to_be_bytes()); // type_modifier
    payload.extend_from_slice(&0i16.to_be_bytes()); // format (text)
    let buf = wire_msg(b'T', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::RowDescription(fields) => {
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name, "id");
            assert_eq!(fields[0].type_oid, 23); // int4
        }
        _ => panic!("Expected RowDescription"),
    }
}

#[test]
fn decode_row_description_with_trailing_bytes_returns_error() {
    let mut payload = 0i16.to_be_bytes().to_vec();
    payload.push(0xAA);
    let buf = wire_msg(b'T', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("trailing")
    );
}

#[test]
fn decode_row_description_invalid_format_code_returns_error() {
    let mut payload = 1i16.to_be_bytes().to_vec();
    payload.extend_from_slice(b"id\0"); // name
    payload.extend_from_slice(&0u32.to_be_bytes()); // table_oid
    payload.extend_from_slice(&0i16.to_be_bytes()); // column_attr
    payload.extend_from_slice(&23u32.to_be_bytes()); // type_oid
    payload.extend_from_slice(&4i16.to_be_bytes()); // type_size
    payload.extend_from_slice(&(-1i32).to_be_bytes()); // type_modifier
    payload.extend_from_slice(&7i16.to_be_bytes()); // invalid format
    let buf = wire_msg(b'T', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("invalid format code")
    );
}

// ========== BackendKeyData tests ==========

#[test]
fn decode_backend_key_data() {
    let mut payload = 42i32.to_be_bytes().to_vec();
    payload.extend_from_slice(&99i32.to_be_bytes());
    let buf = wire_msg(b'K', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::BackendKeyData {
            process_id,
            secret_key,
        } => {
            assert_eq!(process_id, 42);
            assert_eq!(secret_key, 99i32.to_be_bytes());
        }
        _ => panic!("Expected BackendKeyData"),
    }
}

#[test]
fn decode_backend_key_too_short() {
    let buf = wire_msg(b'K', &[0, 0, 0, 42]); // only 4 bytes, need 8
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("too short")
    );
}

#[test]
fn decode_backend_key_extended_secret_key() {
    let mut payload = 7i32.to_be_bytes().to_vec();
    payload.extend_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55]);
    let buf = wire_msg(b'K', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::BackendKeyData {
            process_id,
            secret_key,
        } => {
            assert_eq!(process_id, 7);
            assert_eq!(secret_key, vec![0x11, 0x22, 0x33, 0x44, 0x55]);
        }
        _ => panic!("Expected BackendKeyData"),
    }
}

#[test]
fn decode_backend_key_secret_key_too_small_or_too_large() {
    let mut too_small = 1i32.to_be_bytes().to_vec();
    too_small.extend_from_slice(&[0xAA, 0xBB, 0xCC]); // 3 bytes
    let err_small = BackendMessage::decode(&wire_msg(b'K', &too_small)).unwrap_err();
    assert!(err_small.contains("too short") || err_small.contains("invalid secret key length"));

    let mut too_large = 1i32.to_be_bytes().to_vec();
    too_large.extend_from_slice(&vec![0u8; 257]); // >256
    let err_large = BackendMessage::decode(&wire_msg(b'K', &too_large)).unwrap_err();
    assert!(err_large.contains("invalid secret key length"));
}

// ========== NegotiateProtocolVersion tests ==========

#[test]
fn decode_negotiate_protocol_version() {
    let mut payload = 2i32.to_be_bytes().to_vec(); // newest_minor_supported
    payload.extend_from_slice(&2i32.to_be_bytes()); // 2 option strings
    payload.extend_from_slice(b"foo\0bar\0");
    let (msg, _) = BackendMessage::decode(&wire_msg(b'v', &payload)).unwrap();
    match msg {
        BackendMessage::NegotiateProtocolVersion {
            newest_minor_supported,
            unrecognized_protocol_options,
        } => {
            assert_eq!(newest_minor_supported, 2);
            assert_eq!(
                unrecognized_protocol_options,
                vec!["foo".to_string(), "bar".to_string()]
            );
        }
        _ => panic!("Expected NegotiateProtocolVersion"),
    }
}

#[test]
fn decode_negotiate_protocol_version_rejects_malformed_payloads() {
    let err_short = BackendMessage::decode(&wire_msg(b'v', &[0, 0, 0, 2])).unwrap_err();
    assert!(err_short.contains("too short"));

    let mut negative_count = 2i32.to_be_bytes().to_vec();
    negative_count.extend_from_slice(&(-1i32).to_be_bytes());
    let err_neg = BackendMessage::decode(&wire_msg(b'v', &negative_count)).unwrap_err();
    assert!(err_neg.contains("negative"));

    let mut missing_terminator = 2i32.to_be_bytes().to_vec();
    missing_terminator.extend_from_slice(&1i32.to_be_bytes());
    missing_terminator.extend_from_slice(b"unterminated");
    let err_term = BackendMessage::decode(&wire_msg(b'v', &missing_terminator)).unwrap_err();
    assert!(err_term.contains("terminator"));
}

#[test]
fn decode_negotiate_protocol_version_rejects_count_exceeding_payload_capacity() {
    let mut payload = 2i32.to_be_bytes().to_vec();
    payload.extend_from_slice(&1024i32.to_be_bytes());
    let err = BackendMessage::decode(&wire_msg(b'v', &payload)).unwrap_err();
    assert!(err.contains("exceeds payload capacity"));
}

// ========== ErrorResponse tests ==========

#[test]
fn decode_error_response_with_fields() {
    let mut payload = Vec::new();
    payload.push(b'S');
    payload.extend_from_slice(b"ERROR\0");
    payload.push(b'C');
    payload.extend_from_slice(b"42P01\0");
    payload.push(b'M');
    payload.extend_from_slice(b"relation does not exist\0");
    payload.push(0); // terminator
    let buf = wire_msg(b'E', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::ErrorResponse(fields) => {
            assert_eq!(fields.severity, "ERROR");
            assert_eq!(fields.code, "42P01");
            assert_eq!(fields.message, "relation does not exist");
        }
        _ => panic!("Expected ErrorResponse"),
    }
}

#[test]
fn decode_error_response_empty() {
    let buf = wire_msg(b'E', &[0]); // just terminator
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::ErrorResponse(fields) => {
            assert!(fields.message.is_empty());
        }
        _ => panic!("Expected ErrorResponse"),
    }
}

#[test]
fn decode_error_response_missing_final_terminator_returns_error() {
    let payload = vec![b'S', b'E', b'R', b'R', b'O', b'R'];
    let buf = wire_msg(b'E', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("missing final terminator")
    );
}

// ========== CommandComplete tests ==========

#[test]
fn decode_command_complete() {
    let buf = wire_msg(b'C', b"INSERT 0 1\0");
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::CommandComplete(tag) => assert_eq!(tag, "INSERT 0 1"),
        _ => panic!("Expected CommandComplete"),
    }
}

#[test]
fn decode_command_complete_missing_null_returns_error() {
    let buf = wire_msg(b'C', b"INSERT 0 1");
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("missing null")
    );
}

#[test]
fn decode_command_complete_interior_null_returns_error() {
    let buf = wire_msg(b'C', b"INSERT\0junk\0");
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("interior null")
    );
}

// ========== Simple type tests ==========

#[test]
fn decode_parse_complete() {
    let buf = wire_msg(b'1', &[]);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(msg, BackendMessage::ParseComplete));
}

#[test]
fn decode_parse_complete_with_payload_returns_error() {
    let buf = wire_msg(b'1', &[0xAA]);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("ParseComplete")
    );
}

#[test]
fn decode_bind_complete() {
    let buf = wire_msg(b'2', &[]);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(msg, BackendMessage::BindComplete));
}

#[test]
fn decode_bind_complete_with_payload_returns_error() {
    let buf = wire_msg(b'2', &[0xAA]);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("BindComplete")
    );
}

#[test]
fn decode_close_complete() {
    let buf = wire_msg(b'3', &[]);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(msg, BackendMessage::CloseComplete));
}

#[test]
fn decode_close_complete_with_payload_returns_error() {
    let buf = wire_msg(b'3', &[0xAA]);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("CloseComplete")
    );
}

#[test]
fn decode_no_data() {
    let buf = wire_msg(b'n', &[]);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(msg, BackendMessage::NoData));
}

#[test]
fn decode_no_data_with_payload_returns_error() {
    let buf = wire_msg(b'n', &[0xAA]);
    assert!(BackendMessage::decode(&buf).unwrap_err().contains("NoData"));
}

#[test]
fn decode_portal_suspended() {
    let buf = wire_msg(b's', &[]);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(msg, BackendMessage::PortalSuspended));
}

#[test]
fn decode_portal_suspended_with_payload_returns_error() {
    let buf = wire_msg(b's', &[0xAA]);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("PortalSuspended")
    );
}

#[test]
fn decode_empty_query_response() {
    let buf = wire_msg(b'I', &[]);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(msg, BackendMessage::EmptyQueryResponse));
}

#[test]
fn decode_empty_query_response_with_payload_returns_error() {
    let buf = wire_msg(b'I', &[0xAA]);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("EmptyQueryResponse")
    );
}

// ========== NotificationResponse tests ==========

#[test]
fn decode_notification_response() {
    let mut payload = 1i32.to_be_bytes().to_vec();
    payload.extend_from_slice(b"my_channel\0");
    payload.extend_from_slice(b"hello world\0");
    let buf = wire_msg(b'A', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::NotificationResponse {
            process_id,
            channel,
            payload,
        } => {
            assert_eq!(process_id, 1);
            assert_eq!(channel, "my_channel");
            assert_eq!(payload, "hello world");
        }
        _ => panic!("Expected NotificationResponse"),
    }
}

#[test]
fn decode_notification_too_short() {
    let buf = wire_msg(b'A', &[0, 0]); // need at least 4 bytes
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("too short")
    );
}

#[test]
fn decode_notification_missing_payload_terminator_returns_error() {
    let mut payload = 1i32.to_be_bytes().to_vec();
    payload.extend_from_slice(b"my_channel\0");
    payload.extend_from_slice(b"hello world"); // no final null terminator
    let buf = wire_msg(b'A', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("payload null terminator")
    );
}

// ========== CopyInResponse / CopyOutResponse tests ==========

#[test]
fn decode_copy_in_response_empty_payload() {
    let buf = wire_msg(b'G', &[]);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("too short")
    );
}

#[test]
fn decode_copy_out_response_empty_payload() {
    let buf = wire_msg(b'H', &[]);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("too short")
    );
}

#[test]
fn decode_copy_in_response_text_format() {
    let mut payload = vec![0u8]; // text format
    payload.extend_from_slice(&1i16.to_be_bytes()); // 1 column
    payload.extend_from_slice(&0i16.to_be_bytes()); // column format: text
    let buf = wire_msg(b'G', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::CopyInResponse {
            format,
            column_formats,
        } => {
            assert_eq!(format, 0);
            assert_eq!(column_formats, vec![0]);
        }
        _ => panic!("Expected CopyInResponse"),
    }
}

#[test]
fn decode_copy_in_response_truncated_column_formats_returns_error() {
    let mut payload = vec![0u8]; // text format
    payload.extend_from_slice(&1i16.to_be_bytes()); // claims 1 column
    payload.push(0u8); // only half of i16 format code
    let buf = wire_msg(b'G', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("truncated column format")
    );
}

#[test]
fn decode_copy_in_response_invalid_overall_format_returns_error() {
    let mut payload = vec![2u8]; // invalid overall format (must be 0 or 1)
    payload.extend_from_slice(&0i16.to_be_bytes());
    let buf = wire_msg(b'G', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("invalid overall format")
    );
}

#[test]
fn decode_copy_in_response_invalid_column_format_returns_error() {
    let mut payload = vec![0u8];
    payload.extend_from_slice(&1i16.to_be_bytes());
    payload.extend_from_slice(&2i16.to_be_bytes()); // invalid per-column format
    let buf = wire_msg(b'G', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("invalid format code")
    );
}

#[test]
fn decode_copy_out_response_trailing_bytes_returns_error() {
    let mut payload = vec![0u8];
    payload.extend_from_slice(&0i16.to_be_bytes());
    payload.push(0xAA); // trailing garbage
    let buf = wire_msg(b'H', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("trailing")
    );
}

#[test]
fn decode_copy_both_response_binary_format() {
    let mut payload = vec![1u8]; // binary format
    payload.extend_from_slice(&2i16.to_be_bytes()); // 2 columns
    payload.extend_from_slice(&1i16.to_be_bytes()); // column 1 binary
    payload.extend_from_slice(&0i16.to_be_bytes()); // column 2 text
    let buf = wire_msg(b'W', &payload);
    let (msg, _) = BackendMessage::decode(&buf).unwrap();
    match msg {
        BackendMessage::CopyBothResponse {
            format,
            column_formats,
        } => {
            assert_eq!(format, 1);
            assert_eq!(column_formats, vec![1, 0]);
        }
        _ => panic!("Expected CopyBothResponse"),
    }
}

#[test]
fn decode_copy_both_response_invalid_column_format_returns_error() {
    let mut payload = vec![0u8];
    payload.extend_from_slice(&1i16.to_be_bytes());
    payload.extend_from_slice(&2i16.to_be_bytes()); // invalid per-column format
    let buf = wire_msg(b'W', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("CopyBothResponse invalid format code")
    );
}

#[test]
fn decode_copy_done_with_payload_returns_error() {
    let buf = wire_msg(b'c', &[0xAA]);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("CopyDone")
    );
}

#[test]
fn decode_parameter_status_missing_terminator_returns_error() {
    let buf = wire_msg(b'S', b"client_encoding");
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("missing name terminator")
    );
}

#[test]
fn decode_parameter_status_trailing_bytes_returns_error() {
    let payload = b"client_encoding\0UTF8\0X";
    let buf = wire_msg(b'S', payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("trailing")
    );
}

#[test]
fn decode_parameter_description_short_payload_returns_error() {
    let buf = wire_msg(b't', &[0u8]);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("payload too short")
    );
}

#[test]
fn decode_parameter_description_truncated_returns_error() {
    let mut payload = 2i16.to_be_bytes().to_vec(); // claims 2 params => needs 8 bytes
    payload.extend_from_slice(&23u32.to_be_bytes()); // only 1 OID provided
    let buf = wire_msg(b't', &payload);
    assert!(
        BackendMessage::decode(&buf)
            .unwrap_err()
            .contains("ParameterDescription truncated")
    );
}

// ========== Message consumed length test ==========

#[test]
fn decode_consumed_length_is_correct() {
    let buf = wire_msg(b'Z', b"I");
    let (_, consumed) = BackendMessage::decode(&buf).unwrap();
    assert_eq!(consumed, buf.len());
}

#[test]
fn decode_with_trailing_data_only_consumes_one_message() {
    let mut buf = wire_msg(b'Z', b"I");
    buf.extend_from_slice(&wire_msg(b'Z', b"T")); // second message appended
    let (msg, consumed) = BackendMessage::decode(&buf).unwrap();
    assert!(matches!(
        msg,
        BackendMessage::ReadyForQuery(TransactionStatus::Idle)
    ));
    // Should only consume the first message
    assert_eq!(consumed, 6); // 1 type + 4 length + 1 payload
}

// ========== FrontendMessage encode roundtrip tests ==========

#[test]
fn encode_sync() {
    let msg = FrontendMessage::Sync;
    let encoded = msg.encode_checked().unwrap();
    assert_eq!(encoded, vec![b'S', 0, 0, 0, 4]);
}

#[test]
fn encode_gss_response() {
    let msg = FrontendMessage::GSSResponse(vec![1, 2, 3, 4]);
    let encoded = msg.encode_checked().unwrap();
    assert_eq!(encoded[0], b'p');
    let len = i32::from_be_bytes([encoded[1], encoded[2], encoded[3], encoded[4]]);
    assert_eq!(len, 8);
    assert_eq!(&encoded[5..], &[1, 2, 3, 4]);
}

#[test]
fn encode_query_with_interior_nul_returns_error() {
    let msg = FrontendMessage::Query("select 1\0drop table x".to_string());
    assert!(msg.encode_checked().is_err());
}

#[test]
fn encode_parse_too_many_param_types_returns_error() {
    let msg = FrontendMessage::Parse {
        name: "".to_string(),
        query: "select 1".to_string(),
        param_types: vec![0u32; 32768],
    };
    assert!(msg.encode_checked().is_err());
}

#[test]
fn encode_bind_too_many_params_returns_error() {
    let msg = FrontendMessage::Bind {
        portal: "".to_string(),
        statement: "".to_string(),
        params: vec![None; 32768],
    };
    assert!(msg.encode_checked().is_err());
}

#[test]
fn encode_execute_negative_max_rows_returns_error() {
    let msg = FrontendMessage::Execute {
        portal: "".to_string(),
        max_rows: -1,
    };
    assert!(msg.encode_checked().is_err());
}

#[test]
fn encode_startup_with_interior_nul_returns_error() {
    let msg = FrontendMessage::Startup {
        user: "user\0x".to_string(),
        database: "db".to_string(),
        protocol_version: PROTOCOL_VERSION_3_2,
        startup_params: Vec::new(),
    };
    assert!(msg.encode_checked().is_err());
}

#[test]
fn encode_startup_with_extra_params() {
    let msg = FrontendMessage::Startup {
        user: "alice".to_string(),
        database: "app".to_string(),
        protocol_version: PROTOCOL_VERSION_3_2,
        startup_params: vec![("replication".to_string(), "database".to_string())],
    };
    let encoded = msg.encode_checked().unwrap();
    assert_eq!(&encoded[4..8], &PROTOCOL_VERSION_3_2.to_be_bytes());
    assert!(
        encoded
            .windows("user\0alice\0".len())
            .any(|w| w == b"user\0alice\0")
    );
    assert!(
        encoded
            .windows("database\0app\0".len())
            .any(|w| w == b"database\0app\0")
    );
    assert!(
        encoded
            .windows("replication\0database\0".len())
            .any(|w| w == b"replication\0database\0")
    );
    assert_eq!(encoded.last().copied(), Some(0));
}

#[test]
fn encode_startup_with_protocol_3_0_compat() {
    let msg = FrontendMessage::Startup {
        user: "alice".to_string(),
        database: "app".to_string(),
        protocol_version: PROTOCOL_VERSION_3_0,
        startup_params: Vec::new(),
    };
    let encoded = msg.encode_checked().expect("encode startup");
    assert_eq!(&encoded[4..8], &PROTOCOL_VERSION_3_0.to_be_bytes());
}

#[test]
fn encode_startup_with_reserved_param_key_returns_error() {
    let msg = FrontendMessage::Startup {
        user: "alice".to_string(),
        database: "app".to_string(),
        protocol_version: PROTOCOL_VERSION_3_2,
        startup_params: vec![("user".to_string(), "mallory".to_string())],
    };
    assert!(msg.encode_checked().is_err());
}

#[test]
fn encode_startup_with_duplicate_param_keys_returns_error() {
    let msg = FrontendMessage::Startup {
        user: "alice".to_string(),
        database: "app".to_string(),
        protocol_version: PROTOCOL_VERSION_3_2,
        startup_params: vec![
            ("application_name".to_string(), "a".to_string()),
            ("APPLICATION_NAME".to_string(), "b".to_string()),
        ],
    };
    assert!(msg.encode_checked().is_err());
}

#[test]
fn encode_terminate() {
    let msg = FrontendMessage::Terminate;
    let encoded = msg.encode_checked().unwrap();
    assert_eq!(encoded, vec![b'X', 0, 0, 0, 4]);
}
