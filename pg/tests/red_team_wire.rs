//! Wire Protocol Red-Team Tests
//!
//! Adversarial edge cases for the PostgreSQL wire protocol encoder/decoder.
//! Covers scenarios #8 (wide tables) and #15 (binary protocol edge types).

use qail_pg::protocol::wire::{
    BackendMessage, FrontendMessage,
};

// ══════════════════════════════════════════════════════════════════════
// #8: Extremely Wide Table — 200+ columns RowDescription
// ══════════════════════════════════════════════════════════════════════

/// Build a valid RowDescription wire message with N columns.
fn build_row_description(n_columns: usize) -> Vec<u8> {
    let mut payload = Vec::new();
    // Field count (i16)
    payload.extend_from_slice(&(n_columns as i16).to_be_bytes());
    
    for i in 0..n_columns {
        let name = format!("col_{}\0", i);
        payload.extend_from_slice(name.as_bytes());
        // table_oid (4) + column_attr (2) + type_oid (4) + type_size (2) + type_modifier (4) + format (2) = 18
        payload.extend_from_slice(&0u32.to_be_bytes()); // table_oid
        payload.extend_from_slice(&0i16.to_be_bytes()); // column_attr
        payload.extend_from_slice(&23u32.to_be_bytes()); // type_oid (int4)
        payload.extend_from_slice(&4i16.to_be_bytes()); // type_size
        payload.extend_from_slice(&(-1i32).to_be_bytes()); // type_modifier
        payload.extend_from_slice(&0i16.to_be_bytes()); // format (text)
    }
    
    // Wrap in message envelope: 'T' + length
    let len = (payload.len() + 4) as i32;
    let mut msg = vec![b'T'];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&payload);
    msg
}

#[test]
fn redteam_wide_table_200_columns() {
    let msg = build_row_description(200);
    let (decoded, consumed) = BackendMessage::decode(&msg).expect("Should decode 200 columns");
    assert_eq!(consumed, msg.len());
    match decoded {
        BackendMessage::RowDescription(fields) => {
            assert_eq!(fields.len(), 200, "Must decode all 200 columns");
            assert_eq!(fields[0].name, "col_0");
            assert_eq!(fields[199].name, "col_199");
        }
        other => panic!("Expected RowDescription, got {:?}", other),
    }
}

#[test]
fn redteam_wide_table_500_columns() {
    let msg = build_row_description(500);
    let (decoded, _) = BackendMessage::decode(&msg).expect("Should decode 500 columns");
    match decoded {
        BackendMessage::RowDescription(fields) => {
            assert_eq!(fields.len(), 500, "Must decode all 500 columns");
        }
        _ => panic!("Expected RowDescription"),
    }
}

// ══════════════════════════════════════════════════════════════════════
// #15: Binary Protocol Edge Types
// ══════════════════════════════════════════════════════════════════════

/// Build a valid DataRow wire message with given column values.
fn build_data_row(columns: &[Option<&[u8]>]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for col in columns {
        match col {
            None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(data) => {
                payload.extend_from_slice(&(data.len() as i32).to_be_bytes());
                payload.extend_from_slice(data);
            }
        }
    }
    let len = (payload.len() + 4) as i32;
    let mut msg = vec![b'D'];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&payload);
    msg
}

#[test]
fn redteam_data_row_all_nulls() {
    // Array of 10 NULLs
    let nulls: Vec<Option<&[u8]>> = vec![None; 10];
    let msg = build_data_row(&nulls);
    let (decoded, _) = BackendMessage::decode(&msg).expect("Should decode 10 NULLs");
    match decoded {
        BackendMessage::DataRow(cols) => {
            assert_eq!(cols.len(), 10);
            for col in &cols {
                assert!(col.is_none(), "All columns must be NULL");
            }
        }
        _ => panic!("Expected DataRow"),
    }
}

#[test]
fn redteam_data_row_mixed_null_and_data() {
    let data = b"hello";
    let columns: Vec<Option<&[u8]>> = vec![
        Some(data), None, Some(b""), None, Some(b"\x00\x01\x02"),
    ];
    let msg = build_data_row(&columns);
    let (decoded, _) = BackendMessage::decode(&msg).expect("Should decode mixed row");
    match decoded {
        BackendMessage::DataRow(cols) => {
            assert_eq!(cols.len(), 5);
            assert_eq!(cols[0].as_deref(), Some(b"hello".as_ref()));
            assert!(cols[1].is_none());
            assert_eq!(cols[2].as_deref(), Some(b"".as_ref())); // empty but not NULL
            assert!(cols[3].is_none());
            assert_eq!(cols[4].as_deref(), Some(b"\x00\x01\x02".as_ref()));
        }
        _ => panic!("Expected DataRow"),
    }
}

#[test]
fn redteam_data_row_zero_columns() {
    let msg = build_data_row(&[]);
    let (decoded, _) = BackendMessage::decode(&msg).expect("Should decode 0-column row");
    match decoded {
        BackendMessage::DataRow(cols) => assert_eq!(cols.len(), 0),
        _ => panic!("Expected DataRow"),
    }
}

#[test]
fn redteam_empty_query_response() {
    // EmptyQueryResponse: 'I' + length(4)
    let msg = vec![b'I', 0, 0, 0, 4];
    let (decoded, consumed) = BackendMessage::decode(&msg).expect("Should decode EmptyQueryResponse");
    assert_eq!(consumed, 5);
    assert!(matches!(decoded, BackendMessage::EmptyQueryResponse));
}

#[test]
fn redteam_error_response_with_all_fields() {
    // Build a complete ErrorResponse with all field types
    let mut payload = Vec::new();
    payload.push(b'S'); payload.extend_from_slice(b"FATAL\0");
    payload.push(b'C'); payload.extend_from_slice(b"42P01\0");
    payload.push(b'M'); payload.extend_from_slice(b"relation does not exist\0");
    payload.push(b'D'); payload.extend_from_slice(b"Table \"orders\" not found\0");
    payload.push(b'H'); payload.extend_from_slice(b"Check spelling\0");
    payload.push(0); // terminator
    
    let len = (payload.len() + 4) as i32;
    let mut msg = vec![b'E'];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&payload);
    
    let (decoded, _) = BackendMessage::decode(&msg).expect("Should decode error with all fields");
    match decoded {
        BackendMessage::ErrorResponse(fields) => {
            assert_eq!(fields.severity, "FATAL");
            assert_eq!(fields.code, "42P01");
            assert_eq!(fields.message, "relation does not exist");
            assert_eq!(fields.detail, Some("Table \"orders\" not found".to_string()));
            assert_eq!(fields.hint, Some("Check spelling".to_string()));
        }
        _ => panic!("Expected ErrorResponse"),
    }
}

#[test]
fn redteam_error_response_empty_payload() {
    // ErrorResponse with just a terminator
    let payload = vec![0u8]; // only terminator
    let len = (payload.len() + 4) as i32;
    let mut msg = vec![b'E'];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&payload);
    
    let (decoded, _) = BackendMessage::decode(&msg).expect("Should decode empty error");
    match decoded {
        BackendMessage::ErrorResponse(fields) => {
            assert_eq!(fields.severity, "");
            assert_eq!(fields.code, "");
        }
        _ => panic!("Expected ErrorResponse"),
    }
}

// ══════════════════════════════════════════════════════════════════════
// #15: FrontendMessage edge cases
// ══════════════════════════════════════════════════════════════════════

#[test]
fn redteam_encode_bind_many_null_params() {
    let params: Vec<Option<Vec<u8>>> = vec![None; 100];
    let msg = FrontendMessage::Bind {
        portal: String::new(),
        statement: String::new(),
        params,
    };
    let bytes = msg.encode();
    // Must not panic and must have correct structure
    assert_eq!(bytes[0], b'B');
    let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    assert_eq!(bytes.len(), len + 1);
}

#[test]
fn redteam_encode_query_empty_string() {
    let msg = FrontendMessage::Query(String::new());
    let bytes = msg.encode();
    assert_eq!(bytes[0], b'Q');
    // SQL "" + null terminator = 1 byte
    assert_eq!(*bytes.last().unwrap(), 0u8);
}

#[test]
fn redteam_encode_parse_many_param_types() {
    let msg = FrontendMessage::Parse {
        name: String::new(),
        query: "SELECT $1, $2, $3, $4, $5".to_string(),
        param_types: vec![23, 25, 700, 1043, 1184], // int4, text, float4, varchar, timestamptz
    };
    let bytes = msg.encode();
    assert_eq!(bytes[0], b'P');
    let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    assert_eq!(bytes.len(), len + 1);
}

// ══════════════════════════════════════════════════════════════════════
// #13: Duplicate relation in backend decode (invalid column count)
// ══════════════════════════════════════════════════════════════════════

#[test]
fn redteam_data_row_declares_more_columns_than_data() {
    // DataRow says 5 columns but only has data for 2
    let mut payload = Vec::new();
    payload.extend_from_slice(&5i16.to_be_bytes()); // claims 5
    // Only provide 2 columns
    payload.extend_from_slice(&(-1i32).to_be_bytes()); // col 0: NULL
    payload.extend_from_slice(&4i32.to_be_bytes()); // col 1: 4 bytes
    payload.extend_from_slice(b"data");
    // Missing col 2-4 data
    
    let len = (payload.len() + 4) as i32;
    let mut msg = vec![b'D'];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&payload);
    
    let result = BackendMessage::decode(&msg);
    assert!(result.is_err(), "Truncated DataRow must return error");
}

#[test]
fn redteam_row_description_declares_more_fields_than_data() {
    // Claims 3 fields but only has data for 1
    let mut payload = Vec::new();
    payload.extend_from_slice(&3i16.to_be_bytes());
    payload.extend_from_slice(b"col_0\0");
    payload.extend_from_slice(&0u32.to_be_bytes()); // table_oid
    payload.extend_from_slice(&0i16.to_be_bytes()); // column_attr
    payload.extend_from_slice(&23u32.to_be_bytes()); // type_oid
    payload.extend_from_slice(&4i16.to_be_bytes()); // type_size
    // Missing type_modifier and format for field 0, and all of fields 1-2
    
    let len = (payload.len() + 4) as i32;
    let mut msg = vec![b'T'];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&payload);
    
    let result = BackendMessage::decode(&msg);
    assert!(result.is_err(), "Truncated RowDescription must return error");
}

// ══════════════════════════════════════════════════════════════════════
// TIER X: Extreme Wire Protocol Edge Cases
// ══════════════════════════════════════════════════════════════════════

/// Unknown message type byte — decoder must not panic
#[test]
fn tierx_unknown_message_type() {
    // 0xFF is not a valid PostgreSQL backend message type
    let msg = vec![0xFF, 0, 0, 0, 4]; // type=0xFF, length=4 (just the length field)
    let result = BackendMessage::decode(&msg);
    // Should either parse as unknown or return error — must NOT panic
    assert!(result.is_ok() || result.is_err());
}

/// Negative column count in RowDescription (i16 = -1)
#[test]
fn tierx_negative_column_count_row_description() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(-1i16).to_be_bytes()); // -1 columns
    let len = (payload.len() + 4) as i32;
    let mut msg = vec![b'T'];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&payload);
    
    let result = BackendMessage::decode(&msg);
    // Must not panic. Should return error or parse as 0 columns.
    assert!(result.is_ok() || result.is_err(),
            "Negative column count must not panic");
}

/// DataRow with maximum i32 column length (claims 2GB column)
#[test]
fn tierx_data_row_gigantic_column_length() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1i16.to_be_bytes()); // 1 column
    payload.extend_from_slice(&i32::MAX.to_be_bytes()); // claims 2GB data
    // But we don't provide 2GB of data — only a few bytes
    payload.extend_from_slice(b"tiny");
    
    let len = (payload.len() + 4) as i32;
    let mut msg = vec![b'D'];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&payload);
    
    let result = BackendMessage::decode(&msg);
    assert!(result.is_err(), "Oversized column length claim must error, not OOM");
}

/// ErrorResponse with extremely long message field (10KB)
#[test]
fn tierx_error_response_huge_message() {
    let huge_msg = "X".repeat(10_000);
    let mut payload = Vec::new();
    payload.push(b'S'); payload.extend_from_slice(b"ERROR\0");
    payload.push(b'C'); payload.extend_from_slice(b"99999\0");
    payload.push(b'M'); 
    payload.extend_from_slice(huge_msg.as_bytes());
    payload.push(0); // null terminator for M field
    payload.push(0); // terminator
    
    let len = (payload.len() + 4) as i32;
    let mut msg = vec![b'E'];
    msg.extend_from_slice(&len.to_be_bytes());
    msg.extend_from_slice(&payload);
    
    let (decoded, _) = BackendMessage::decode(&msg).expect("Should handle large error");
    match decoded {
        BackendMessage::ErrorResponse(fields) => {
            assert_eq!(fields.message.len(), 10_000);
        }
        _ => panic!("Expected ErrorResponse"),
    }
}

/// FrontendMessage::Query with embedded NULL byte (should be caught by driver)
#[test]
fn tierx_query_with_null_byte() {
    let msg = FrontendMessage::Query("SELECT 1\0; DROP TABLE users".to_string());
    let bytes = msg.encode();
    // Encoder should not panic — NULL byte handling is at the driver layer
    assert_eq!(bytes[0], b'Q');
}

/// Bind with 32767 (i16::MAX) params
#[test]
fn tierx_bind_max_params() {
    let params: Vec<Option<Vec<u8>>> = vec![Some(b"x".to_vec()); 32767];
    let msg = FrontendMessage::Bind {
        portal: String::new(),
        statement: "bulk_stmt".to_string(),
        params,
    };
    let bytes = msg.encode();
    assert_eq!(bytes[0], b'B');
    // Must not panic and must produce valid wire format
    let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    assert_eq!(bytes.len(), len + 1);
}

/// Parse with empty query string
#[test]
fn tierx_parse_empty_query() {
    let msg = FrontendMessage::Parse {
        name: "empty_stmt".to_string(),
        query: String::new(),
        param_types: vec![],
    };
    let bytes = msg.encode();
    assert_eq!(bytes[0], b'P');
    let len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    assert_eq!(bytes.len(), len + 1);
}
