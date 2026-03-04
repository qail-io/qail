//! Wire-Protocol Property-Based Tests
//!
//! Fuzz the PostgreSQL wire protocol encoder/decoder using proptest.
//!
//! Properties verified:
//! 1. FrontendMessage::encode_checked() always produces well-formed wire messages
//! 2. BackendMessage::decode() never panics on arbitrary bytes
//! 3. PgEncoder produces valid message boundaries (type + length + payload)

use proptest::prelude::*;
use qail_pg::protocol::wire::{BackendMessage, FrontendMessage};

// ============================================================================
// Strategy: arbitrary safe strings (no interior NULLs - PG protocol invariant)
// ============================================================================

/// Generate strings without interior null bytes (PG protocol uses null terminators)
fn arb_pg_string() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_]{0,64}".prop_map(|s| s)
}

/// Generate optional binary params (Some(bytes) or None for NULL)
fn arb_param() -> impl Strategy<Value = Option<Vec<u8>>> {
    prop_oneof![
        3 => Just(None),
        7 => proptest::collection::vec(any::<u8>(), 0..128).prop_map(Some),
    ]
}

/// Generate arbitrary FrontendMessage variants
fn arb_frontend_message() -> impl Strategy<Value = FrontendMessage> {
    prop_oneof![
        // Startup
        (arb_pg_string(), arb_pg_string())
            .prop_map(|(user, database)| FrontendMessage::Startup {
                user,
                database,
                startup_params: Vec::new(),
            }),
        // Query (simple protocol)
        arb_pg_string().prop_map(FrontendMessage::Query),
        // Parse (extended protocol)
        (
            arb_pg_string(),
            arb_pg_string(),
            proptest::collection::vec(any::<u32>(), 0..8),
        )
            .prop_map(|(name, query, param_types)| FrontendMessage::Parse {
                name,
                query,
                param_types,
            }),
        // Bind (extended protocol)
        (
            arb_pg_string(),
            arb_pg_string(),
            proptest::collection::vec(arb_param(), 0..8),
        )
            .prop_map(|(portal, statement, params)| FrontendMessage::Bind {
                portal,
                statement,
                params,
            }),
        // Execute
        (arb_pg_string(), 0i32..=i32::MAX)
            .prop_map(|(portal, max_rows)| FrontendMessage::Execute { portal, max_rows }),
        // Sync
        Just(FrontendMessage::Sync),
        // Terminate
        Just(FrontendMessage::Terminate),
        // Password
        arb_pg_string().prop_map(FrontendMessage::PasswordMessage),
        // SASL
        (
            arb_pg_string(),
            proptest::collection::vec(any::<u8>(), 0..64)
        )
            .prop_map(|(mechanism, data)| FrontendMessage::SASLInitialResponse { mechanism, data }),
        proptest::collection::vec(any::<u8>(), 0..64).prop_map(FrontendMessage::SASLResponse),
    ]
}

// ============================================================================
// Property: FrontendMessage::encode_checked() produces well-formed wire bytes
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    /// Every encoded FrontendMessage must have a valid wire structure:
    /// - Non-Startup: type_byte (1) + length (4) + payload (length - 4)
    /// - Startup: length (4) + protocol_version (4) + params
    #[test]
    fn frontend_encode_valid_structure(msg in arb_frontend_message()) {
        let bytes = msg
            .encode_checked()
            .expect("safe generated frontend message must encode");

        match msg {
            FrontendMessage::Startup { .. } => {
                // Startup: first 4 bytes = total length (includes itself)
                prop_assert!(bytes.len() >= 8, "Startup must be ≥8 bytes");
                let declared_len = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
                prop_assert_eq!(bytes.len(), declared_len, "Startup length mismatch");

                // Protocol version 3.0 = 196608
                let version = i32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
                prop_assert_eq!(version, 196608, "Must use protocol version 3.0");
            }
            FrontendMessage::Sync | FrontendMessage::Terminate => {
                // Fixed 5-byte messages
                prop_assert_eq!(bytes.len(), 5, "Sync/Terminate must be exactly 5 bytes");
                let declared_len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
                prop_assert_eq!(declared_len, 4, "Fixed messages have length = 4");
            }
            _ => {
                // General: type_byte + i32 length + payload
                prop_assert!(bytes.len() >= 5, "Messages must be ≥5 bytes");
                let type_byte = bytes[0];
                prop_assert!(
                    [b'Q', b'P', b'B', b'E', b'p', b'X', b'S'].contains(&type_byte),
                    "Unknown type byte: {}",
                    type_byte as char
                );

                let declared_len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
                // declared_len includes itself (4 bytes) but NOT the type byte
                prop_assert_eq!(
                    bytes.len(),
                    declared_len + 1,
                    "Total length must be type_byte(1) + declared_length({})",
                    declared_len
                );
            }
        }
    }

    /// Every Query message must contain a null-terminated SQL string
    #[test]
    fn query_encode_null_terminated(sql in arb_pg_string()) {
        let msg = FrontendMessage::Query(sql);
        let bytes = msg.encode_checked().expect("query must encode");
        // Last byte of payload must be 0x00 (null terminator)
        prop_assert_eq!(*bytes.last().unwrap(), 0u8, "Query must be null-terminated");
    }

    /// Parse messages must contain two null-terminated strings (name + query)
    #[test]
    fn parse_encode_two_null_terminated_strings(
        name in arb_pg_string(),
        query in arb_pg_string(),
        param_types in proptest::collection::vec(any::<u32>(), 0..4),
    ) {
        let msg = FrontendMessage::Parse { name: name.clone(), query: query.clone(), param_types: param_types.clone() };
        let bytes = msg.encode_checked().expect("parse must encode");

        // Payload starts at byte 5 (after type + length)
        let payload = &bytes[5..];
        let null_positions: Vec<usize> = payload.iter().enumerate()
            .filter(|&(_, b)| *b == 0)
            .map(|(i, _)| i)
            .collect();

        // Must have at least 2 nulls (name terminator + query terminator)
        prop_assert!(
            null_positions.len() >= 2,
            "Parse must have ≥2 null terminators, found {}",
            null_positions.len()
        );

        // After the two null-terminated strings, we should have param_count (i16) + OIDs
        let after_query_null = null_positions[1] + 1;
        let remaining = &payload[after_query_null..];
        prop_assert!(remaining.len() >= 2, "Must have param count after strings");
        let param_count = i16::from_be_bytes([remaining[0], remaining[1]]) as usize;
        prop_assert_eq!(param_count, param_types.len(), "Param count mismatch");
    }
}

// ============================================================================
// Property: BackendMessage::decode() never panics on arbitrary input
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    /// BackendMessage::decode must never panic, regardless of input.
    /// It's allowed to return Err — but must not crash.
    #[test]
    fn backend_decode_never_panics(data in proptest::collection::vec(any::<u8>(), 0..256)) {
        // This must not panic — that's the entire property
        let _result = BackendMessage::decode(&data);
    }

    /// Backend decoder must return Err on too-short buffers
    #[test]
    fn backend_decode_rejects_short_buffers(data in proptest::collection::vec(any::<u8>(), 0..5)) {
        let result = BackendMessage::decode(&data);
        prop_assert!(result.is_err(), "Buffer < 5 bytes must be rejected");
    }

    /// Backend decoder must handle truncated messages (declared length > actual bytes)
    #[test]
    fn backend_decode_handles_truncated_messages(
        msg_type in any::<u8>(),
        extra_len in 10u32..1000u32,
        payload in proptest::collection::vec(any::<u8>(), 0..8),
    ) {
        // Build a message that declares more data than exists
        let declared_len = (payload.len() as u32) + 4 + extra_len;
        let mut buf = vec![msg_type];
        buf.extend_from_slice(&(declared_len as i32).to_be_bytes());
        buf.extend_from_slice(&payload);

        let result = BackendMessage::decode(&buf);
        prop_assert!(result.is_err(), "Truncated messages must return Err");
    }
}

// ============================================================================
// Property: PgEncoder produces valid message boundaries
// ============================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    /// PgEncoder::try_encode_query_string produces valid SimpleQuery wire messages
    #[test]
    fn pg_encoder_query_valid_wire(sql in arb_pg_string()) {
        use bytes::BytesMut;
        use qail_pg::protocol::encoder::PgEncoder;

        let buf: BytesMut = PgEncoder::try_encode_query_string(&sql).expect("safe sql must encode");
        let bytes = &buf[..];

        // Must start with 'Q'
        prop_assert_eq!(bytes[0], b'Q', "Query must start with 'Q'");

        // Length field
        let declared_len = i32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        prop_assert_eq!(bytes.len(), declared_len + 1, "Length must match actual size");

        // Must end with null terminator
        prop_assert_eq!(*bytes.last().unwrap(), 0u8, "Must be null-terminated");
    }

    /// PgEncoder::encode_extended_query produces valid multi-message pipeline
    #[test]
    fn pg_encoder_extended_pipeline_valid(
        sql in arb_pg_string(),
        params in proptest::collection::vec(arb_param(), 0..4),
    ) {
        use qail_pg::protocol::encoder::PgEncoder;

        let result = PgEncoder::encode_extended_query(&sql, &params);
        match result {
            Ok(buf) => {
                let bytes = &buf[..];
                // Must start with 'P' (Parse)
                prop_assert_eq!(bytes[0], b'P', "Extended query must start with Parse ('P')");

                // Walk through message boundaries
                let mut pos = 0;
                let mut msg_types = Vec::new();
                while pos < bytes.len() {
                    let msg_type = bytes[pos];
                    prop_assert!(pos + 5 <= bytes.len(), "Truncated message at pos {}", pos);
                    let len = i32::from_be_bytes([
                        bytes[pos + 1], bytes[pos + 2], bytes[pos + 3], bytes[pos + 4],
                    ]) as usize;
                    prop_assert!(len >= 4, "Message length must be ≥4");
                    prop_assert!(
                        pos + 1 + len <= bytes.len(),
                        "Message at pos {} overflows buffer",
                        pos
                    );
                    msg_types.push(msg_type as char);
                    pos += 1 + len;
                }

                // Must contain Parse + Bind + Execute + Sync = P B E S
                prop_assert_eq!(
                    msg_types,
                    vec!['P', 'B', 'E', 'S'],
                    "Extended query must be Parse+Bind+Execute+Sync"
                );
            }
            Err(_e) => {
                // EncodeError is acceptable for edge cases (too many params, etc.)
            }
        }
    }
}
