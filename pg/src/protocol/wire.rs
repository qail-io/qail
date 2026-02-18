//! PostgreSQL Wire Protocol Messages
//!
//! Implementation of the PostgreSQL Frontend/Backend Protocol.
//! Reference: https://www.postgresql.org/docs/current/protocol-message-formats.html

/// Frontend (client → server) message types
#[derive(Debug, Clone)]
pub enum FrontendMessage {
    /// Startup message (sent first, no type byte)
    Startup { user: String, database: String },
    PasswordMessage(String),
    Query(String),
    /// Parse (prepared statement)
    Parse {
        name: String,
        query: String,
        param_types: Vec<u32>,
    },
    /// Bind parameters to prepared statement
    Bind {
        portal: String,
        statement: String,
        params: Vec<Option<Vec<u8>>>,
    },
    /// Execute portal
    Execute { portal: String, max_rows: i32 },
    Sync,
    Terminate,
    /// SASL initial response (first message in SCRAM)
    SASLInitialResponse { mechanism: String, data: Vec<u8> },
    /// SASL response (subsequent messages in SCRAM)
    SASLResponse(Vec<u8>),
    /// CopyFail — abort a COPY IN with an error message
    CopyFail(String),
    /// Close — explicitly release a prepared statement or portal
    Close { is_portal: bool, name: String },
}

/// Backend (server → client) message types
#[derive(Debug, Clone)]
pub enum BackendMessage {
    /// Authentication request
    AuthenticationOk,
    AuthenticationMD5Password([u8; 4]),
    AuthenticationSASL(Vec<String>),
    AuthenticationSASLContinue(Vec<u8>),
    AuthenticationSASLFinal(Vec<u8>),
    /// Parameter status (server config)
    ParameterStatus {
        name: String,
        value: String,
    },
    /// Backend key data (for cancel)
    BackendKeyData {
        process_id: i32,
        secret_key: i32,
    },
    ReadyForQuery(TransactionStatus),
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<Vec<u8>>>),
    CommandComplete(String),
    ErrorResponse(ErrorFields),
    ParseComplete,
    BindComplete,
    NoData,
    /// Copy in response (server ready to receive COPY data)
    CopyInResponse {
        format: u8,
        column_formats: Vec<u8>,
    },
    /// Copy out response (server will send COPY data)
    CopyOutResponse {
        format: u8,
        column_formats: Vec<u8>,
    },
    CopyData(Vec<u8>),
    CopyDone,
    /// Notification response (async notification from LISTEN/NOTIFY)
    NotificationResponse {
        process_id: i32,
        channel: String,
        payload: String,
    },
    EmptyQueryResponse,
    /// Notice response (warning/info messages, not errors)
    NoticeResponse(ErrorFields),
    /// Parameter description (OIDs of parameters in a prepared statement)
    /// Sent by server in response to Describe(Statement)
    ParameterDescription(Vec<u32>),
    /// Close complete (server confirmation that a prepared statement/portal was released)
    CloseComplete,
}

/// Transaction status
#[derive(Debug, Clone, Copy)]
pub enum TransactionStatus {
    Idle,    // 'I'
    InBlock, // 'T'
    Failed,  // 'E'
}

/// Field description in RowDescription
#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: u32,
    pub column_attr: i16,
    pub type_oid: u32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16,
}

/// Error fields from ErrorResponse
#[derive(Debug, Clone, Default)]
pub struct ErrorFields {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl FrontendMessage {
    /// Encode message to bytes for sending over the wire.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            FrontendMessage::Startup { user, database } => {
                let mut buf = Vec::new();
                // Protocol version 3.0
                buf.extend_from_slice(&196608i32.to_be_bytes());
                // Parameters
                buf.extend_from_slice(b"user\0");
                buf.extend_from_slice(user.as_bytes());
                buf.push(0);
                buf.extend_from_slice(b"database\0");
                buf.extend_from_slice(database.as_bytes());
                buf.push(0);
                buf.push(0); // Terminator

                // Prepend length (includes length itself)
                let len = (buf.len() + 4) as i32;
                let mut result = len.to_be_bytes().to_vec();
                result.extend(buf);
                result
            }
            FrontendMessage::Query(sql) => {
                let mut buf = Vec::new();
                buf.push(b'Q');
                let content = format!("{}\0", sql);
                let len = (content.len() + 4) as i32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(content.as_bytes());
                buf
            }
            FrontendMessage::Terminate => {
                vec![b'X', 0, 0, 0, 4]
            }
            FrontendMessage::SASLInitialResponse { mechanism, data } => {
                let mut buf = Vec::new();
                buf.push(b'p'); // SASLInitialResponse uses 'p'

                let mut content = Vec::new();
                content.extend_from_slice(mechanism.as_bytes());
                content.push(0); // null-terminated mechanism
                content.extend_from_slice(&(data.len() as i32).to_be_bytes());
                content.extend_from_slice(data);

                let len = (content.len() + 4) as i32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                buf
            }
            FrontendMessage::SASLResponse(data) => {
                let mut buf = Vec::new();
                buf.push(b'p');

                let len = (data.len() + 4) as i32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(data);
                buf
            }
            FrontendMessage::PasswordMessage(password) => {
                let mut buf = Vec::new();
                buf.push(b'p');
                let content = format!("{}\0", password);
                let len = (content.len() + 4) as i32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(content.as_bytes());
                buf
            }
            FrontendMessage::Parse { name, query, param_types } => {
                let mut buf = Vec::new();
                buf.push(b'P');

                let mut content = Vec::new();
                content.extend_from_slice(name.as_bytes());
                content.push(0);
                content.extend_from_slice(query.as_bytes());
                content.push(0);
                content.extend_from_slice(&(param_types.len() as i16).to_be_bytes());
                for oid in param_types {
                    content.extend_from_slice(&oid.to_be_bytes());
                }

                let len = (content.len() + 4) as i32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                buf
            }
            FrontendMessage::Bind { portal, statement, params } => {
                let mut buf = Vec::new();
                buf.push(b'B');

                let mut content = Vec::new();
                content.extend_from_slice(portal.as_bytes());
                content.push(0);
                content.extend_from_slice(statement.as_bytes());
                content.push(0);
                // Format codes (0 = all text)
                content.extend_from_slice(&0i16.to_be_bytes());
                // Parameter count
                content.extend_from_slice(&(params.len() as i16).to_be_bytes());
                for param in params {
                    match param {
                        Some(data) => {
                            content.extend_from_slice(&(data.len() as i32).to_be_bytes());
                            content.extend_from_slice(data);
                        }
                        None => content.extend_from_slice(&(-1i32).to_be_bytes()),
                    }
                }
                // Result format codes (0 = all text)
                content.extend_from_slice(&0i16.to_be_bytes());

                let len = (content.len() + 4) as i32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                buf
            }
            FrontendMessage::Execute { portal, max_rows } => {
                let mut buf = Vec::new();
                buf.push(b'E');

                let mut content = Vec::new();
                content.extend_from_slice(portal.as_bytes());
                content.push(0);
                content.extend_from_slice(&max_rows.to_be_bytes());

                let len = (content.len() + 4) as i32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                buf
            }
            FrontendMessage::Sync => {
                vec![b'S', 0, 0, 0, 4]
            }
            FrontendMessage::CopyFail(msg) => {
                let mut buf = Vec::new();
                buf.push(b'f');
                let content = format!("{}\0", msg);
                let len = (content.len() + 4) as i32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(content.as_bytes());
                buf
            }
            FrontendMessage::Close { is_portal, name } => {
                let mut buf = Vec::new();
                buf.push(b'C');
                let type_byte = if *is_portal { b'P' } else { b'S' };
                let mut content = vec![type_byte];
                content.extend_from_slice(name.as_bytes());
                content.push(0);
                let len = (content.len() + 4) as i32;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                buf
            }
        }
    }
}

impl BackendMessage {
    /// Decode a message from wire bytes.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), String> {
        if buf.len() < 5 {
            return Err("Buffer too short".to_string());
        }

        let msg_type = buf[0];
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

        if buf.len() < len + 1 {
            return Err("Incomplete message".to_string());
        }

        let payload = &buf[5..len + 1];

        let message = match msg_type {
            b'R' => Self::decode_auth(payload)?,
            b'S' => Self::decode_parameter_status(payload)?,
            b'K' => Self::decode_backend_key(payload)?,
            b'Z' => Self::decode_ready_for_query(payload)?,
            b'T' => Self::decode_row_description(payload)?,
            b'D' => Self::decode_data_row(payload)?,
            b'C' => Self::decode_command_complete(payload)?,
            b'E' => Self::decode_error_response(payload)?,
            b'1' => BackendMessage::ParseComplete,
            b'2' => BackendMessage::BindComplete,
            b'3' => BackendMessage::CloseComplete,
            b'n' => BackendMessage::NoData,
            b't' => Self::decode_parameter_description(payload)?,
            b'G' => Self::decode_copy_in_response(payload)?,
            b'H' => Self::decode_copy_out_response(payload)?,
            b'd' => BackendMessage::CopyData(payload.to_vec()),
            b'c' => BackendMessage::CopyDone,
            b'A' => Self::decode_notification_response(payload)?,
            b'I' => BackendMessage::EmptyQueryResponse,
            b'N' => BackendMessage::NoticeResponse(Self::parse_error_fields(payload)?),
            _ => return Err(format!("Unknown message type: {}", msg_type as char)),
        };

        Ok((message, len + 1))
    }

    fn decode_auth(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 4 {
            return Err("Auth payload too short".to_string());
        }
        let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        match auth_type {
            0 => Ok(BackendMessage::AuthenticationOk),
            5 => {
                if payload.len() < 8 {
                    return Err("MD5 auth payload too short (need salt)".to_string());
                }
                // SAFETY: Length is verified on the check above (payload.len() < 8 returns Err).
                let salt: [u8; 4] = payload[4..8].try_into().expect("salt slice is exactly 4 bytes");
                Ok(BackendMessage::AuthenticationMD5Password(salt))
            }
            10 => {
                // SASL - parse mechanism list
                let mut mechanisms = Vec::new();
                let mut pos = 4;
                while pos < payload.len() && payload[pos] != 0 {
                    let end = payload[pos..]
                        .iter()
                        .position(|&b| b == 0)
                        .map(|p| pos + p)
                        .unwrap_or(payload.len());
                    mechanisms.push(String::from_utf8_lossy(&payload[pos..end]).to_string());
                    pos = end + 1;
                }
                Ok(BackendMessage::AuthenticationSASL(mechanisms))
            }
            11 => {
                // SASL Continue - server challenge
                Ok(BackendMessage::AuthenticationSASLContinue(
                    payload[4..].to_vec(),
                ))
            }
            12 => {
                // SASL Final - server signature
                Ok(BackendMessage::AuthenticationSASLFinal(
                    payload[4..].to_vec(),
                ))
            }
            _ => Err(format!("Unknown auth type: {}", auth_type)),
        }
    }

    fn decode_parameter_status(payload: &[u8]) -> Result<Self, String> {
        let parts: Vec<&[u8]> = payload.split(|&b| b == 0).collect();
        let empty: &[u8] = b"";
        Ok(BackendMessage::ParameterStatus {
            name: String::from_utf8_lossy(parts.first().unwrap_or(&empty)).to_string(),
            value: String::from_utf8_lossy(parts.get(1).unwrap_or(&empty)).to_string(),
        })
    }

    fn decode_backend_key(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 8 {
            return Err("BackendKeyData payload too short".to_string());
        }
        Ok(BackendMessage::BackendKeyData {
            process_id: i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]),
            secret_key: i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]),
        })
    }

    fn decode_ready_for_query(payload: &[u8]) -> Result<Self, String> {
        if payload.is_empty() {
            return Err("ReadyForQuery payload empty".to_string());
        }
        let status = match payload[0] {
            b'I' => TransactionStatus::Idle,
            b'T' => TransactionStatus::InBlock,
            b'E' => TransactionStatus::Failed,
            _ => return Err("Unknown transaction status".to_string()),
        };
        Ok(BackendMessage::ReadyForQuery(status))
    }

    fn decode_row_description(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 2 {
            return Err("RowDescription payload too short".to_string());
        }

        let raw_count = i16::from_be_bytes([payload[0], payload[1]]);
        if raw_count < 0 {
            return Err(format!("RowDescription invalid field count: {}", raw_count));
        }
        let field_count = raw_count as usize;
        let mut fields = Vec::with_capacity(field_count);
        let mut pos = 2;

        for _ in 0..field_count {
            // Field name (null-terminated string)
            let name_end = payload[pos..]
                .iter()
                .position(|&b| b == 0)
                .ok_or("Missing null terminator in field name")?;
            let name = String::from_utf8_lossy(&payload[pos..pos + name_end]).to_string();
            pos += name_end + 1; // Skip null terminator

            // Ensure we have enough bytes for the fixed fields
            if pos + 18 > payload.len() {
                return Err("RowDescription field truncated".to_string());
            }

            let table_oid = u32::from_be_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;

            let column_attr = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            pos += 2;

            let type_oid = u32::from_be_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;

            let type_size = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            pos += 2;

            let type_modifier = i32::from_be_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;

            let format = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            pos += 2;

            fields.push(FieldDescription {
                name,
                table_oid,
                column_attr,
                type_oid,
                type_size,
                type_modifier,
                format,
            });
        }

        Ok(BackendMessage::RowDescription(fields))
    }

    fn decode_data_row(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 2 {
            return Err("DataRow payload too short".to_string());
        }

        let raw_count = i16::from_be_bytes([payload[0], payload[1]]);
        if raw_count < 0 {
            return Err(format!("DataRow invalid column count: {}", raw_count));
        }
        let column_count = raw_count as usize;
        // Sanity check: each column needs at least 4 bytes (length field)
        if column_count > (payload.len() - 2) / 4 + 1 {
            return Err(format!(
                "DataRow claims {} columns but payload is only {} bytes",
                column_count,
                payload.len()
            ));
        }
        let mut columns = Vec::with_capacity(column_count);
        let mut pos = 2;

        for _ in 0..column_count {
            if pos + 4 > payload.len() {
                return Err("DataRow truncated".to_string());
            }

            let len = i32::from_be_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;

            if len == -1 {
                // NULL value
                columns.push(None);
            } else {
                let len = len as usize;
                if pos + len > payload.len() {
                    return Err("DataRow column data truncated".to_string());
                }
                let data = payload[pos..pos + len].to_vec();
                pos += len;
                columns.push(Some(data));
            }
        }

        Ok(BackendMessage::DataRow(columns))
    }

    fn decode_command_complete(payload: &[u8]) -> Result<Self, String> {
        let tag = String::from_utf8_lossy(payload)
            .trim_end_matches('\0')
            .to_string();
        Ok(BackendMessage::CommandComplete(tag))
    }

    fn decode_error_response(payload: &[u8]) -> Result<Self, String> {
        Ok(BackendMessage::ErrorResponse(Self::parse_error_fields(
            payload,
        )?))
    }

    fn parse_error_fields(payload: &[u8]) -> Result<ErrorFields, String> {
        let mut fields = ErrorFields::default();
        let mut i = 0;
        while i < payload.len() && payload[i] != 0 {
            let field_type = payload[i];
            i += 1;
            let end = payload[i..].iter().position(|&b| b == 0).unwrap_or(0) + i;
            let value = String::from_utf8_lossy(&payload[i..end]).to_string();
            i = end + 1;

            match field_type {
                b'S' => fields.severity = value,
                b'C' => fields.code = value,
                b'M' => fields.message = value,
                b'D' => fields.detail = Some(value),
                b'H' => fields.hint = Some(value),
                _ => {}
            }
        }
        Ok(fields)
    }

    fn decode_parameter_description(payload: &[u8]) -> Result<Self, String> {
        let count = if payload.len() >= 2 {
            i16::from_be_bytes([payload[0], payload[1]]) as usize
        } else {
            0
        };
        let mut oids = Vec::with_capacity(count);
        let mut pos = 2;
        for _ in 0..count {
            if pos + 4 <= payload.len() {
                oids.push(u32::from_be_bytes([
                    payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3],
                ]));
                pos += 4;
            }
        }
        Ok(BackendMessage::ParameterDescription(oids))
    }

    fn decode_copy_in_response(payload: &[u8]) -> Result<Self, String> {
        if payload.is_empty() {
            return Err("Empty CopyInResponse payload".to_string());
        }
        let format = payload[0];
        let num_columns = if payload.len() >= 3 {
            i16::from_be_bytes([payload[1], payload[2]]) as usize
        } else {
            0
        };
        let column_formats: Vec<u8> = if payload.len() > 3 && num_columns > 0 {
            payload[3..].iter().take(num_columns).copied().collect()
        } else {
            vec![]
        };
        Ok(BackendMessage::CopyInResponse {
            format,
            column_formats,
        })
    }

    fn decode_copy_out_response(payload: &[u8]) -> Result<Self, String> {
        if payload.is_empty() {
            return Err("Empty CopyOutResponse payload".to_string());
        }
        let format = payload[0];
        let num_columns = if payload.len() >= 3 {
            i16::from_be_bytes([payload[1], payload[2]]) as usize
        } else {
            0
        };
        let column_formats: Vec<u8> = if payload.len() > 3 && num_columns > 0 {
            payload[3..].iter().take(num_columns).copied().collect()
        } else {
            vec![]
        };
        Ok(BackendMessage::CopyOutResponse {
            format,
            column_formats,
        })
    }

    fn decode_notification_response(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 4 {
            return Err("NotificationResponse too short".to_string());
        }
        let process_id = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);

        // Channel name (null-terminated)
        let mut i = 4;
        let channel_end = payload[i..].iter().position(|&b| b == 0).unwrap_or(0) + i;
        let channel = String::from_utf8_lossy(&payload[i..channel_end]).to_string();
        i = channel_end + 1;

        // Payload (null-terminated)
        let payload_end = payload[i..].iter().position(|&b| b == 0).unwrap_or(0) + i;
        let notification_payload = String::from_utf8_lossy(&payload[i..payload_end]).to_string();

        Ok(BackendMessage::NotificationResponse {
            process_id,
            channel,
            payload: notification_payload,
        })
    }
}

#[cfg(test)]
mod tests {
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
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("Incomplete"));
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
    fn decode_auth_payload_too_short() {
        // Auth needs at least 4 bytes for type field
        let buf = wire_msg(b'R', &[0, 0]);
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("too short"));
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
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("Unknown auth type"));
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

    // ========== ReadyForQuery tests ==========

    #[test]
    fn decode_ready_for_query_idle() {
        let buf = wire_msg(b'Z', &[b'I']);
        let (msg, _) = BackendMessage::decode(&buf).unwrap();
        assert!(matches!(msg, BackendMessage::ReadyForQuery(TransactionStatus::Idle)));
    }

    #[test]
    fn decode_ready_for_query_in_transaction() {
        let buf = wire_msg(b'Z', &[b'T']);
        let (msg, _) = BackendMessage::decode(&buf).unwrap();
        assert!(matches!(msg, BackendMessage::ReadyForQuery(TransactionStatus::InBlock)));
    }

    #[test]
    fn decode_ready_for_query_failed() {
        let buf = wire_msg(b'Z', &[b'E']);
        let (msg, _) = BackendMessage::decode(&buf).unwrap();
        assert!(matches!(msg, BackendMessage::ReadyForQuery(TransactionStatus::Failed)));
    }

    #[test]
    fn decode_ready_for_query_empty_payload() {
        let buf = wire_msg(b'Z', &[]);
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("empty"));
    }

    #[test]
    fn decode_ready_for_query_unknown_status() {
        let buf = wire_msg(b'Z', &[b'X']);
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("Unknown transaction"));
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
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("invalid column count"));
    }

    #[test]
    fn decode_data_row_truncated_column_data() {
        let mut payload = 1i16.to_be_bytes().to_vec();
        // Claims 100 bytes of data but payload ends immediately
        payload.extend_from_slice(&100i32.to_be_bytes());
        let buf = wire_msg(b'D', &payload);
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("truncated"));
    }

    #[test]
    fn decode_data_row_payload_too_short() {
        let buf = wire_msg(b'D', &[0]); // only 1 byte, need 2
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("too short"));
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
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("invalid field count"));
    }

    #[test]
    fn decode_row_description_truncated_field() {
        let mut payload = 1i16.to_be_bytes().to_vec();
        payload.extend_from_slice(b"id\0"); // field name
        // Missing 18 bytes of fixed field data
        let buf = wire_msg(b'T', &payload);
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("truncated"));
    }

    #[test]
    fn decode_row_description_single_field() {
        let mut payload = 1i16.to_be_bytes().to_vec();
        payload.extend_from_slice(b"id\0");         // name
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

    // ========== BackendKeyData tests ==========

    #[test]
    fn decode_backend_key_data() {
        let mut payload = 42i32.to_be_bytes().to_vec();
        payload.extend_from_slice(&99i32.to_be_bytes());
        let buf = wire_msg(b'K', &payload);
        let (msg, _) = BackendMessage::decode(&buf).unwrap();
        match msg {
            BackendMessage::BackendKeyData { process_id, secret_key } => {
                assert_eq!(process_id, 42);
                assert_eq!(secret_key, 99);
            }
            _ => panic!("Expected BackendKeyData"),
        }
    }

    #[test]
    fn decode_backend_key_too_short() {
        let buf = wire_msg(b'K', &[0, 0, 0, 42]); // only 4 bytes, need 8
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("too short"));
    }

    // ========== ErrorResponse tests ==========

    #[test]
    fn decode_error_response_with_fields() {
        let mut payload = Vec::new();
        payload.push(b'S'); payload.extend_from_slice(b"ERROR\0");
        payload.push(b'C'); payload.extend_from_slice(b"42P01\0");
        payload.push(b'M'); payload.extend_from_slice(b"relation does not exist\0");
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

    // ========== Simple type tests ==========

    #[test]
    fn decode_parse_complete() {
        let buf = wire_msg(b'1', &[]);
        let (msg, _) = BackendMessage::decode(&buf).unwrap();
        assert!(matches!(msg, BackendMessage::ParseComplete));
    }

    #[test]
    fn decode_bind_complete() {
        let buf = wire_msg(b'2', &[]);
        let (msg, _) = BackendMessage::decode(&buf).unwrap();
        assert!(matches!(msg, BackendMessage::BindComplete));
    }

    #[test]
    fn decode_no_data() {
        let buf = wire_msg(b'n', &[]);
        let (msg, _) = BackendMessage::decode(&buf).unwrap();
        assert!(matches!(msg, BackendMessage::NoData));
    }

    #[test]
    fn decode_empty_query_response() {
        let buf = wire_msg(b'I', &[]);
        let (msg, _) = BackendMessage::decode(&buf).unwrap();
        assert!(matches!(msg, BackendMessage::EmptyQueryResponse));
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
            BackendMessage::NotificationResponse { process_id, channel, payload } => {
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
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("too short"));
    }

    // ========== CopyInResponse / CopyOutResponse tests ==========

    #[test]
    fn decode_copy_in_response_empty_payload() {
        let buf = wire_msg(b'G', &[]);
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("Empty"));
    }

    #[test]
    fn decode_copy_out_response_empty_payload() {
        let buf = wire_msg(b'H', &[]);
        assert!(BackendMessage::decode(&buf).unwrap_err().contains("Empty"));
    }

    #[test]
    fn decode_copy_in_response_text_format() {
        let mut payload = vec![0u8]; // text format
        payload.extend_from_slice(&1i16.to_be_bytes()); // 1 column
        payload.push(0); // column format: text
        let buf = wire_msg(b'G', &payload);
        let (msg, _) = BackendMessage::decode(&buf).unwrap();
        match msg {
            BackendMessage::CopyInResponse { format, column_formats } => {
                assert_eq!(format, 0);
                assert_eq!(column_formats, vec![0]);
            }
            _ => panic!("Expected CopyInResponse"),
        }
    }

    // ========== Message consumed length test ==========

    #[test]
    fn decode_consumed_length_is_correct() {
        let buf = wire_msg(b'Z', &[b'I']);
        let (_, consumed) = BackendMessage::decode(&buf).unwrap();
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn decode_with_trailing_data_only_consumes_one_message() {
        let mut buf = wire_msg(b'Z', &[b'I']);
        buf.extend_from_slice(&wire_msg(b'Z', &[b'T'])); // second message appended
        let (msg, consumed) = BackendMessage::decode(&buf).unwrap();
        assert!(matches!(msg, BackendMessage::ReadyForQuery(TransactionStatus::Idle)));
        // Should only consume the first message
        assert_eq!(consumed, 6); // 1 type + 4 length + 1 payload
    }

    // ========== FrontendMessage encode roundtrip tests ==========

    #[test]
    fn encode_sync() {
        let msg = FrontendMessage::Sync;
        let encoded = msg.encode();
        assert_eq!(encoded, vec![b'S', 0, 0, 0, 4]);
    }

    #[test]
    fn encode_terminate() {
        let msg = FrontendMessage::Terminate;
        let encoded = msg.encode();
        assert_eq!(encoded, vec![b'X', 0, 0, 0, 4]);
    }
}

