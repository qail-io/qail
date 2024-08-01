//! PostgreSQL Wire Protocol Messages
//!
//! Implementation of the PostgreSQL Frontend/Backend Protocol.
//! Reference: https://www.postgresql.org/docs/current/protocol-message-formats.html

/// Frontend (client → server) message types
#[derive(Debug, Clone)]
pub enum FrontendMessage {
    /// Startup message (sent first, no type byte)
    Startup {
        user: String,
        database: String,
    },
    /// Password response
    PasswordMessage(String),
    /// Simple query
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
    Execute {
        portal: String,
        max_rows: i32,
    },
    /// Sync (end of pipeline)
    Sync,
    /// Terminate connection
    Terminate,
}

/// Backend (server → client) message types
#[derive(Debug, Clone)]
pub enum BackendMessage {
    /// Authentication request
    AuthenticationOk,
    AuthenticationMD5Password([u8; 4]),
    AuthenticationSASL(Vec<String>),
    /// Parameter status (server config)
    ParameterStatus { name: String, value: String },
    /// Backend key data (for cancel)
    BackendKeyData { process_id: i32, secret_key: i32 },
    /// Ready for query
    ReadyForQuery(TransactionStatus),
    /// Row description
    RowDescription(Vec<FieldDescription>),
    /// Data row
    DataRow(Vec<Option<Vec<u8>>>),
    /// Command complete (e.g., "SELECT 1")
    CommandComplete(String),
    /// Error response
    ErrorResponse(ErrorFields),
    /// Parse complete
    ParseComplete,
    /// Bind complete
    BindComplete,
    /// No data
    NoData,
}

/// Transaction status
#[derive(Debug, Clone, Copy)]
pub enum TransactionStatus {
    Idle,       // 'I'
    InBlock,    // 'T'
    Failed,     // 'E'
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
            // TODO: Implement other message types
            _ => unimplemented!("Message type not yet implemented"),
        }
    }
}

impl BackendMessage {
    /// Decode a message from wire bytes.
    /// Returns (message, bytes_consumed) or error.
    pub fn decode(buf: &[u8]) -> Result<(Self, usize), String> {
        if buf.len() < 5 {
            return Err("Buffer too short".to_string());
        }
        
        let msg_type = buf[0];
        let len = i32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        
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
            b'n' => BackendMessage::NoData,
            _ => return Err(format!("Unknown message type: {}", msg_type as char)),
        };
        
        Ok((message, len + 1))
    }
    
    fn decode_auth(payload: &[u8]) -> Result<Self, String> {
        let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        match auth_type {
            0 => Ok(BackendMessage::AuthenticationOk),
            5 => {
                let salt: [u8; 4] = payload[4..8].try_into().unwrap();
                Ok(BackendMessage::AuthenticationMD5Password(salt))
            }
            // TODO: SASL
            _ => Err(format!("Unknown auth type: {}", auth_type)),
        }
    }
    
    fn decode_parameter_status(payload: &[u8]) -> Result<Self, String> {
        let parts: Vec<&[u8]> = payload.split(|&b| b == 0).collect();
        let empty: &[u8] = b"";
        Ok(BackendMessage::ParameterStatus {
            name: String::from_utf8_lossy(parts.get(0).unwrap_or(&empty)).to_string(),
            value: String::from_utf8_lossy(parts.get(1).unwrap_or(&empty)).to_string(),
        })
    }
    
    fn decode_backend_key(payload: &[u8]) -> Result<Self, String> {
        Ok(BackendMessage::BackendKeyData {
            process_id: i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]),
            secret_key: i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]),
        })
    }
    
    fn decode_ready_for_query(payload: &[u8]) -> Result<Self, String> {
        let status = match payload[0] {
            b'I' => TransactionStatus::Idle,
            b'T' => TransactionStatus::InBlock,
            b'E' => TransactionStatus::Failed,
            _ => return Err("Unknown transaction status".to_string()),
        };
        Ok(BackendMessage::ReadyForQuery(status))
    }
    
    fn decode_row_description(_payload: &[u8]) -> Result<Self, String> {
        // TODO: Full implementation
        Ok(BackendMessage::RowDescription(vec![]))
    }
    
    fn decode_data_row(_payload: &[u8]) -> Result<Self, String> {
        // TODO: Full implementation
        Ok(BackendMessage::DataRow(vec![]))
    }
    
    fn decode_command_complete(payload: &[u8]) -> Result<Self, String> {
        let tag = String::from_utf8_lossy(payload).trim_end_matches('\0').to_string();
        Ok(BackendMessage::CommandComplete(tag))
    }
    
    fn decode_error_response(payload: &[u8]) -> Result<Self, String> {
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
        Ok(BackendMessage::ErrorResponse(fields))
    }
}
