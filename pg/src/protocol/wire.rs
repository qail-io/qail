//! PostgreSQL Wire Protocol Messages
//!
//! Implementation of the PostgreSQL Frontend/Backend Protocol.
//! Reference: <https://www.postgresql.org/docs/current/protocol-message-formats.html>

/// Maximum backend frame length accepted by the decoder.
///
/// Mirrors driver-side guards to keep standalone `BackendMessage::decode`
/// usage fail-closed against oversized frames.
const MAX_BACKEND_FRAME_LEN: usize = 64 * 1024 * 1024;

/// Frontend (client → server) message types
#[derive(Debug, Clone)]
pub enum FrontendMessage {
    /// Startup message (sent first, no type byte)
    Startup {
        /// Database role / user name.
        user: String,
        /// Target database name.
        database: String,
        /// Additional startup parameters (e.g. `replication=database`).
        startup_params: Vec<(String, String)>,
    },
    /// Password response (MD5 or cleartext).
    PasswordMessage(String),
    /// Simple query (SQL text).
    Query(String),
    /// Parse (prepared statement)
    Parse {
        /// Prepared statement name (empty string = unnamed).
        name: String,
        /// SQL query text with `$1`-style parameter placeholders.
        query: String,
        /// OIDs of the parameter types (empty = server infers).
        param_types: Vec<u32>,
    },
    /// Bind parameters to prepared statement
    Bind {
        /// Destination portal name (empty = unnamed).
        portal: String,
        /// Source prepared statement name.
        statement: String,
        /// Parameter values (`None` = SQL NULL).
        params: Vec<Option<Vec<u8>>>,
    },
    /// Execute portal
    Execute {
        /// Portal name to execute.
        portal: String,
        /// Maximum rows to return (0 = no limit).
        max_rows: i32,
    },
    /// Sync — marks the end of an extended-query pipeline.
    Sync,
    /// Terminate — closes the connection.
    Terminate,
    /// SASL initial response (first message in SCRAM)
    SASLInitialResponse {
        /// SASL mechanism name (e.g. `SCRAM-SHA-256`).
        mechanism: String,
        /// Client-first message bytes.
        data: Vec<u8>,
    },
    /// SASL response (subsequent messages in SCRAM)
    SASLResponse(Vec<u8>),
    /// GSS/SSPI response token.
    GSSResponse(Vec<u8>),
    /// CopyFail — abort a COPY IN with an error message
    CopyFail(String),
    /// Close — explicitly release a prepared statement or portal
    Close {
        /// `true` for portal, `false` for prepared statement.
        is_portal: bool,
        /// Name of the portal or statement to close.
        name: String,
    },
}

/// Backend (server → client) message types
#[derive(Debug, Clone)]
pub enum BackendMessage {
    /// Authentication request
    /// Authentication succeeded.
    AuthenticationOk,
    /// Server requests cleartext password.
    AuthenticationCleartextPassword,
    /// Server requests MD5-hashed password; salt provided.
    AuthenticationMD5Password([u8; 4]),
    /// Server requests Kerberos V5 authentication.
    AuthenticationKerberosV5,
    /// Server requests GSSAPI authentication.
    AuthenticationGSS,
    /// Server sends GSSAPI/SSPI continuation token.
    AuthenticationGSSContinue(Vec<u8>),
    /// Server requests SSPI authentication.
    AuthenticationSSPI,
    /// Server initiates SASL handshake with supported mechanisms.
    AuthenticationSASL(Vec<String>),
    /// SASL challenge from server.
    AuthenticationSASLContinue(Vec<u8>),
    /// SASL authentication complete; final server data.
    AuthenticationSASLFinal(Vec<u8>),
    /// Parameter status (server config)
    ParameterStatus {
        /// Parameter name (e.g. `server_version`, `TimeZone`).
        name: String,
        /// Current parameter value.
        value: String,
    },
    /// Backend key data (for cancel)
    BackendKeyData {
        /// Backend process ID (used for cancel requests).
        process_id: i32,
        /// Cancel secret key.
        secret_key: i32,
    },
    /// Server is ready; transaction state indicated.
    ReadyForQuery(TransactionStatus),
    /// Column metadata for the upcoming data rows.
    RowDescription(Vec<FieldDescription>),
    /// One data row; each element is `None` for SQL NULL or the raw bytes.
    DataRow(Vec<Option<Vec<u8>>>),
    /// Command completed with a tag like `SELECT 5` or `INSERT 0 1`.
    CommandComplete(String),
    /// Error response with structured fields (severity, code, message, etc.).
    ErrorResponse(ErrorFields),
    /// Parse step succeeded.
    ParseComplete,
    /// Bind step succeeded.
    BindComplete,
    /// Describe returned no row description (e.g. for DML statements).
    NoData,
    /// Execute reached row limit (`max_rows`) and suspended the portal.
    PortalSuspended,
    /// Copy in response (server ready to receive COPY data)
    CopyInResponse {
        /// Overall format: 0 = text, 1 = binary.
        format: u8,
        /// Per-column format codes.
        column_formats: Vec<u8>,
    },
    /// Copy out response (server will send COPY data)
    CopyOutResponse {
        /// Overall format: 0 = text, 1 = binary.
        format: u8,
        /// Per-column format codes.
        column_formats: Vec<u8>,
    },
    /// Copy both response (used by streaming replication).
    CopyBothResponse {
        /// Overall format: 0 = text, 1 = binary.
        format: u8,
        /// Per-column format codes.
        column_formats: Vec<u8>,
    },
    /// Raw COPY data chunk from the server.
    CopyData(Vec<u8>),
    /// COPY transfer complete.
    CopyDone,
    /// Notification response (async notification from LISTEN/NOTIFY)
    NotificationResponse {
        /// Backend process ID that sent the notification.
        process_id: i32,
        /// Channel name.
        channel: String,
        /// Notification payload string.
        payload: String,
    },
    /// Empty query string was submitted.
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
    /// Not inside a transaction block (`I`).
    Idle,
    /// Inside a transaction block (`T`).
    InBlock,
    /// Inside a failed transaction block (`E`).
    Failed,
}

/// Field description in RowDescription
#[derive(Debug, Clone)]
pub struct FieldDescription {
    /// Column name (or alias).
    pub name: String,
    /// OID of the source table (0 if not a table column).
    pub table_oid: u32,
    /// Column attribute number within the table (0 if not a table column).
    pub column_attr: i16,
    /// OID of the column's data type.
    pub type_oid: u32,
    /// Data type size in bytes (negative = variable-length).
    pub type_size: i16,
    /// Type-specific modifier (e.g. precision for `numeric`).
    pub type_modifier: i32,
    /// Format code: 0 = text, 1 = binary.
    pub format: i16,
}

/// Error fields from ErrorResponse
#[derive(Debug, Clone, Default)]
pub struct ErrorFields {
    /// Severity level (e.g. `ERROR`, `FATAL`, `WARNING`).
    pub severity: String,
    /// SQLSTATE error code (e.g. `23505` for unique violation).
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// Optional detailed error description.
    pub detail: Option<String>,
    /// Optional hint for resolving the error.
    pub hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendEncodeError {
    InteriorNul(&'static str),
    MessageTooLarge(usize),
    TooManyParams(usize),
    InvalidMaxRows(i32),
    InvalidStartupParam(String),
}

impl std::fmt::Display for FrontendEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InteriorNul(field) => write!(f, "field `{}` contains interior NUL byte", field),
            Self::MessageTooLarge(len) => write!(f, "message too large for wire length: {}", len),
            Self::TooManyParams(n) => write!(f, "too many params for i16 wire count: {}", n),
            Self::InvalidMaxRows(v) => write!(f, "invalid Execute max_rows (must be >= 0): {}", v),
            Self::InvalidStartupParam(msg) => write!(f, "invalid startup parameter: {}", msg),
        }
    }
}

impl std::error::Error for FrontendEncodeError {}

impl FrontendMessage {
    #[inline]
    fn has_nul(s: &str) -> bool {
        s.as_bytes().contains(&0)
    }

    #[inline]
    fn content_len_to_wire_len(content_len: usize) -> Result<i32, FrontendEncodeError> {
        let total = content_len
            .checked_add(4)
            .ok_or(FrontendEncodeError::MessageTooLarge(usize::MAX))?;
        i32::try_from(total).map_err(|_| FrontendEncodeError::MessageTooLarge(total))
    }

    /// Fallible encoder that returns explicit reason on invalid input.
    pub fn encode_checked(&self) -> Result<Vec<u8>, FrontendEncodeError> {
        match self {
            FrontendMessage::Startup {
                user,
                database,
                startup_params,
            } => {
                if Self::has_nul(user) {
                    return Err(FrontendEncodeError::InteriorNul("user"));
                }
                if Self::has_nul(database) {
                    return Err(FrontendEncodeError::InteriorNul("database"));
                }
                let mut seen_startup_keys = std::collections::HashSet::new();
                let mut buf = Vec::new();
                buf.extend_from_slice(&196608i32.to_be_bytes());
                buf.extend_from_slice(b"user\0");
                buf.extend_from_slice(user.as_bytes());
                buf.push(0);
                buf.extend_from_slice(b"database\0");
                buf.extend_from_slice(database.as_bytes());
                buf.push(0);
                for (key, value) in startup_params {
                    let key_trimmed = key.trim();
                    if key_trimmed.is_empty() {
                        return Err(FrontendEncodeError::InvalidStartupParam(
                            "key must not be empty".to_string(),
                        ));
                    }
                    let key_lc = key_trimmed.to_ascii_lowercase();
                    if key_lc == "user" || key_lc == "database" {
                        return Err(FrontendEncodeError::InvalidStartupParam(format!(
                            "reserved key '{}'",
                            key_trimmed
                        )));
                    }
                    if !seen_startup_keys.insert(key_lc) {
                        return Err(FrontendEncodeError::InvalidStartupParam(format!(
                            "duplicate key '{}'",
                            key_trimmed
                        )));
                    }
                    if Self::has_nul(key) {
                        return Err(FrontendEncodeError::InteriorNul("startup_param_key"));
                    }
                    if Self::has_nul(value) {
                        return Err(FrontendEncodeError::InteriorNul("startup_param_value"));
                    }
                    buf.extend_from_slice(key.as_bytes());
                    buf.push(0);
                    buf.extend_from_slice(value.as_bytes());
                    buf.push(0);
                }
                buf.push(0);

                let len = Self::content_len_to_wire_len(buf.len())?;
                let mut result = len.to_be_bytes().to_vec();
                result.extend(buf);
                Ok(result)
            }
            FrontendMessage::Query(sql) => {
                if Self::has_nul(sql) {
                    return Err(FrontendEncodeError::InteriorNul("sql"));
                }
                let mut buf = Vec::new();
                buf.push(b'Q');
                let mut content = Vec::with_capacity(sql.len() + 1);
                content.extend_from_slice(sql.as_bytes());
                content.push(0);
                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Terminate => Ok(vec![b'X', 0, 0, 0, 4]),
            FrontendMessage::SASLInitialResponse { mechanism, data } => {
                if Self::has_nul(mechanism) {
                    return Err(FrontendEncodeError::InteriorNul("mechanism"));
                }
                if data.len() > i32::MAX as usize {
                    return Err(FrontendEncodeError::MessageTooLarge(data.len()));
                }
                let mut buf = Vec::new();
                buf.push(b'p');

                let mut content = Vec::new();
                content.extend_from_slice(mechanism.as_bytes());
                content.push(0);
                let data_len = i32::try_from(data.len())
                    .map_err(|_| FrontendEncodeError::MessageTooLarge(data.len()))?;
                content.extend_from_slice(&data_len.to_be_bytes());
                content.extend_from_slice(data);

                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::SASLResponse(data) | FrontendMessage::GSSResponse(data) => {
                if data.len() > i32::MAX as usize {
                    return Err(FrontendEncodeError::MessageTooLarge(data.len()));
                }
                let mut buf = Vec::new();
                buf.push(b'p');
                let len = Self::content_len_to_wire_len(data.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(data);
                Ok(buf)
            }
            FrontendMessage::PasswordMessage(password) => {
                if Self::has_nul(password) {
                    return Err(FrontendEncodeError::InteriorNul("password"));
                }
                let mut buf = Vec::new();
                buf.push(b'p');
                let mut content = Vec::with_capacity(password.len() + 1);
                content.extend_from_slice(password.as_bytes());
                content.push(0);
                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Parse {
                name,
                query,
                param_types,
            } => {
                if Self::has_nul(name) {
                    return Err(FrontendEncodeError::InteriorNul("name"));
                }
                if Self::has_nul(query) {
                    return Err(FrontendEncodeError::InteriorNul("query"));
                }
                if param_types.len() > i16::MAX as usize {
                    return Err(FrontendEncodeError::TooManyParams(param_types.len()));
                }
                let mut buf = Vec::new();
                buf.push(b'P');

                let mut content = Vec::new();
                content.extend_from_slice(name.as_bytes());
                content.push(0);
                content.extend_from_slice(query.as_bytes());
                content.push(0);
                let param_count = i16::try_from(param_types.len())
                    .map_err(|_| FrontendEncodeError::TooManyParams(param_types.len()))?;
                content.extend_from_slice(&param_count.to_be_bytes());
                for oid in param_types {
                    content.extend_from_slice(&oid.to_be_bytes());
                }

                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Bind {
                portal,
                statement,
                params,
            } => {
                if Self::has_nul(portal) {
                    return Err(FrontendEncodeError::InteriorNul("portal"));
                }
                if Self::has_nul(statement) {
                    return Err(FrontendEncodeError::InteriorNul("statement"));
                }
                if params.len() > i16::MAX as usize {
                    return Err(FrontendEncodeError::TooManyParams(params.len()));
                }
                if let Some(too_large) = params
                    .iter()
                    .flatten()
                    .find(|p| p.len() > i32::MAX as usize)
                {
                    return Err(FrontendEncodeError::MessageTooLarge(too_large.len()));
                }

                let mut buf = Vec::new();
                buf.push(b'B');

                let mut content = Vec::new();
                content.extend_from_slice(portal.as_bytes());
                content.push(0);
                content.extend_from_slice(statement.as_bytes());
                content.push(0);
                content.extend_from_slice(&0i16.to_be_bytes());
                let param_count = i16::try_from(params.len())
                    .map_err(|_| FrontendEncodeError::TooManyParams(params.len()))?;
                content.extend_from_slice(&param_count.to_be_bytes());
                for param in params {
                    match param {
                        Some(data) => {
                            let data_len = i32::try_from(data.len())
                                .map_err(|_| FrontendEncodeError::MessageTooLarge(data.len()))?;
                            content.extend_from_slice(&data_len.to_be_bytes());
                            content.extend_from_slice(data);
                        }
                        None => content.extend_from_slice(&(-1i32).to_be_bytes()),
                    }
                }
                content.extend_from_slice(&0i16.to_be_bytes());

                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Execute { portal, max_rows } => {
                if Self::has_nul(portal) {
                    return Err(FrontendEncodeError::InteriorNul("portal"));
                }
                if *max_rows < 0 {
                    return Err(FrontendEncodeError::InvalidMaxRows(*max_rows));
                }
                let mut buf = Vec::new();
                buf.push(b'E');
                let mut content = Vec::new();
                content.extend_from_slice(portal.as_bytes());
                content.push(0);
                content.extend_from_slice(&max_rows.to_be_bytes());
                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Sync => Ok(vec![b'S', 0, 0, 0, 4]),
            FrontendMessage::CopyFail(msg) => {
                if Self::has_nul(msg) {
                    return Err(FrontendEncodeError::InteriorNul("copy_fail"));
                }
                let mut buf = Vec::new();
                buf.push(b'f');
                let mut content = Vec::with_capacity(msg.len() + 1);
                content.extend_from_slice(msg.as_bytes());
                content.push(0);
                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
            }
            FrontendMessage::Close { is_portal, name } => {
                if Self::has_nul(name) {
                    return Err(FrontendEncodeError::InteriorNul("name"));
                }
                let mut buf = Vec::new();
                buf.push(b'C');
                let type_byte = if *is_portal { b'P' } else { b'S' };
                let mut content = vec![type_byte];
                content.extend_from_slice(name.as_bytes());
                content.push(0);
                let len = Self::content_len_to_wire_len(content.len())?;
                buf.extend_from_slice(&len.to_be_bytes());
                buf.extend_from_slice(&content);
                Ok(buf)
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

        // PG protocol: length includes itself (4 bytes), so minimum valid length is 4.
        // Anything less is a malformed message.
        if len < 4 {
            return Err(format!("Invalid message length: {} (minimum is 4)", len));
        }
        if len > MAX_BACKEND_FRAME_LEN {
            return Err(format!(
                "Message too large: {} bytes (max {})",
                len, MAX_BACKEND_FRAME_LEN
            ));
        }

        let frame_len = len
            .checked_add(1)
            .ok_or_else(|| "Message length overflow".to_string())?;

        if buf.len() < frame_len {
            return Err("Incomplete message".to_string());
        }

        let payload = &buf[5..frame_len];

        let message = match msg_type {
            b'R' => Self::decode_auth(payload)?,
            b'S' => Self::decode_parameter_status(payload)?,
            b'K' => Self::decode_backend_key(payload)?,
            b'Z' => Self::decode_ready_for_query(payload)?,
            b'T' => Self::decode_row_description(payload)?,
            b'D' => Self::decode_data_row(payload)?,
            b'C' => Self::decode_command_complete(payload)?,
            b'E' => Self::decode_error_response(payload)?,
            b'1' => {
                if !payload.is_empty() {
                    return Err("ParseComplete must have empty payload".to_string());
                }
                BackendMessage::ParseComplete
            }
            b'2' => {
                if !payload.is_empty() {
                    return Err("BindComplete must have empty payload".to_string());
                }
                BackendMessage::BindComplete
            }
            b'3' => {
                if !payload.is_empty() {
                    return Err("CloseComplete must have empty payload".to_string());
                }
                BackendMessage::CloseComplete
            }
            b'n' => {
                if !payload.is_empty() {
                    return Err("NoData must have empty payload".to_string());
                }
                BackendMessage::NoData
            }
            b's' => {
                if !payload.is_empty() {
                    return Err("PortalSuspended must have empty payload".to_string());
                }
                BackendMessage::PortalSuspended
            }
            b't' => Self::decode_parameter_description(payload)?,
            b'G' => Self::decode_copy_in_response(payload)?,
            b'H' => Self::decode_copy_out_response(payload)?,
            b'W' => Self::decode_copy_both_response(payload)?,
            b'd' => BackendMessage::CopyData(payload.to_vec()),
            b'c' => {
                if !payload.is_empty() {
                    return Err("CopyDone must have empty payload".to_string());
                }
                BackendMessage::CopyDone
            }
            b'A' => Self::decode_notification_response(payload)?,
            b'I' => {
                if !payload.is_empty() {
                    return Err("EmptyQueryResponse must have empty payload".to_string());
                }
                BackendMessage::EmptyQueryResponse
            }
            b'N' => BackendMessage::NoticeResponse(Self::parse_error_fields(payload)?),
            _ => return Err(format!("Unknown message type: {}", msg_type as char)),
        };

        Ok((message, frame_len))
    }

    fn decode_auth(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 4 {
            return Err("Auth payload too short".to_string());
        }
        let auth_type = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        match auth_type {
            0 => {
                if payload.len() != 4 {
                    return Err(format!(
                        "AuthenticationOk invalid payload length: {}",
                        payload.len()
                    ));
                }
                Ok(BackendMessage::AuthenticationOk)
            }
            2 => {
                if payload.len() != 4 {
                    return Err(format!(
                        "AuthenticationKerberosV5 invalid payload length: {}",
                        payload.len()
                    ));
                }
                Ok(BackendMessage::AuthenticationKerberosV5)
            }
            3 => {
                if payload.len() != 4 {
                    return Err(format!(
                        "AuthenticationCleartextPassword invalid payload length: {}",
                        payload.len()
                    ));
                }
                Ok(BackendMessage::AuthenticationCleartextPassword)
            }
            5 => {
                if payload.len() != 8 {
                    return Err("MD5 auth payload too short (need salt)".to_string());
                }
                let mut salt = [0u8; 4];
                salt.copy_from_slice(&payload[4..8]);
                Ok(BackendMessage::AuthenticationMD5Password(salt))
            }
            7 => {
                if payload.len() != 4 {
                    return Err(format!(
                        "AuthenticationGSS invalid payload length: {}",
                        payload.len()
                    ));
                }
                Ok(BackendMessage::AuthenticationGSS)
            }
            8 => Ok(BackendMessage::AuthenticationGSSContinue(
                payload[4..].to_vec(),
            )),
            9 => {
                if payload.len() != 4 {
                    return Err(format!(
                        "AuthenticationSSPI invalid payload length: {}",
                        payload.len()
                    ));
                }
                Ok(BackendMessage::AuthenticationSSPI)
            }
            10 => {
                // SASL - parse mechanism list
                let mut mechanisms = Vec::new();
                let mut pos = 4;
                while pos < payload.len() {
                    if payload[pos] == 0 {
                        break; // list terminator
                    }
                    let end = payload[pos..]
                        .iter()
                        .position(|&b| b == 0)
                        .map(|p| pos + p)
                        .ok_or("SASL mechanism list missing null terminator")?;
                    mechanisms.push(String::from_utf8_lossy(&payload[pos..end]).to_string());
                    pos = end + 1;
                }
                if pos >= payload.len() {
                    return Err("SASL mechanism list missing final terminator".to_string());
                }
                if pos + 1 != payload.len() {
                    return Err("SASL mechanism list has trailing bytes".to_string());
                }
                if mechanisms.is_empty() {
                    return Err("SASL mechanism list is empty".to_string());
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
        let name_end = payload
            .iter()
            .position(|&b| b == 0)
            .ok_or("ParameterStatus missing name terminator")?;
        let value_start = name_end + 1;
        if value_start > payload.len() {
            return Err("ParameterStatus missing value".to_string());
        }
        let value_end_rel = payload[value_start..]
            .iter()
            .position(|&b| b == 0)
            .ok_or("ParameterStatus missing value terminator")?;
        let value_end = value_start + value_end_rel;
        if value_end + 1 != payload.len() {
            return Err("ParameterStatus has trailing bytes".to_string());
        }
        Ok(BackendMessage::ParameterStatus {
            name: String::from_utf8_lossy(&payload[..name_end]).to_string(),
            value: String::from_utf8_lossy(&payload[value_start..value_end]).to_string(),
        })
    }

    fn decode_backend_key(payload: &[u8]) -> Result<Self, String> {
        if payload.len() != 8 {
            return Err("BackendKeyData payload too short".to_string());
        }
        Ok(BackendMessage::BackendKeyData {
            process_id: i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]),
            secret_key: i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]),
        })
    }

    fn decode_ready_for_query(payload: &[u8]) -> Result<Self, String> {
        if payload.len() != 1 {
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
            if !(0..=1).contains(&format) {
                return Err(format!("RowDescription invalid format code: {}", format));
            }
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

        if pos != payload.len() {
            return Err("RowDescription has trailing bytes".to_string());
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
                if len < -1 {
                    return Err(format!("DataRow invalid column length: {}", len));
                }
                let len = len as usize;
                if len > payload.len().saturating_sub(pos) {
                    return Err("DataRow column data truncated".to_string());
                }
                let data = payload[pos..pos + len].to_vec();
                pos += len;
                columns.push(Some(data));
            }
        }

        if pos != payload.len() {
            return Err("DataRow has trailing bytes".to_string());
        }

        Ok(BackendMessage::DataRow(columns))
    }

    fn decode_command_complete(payload: &[u8]) -> Result<Self, String> {
        if payload.last().copied() != Some(0) {
            return Err("CommandComplete missing null terminator".to_string());
        }
        let tag_bytes = &payload[..payload.len() - 1];
        if tag_bytes.contains(&0) {
            return Err("CommandComplete contains interior null byte".to_string());
        }
        let tag = String::from_utf8_lossy(tag_bytes).to_string();
        Ok(BackendMessage::CommandComplete(tag))
    }

    fn decode_error_response(payload: &[u8]) -> Result<Self, String> {
        Ok(BackendMessage::ErrorResponse(Self::parse_error_fields(
            payload,
        )?))
    }

    fn parse_error_fields(payload: &[u8]) -> Result<ErrorFields, String> {
        if payload.last().copied() != Some(0) {
            return Err("ErrorResponse missing final terminator".to_string());
        }
        let mut fields = ErrorFields::default();
        let mut i = 0;
        while i < payload.len() && payload[i] != 0 {
            let field_type = payload[i];
            i += 1;
            let end = payload[i..]
                .iter()
                .position(|&b| b == 0)
                .map(|p| p + i)
                .ok_or("ErrorResponse field missing null terminator")?;
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
        if i + 1 != payload.len() {
            return Err("ErrorResponse has trailing bytes after terminator".to_string());
        }
        Ok(fields)
    }

    fn decode_parameter_description(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 2 {
            return Err("ParameterDescription payload too short".to_string());
        }
        let raw_count = i16::from_be_bytes([payload[0], payload[1]]);
        if raw_count < 0 {
            return Err(format!("ParameterDescription invalid count: {}", raw_count));
        }
        let count = raw_count as usize;
        let expected_len = 2 + count * 4;
        if payload.len() < expected_len {
            return Err(format!(
                "ParameterDescription truncated: expected {} bytes, got {}",
                expected_len,
                payload.len()
            ));
        }
        let mut oids = Vec::with_capacity(count);
        let mut pos = 2;
        for _ in 0..count {
            oids.push(u32::from_be_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]));
            pos += 4;
        }
        if pos != payload.len() {
            return Err("ParameterDescription has trailing bytes".to_string());
        }
        Ok(BackendMessage::ParameterDescription(oids))
    }

    fn decode_copy_in_response(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 3 {
            return Err("CopyInResponse payload too short".to_string());
        }
        let format = payload[0];
        if format > 1 {
            return Err(format!(
                "CopyInResponse invalid overall format code: {}",
                format
            ));
        }
        let num_columns = if payload.len() >= 3 {
            let raw = i16::from_be_bytes([payload[1], payload[2]]);
            if raw < 0 {
                return Err(format!(
                    "CopyInResponse invalid negative column count: {}",
                    raw
                ));
            }
            raw as usize
        } else {
            0
        };
        let mut column_formats = Vec::with_capacity(num_columns);
        let mut pos = 3usize;
        for _ in 0..num_columns {
            if pos + 2 > payload.len() {
                return Err("CopyInResponse truncated column format list".to_string());
            }
            let raw = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            if !(0..=1).contains(&raw) {
                return Err(format!("CopyInResponse invalid format code: {}", raw));
            }
            column_formats.push(raw as u8);
            pos += 2;
        }
        if pos != payload.len() {
            return Err("CopyInResponse has trailing bytes".to_string());
        }
        Ok(BackendMessage::CopyInResponse {
            format,
            column_formats,
        })
    }

    fn decode_copy_out_response(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 3 {
            return Err("CopyOutResponse payload too short".to_string());
        }
        let format = payload[0];
        if format > 1 {
            return Err(format!(
                "CopyOutResponse invalid overall format code: {}",
                format
            ));
        }
        let num_columns = if payload.len() >= 3 {
            let raw = i16::from_be_bytes([payload[1], payload[2]]);
            if raw < 0 {
                return Err(format!(
                    "CopyOutResponse invalid negative column count: {}",
                    raw
                ));
            }
            raw as usize
        } else {
            0
        };
        let mut column_formats = Vec::with_capacity(num_columns);
        let mut pos = 3usize;
        for _ in 0..num_columns {
            if pos + 2 > payload.len() {
                return Err("CopyOutResponse truncated column format list".to_string());
            }
            let raw = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            if !(0..=1).contains(&raw) {
                return Err(format!("CopyOutResponse invalid format code: {}", raw));
            }
            column_formats.push(raw as u8);
            pos += 2;
        }
        if pos != payload.len() {
            return Err("CopyOutResponse has trailing bytes".to_string());
        }
        Ok(BackendMessage::CopyOutResponse {
            format,
            column_formats,
        })
    }

    fn decode_copy_both_response(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 3 {
            return Err("CopyBothResponse payload too short".to_string());
        }
        let format = payload[0];
        if format > 1 {
            return Err(format!(
                "CopyBothResponse invalid overall format code: {}",
                format
            ));
        }
        let num_columns = if payload.len() >= 3 {
            let raw = i16::from_be_bytes([payload[1], payload[2]]);
            if raw < 0 {
                return Err(format!(
                    "CopyBothResponse invalid negative column count: {}",
                    raw
                ));
            }
            raw as usize
        } else {
            0
        };
        let mut column_formats = Vec::with_capacity(num_columns);
        let mut pos = 3usize;
        for _ in 0..num_columns {
            if pos + 2 > payload.len() {
                return Err("CopyBothResponse truncated column format list".to_string());
            }
            let raw = i16::from_be_bytes([payload[pos], payload[pos + 1]]);
            if !(0..=1).contains(&raw) {
                return Err(format!("CopyBothResponse invalid format code: {}", raw));
            }
            column_formats.push(raw as u8);
            pos += 2;
        }
        if pos != payload.len() {
            return Err("CopyBothResponse has trailing bytes".to_string());
        }
        Ok(BackendMessage::CopyBothResponse {
            format,
            column_formats,
        })
    }

    fn decode_notification_response(payload: &[u8]) -> Result<Self, String> {
        if payload.len() < 6 {
            // Minimum: 4 (process_id) + 1 (channel NUL) + 1 (payload NUL)
            return Err("NotificationResponse too short".to_string());
        }
        let process_id = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);

        // Channel name (null-terminated)
        let mut i = 4;
        let remaining = payload.get(i..).unwrap_or(&[]);
        let channel_end = remaining
            .iter()
            .position(|&b| b == 0)
            .ok_or("NotificationResponse: missing channel null terminator")?;
        let channel = String::from_utf8_lossy(&remaining[..channel_end]).to_string();
        i += channel_end + 1;

        // Payload (null-terminated)
        let remaining = payload.get(i..).unwrap_or(&[]);
        let payload_end = remaining
            .iter()
            .position(|&b| b == 0)
            .ok_or("NotificationResponse: missing payload null terminator")?;
        let notification_payload = String::from_utf8_lossy(&remaining[..payload_end]).to_string();
        if i + payload_end + 1 != payload.len() {
            return Err("NotificationResponse has trailing bytes".to_string());
        }

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
                assert_eq!(secret_key, 99);
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
            startup_params: Vec::new(),
        };
        assert!(msg.encode_checked().is_err());
    }

    #[test]
    fn encode_startup_with_extra_params() {
        let msg = FrontendMessage::Startup {
            user: "alice".to_string(),
            database: "app".to_string(),
            startup_params: vec![("replication".to_string(), "database".to_string())],
        };
        let encoded = msg.encode_checked().unwrap();
        assert_eq!(&encoded[4..8], &196608i32.to_be_bytes());
        assert!(encoded.windows("user\0alice\0".len()).any(|w| w == b"user\0alice\0"));
        assert!(encoded.windows("database\0app\0".len()).any(|w| w == b"database\0app\0"));
        assert!(
            encoded
                .windows("replication\0database\0".len())
                .any(|w| w == b"replication\0database\0")
        );
        assert_eq!(encoded.last().copied(), Some(0));
    }

    #[test]
    fn encode_startup_with_reserved_param_key_returns_error() {
        let msg = FrontendMessage::Startup {
            user: "alice".to_string(),
            database: "app".to_string(),
            startup_params: vec![("user".to_string(), "mallory".to_string())],
        };
        assert!(msg.encode_checked().is_err());
    }

    #[test]
    fn encode_startup_with_duplicate_param_keys_returns_error() {
        let msg = FrontendMessage::Startup {
            user: "alice".to_string(),
            database: "app".to_string(),
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
}
