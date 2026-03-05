//! PostgreSQL wire protocol types — message enums, structs, and error types.
//!
//! Reference: <https://www.postgresql.org/docs/current/protocol-message-formats.html>

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
