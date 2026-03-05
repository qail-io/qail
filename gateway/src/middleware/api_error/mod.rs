use axum::http::StatusCode;
use serde::Serialize;

mod pg;
mod response;

/// Structured error response.
///
/// Wraps `Box<ApiErrorData>` to keep `Result<T, ApiError>` small on the stack
/// (~8 bytes instead of ~168). All fields are accessible via `Deref`.
#[derive(Debug)]
pub struct ApiError(Box<ApiErrorData>);

impl ApiError {
    /// Construct from raw fields (allocates on the heap).
    pub(super) fn new(data: ApiErrorData) -> Self {
        Self(Box::new(data))
    }
}

impl std::ops::Deref for ApiError {
    type Target = ApiErrorData;
    fn deref(&self) -> &ApiErrorData {
        &self.0
    }
}

impl std::ops::DerefMut for ApiError {
    fn deref_mut(&mut self) -> &mut ApiErrorData {
        &mut self.0
    }
}

impl Serialize for ApiError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

/// Inner data for [`ApiError`] - heap-allocated to keep `Result<T, ApiError>` small.
#[derive(Debug, Serialize)]
pub struct ApiErrorData {
    /// Error code (e.g., "RATE_LIMITED", "TIMEOUT", "INTERNAL_ERROR").
    pub code: String,
    /// Human-readable error message.
    pub message: String,
    /// Optional details for debugging.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    /// Request ID for tracing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Hint for resolving the error (safe for client display).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    /// Table that caused the error (when relevant).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// Column that caused the error (when relevant).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
}

impl ApiError {
    /// Create a 429 rate-limited error.
    pub fn rate_limited() -> Self {
        Self::new(ApiErrorData {
            code: "RATE_LIMITED".to_string(),
            message: "Too many requests. Please slow down.".to_string(),
            details: None,
            request_id: None,
            hint: None,
            table: None,
            column: None,
        })
    }

    /// Create a 408 request timeout error.
    pub fn timeout() -> Self {
        Self::new(ApiErrorData {
            code: "TIMEOUT".to_string(),
            message: "Request timed out.".to_string(),
            details: None,
            request_id: None,
            hint: None,
            table: None,
            column: None,
        })
    }

    /// Create a 400 parse error (invalid query syntax).
    pub fn parse_error(msg: impl Into<String>) -> Self {
        Self::new(ApiErrorData {
            code: "PARSE_ERROR".to_string(),
            message: "Failed to parse query.".to_string(),
            details: Some(msg.into()),
            request_id: None,
            hint: None,
            table: None,
            column: None,
        })
    }

    /// Create a 500 query error (raw detail is logged but not sent to client).
    pub fn query_error(msg: impl Into<String>) -> Self {
        let detail = msg.into();
        tracing::error!(detail = %detail, "query_error");
        Self::new(ApiErrorData {
            code: "QUERY_ERROR".to_string(),
            message: "Query execution failed.".to_string(),
            details: None,
            request_id: None,
            hint: None,
            table: None,
            column: None,
        })
    }

    /// Query rejected by EXPLAIN pre-check - cost or row estimate too high.
    ///
    /// The `detail` is embedded as JSON in the `details` field so that
    /// client SDKs can programmatically parse cost/row/suggestion data.
    pub fn too_expensive(
        msg: impl Into<String>,
        detail: qail_pg::explain::ExplainRejectionDetail,
    ) -> Self {
        let suggestions: Vec<String> = detail
            .suggestions
            .iter()
            .map(|s| format!("\"{}\"", s))
            .collect();
        let detail_json = format!(
            r#"{{"estimated_cost":{:.0},"cost_limit":{:.0},"estimated_rows":{},"row_limit":{},"suggestions":[{}]}}"#,
            detail.estimated_cost,
            detail.cost_limit,
            detail.estimated_rows,
            detail.row_limit,
            suggestions.join(","),
        );
        crate::metrics::record_explain_rejected(detail.estimated_cost, detail.cost_limit);
        Self::new(ApiErrorData {
            code: "QUERY_TOO_EXPENSIVE".to_string(),
            message: msg.into(),
            details: Some(detail_json),
            request_id: None,
            hint: Some("Add filters, reduce columns, or add indexes".to_string()),
            table: None,
            column: None,
        })
    }

    /// Create a 401 authentication error.
    pub fn auth_error(msg: impl Into<String>) -> Self {
        Self::new(ApiErrorData {
            code: "UNAUTHORIZED".to_string(),
            message: msg.into(),
            details: None,
            request_id: None,
            hint: Some("Provide a valid JWT in the Authorization header".to_string()),
            table: None,
            column: None,
        })
    }

    /// Create a 403 forbidden error.
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::new(ApiErrorData {
            code: "FORBIDDEN".to_string(),
            message: msg.into(),
            details: None,
            request_id: None,
            hint: None,
            table: None,
            column: None,
        })
    }

    /// Create a 404 not-found error.
    pub fn not_found(resource: impl Into<String>) -> Self {
        Self::new(ApiErrorData {
            code: "NOT_FOUND".to_string(),
            message: format!("{} not found", resource.into()),
            details: None,
            request_id: None,
            hint: None,
            table: None,
            column: None,
        })
    }

    /// Create a 500 internal error (detail is logged but not leaked to client).
    pub fn internal(msg: impl Into<String>) -> Self {
        let detail = msg.into();
        tracing::error!(detail = %detail, "internal_error");
        Self::new(ApiErrorData {
            code: "INTERNAL_ERROR".to_string(),
            message: "An internal error occurred.".to_string(),
            details: None,
            request_id: None,
            hint: None,
            table: None,
            column: None,
        })
    }

    /// Validation error with table/column context.
    pub fn validation_error(
        table: impl Into<String>,
        column: impl Into<String>,
        msg: impl Into<String>,
    ) -> Self {
        Self::new(ApiErrorData {
            code: "VALIDATION_ERROR".to_string(),
            message: msg.into(),
            details: None,
            request_id: None,
            hint: None,
            table: Some(table.into()),
            column: Some(column.into()),
        })
    }

    /// Bad request with a custom error code - for handler-specific validation errors.
    pub fn bad_request(code: impl Into<String>, msg: impl Into<String>) -> Self {
        Self::new(ApiErrorData {
            code: code.into(),
            message: msg.into(),
            details: None,
            request_id: None,
            hint: None,
            table: None,
            column: None,
        })
    }

    /// Database connection / pool error - maps to 503 SERVICE_UNAVAILABLE.
    pub fn connection_error(msg: impl Into<String>) -> Self {
        let detail = msg.into();
        tracing::error!(detail = %detail, "connection_error");
        Self::new(ApiErrorData {
            code: "CONNECTION_ERROR".to_string(),
            message: "Database connection failed.".to_string(),
            details: None,
            request_id: None,
            hint: None,
            table: None,
            column: None,
        })
    }

    /// Generic constructor with an explicit error code.
    pub fn with_code(code: impl Into<String>, msg: impl Into<String>) -> Self {
        Self::new(ApiErrorData {
            code: code.into(),
            message: msg.into(),
            details: None,
            request_id: None,
            hint: None,
            table: None,
            column: None,
        })
    }

    /// Attach a request ID for tracing.
    pub fn with_request_id(mut self, id: impl Into<String>) -> Self {
        self.request_id = Some(id.into());
        self
    }

    /// Attach a hint for resolving the error.
    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    /// Attach the table that caused the error.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    /// Attach the column that caused the error.
    pub fn with_column(mut self, column: impl Into<String>) -> Self {
        self.column = Some(column.into());
        self
    }

    /// Get HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self.code.as_str() {
            "RATE_LIMITED" => StatusCode::TOO_MANY_REQUESTS,
            "TIMEOUT" => StatusCode::GATEWAY_TIMEOUT,
            "PARSE_ERROR"
            | "VALIDATION_ERROR"
            | "EMPTY_QUERY"
            | "EMPTY_BATCH"
            | "DECODE_ERROR"
            | "UNSUPPORTED_ACTION"
            | "MISSING_VECTOR"
            | "EXPORT_ONLY"
            | "AST_VALIDATION_FAILED"
            | "PAYLOAD_TOO_LARGE" => StatusCode::BAD_REQUEST,
            "CONFLICT" | "TXN_SESSION_EXPIRED" | "TXN_STATEMENT_LIMIT" | "TXN_ABORTED" => {
                StatusCode::CONFLICT
            }
            "QUERY_ERROR" | "QDRANT_ERROR" | "TXN_ERROR" | "TENANT_BOUNDARY_VIOLATION" => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            "QUERY_TOO_EXPENSIVE" | "QUERY_TOO_COMPLEX" => StatusCode::UNPROCESSABLE_ENTITY,
            "UNAUTHORIZED" | "AUTH_DENIED" | "AUTH_REQUIRED" => StatusCode::UNAUTHORIZED,
            "FORBIDDEN" | "QUERY_NOT_ALLOWED" | "POLICY_DENIED" => StatusCode::FORBIDDEN,
            "NOT_FOUND" => StatusCode::NOT_FOUND,
            "CONNECTION_ERROR"
            | "POOL_BACKPRESSURE"
            | "TXN_SESSION_LIMIT"
            | "QDRANT_NOT_CONFIGURED"
            | "QDRANT_CONNECTION_ERROR" => StatusCode::SERVICE_UNAVAILABLE,
            "BATCH_TOO_LARGE" => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<crate::error::GatewayError> for ApiError {
    fn from(err: crate::error::GatewayError) -> Self {
        match &err {
            crate::error::GatewayError::Config(_) => Self::internal(err.to_string()),
            crate::error::GatewayError::Schema(_) => Self::internal(err.to_string()),
            crate::error::GatewayError::Policy(_) => Self::internal(err.to_string()),
            crate::error::GatewayError::Database(_) => Self::connection_error(err.to_string()),
            crate::error::GatewayError::Auth(_) => Self::auth_error(err.to_string()),
            crate::error::GatewayError::AccessDenied(_) => Self::forbidden(err.to_string()),
            crate::error::GatewayError::InvalidQuery(_) => Self::parse_error(err.to_string()),
            crate::error::GatewayError::Internal(_) => Self::internal(err.to_string()),
        }
    }
}
