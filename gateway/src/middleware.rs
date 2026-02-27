//! Production middleware
//!
//! Rate limiting, timeouts, structured error responses, and request tracing.

use axum::{
    Json,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

/// Request ID extension — inserted by `request_tracer` middleware,
/// extracted by handlers to populate `ApiError.request_id`.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

/// Request timeout duration
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Token bucket rate limiter
#[derive(Debug)]
pub struct RateLimiter {
    /// Requests per second
    rate: f64,
    /// Maximum burst capacity
    burst: u32,
    /// Per-key buckets
    buckets: RwLock<HashMap<String, TokenBucket>>,
    /// Max number of tracked keys to prevent unbounded growth
    max_buckets: usize,
}

#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    last_update: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter
    ///
    /// - `rate`: requests per second
    /// - `burst`: maximum burst capacity
    pub fn new(rate: f64, burst: u32) -> Arc<Self> {
        Arc::new(Self {
            rate,
            burst,
            buckets: RwLock::new(HashMap::new()),
            max_buckets: 100_000, // Cap to prevent OOM from spoofed IPs
        })
    }

    /// Check if request is allowed (returns remaining tokens)
    pub async fn check(&self, key: &str) -> Result<u32, ()> {
        let now = Instant::now();
        let mut buckets = self.buckets.write().await;

        // Enforce max bucket count to prevent OOM from spoofed keys.
        // When at capacity, evict oldest bucket before inserting new one.
        if !buckets.contains_key(key) && buckets.len() >= self.max_buckets {
            // Evict the oldest bucket
            if let Some(oldest_key) = buckets
                .iter()
                .min_by_key(|(_, b)| b.last_update)
                .map(|(k, _)| k.clone())
            {
                buckets.remove(&oldest_key);
            }
        }

        let bucket = buckets
            .entry(key.to_string())
            .or_insert_with(|| TokenBucket {
                tokens: self.burst as f64,
                last_update: now,
            });

        // Refill tokens based on time elapsed
        let elapsed = now.duration_since(bucket.last_update).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rate).min(self.burst as f64);
        bucket.last_update = now;

        // Try to consume a token
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(bucket.tokens as u32)
        } else {
            Err(())
        }
    }

    /// Clean up old buckets (call periodically)
    pub async fn cleanup(&self, max_age: Duration) {
        let now = Instant::now();
        let mut buckets = self.buckets.write().await;
        buckets.retain(|_, bucket| now.duration_since(bucket.last_update) < max_age);
    }
}

/// Rate limiting middleware — dual-key: per-IP AND per-tenant.
///
/// Both checks must pass. This prevents:
/// - A single IP from flooding the system (per-IP bucket)
/// - A single tenant (operator) from starving others (per-tenant bucket)
pub async fn rate_limit_middleware(
    State(state): State<Arc<crate::GatewayState>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let method = request.method().to_string();
    let start = std::time::Instant::now();

    // Per-IP bucket key
    // SECURITY: Use the LAST XFF entry (closest to our reverse proxy),
    // not the first (client-controlled). Without a reverse proxy, fall back
    // to "unknown" — ConnectInfo would be better but requires Hyper config.
    let ip_key = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            s.split(',')
                .next_back()
                .unwrap_or("unknown")
                .trim()
                .to_string()
        })
        .unwrap_or_else(|| "unknown".to_string());

    // SECURITY: Tenant-level rate limiting is deferred to post-auth middleware.
    // Reading x-operator-id from raw headers is spoofable — an attacker can send
    // any tenant ID to exhaust another tenant's bucket or bypass their own.
    // For now, only IP-based rate limiting runs pre-auth.

    // IP-only rate limiting (tenant-level deferred to post-auth)
    match state.rate_limiter.check(&ip_key).await {
        Ok(remaining) => {
            let mut response = next.run(request).await;
            response.headers_mut().insert(
                "x-ratelimit-remaining",
                remaining
                    .to_string()
                    .parse()
                    .unwrap_or_else(|_| axum::http::HeaderValue::from_static("0")),
            );
            let status = response.status().as_u16();
            let duration = start.elapsed().as_secs_f64();
            crate::metrics::record_http_request(&method, status, duration);
            response
        }
        Err(()) => {
            tracing::warn!(ip = %ip_key, "IP rate limited");
            crate::metrics::record_rate_limited();
            let response = ApiError::rate_limited().into_response();
            let duration = start.elapsed().as_secs_f64();
            crate::metrics::record_http_request(&method, 429, duration);
            response
        }
    }
}

/// Structured error response.
///
/// Wraps `Box<ApiErrorData>` to keep `Result<T, ApiError>` small on the stack
/// (~8 bytes instead of ~168). All fields are accessible via `Deref`.
#[derive(Debug)]
pub struct ApiError(Box<ApiErrorData>);

impl ApiError {
    /// Construct from raw fields (allocates on the heap).
    fn new(data: ApiErrorData) -> Self {
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

/// Inner data for [`ApiError`] — heap-allocated to keep `Result<T, ApiError>` small.
#[derive(Debug, Serialize)]
pub struct ApiErrorData {
    /// Error code (e.g., "RATE_LIMITED", "TIMEOUT", "INTERNAL_ERROR")
    pub code: String,
    /// Human-readable error message
    pub message: String,
    /// Optional details for debugging
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    /// Request ID for tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Hint for resolving the error (safe for client display)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    /// Table that caused the error (when relevant)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// Column that caused the error (when relevant)
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
        // SECURITY: Log the raw error server-side, do NOT send to client.
        // Raw PG errors contain table names, constraint names, column types.
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

    /// Query rejected by EXPLAIN pre-check — cost or row estimate too high.
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
        // SECURITY: Log the raw error server-side, do NOT leak stack traces
        // or PG internals to the client.
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

    /// Validation error with table/column context
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

    /// Parse a Postgres error string and extract structured hints.
    ///
    /// SECURITY: Only safe, generic hints are exposed to clients.
    /// Raw constraint names and PG internals are never leaked.
    pub fn from_pg_error(pg_err: &str, table_name: Option<&str>) -> Self {
        let lower = pg_err.to_lowercase();

        // Unique constraint violation → 23505
        if lower.contains("unique") || lower.contains("duplicate key") || lower.contains("23505") {
            tracing::warn!(raw = %pg_err, "unique_violation");
            return Self::new(ApiErrorData {
                code: "CONFLICT".to_string(),
                message: "A record with this value already exists.".to_string(),
                details: None,
                request_id: None,
                hint: Some("Use a different value or update the existing record".to_string()),
                table: table_name.map(|s| s.to_string()),
                column: extract_column_from_constraint(pg_err),
            });
        }

        // Foreign key violation → 23503
        if lower.contains("foreign key") || lower.contains("23503") {
            tracing::warn!(raw = %pg_err, "fk_violation");
            return Self::new(ApiErrorData {
                code: "VALIDATION_ERROR".to_string(),
                message: "Referenced record does not exist.".to_string(),
                details: None,
                request_id: None,
                hint: Some("Ensure the referenced ID exists before inserting".to_string()),
                table: table_name.map(|s| s.to_string()),
                column: extract_column_from_constraint(pg_err),
            });
        }

        // NOT NULL violation → 23502
        if lower.contains("not-null") || lower.contains("null value") || lower.contains("23502") {
            tracing::warn!(raw = %pg_err, "not_null_violation");
            return Self::new(ApiErrorData {
                code: "VALIDATION_ERROR".to_string(),
                message: "A required field is missing.".to_string(),
                details: None,
                request_id: None,
                hint: Some("Provide all required fields".to_string()),
                table: table_name.map(|s| s.to_string()),
                column: extract_column_from_pg_null_error(pg_err),
            });
        }

        // RLS violation
        if lower.contains("row-level security") || lower.contains("new row violates") {
            tracing::warn!(raw = %pg_err, "rls_violation");
            return Self::new(ApiErrorData {
                code: "FORBIDDEN".to_string(),
                message: "Access denied by row-level security policy.".to_string(),
                details: None,
                request_id: None,
                hint: Some("Your session does not have permission for this operation".to_string()),
                table: table_name.map(|s| s.to_string()),
                column: None,
            });
        }

        // Fallback: generic query error (no PG internals leaked)
        Self::query_error(pg_err)
    }

    /// Map a `qail-pg` driver error into stable API semantics.
    ///
    /// Prefers SQLSTATE from structured server errors and falls back to
    /// conservative string matching only when SQLSTATE is unavailable.
    pub fn from_pg_driver_error(err: &qail_pg::PgError, table_name: Option<&str>) -> Self {
        if let Some(server) = err.server_error() {
            return Self::from_pg_server_error(server, table_name);
        }

        match err {
            qail_pg::PgError::Query(msg) => Self::from_pg_error(msg, table_name),
            qail_pg::PgError::Timeout(_) => Self::timeout(),
            qail_pg::PgError::Auth(msg) => {
                tracing::warn!(detail = %msg, "pg_auth_error");
                Self::forbidden("Database authentication failed.")
            }
            qail_pg::PgError::Connection(_)
            | qail_pg::PgError::Io(_)
            | qail_pg::PgError::PoolClosed
            | qail_pg::PgError::PoolExhausted { .. } => Self::connection_error(err.to_string()),
            _ => Self::query_error(err.to_string()),
        }
    }

    fn from_pg_server_error(server: &qail_pg::PgServerError, table_name: Option<&str>) -> Self {
        let sqlstate = server.code.as_str();
        let class = if sqlstate.len() >= 2 {
            &sqlstate[..2]
        } else {
            "??"
        };
        crate::metrics::record_db_error(sqlstate, class);

        match sqlstate {
            // unique_violation
            "23505" => {
                tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_unique_violation");
                Self::new(ApiErrorData {
                    code: "CONFLICT".to_string(),
                    message: "A record with this value already exists.".to_string(),
                    details: None,
                    request_id: None,
                    hint: Some("Use a different value or update the existing record".to_string()),
                    table: table_name.map(|s| s.to_string()),
                    column: extract_column_from_constraint(&server.message),
                })
            }
            // foreign_key_violation / not_null_violation / check_violation
            "23503" | "23502" | "23514" => {
                tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_validation_violation");
                let hint = match sqlstate {
                    "23503" => "Ensure the referenced ID exists before inserting",
                    "23502" => "Provide all required fields",
                    "23514" => "Ensure values satisfy database constraints",
                    _ => "Check request payload values",
                };
                let column = if sqlstate == "23502" {
                    extract_column_from_pg_null_error(&server.message)
                } else {
                    extract_column_from_constraint(&server.message)
                };
                Self::new(ApiErrorData {
                    code: "VALIDATION_ERROR".to_string(),
                    message: "Input violates a database constraint.".to_string(),
                    details: None,
                    request_id: None,
                    hint: Some(hint.to_string()),
                    table: table_name.map(|s| s.to_string()),
                    column,
                })
            }
            // insufficient_privilege
            "42501" => {
                tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_insufficient_privilege");
                Self::new(ApiErrorData {
                    code: "FORBIDDEN".to_string(),
                    message: "Access denied by database policy.".to_string(),
                    details: None,
                    request_id: None,
                    hint: Some(
                        "Your session does not have permission for this operation".to_string(),
                    ),
                    table: table_name.map(|s| s.to_string()),
                    column: None,
                })
            }
            // query_canceled (including statement_timeout)
            "57014" => {
                tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_query_canceled");
                Self::timeout()
            }
            // connection-related sqlstates
            "57P03" | "53300" | "08000" | "08001" | "08003" | "08004" | "08006" => {
                tracing::error!(sqlstate = %sqlstate, message = %server.message, "pg_connection_failure");
                Self::connection_error(format!("[{}] {}", server.code, server.message))
            }
            _ => {
                // Fallback by SQLSTATE class
                match class {
                    "22" | "23" => {
                        tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_data_exception");
                        Self::new(ApiErrorData {
                            code: "VALIDATION_ERROR".to_string(),
                            message: "Input violates a database constraint.".to_string(),
                            details: None,
                            request_id: None,
                            hint: Some("Check request payload and field values".to_string()),
                            table: table_name.map(|s| s.to_string()),
                            column: None,
                        })
                    }
                    "40" => {
                        tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_txn_retryable");
                        Self::new(ApiErrorData {
                            code: "QUERY_ERROR".to_string(),
                            message: "Transaction conflict occurred.".to_string(),
                            details: None,
                            request_id: None,
                            hint: Some("Retry the request".to_string()),
                            table: table_name.map(|s| s.to_string()),
                            column: None,
                        })
                    }
                    _ => Self::query_error(format!("[{}] {}", server.code, server.message)),
                }
            }
        }
    }

    /// Bad request with a custom error code — for handler-specific validation errors
    /// (e.g., EMPTY_QUERY, DECODE_ERROR, BATCH_TOO_LARGE, UNSUPPORTED_ACTION).
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

    /// Database connection / pool error — maps to 503 SERVICE_UNAVAILABLE.
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

    /// Generic constructor with an explicit error code — use for ad-hoc codes
    /// that don't warrant their own factory (e.g., QDRANT_ERROR, TXN_ERROR).
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

    // -- Builder methods --

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

    /// Get HTTP status code for this error
    pub fn status_code(&self) -> StatusCode {
        match self.code.as_str() {
            "RATE_LIMITED" => StatusCode::TOO_MANY_REQUESTS,
            "TIMEOUT" => StatusCode::GATEWAY_TIMEOUT,
            "PARSE_ERROR" | "VALIDATION_ERROR" | "EMPTY_QUERY" | "EMPTY_BATCH" | "DECODE_ERROR"
            | "UNSUPPORTED_ACTION" | "MISSING_VECTOR" => StatusCode::BAD_REQUEST,
            "CONFLICT" => StatusCode::CONFLICT,
            "QUERY_ERROR" | "QDRANT_ERROR" | "TXN_ERROR" | "TENANT_BOUNDARY_VIOLATION" => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            "QUERY_TOO_EXPENSIVE" | "QUERY_TOO_COMPLEX" => StatusCode::UNPROCESSABLE_ENTITY,
            "UNAUTHORIZED" => StatusCode::UNAUTHORIZED,
            "FORBIDDEN" | "QUERY_NOT_ALLOWED" | "POLICY_DENIED" => StatusCode::FORBIDDEN,
            "NOT_FOUND" => StatusCode::NOT_FOUND,
            "CONNECTION_ERROR" | "QDRANT_NOT_CONFIGURED" | "QDRANT_CONNECTION_ERROR" => {
                StatusCode::SERVICE_UNAVAILABLE
            }
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

/// Extract column name from a PG constraint error message.
///
/// Example: `duplicate key value violates unique constraint "users_email_key"` → `email`
fn extract_column_from_constraint(err: &str) -> Option<String> {
    if let Some(start) = err.find('"')
        && let Some(end) = err[start + 1..].find('"')
    {
        let constraint = &err[start + 1..start + 1 + end];
        let parts: Vec<&str> = constraint.rsplitn(2, '_').collect();
        if parts.len() == 2 {
            let prefix = parts[1];
            if let Some(col_start) = prefix.find('_') {
                return Some(prefix[col_start + 1..].to_string());
            }
        }
    }
    None
}

/// Extract column name from a PG NOT NULL violation.
///
/// Example: `null value in column "email" of relation "users" violates not-null constraint`
fn extract_column_from_pg_null_error(err: &str) -> Option<String> {
    let marker = "column \"";
    if let Some(start) = err.find(marker) {
        let rest = &err[start + marker.len()..];
        if let Some(end) = rest.find('"') {
            return Some(rest[..end].to_string());
        }
    }
    None
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let is_rate_limited = self.code == "RATE_LIMITED";
        let request_id = self.request_id.clone();
        let mut response = (status, Json(self)).into_response();

        // P1-B: Add Retry-After header for rate-limited responses
        if is_rate_limited && let Ok(v) = "1".parse() {
            response.headers_mut().insert("retry-after", v);
        }

        // P1-A: Echo request_id in response header for tracing
        if let Some(ref id) = request_id
            && let Ok(v) = id.parse()
        {
            response.headers_mut().insert("x-request-id", v);
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_limiter() {
        let limiter = RateLimiter::new(10.0, 5); // 10/s, burst 5

        // First 5 requests should pass (burst)
        for i in 0..5 {
            assert!(
                limiter.check("test").await.is_ok(),
                "Request {} should pass",
                i
            );
        }

        // 6th request should fail (bucket empty)
        assert!(
            limiter.check("test").await.is_err(),
            "Request 6 should fail"
        );

        // Different key should have its own bucket
        assert!(
            limiter.check("other").await.is_ok(),
            "Other key should pass"
        );
    }

    #[test]
    fn test_allow_list() {
        let mut allow_list = QueryAllowList::new();
        allow_list.allow("SELECT users");
        assert!(allow_list.is_allowed("SELECT users"));
        assert!(!allow_list.is_allowed("DROP TABLE users"));
    }

    #[test]
    fn test_complexity_guard() {
        let guard = QueryComplexityGuard::new(3, 10, 5);

        // Within limits
        assert!(guard.check(2, 5, 3).is_ok());

        // Exceeds depth
        assert!(guard.check(4, 5, 3).is_err());

        // Exceeds filters
        assert!(guard.check(1, 11, 3).is_err());

        // Exceeds joins
        assert!(guard.check(1, 5, 6).is_err());
    }

    #[test]
    fn test_from_pg_driver_error_unique_violation_sqlstate() {
        let err = qail_pg::PgError::QueryServer(qail_pg::PgServerError {
            severity: "ERROR".to_string(),
            code: "23505".to_string(),
            message: "duplicate key value violates unique constraint \"users_email_key\""
                .to_string(),
            detail: None,
            hint: None,
        });

        let api = ApiError::from_pg_driver_error(&err, Some("users"));
        assert_eq!(api.code, "CONFLICT");
        assert_eq!(api.status_code(), StatusCode::CONFLICT);
        assert_eq!(api.table.as_deref(), Some("users"));
    }

    #[test]
    fn test_from_pg_driver_error_query_canceled_sqlstate() {
        let err = qail_pg::PgError::QueryServer(qail_pg::PgServerError {
            severity: "ERROR".to_string(),
            code: "57014".to_string(),
            message: "canceling statement due to statement timeout".to_string(),
            detail: None,
            hint: None,
        });

        let api = ApiError::from_pg_driver_error(&err, Some("users"));
        assert_eq!(api.code, "TIMEOUT");
        assert_eq!(api.status_code(), StatusCode::GATEWAY_TIMEOUT);
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 4: Error code → HTTP status contract (exhaustive)
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn error_code_rate_limited_is_429() {
        assert_eq!(
            ApiError::rate_limited().status_code(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[test]
    fn error_code_timeout_is_504() {
        assert_eq!(
            ApiError::timeout().status_code(),
            StatusCode::GATEWAY_TIMEOUT
        );
    }

    #[test]
    fn error_code_parse_error_is_400() {
        assert_eq!(
            ApiError::parse_error("test").status_code(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn error_code_validation_error_is_400() {
        assert_eq!(
            ApiError::validation_error("t", "c", "msg").status_code(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn error_code_bad_request_codes_are_400() {
        for code in [
            "EMPTY_QUERY",
            "EMPTY_BATCH",
            "DECODE_ERROR",
            "UNSUPPORTED_ACTION",
            "MISSING_VECTOR",
        ] {
            let err = ApiError::bad_request(code, "test");
            assert_eq!(
                err.status_code(),
                StatusCode::BAD_REQUEST,
                "{} should be 400",
                code
            );
        }
    }

    #[test]
    fn error_code_conflict_is_409() {
        let err = ApiError::with_code("CONFLICT", "dup");
        assert_eq!(err.status_code(), StatusCode::CONFLICT);
    }

    #[test]
    fn error_code_query_error_is_500() {
        let err = ApiError::with_code("QUERY_ERROR", "fail");
        assert_eq!(err.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn error_code_expensive_is_422() {
        let err = ApiError::with_code("QUERY_TOO_EXPENSIVE", "cost");
        assert_eq!(err.status_code(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn error_code_complex_is_422() {
        let err = ApiError::with_code("QUERY_TOO_COMPLEX", "depth");
        assert_eq!(err.status_code(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn error_code_unauthorized_is_401() {
        assert_eq!(
            ApiError::auth_error("test").status_code(),
            StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn error_code_forbidden_variants_are_403() {
        for code in ["FORBIDDEN", "QUERY_NOT_ALLOWED", "POLICY_DENIED"] {
            let err = ApiError::with_code(code, "test");
            assert_eq!(
                err.status_code(),
                StatusCode::FORBIDDEN,
                "{} should be 403",
                code
            );
        }
    }

    #[test]
    fn error_code_not_found_is_404() {
        assert_eq!(
            ApiError::not_found("resource").status_code(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn error_code_connection_errors_are_503() {
        for code in [
            "CONNECTION_ERROR",
            "QDRANT_NOT_CONFIGURED",
            "QDRANT_CONNECTION_ERROR",
        ] {
            let err = ApiError::with_code(code, "test");
            assert_eq!(
                err.status_code(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{} should be 503",
                code
            );
        }
    }

    #[test]
    fn error_code_batch_too_large_is_413() {
        let err = ApiError::bad_request("BATCH_TOO_LARGE", "test");
        assert_eq!(err.status_code(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn error_code_internal_errors_are_500() {
        for code in [
            "INTERNAL_ERROR",
            "TXN_ERROR",
            "QDRANT_ERROR",
            "TENANT_BOUNDARY_VIOLATION",
        ] {
            let err = ApiError::with_code(code, "test");
            assert_eq!(
                err.status_code(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "{} should be 500",
                code
            );
        }
    }

    #[test]
    fn error_code_unknown_falls_back_to_500() {
        let err = ApiError::with_code("TOTALLY_UNKNOWN_CODE", "test");
        assert_eq!(
            err.status_code(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "unknown codes should fall back to 500"
        );
    }

    #[test]
    fn error_serialization_includes_code_and_message() {
        let err = ApiError::rate_limited();
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("\"code\":\"RATE_LIMITED\""));
        assert!(json.contains("\"message\":"));
    }

    #[test]
    fn error_serialization_omits_null_fields() {
        let err = ApiError::timeout();
        let json = serde_json::to_string(&err).unwrap();
        // skip_serializing_if fields should not appear when None
        assert!(!json.contains("\"details\""));
        assert!(!json.contains("\"request_id\""));
        assert!(!json.contains("\"hint\""));
        assert!(!json.contains("\"table\""));
        assert!(!json.contains("\"column\""));
    }

    #[test]
    fn error_builder_chain_attaches_metadata() {
        let err = ApiError::not_found("users")
            .with_request_id("req-123")
            .with_hint("try a different ID")
            .with_table("users")
            .with_column("id");

        assert_eq!(err.request_id.as_deref(), Some("req-123"));
        assert_eq!(err.hint.as_deref(), Some("try a different ID"));
        assert_eq!(err.table.as_deref(), Some("users"));
        assert_eq!(err.column.as_deref(), Some("id"));
    }

    // ══════════════════════════════════════════════════════════════════
    // Phase 5: W3C Trace Context parsing
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn traceparent_valid_sampled() {
        let ctx =
            super::parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .unwrap();
        assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.parent_id, "00f067aa0ba902b7");
        assert_eq!(ctx.flags, 1);
    }

    #[test]
    fn traceparent_valid_not_sampled() {
        let ctx =
            super::parse_traceparent("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1-bbbbbbbbbbbbbb01-00")
                .unwrap();
        assert_eq!(ctx.flags, 0);
    }

    #[test]
    fn traceparent_rejects_unsupported_version() {
        assert!(
            super::parse_traceparent("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .is_none()
        );
    }

    #[test]
    fn traceparent_rejects_all_zero_trace_id() {
        assert!(
            super::parse_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01")
                .is_none()
        );
    }

    #[test]
    fn traceparent_rejects_all_zero_parent_id() {
        assert!(
            super::parse_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01")
                .is_none()
        );
    }

    #[test]
    fn traceparent_rejects_short_trace_id() {
        assert!(super::parse_traceparent("00-4bf92f3577b34da6-00f067aa0ba902b7-01").is_none());
    }

    #[test]
    fn traceparent_rejects_wrong_part_count() {
        assert!(super::parse_traceparent("00-abc-01").is_none());
        assert!(super::parse_traceparent("").is_none());
    }

    #[test]
    fn traceparent_rejects_non_hex() {
        assert!(
            super::parse_traceparent("00-ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ-00f067aa0ba902b7-01")
                .is_none()
        );
    }
}

// ============================================================================
// Query Allow-List
// ============================================================================

/// Query allow-list: only pre-approved query patterns are executed.
///
/// When enabled, any query not in the allow-list is rejected.
/// This prevents arbitrary query injection and limits the attack surface.
#[derive(Debug, Default)]
pub struct QueryAllowList {
    enabled: bool,
    allowed: std::collections::HashSet<String>,
}

impl QueryAllowList {
    /// Create a new, disabled allow-list.
    pub fn new() -> Self {
        Self {
            enabled: false,
            allowed: std::collections::HashSet::new(),
        }
    }

    /// Enable the allow-list (queries not in the list will be rejected)
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Returns whether allow-list enforcement is active.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Add a query pattern to the allow-list
    pub fn allow(&mut self, pattern: &str) {
        self.enabled = true;
        self.allowed.insert(pattern.to_string());
    }

    /// Load allow-list from a file (one pattern per line)
    pub fn load_from_file(&mut self, path: &str) -> Result<(), std::io::Error> {
        let content = std::fs::read_to_string(path)?;
        for line in content.lines() {
            let trimmed = line.trim();
            if !trimmed.is_empty() && !trimmed.starts_with('#') {
                self.allow(trimmed);
            }
        }
        Ok(())
    }

    /// Check if a query pattern is allowed
    pub fn is_allowed(&self, pattern: &str) -> bool {
        if !self.enabled {
            return true; // Allow-list disabled: all queries pass
        }
        self.allowed.contains(pattern)
    }

    /// Number of patterns in the allow-list.
    pub fn len(&self) -> usize {
        self.allowed.len()
    }

    /// Returns `true` if the allow-list has no patterns.
    pub fn is_empty(&self) -> bool {
        self.allowed.is_empty()
    }
}

// ============================================================================
// Query Complexity Guard
// ============================================================================

/// Guards against excessively complex queries.
///
/// Limits:
/// - `max_depth`: Maximum nesting depth (joins, subqueries)
/// - `max_filters`: Maximum number of filter conditions
/// - `max_joins`: Maximum number of JOIN operations
#[derive(Debug, Clone)]
pub struct QueryComplexityGuard {
    /// Maximum nesting depth (subqueries, CTEs, set operations).
    pub max_depth: usize,
    /// Maximum number of filter conditions.
    pub max_filters: usize,
    /// Maximum number of JOIN operations.
    pub max_joins: usize,
}

impl QueryComplexityGuard {
    /// Create a complexity guard with custom limits.
    pub fn new(max_depth: usize, max_filters: usize, max_joins: usize) -> Self {
        Self {
            max_depth,
            max_filters,
            max_joins,
        }
    }

    /// Default production limits
    pub fn production() -> Self {
        Self {
            max_depth: 5,
            max_filters: 20,
            max_joins: 10,
        }
    }

    /// Check query complexity against limits
    pub fn check(
        &self,
        depth: usize,
        filter_count: usize,
        join_count: usize,
    ) -> Result<(), ApiError> {
        if depth > self.max_depth {
            return Err(ApiError::new(ApiErrorData {
                code: "QUERY_TOO_COMPLEX".to_string(),
                message: format!("Query depth {} exceeds maximum {}", depth, self.max_depth),
                details: None,
                request_id: None,
                hint: None,
                table: None,
                column: None,
            }));
        }
        if filter_count > self.max_filters {
            return Err(ApiError::new(ApiErrorData {
                code: "QUERY_TOO_COMPLEX".to_string(),
                message: format!(
                    "Filter count {} exceeds maximum {}",
                    filter_count, self.max_filters
                ),
                details: None,
                request_id: None,
                hint: None,
                table: None,
                column: None,
            }));
        }
        if join_count > self.max_joins {
            return Err(ApiError::new(ApiErrorData {
                code: "QUERY_TOO_COMPLEX".to_string(),
                message: format!(
                    "Join count {} exceeds maximum {}",
                    join_count, self.max_joins
                ),
                details: None,
                request_id: None,
                hint: None,
                table: None,
                column: None,
            }));
        }
        Ok(())
    }
}

// ============================================================================
// Request tracing middleware
// ============================================================================

/// Request tracing middleware — wraps each request with a structured tracing span.
///
/// Logs: request_id (UUID), method, path, status, duration_ms.
/// Injects `x-request-id` and `x-response-time` headers.
///
/// **W3C Trace Context (Phase 5)**: If the client sends a `traceparent` header,
/// the trace_id and parent_id are extracted and included in log entries.
/// Both `traceparent` and `tracestate` are propagated in the response.
pub async fn request_tracer(mut request: Request<axum::body::Body>, next: Next) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let start = Instant::now();

    // P1-A: Store request_id in extensions for downstream handlers
    request
        .extensions_mut()
        .insert(RequestId(request_id.clone()));

    // Phase 5: Extract W3C Trace Context from incoming headers
    let trace_ctx = request
        .headers()
        .get("traceparent")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_traceparent);

    let tracestate = request
        .headers()
        .get("tracestate")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    // Extract table from path: /api/{table}/... → table
    let table = path
        .trim_start_matches('/')
        .split('/')
        .nth(1)
        .unwrap_or("unknown")
        .to_string();

    // Log with trace context when available
    if let Some(ref ctx) = trace_ctx {
        tracing::info!(
            request_id = %request_id,
            trace_id = %ctx.trace_id,
            parent_id = %ctx.parent_id,
            method = %method,
            path = %path,
            table = %table,
            "→ request started"
        );
    } else {
        tracing::info!(
            request_id = %request_id,
            method = %method,
            path = %path,
            table = %table,
            "→ request started"
        );
    }

    let mut response = next.run(request).await;

    let duration = start.elapsed();
    let status = response.status().as_u16();
    let duration_ms = duration.as_secs_f64() * 1000.0;

    if let Some(ref ctx) = trace_ctx {
        tracing::info!(
            request_id = %request_id,
            trace_id = %ctx.trace_id,
            parent_id = %ctx.parent_id,
            method = %method,
            path = %path,
            status = status,
            duration_ms = %format!("{:.2}", duration_ms),
            "← request completed"
        );
    } else {
        tracing::info!(
            request_id = %request_id,
            method = %method,
            path = %path,
            status = status,
            duration_ms = %format!("{:.2}", duration_ms),
            "← request completed"
        );
    }

    // Inject tracing headers
    if let Ok(v) = request_id.parse() {
        response.headers_mut().insert("x-request-id", v);
    }
    if let Ok(v) = format!("{:.2}ms", duration_ms).parse() {
        response.headers_mut().insert("x-response-time", v);
    }

    // Phase 5: Propagate W3C trace context in response
    if let Some(ref ctx) = trace_ctx {
        // Generate a new span_id for this gateway hop
        let span_id = &request_id[..16].replace('-', "");
        let traceparent = format!("00-{}-{}-{:02x}", ctx.trace_id, span_id, ctx.flags);
        if let Ok(v) = traceparent.parse() {
            response.headers_mut().insert("traceparent", v);
        }
    }
    if let Some(ref ts) = tracestate
        && let Ok(v) = ts.parse()
    {
        response.headers_mut().insert("tracestate", v);
    }

    response
}

/// Parsed W3C Trace Context from `traceparent` header.
#[derive(Debug, Clone)]
struct TraceContext {
    /// 32-hex-char trace ID
    trace_id: String,
    /// 16-hex-char parent span ID
    parent_id: String,
    /// Trace flags (bit 0 = sampled)
    flags: u8,
}

/// Parse a W3C `traceparent` header value.
///
/// Format: `{version}-{trace_id}-{parent_id}-{trace_flags}`
/// Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
fn parse_traceparent(value: &str) -> Option<TraceContext> {
    let parts: Vec<&str> = value.trim().split('-').collect();
    if parts.len() != 4 {
        return None;
    }

    let version = parts[0];
    let trace_id = parts[1];
    let parent_id = parts[2];
    let flags_hex = parts[3];

    // Version must be "00" (currently only version supported)
    if version != "00" {
        return None;
    }

    // trace_id: 32 hex chars, must not be all zeros
    if trace_id.len() != 32 || !trace_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    if trace_id.chars().all(|c| c == '0') {
        return None;
    }

    // parent_id: 16 hex chars, must not be all zeros
    if parent_id.len() != 16 || !parent_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    if parent_id.chars().all(|c| c == '0') {
        return None;
    }

    // flags: 2 hex chars
    let flags = u8::from_str_radix(flags_hex, 16).ok()?;

    Some(TraceContext {
        trace_id: trace_id.to_string(),
        parent_id: parent_id.to_string(),
        flags,
    })
}
