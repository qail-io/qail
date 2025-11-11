//! Production middleware
//!
//! Rate limiting, timeouts, structured error responses, and request tracing.

use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
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

        let bucket = buckets.entry(key.to_string()).or_insert_with(|| TokenBucket {
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
        .map(|s| s.split(',').next_back().unwrap_or("unknown").trim().to_string())
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
                remaining.to_string().parse().unwrap_or_else(|_| axum::http::HeaderValue::from_static("0")),
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

/// Structured error response
#[derive(Debug, Serialize)]
pub struct ApiError {
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
}

impl ApiError {
    pub fn rate_limited() -> Self {
        Self {
            code: "RATE_LIMITED".to_string(),
            message: "Too many requests. Please slow down.".to_string(),
            details: None,
            request_id: None,
        }
    }
    
    pub fn timeout() -> Self {
        Self {
            code: "TIMEOUT".to_string(),
            message: "Request timed out.".to_string(),
            details: None,
            request_id: None,
        }
    }
    
    pub fn parse_error(msg: impl Into<String>) -> Self {
        Self {
            code: "PARSE_ERROR".to_string(),
            message: "Failed to parse query.".to_string(),
            details: Some(msg.into()),
            request_id: None,
        }
    }
    
    pub fn query_error(msg: impl Into<String>) -> Self {
        // SECURITY: Log the raw error server-side, do NOT send to client.
        // Raw PG errors contain table names, constraint names, column types.
        let detail = msg.into();
        tracing::error!(detail = %detail, "query_error");
        Self {
            code: "QUERY_ERROR".to_string(),
            message: "Query execution failed.".to_string(),
            details: None,
            request_id: None,
        }
    }
    
    /// Query rejected by EXPLAIN pre-check — cost or row estimate too high.
    ///
    /// The `detail` is embedded as JSON in the `details` field so that
    /// client SDKs can programmatically parse cost/row/suggestion data.
    pub fn too_expensive(
        msg: impl Into<String>,
        detail: qail_pg::explain::ExplainRejectionDetail,
    ) -> Self {
        let suggestions: Vec<String> = detail.suggestions
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
        Self {
            code: "QUERY_TOO_EXPENSIVE".to_string(),
            message: msg.into(),
            details: Some(detail_json),
            request_id: None,
        }
    }
    
    pub fn auth_error(msg: impl Into<String>) -> Self {
        Self {
            code: "UNAUTHORIZED".to_string(),
            message: msg.into(),
            details: None,
            request_id: None,
        }
    }
    
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self {
            code: "FORBIDDEN".to_string(),
            message: msg.into(),
            details: None,
            request_id: None,
        }
    }
    
    pub fn not_found(resource: impl Into<String>) -> Self {
        Self {
            code: "NOT_FOUND".to_string(),
            message: format!("{} not found", resource.into()),
            details: None,
            request_id: None,
        }
    }
    
    pub fn internal(msg: impl Into<String>) -> Self {
        // SECURITY: Log the raw error server-side, do NOT leak stack traces
        // or PG internals to the client.
        let detail = msg.into();
        tracing::error!(detail = %detail, "internal_error");
        Self {
            code: "INTERNAL_ERROR".to_string(),
            message: "An internal error occurred.".to_string(),
            details: None,
            request_id: None,
        }
    }
    
    pub fn with_request_id(mut self, id: impl Into<String>) -> Self {
        self.request_id = Some(id.into());
        self
    }
    
    /// Get HTTP status code for this error
    pub fn status_code(&self) -> StatusCode {
        match self.code.as_str() {
            "RATE_LIMITED" => StatusCode::TOO_MANY_REQUESTS,
            "TIMEOUT" => StatusCode::GATEWAY_TIMEOUT,
            "PARSE_ERROR" => StatusCode::BAD_REQUEST,
            "QUERY_ERROR" => StatusCode::INTERNAL_SERVER_ERROR,
            "QUERY_TOO_EXPENSIVE" => StatusCode::UNPROCESSABLE_ENTITY,
            "QUERY_TOO_COMPLEX" => StatusCode::UNPROCESSABLE_ENTITY,
            "UNAUTHORIZED" => StatusCode::UNAUTHORIZED,
            "FORBIDDEN" => StatusCode::FORBIDDEN,
            "NOT_FOUND" => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let is_rate_limited = self.code == "RATE_LIMITED";
        let request_id = self.request_id.clone();
        let mut response = (status, Json(self)).into_response();

        // P1-B: Add Retry-After header for rate-limited responses
        if is_rate_limited {
            if let Ok(v) = "1".parse() {
                response.headers_mut().insert("retry-after", v);
            }
        }

        // P1-A: Echo request_id in response header for tracing
        if let Some(ref id) = request_id {
            if let Ok(v) = id.parse() {
                response.headers_mut().insert("x-request-id", v);
            }
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
            assert!(limiter.check("test").await.is_ok(), "Request {} should pass", i);
        }
        
        // 6th request should fail (bucket empty)
        assert!(limiter.check("test").await.is_err(), "Request 6 should fail");
        
        // Different key should have its own bucket
        assert!(limiter.check("other").await.is_ok(), "Other key should pass");
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
    pub max_depth: usize,
    pub max_filters: usize,
    pub max_joins: usize,
}

impl QueryComplexityGuard {
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
            return Err(ApiError {
                code: "QUERY_TOO_COMPLEX".to_string(),
                message: format!("Query depth {} exceeds maximum {}", depth, self.max_depth),
                details: None,
                request_id: None,
            });
        }
        if filter_count > self.max_filters {
            return Err(ApiError {
                code: "QUERY_TOO_COMPLEX".to_string(),
                message: format!(
                    "Filter count {} exceeds maximum {}",
                    filter_count, self.max_filters
                ),
                details: None,
                request_id: None,
            });
        }
        if join_count > self.max_joins {
            return Err(ApiError {
                code: "QUERY_TOO_COMPLEX".to_string(),
                message: format!(
                    "Join count {} exceeds maximum {}",
                    join_count, self.max_joins
                ),
                details: None,
                request_id: None,
            });
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
pub async fn request_tracer(
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let request_id = uuid::Uuid::new_v4().to_string();
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let start = Instant::now();

    // P1-A: Store request_id in extensions for downstream handlers
    request.extensions_mut().insert(RequestId(request_id.clone()));

    // Extract table from path: /api/{table}/... → table
    let table = path
        .trim_start_matches('/')
        .split('/')
        .nth(1)
        .unwrap_or("unknown")
        .to_string();

    tracing::info!(
        request_id = %request_id,
        method = %method,
        path = %path,
        table = %table,
        "→ request started"
    );

    let mut response = next.run(request).await;

    let duration = start.elapsed();
    let status = response.status().as_u16();
    let duration_ms = duration.as_secs_f64() * 1000.0;

    tracing::info!(
        request_id = %request_id,
        method = %method,
        path = %path,
        status = status,
        duration_ms = %format!("{:.2}", duration_ms),
        "← request completed"
    );

    // Inject tracing headers
    if let Ok(v) = request_id.parse() {
        response.headers_mut().insert("x-request-id", v);
    }
    if let Ok(v) = format!("{:.2}ms", duration_ms).parse() {
        response.headers_mut().insert("x-response-time", v);
    }

    response
}
