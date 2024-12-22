//! Production middleware
//!
//! Rate limiting, timeouts, and structured error responses.

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

/// Request timeout duration
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Token bucket rate limiter
#[derive(Debug)]
pub struct RateLimiter {
    /// Requests per second
    rate: f64,
    /// Maximum burst capacity
    burst: u32,
    /// Per-IP buckets
    buckets: RwLock<HashMap<String, TokenBucket>>,
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
        })
    }
    
    /// Check if request is allowed (returns remaining tokens)
    pub async fn check(&self, key: &str) -> Result<u32, ()> {
        let now = Instant::now();
        let mut buckets = self.buckets.write().await;
        
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

/// Rate limiting middleware
pub async fn rate_limit_middleware(
    State(limiter): State<Arc<RateLimiter>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    // Extract client IP (use X-Forwarded-For if behind proxy)
    let key = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or("unknown").trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    
    match limiter.check(&key).await {
        Ok(remaining) => {
            let mut response = next.run(request).await;
            response.headers_mut().insert(
                "x-ratelimit-remaining",
                remaining.to_string().parse().unwrap(),
            );
            response
        }
        Err(()) => {
            tracing::warn!("Rate limited: {}", key);
            ApiError::rate_limited().into_response()
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
        Self {
            code: "QUERY_ERROR".to_string(),
            message: "Query execution failed.".to_string(),
            details: Some(msg.into()),
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
        Self {
            code: "INTERNAL_ERROR".to_string(),
            message: "An internal error occurred.".to_string(),
            details: Some(msg.into()),
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
        (status, Json(self)).into_response()
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
}
