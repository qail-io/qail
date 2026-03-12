use axum::{
    extract::State,
    http::Request,
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

use super::ApiError;

/// Token bucket rate limiter.
#[derive(Debug)]
pub struct RateLimiter {
    /// Requests per second.
    rate: f64,
    /// Maximum burst capacity.
    burst: u32,
    /// Per-key buckets.
    buckets: RwLock<HashMap<String, TokenBucket>>,
    /// Max number of tracked keys to prevent unbounded growth.
    max_buckets: usize,
}

#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    last_update: Instant,
}

impl RateLimiter {
    /// Create a new rate limiter.
    ///
    /// - `rate`: requests per second
    /// - `burst`: maximum burst capacity
    pub fn new(rate: f64, burst: u32) -> Arc<Self> {
        Arc::new(Self {
            rate,
            burst,
            buckets: RwLock::new(HashMap::new()),
            max_buckets: 100_000,
        })
    }

    /// Check if request is allowed (returns remaining tokens).
    pub async fn check(&self, key: &str) -> Result<u32, ()> {
        let now = Instant::now();
        let mut buckets = self.buckets.write().await;

        if !buckets.contains_key(key)
            && buckets.len() >= self.max_buckets
            && let Some(oldest_key) = buckets
                .iter()
                .min_by_key(|(_, b)| b.last_update)
                .map(|(k, _)| k.clone())
        {
            buckets.remove(&oldest_key);
        }

        let bucket = buckets
            .entry(key.to_string())
            .or_insert_with(|| TokenBucket {
                tokens: self.burst as f64,
                last_update: now,
            });

        let elapsed = now.duration_since(bucket.last_update).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rate).min(self.burst as f64);
        bucket.last_update = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(bucket.tokens as u32)
        } else {
            Err(())
        }
    }

    /// Clean up old buckets (call periodically).
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

    let ip_key = request
        .headers()
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .or_else(|| {
            request
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
        })
        .or_else(|| {
            request
                .extensions()
                .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
                .map(|ci| ci.0.ip().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());

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
