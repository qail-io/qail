use axum::{
    extract::State,
    http::Request,
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    collections::HashMap,
    net::IpAddr,
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
        let rate = if rate.is_finite() && rate > 0.0 {
            rate
        } else {
            0.0
        };

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

fn trust_proxy_headers() -> bool {
    static TRUST_PROXY_HEADERS: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TRUST_PROXY_HEADERS.get_or_init(|| {
        std::env::var("QAIL_TRUST_PROXY_HEADERS")
            .map(|v| {
                let n = v.trim().to_ascii_lowercase();
                n == "1" || n == "true" || n == "yes"
            })
            .unwrap_or(false)
    })
}

fn parse_proxy_ip_key(value: &str) -> Option<String> {
    let ip = value.trim().parse::<IpAddr>().ok()?;
    Some(ip.to_string())
}

fn first_forwarded_for_ip_key(value: &str) -> Option<String> {
    value.split(',').find_map(parse_proxy_ip_key)
}

fn client_ip_key(request: &Request<axum::body::Body>) -> String {
    if let Some(ci) = request
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
    {
        return ci.0.ip().to_string();
    }

    if !trust_proxy_headers() {
        return "unknown".to_string();
    }

    request
        .headers()
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .and_then(parse_proxy_ip_key)
        .or_else(|| {
            request
                .headers()
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
                .and_then(first_forwarded_for_ip_key)
        })
        .unwrap_or_else(|| "unknown".to_string())
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

    let ip_key = client_ip_key(&request);

    let ip_remaining = match state.rate_limiter.check(&ip_key).await {
        Ok(remaining) => remaining,
        Err(()) => {
            tracing::warn!(ip = %ip_key, "IP rate limited");
            crate::metrics::record_rate_limited();
            let response = ApiError::rate_limited().into_response();
            let duration = start.elapsed().as_secs_f64();
            crate::metrics::record_http_request(&method, 429, duration);
            return response;
        }
    };

    let auth = crate::auth::extract_auth_for_state(request.headers(), state.as_ref()).await;
    let tenant_remaining = if auth.is_authenticated() {
        let tenant_key = format!(
            "{}:{}",
            auth.tenant_id.as_deref().unwrap_or("_"),
            auth.user_id
        );
        match state.tenant_rate_limiter.check(&tenant_key).await {
            Ok(remaining) => Some(remaining),
            Err(()) => {
                tracing::warn!(tenant_key = %tenant_key, "Tenant rate limited");
                crate::metrics::record_rate_limited();
                let response =
                    ApiError::with_code("TENANT_RATE_LIMIT", "Tenant rate limit exceeded")
                        .into_response();
                let duration = start.elapsed().as_secs_f64();
                crate::metrics::record_http_request(&method, 429, duration);
                return response;
            }
        }
    } else {
        None
    };

    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "x-ratelimit-remaining",
        ip_remaining
            .to_string()
            .parse()
            .unwrap_or_else(|_| axum::http::HeaderValue::from_static("0")),
    );
    if let Some(remaining) = tenant_remaining {
        response.headers_mut().insert(
            "x-tenant-ratelimit-remaining",
            remaining
                .to_string()
                .parse()
                .unwrap_or_else(|_| axum::http::HeaderValue::from_static("0")),
        );
    }
    let status = response.status().as_u16();
    let duration = start.elapsed().as_secs_f64();
    crate::metrics::record_http_request(&method, status, duration);
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_proxy_ip_key_rejects_non_ip_values() {
        assert_eq!(
            parse_proxy_ip_key("203.0.113.10"),
            Some("203.0.113.10".to_string())
        );
        assert_eq!(
            parse_proxy_ip_key(" 2001:db8::1 "),
            Some("2001:db8::1".to_string())
        );
        assert_eq!(parse_proxy_ip_key("203.0.113.10:443"), None);
        assert_eq!(parse_proxy_ip_key("attacker-controlled-bucket"), None);
        assert_eq!(parse_proxy_ip_key(""), None);
    }

    #[test]
    fn forwarded_for_uses_first_valid_ip_literal_only() {
        assert_eq!(
            first_forwarded_for_ip_key(" 198.51.100.1, 10.0.0.2 "),
            Some("198.51.100.1".to_string())
        );
        assert_eq!(
            first_forwarded_for_ip_key("bad-bucket, 198.51.100.2"),
            Some("198.51.100.2".to_string())
        );
        assert_eq!(first_forwarded_for_ip_key("bad-bucket, also-bad"), None);
    }
}
