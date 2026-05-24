//! Idempotency key middleware.
//!
//! Prevents duplicate mutations (POST, PATCH, DELETE) caused by network retries
//! or client bugs. Clients send `Idempotency-Key: <uuid>` header; the gateway
//! caches the response and replays it on subsequent requests with the same key.
//!
//! - Keys are scoped per principal (`tenant_id + user_id`) to prevent
//!   cross-tenant and cross-user replay.
//! - Cached responses expire after a configurable TTL (default: 24 hours).
//! - Only mutation methods (POST, PATCH, DELETE) are checked; GET/HEAD are ignored.
//! - The store uses an in-memory moka cache with bounded capacity.
//! - Unauthenticated requests bypass idempotency caching to avoid cross-client
//!   key collisions when auth is disabled.

use axum::{
    body::Body,
    http::{
        HeaderMap, Method, Request, StatusCode, Uri,
        header::{CONTENT_LENGTH, CONTENT_TYPE, HeaderName, HeaderValue},
    },
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use sha2::{Digest, Sha256};

mod middleware;
mod store;
#[cfg(test)]
mod tests;

pub use middleware::idempotency_middleware;
pub use store::IdempotencyStore;
pub(crate) use store::{CachedResponse, lock_in_flight_set};

fn json_response(status: StatusCode, body: &'static str) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

/// Check if a method is a mutation (eligible for idempotency).
fn is_mutation_method(method: &Method) -> bool {
    matches!(*method, Method::POST | Method::PATCH | Method::DELETE)
}

fn is_transaction_path(uri: &Uri) -> bool {
    let path = uri.path();
    path == "/txn" || path.starts_with("/txn/")
}

/// Extract the `Idempotency-Key` header value.
fn extract_idempotency_key(request: &Request<Body>) -> Option<String> {
    request
        .headers()
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn canonical_query(query: Option<&str>) -> String {
    let Some(raw) = query else {
        return String::new();
    };
    if raw.is_empty() {
        return String::new();
    }

    let mut pairs: Vec<(String, String)> = url::form_urlencoded::parse(raw.as_bytes())
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    pairs.sort_unstable();
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish()
}

fn parse_content_length(headers: &HeaderMap) -> Option<usize> {
    headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<usize>().ok())
}

const IDEMPOTENCY_REPLAY_SAFE_HEADERS: &[&str] = &[
    "location",
    "etag",
    "cache-control",
    "content-location",
    "content-disposition",
];
const IDEMPOTENCY_FINGERPRINT_HEADERS: &[&str] = &[
    "prefer",
    "x-transaction-id",
    "x-branch-id",
    "x-branch",
    "x-qail-result-format",
    "x-impersonate-tenant",
];

fn canonical_fingerprint_headers(headers: &HeaderMap) -> String {
    let mut pairs: Vec<(&str, String)> = IDEMPOTENCY_FINGERPRINT_HEADERS
        .iter()
        .filter_map(|name| {
            headers
                .get(*name)
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(|v| (*name, v.to_string()))
        })
        .collect();
    pairs.sort_unstable_by(|a, b| a.0.cmp(b.0));
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs)
        .finish()
}

fn capture_replay_headers(headers: &HeaderMap) -> Vec<(String, String)> {
    IDEMPOTENCY_REPLAY_SAFE_HEADERS
        .iter()
        .filter_map(|name| {
            headers
                .get(*name)
                .and_then(|v| v.to_str().ok())
                .map(|v| ((*name).to_string(), v.to_string()))
        })
        .collect()
}

/// Decide whether a response is safe to capture for idempotency replay.
///
/// We capture successful responses unless an explicit `Content-Length` already
/// proves they exceed the configured body limit. Responses without a length are
/// still attempted because normal Axum JSON responses commonly omit the header
/// until after middleware inspection; `to_bytes(..., body_limit)` enforces the
/// hard cap before insertion.
fn should_capture_response_for_idempotency(
    status: StatusCode,
    headers: &HeaderMap,
    body_limit: usize,
) -> bool {
    if !status.is_success() {
        return false;
    }
    parse_content_length(headers).is_none_or(|content_length| content_length <= body_limit)
}

fn response_exceeds_idempotency_body_limit(headers: &HeaderMap, body_limit: usize) -> bool {
    parse_content_length(headers).is_some_and(|content_length| content_length > body_limit)
}

#[cfg(test)]
fn request_fingerprint(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    content_type: Option<&str>,
    body: &[u8],
) -> String {
    request_fingerprint_with_auth(method, uri, headers, content_type, body, "")
}

fn auth_replay_fingerprint(auth: &crate::auth::AuthContext) -> String {
    let mut canonical = format!(
        "authenticated={}|denied={}|user={}|tenant={}|role={}",
        auth.is_authenticated(),
        auth.is_denied(),
        auth.user_id,
        auth.tenant_id.as_deref().unwrap_or(""),
        auth.role
    );

    let mut claims: Vec<_> = auth.claims.iter().collect();
    claims.sort_by_key(|(left, _)| *left);
    for (key, value) in claims {
        canonical.push('|');
        canonical.push_str(key);
        canonical.push('=');
        canonical.push_str(&crate::auth::canonical_json_value(value));
    }

    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn request_fingerprint_with_auth(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    content_type: Option<&str>,
    body: &[u8],
    auth_fingerprint: &str,
) -> String {
    let mut body_hasher = Sha256::new();
    body_hasher.update(body);
    let body_hash = body_hasher.finalize();

    let ct = content_type.unwrap_or("").trim().to_ascii_lowercase();
    let canonical = format!(
        "{}|{}|{}|{}|{}|{}|{:x}",
        method.as_str(),
        uri.path(),
        canonical_query(uri.query()),
        canonical_fingerprint_headers(headers),
        ct,
        auth_fingerprint,
        body_hash
    );

    let mut fp_hasher = Sha256::new();
    fp_hasher.update(canonical.as_bytes());
    format!("{:x}", fp_hasher.finalize())
}

/// Build idempotency scope from validated auth context.
///
/// Authenticated requests are isolated per principal:
/// - `tenant_id + user_id` when tenant exists
/// - `_ + user_id` when tenant is absent
///
/// Unauthenticated requests fall back to `anonymous`.
///
/// **Security (F3):** Uses the JWT-validated tenant_id — the real SaaS tenant
/// boundary — not the spoofable `x-tenant-id` request header.
fn idempotency_scope_from_auth(auth: &crate::auth::AuthContext) -> String {
    if !auth.is_authenticated() {
        return "anonymous".to_string();
    }
    let tenant_scope = auth
        .tenant_id
        .as_deref()
        .filter(|v| !v.is_empty())
        .unwrap_or("_");
    format!("{}:{}", tenant_scope, auth.user_id)
}

/// Build an HTTP response from a cached entry.
fn build_response_from_cache(cached: CachedResponse) -> Response {
    let status = StatusCode::from_u16(cached.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, Bytes::from(cached.body)).into_response();
    if let Ok(ct) = cached.content_type.parse() {
        response.headers_mut().insert("content-type", ct);
    }
    for (name, value) in cached.replay_headers {
        let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) else {
            continue;
        };
        let Ok(header_value) = HeaderValue::from_str(&value) else {
            continue;
        };
        response.headers_mut().insert(header_name, header_value);
    }
    response
        .headers_mut()
        .insert("x-idempotency-replayed", HeaderValue::from_static("true"));
    response
}
