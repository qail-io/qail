//! Idempotency key middleware.
//!
//! Prevents duplicate mutations (POST, PATCH, DELETE) caused by network retries
//! or client bugs. Clients send `Idempotency-Key: <uuid>` header; the gateway
//! caches the response and replays it on subsequent requests with the same key.
//!
//! - Keys are scoped per-operator (tenant) to prevent cross-tenant replay.
//! - Cached responses expire after a configurable TTL (default: 24 hours).
//! - Only mutation methods (POST, PATCH, DELETE) are checked; GET/HEAD are ignored.
//! - The store uses an in-memory moka cache with bounded capacity.

use axum::{
    body::Body,
    http::{
        HeaderMap, Method, Request, StatusCode, Uri,
        header::{CONTENT_TYPE, HeaderValue},
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

fn request_fingerprint(
    method: &Method,
    uri: &Uri,
    content_type: Option<&str>,
    body: &[u8],
) -> String {
    let mut body_hasher = Sha256::new();
    body_hasher.update(body);
    let body_hash = body_hasher.finalize();

    let ct = content_type.unwrap_or("").trim().to_ascii_lowercase();
    let canonical = format!(
        "{}|{}|{}|{}|{:x}",
        method.as_str(),
        uri.path(),
        canonical_query(uri.query()),
        ct,
        body_hash
    );

    let mut fp_hasher = Sha256::new();
    fp_hasher.update(canonical.as_bytes());
    format!("{:x}", fp_hasher.finalize())
}

/// Extract tenant scope from validated auth context.
/// Returns tenant_id (multi-tenant), user_id (single-user), or "anonymous".
///
/// **Security (F3):** Uses the JWT-validated tenant_id — the real SaaS tenant
/// boundary — not the spoofable `x-operator-id` request header.
async fn extract_tenant_scope(state: &crate::GatewayState, headers: HeaderMap) -> String {
    let mut auth = crate::auth::extract_auth_from_headers_with_jwks(
        &headers,
        state.jwks_store.as_ref(),
        &state.jwt_allowed_algorithms,
    );
    auth.enrich_with_tenant_map(&state.user_operator_map).await;
    auth.tenant_id.clone().unwrap_or_else(|| {
        if auth.is_authenticated() {
            auth.user_id.clone()
        } else {
            "anonymous".to_string()
        }
    })
}

/// Build an HTTP response from a cached entry.
fn build_response_from_cache(cached: CachedResponse) -> Response {
    let status = StatusCode::from_u16(cached.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, Bytes::from(cached.body)).into_response();
    if let Ok(ct) = cached.content_type.parse() {
        response.headers_mut().insert("content-type", ct);
    }
    response
        .headers_mut()
        .insert("x-idempotency-replayed", HeaderValue::from_static("true"));
    response
}
