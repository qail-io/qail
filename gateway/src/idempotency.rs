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
    http::{HeaderMap, Method, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Cached response for an idempotency key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CachedResponse {
    /// HTTP status code of the original response.
    pub status: u16,
    /// Response body bytes.
    pub body: Vec<u8>,
    /// Content-Type header value.
    pub content_type: String,
    /// SECURITY: Request fingerprint (method+path) — used to detect
    /// key reuse across different mutation routes.
    pub request_fingerprint: String,
}

/// In-memory idempotency store backed by moka cache.
#[derive(Debug)]
pub struct IdempotencyStore {
    /// Cache: compound key (tenant_scope + idempotency_key) → cached response.
    cache: moka::sync::Cache<String, CachedResponse>,
    /// SECURITY: In-flight keys currently being processed.
    /// Prevents concurrent duplicate execution of the same idempotency key.
    in_flight: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl IdempotencyStore {
    /// Create a new idempotency store.
    ///
    /// - `max_entries`: maximum number of cached responses (LRU eviction).
    /// - `ttl`: time-to-live for cached entries.
    pub(crate) fn new(max_entries: u64, ttl: Duration) -> Self {
        Self {
            cache: moka::sync::Cache::builder()
                .max_capacity(max_entries)
                .time_to_live(ttl)
                .build(),
            in_flight: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// Default production store: 100k entries, 24-hour TTL.
    pub(crate) fn production() -> Self {
        Self::new(100_000, Duration::from_secs(86400))
    }

    /// Build the composite cache key: `{tenant_scope}:{idempotency_key}`.
    pub(crate) fn cache_key(tenant_scope: &str, idempotency_key: &str) -> String {
        format!("{}:{}", tenant_scope, idempotency_key)
    }

    /// Look up a cached response by tenant scope + idempotency key.
    pub(crate) fn get(&self, tenant_scope: &str, idempotency_key: &str) -> Option<CachedResponse> {
        self.cache
            .get(&Self::cache_key(tenant_scope, idempotency_key))
    }

    /// Store a response in the idempotency cache.
    pub(crate) fn insert(
        &self,
        tenant_scope: &str,
        idempotency_key: &str,
        response: CachedResponse,
    ) {
        self.cache
            .insert(Self::cache_key(tenant_scope, idempotency_key), response);
    }

    /// Number of entries currently cached (for metrics).
    pub(crate) fn len(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Returns `true` if the cache is empty.
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// RAII guard that removes an in-flight key when dropped.
/// Ensures cleanup even on panic or tokio task cancellation.
struct InFlightGuard {
    store_in_flight: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    key: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.store_in_flight.lock().unwrap().remove(&self.key);
    }
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
    auth.enrich_with_operator_map(&state.user_operator_map)
        .await;
    auth.tenant_id.clone().unwrap_or_else(|| {
        if auth.is_authenticated() {
            auth.user_id.clone()
        } else {
            "anonymous".to_string()
        }
    })
}

/// Idempotency middleware — intercepts mutation requests with `Idempotency-Key` header.
///
/// Flow:
/// 1. GET/HEAD → pass through immediately.
/// 2. No `Idempotency-Key` header → pass through immediately.
/// 3. Key found in cache → return cached response (replay).
/// 4. Key not in cache → execute handler, cache response, return.
pub async fn idempotency_middleware(
    axum::extract::State(state): axum::extract::State<Arc<crate::GatewayState>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    // Only check mutations
    if !is_mutation_method(request.method()) {
        return next.run(request).await;
    }

    // Only act when client sends Idempotency-Key
    let Some(idempotency_key) = extract_idempotency_key(&request) else {
        return next.run(request).await;
    };

    let tenant_scope = extract_tenant_scope(state.as_ref(), request.headers().clone()).await;

    // SECURITY: Compute request fingerprint (method+path+query+body_hash) to detect key reuse
    // across different mutation routes, query variations, or different payloads.
    // Buffer the body, hash it, then reconstruct the request for downstream handlers.
    let (parts_req, body_req) = request.into_parts();
    let body_bytes_req = match axum::body::to_bytes(body_req, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            // SECURITY: Body too large or read error — reject the request outright
            // rather than forwarding a mutation with an empty body.
            return Response::builder()
                .status(StatusCode::PAYLOAD_TOO_LARGE)
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"error":"payload_too_large","message":"Request body exceeds 10MB limit for idempotent mutations"}"#
                ))
                .unwrap();
        }
    };
    let body_hash = {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        body_bytes_req.hash(&mut hasher);
        hasher.finish()
    };
    let request_fingerprint = format!("{}:{}:{:x}", parts_req.method, parts_req.uri, body_hash);
    // Reconstruct the request with the buffered body for downstream handlers.
    let request = Request::from_parts(parts_req, Body::from(body_bytes_req));

    // Check cache for replay
    if let Some(cached) = state.idempotency_store.get(&tenant_scope, &idempotency_key) {
        // SECURITY: Verify the stored fingerprint matches the current request.
        // If a client reuses the same idempotency key for a different route/method,
        // return 409 Conflict instead of replaying the wrong response.
        if cached.request_fingerprint != request_fingerprint {
            tracing::warn!(
                tenant_scope = %tenant_scope,
                idempotency_key = %idempotency_key,
                stored = %cached.request_fingerprint,
                current = %request_fingerprint,
                "Idempotency key fingerprint mismatch — key reuse across routes"
            );
            return Response::builder()
                .status(StatusCode::CONFLICT)
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"error":"idempotency_key_conflict","message":"This idempotency key was already used for a different request (method or path mismatch). Use a new key."}"#
                ))
                .unwrap();
        }

        tracing::info!(
            tenant_scope = %tenant_scope,
            idempotency_key = %idempotency_key,
            "Idempotency key hit — replaying cached response"
        );
        crate::metrics::record_idempotency_hit();
        return build_response_from_cache(cached);
    }

    // SECURITY: Mark this key as in-flight to prevent concurrent duplicate execution.
    // If another request with the same key is already executing, return 409.
    let cache_key = IdempotencyStore::cache_key(&tenant_scope, &idempotency_key);
    {
        let mut in_flight = state.idempotency_store.in_flight.lock().unwrap();
        if !in_flight.insert(cache_key.clone()) {
            tracing::warn!(
                tenant_scope = %tenant_scope,
                idempotency_key = %idempotency_key,
                "Idempotency key in-flight — concurrent request rejected"
            );
            return Response::builder()
                .status(StatusCode::CONFLICT)
                .header("content-type", "application/json")
                .header("retry-after", "1")
                .body(Body::from(
                    r#"{"error":"idempotency_key_in_flight","message":"A request with this idempotency key is already being processed. Retry after completion."}"#
                ))
                .unwrap();
        }
    }
    // RAII guard: ensures in-flight key is removed even on panic or cancellation.
    let _in_flight_guard = InFlightGuard {
        store_in_flight: Arc::clone(&state.idempotency_store.in_flight),
        key: cache_key,
    };

    // Execute the original handler
    let response = next.run(request).await;

    // Cache the response
    let (parts, body) = response.into_parts();
    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(e) => {
            // SECURITY: The mutation has already committed. Returning a synthetic 500
            // would mislead the client and break idempotency (client retries, mutation
            // may run again). Instead: preserve original status, return empty body,
            // skip caching. Client sees success status and can re-fetch if needed.
            tracing::error!(
                error = %e,
                idempotency_key = %idempotency_key,
                status = %parts.status,
                "Failed to capture response body for idempotency cache — returning original status without caching"
            );
            let mut resp = Response::from_parts(parts, Body::empty());
            resp.headers_mut().insert(
                "x-idempotency-body-truncated",
                "true".parse().unwrap(),
            );
            // _in_flight_guard will cleanup on drop here.
            return resp;
        }
    };

    let content_type = parts
        .headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    // SECURITY (Fix 4): Only cache successful (2xx) responses. Transient errors
    // should not be replayed — the client should be able to retry and get a fresh result.
    if parts.status.is_success() {
        let cached = CachedResponse {
            status: parts.status.as_u16(),
            body: body_bytes.to_vec(),
            content_type,
            request_fingerprint,
        };

        state
            .idempotency_store
            .insert(&tenant_scope, &idempotency_key, cached);

        tracing::debug!(
            tenant_scope = %tenant_scope,
            idempotency_key = %idempotency_key,
            "Idempotency key stored"
        );
    } else {
        tracing::debug!(
            tenant_scope = %tenant_scope,
            idempotency_key = %idempotency_key,
            status = %parts.status,
            "Idempotency: skipping cache for non-2xx response"
        );
    }

    // _in_flight_guard will cleanup on drop at end of scope.

    // Reconstruct response
    Response::from_parts(parts, Body::from(body_bytes))
}

/// Build an HTTP response from a cached entry.
fn build_response_from_cache(cached: CachedResponse) -> Response {
    let status = StatusCode::from_u16(cached.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut response = (status, Bytes::from(cached.body)).into_response();
    if let Ok(ct) = cached.content_type.parse() {
        response.headers_mut().insert("content-type", ct);
    }
    // Mark as replayed for clients and observability
    response
        .headers_mut()
        .insert("x-idempotency-replayed", "true".parse().unwrap());
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_insert_and_get() {
        let store = IdempotencyStore::new(100, Duration::from_secs(60));

        store.insert(
            "op-1",
            "key-abc",
            CachedResponse {
                status: 201,
                body: b"created".to_vec(),
                content_type: "application/json".to_string(),
                request_fingerprint: "POST:/test".to_string(),
            },
        );

        let cached = store.get("op-1", "key-abc").unwrap();
        assert_eq!(cached.status, 201);
        assert_eq!(cached.body, b"created");
        assert_eq!(cached.content_type, "application/json");

        // Key that wasn't inserted → miss
        assert!(store.get("op-1", "nonexistent").is_none());
    }

    #[test]
    fn store_scoped_by_tenant() {
        let store = IdempotencyStore::new(100, Duration::from_secs(60));

        store.insert(
            "op-1",
            "key-same",
            CachedResponse {
                status: 201,
                body: b"op1-response".to_vec(),
                content_type: "application/json".to_string(),
                request_fingerprint: "POST:/test".to_string(),
            },
        );

        // Same key, different tenant → miss
        assert!(store.get("op-2", "key-same").is_none());
        // Same tenant, same key → hit
        assert!(store.get("op-1", "key-same").is_some());
    }

    #[test]
    fn store_different_keys_independent() {
        let store = IdempotencyStore::new(100, Duration::from_secs(60));

        store.insert(
            "op-1",
            "key-a",
            CachedResponse {
                status: 201,
                body: b"a".to_vec(),
                content_type: "application/json".to_string(),
                request_fingerprint: "POST:/test".to_string(),
            },
        );
        store.insert(
            "op-1",
            "key-b",
            CachedResponse {
                status: 200,
                body: b"b".to_vec(),
                content_type: "application/json".to_string(),
                request_fingerprint: "POST:/test".to_string(),
            },
        );

        let a = store.get("op-1", "key-a").unwrap();
        let b = store.get("op-1", "key-b").unwrap();
        assert_eq!(a.status, 201);
        assert_eq!(b.status, 200);
    }

    #[test]
    fn is_mutation_method_check() {
        assert!(is_mutation_method(&Method::POST));
        assert!(is_mutation_method(&Method::PATCH));
        assert!(is_mutation_method(&Method::DELETE));
        assert!(!is_mutation_method(&Method::GET));
        assert!(!is_mutation_method(&Method::HEAD));
        assert!(!is_mutation_method(&Method::OPTIONS));
        assert!(!is_mutation_method(&Method::PUT));
    }

    #[test]
    fn extract_idempotency_key_from_header() {
        let req = Request::builder()
            .method(Method::POST)
            .uri("/test")
            .header("idempotency-key", "abc-123")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_idempotency_key(&req), Some("abc-123".to_string()));

        // Missing header
        let req2 = Request::builder()
            .method(Method::POST)
            .uri("/test")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_idempotency_key(&req2), None);

        // Empty header
        let req3 = Request::builder()
            .method(Method::POST)
            .uri("/test")
            .header("idempotency-key", "  ")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_idempotency_key(&req3), None);
    }

    #[test]
    fn cached_response_serde_roundtrip() {
        let original = CachedResponse {
            status: 201,
            body: b"{\"id\":1}".to_vec(),
            content_type: "application/json".to_string(),
            request_fingerprint: "POST:/test".to_string(),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: CachedResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.status, 201);
        assert_eq!(deserialized.content_type, "application/json");
    }

    #[test]
    fn build_response_includes_replay_header() {
        let cached = CachedResponse {
            status: 200,
            body: b"ok".to_vec(),
            content_type: "application/json".to_string(),
            request_fingerprint: "POST:/test".to_string(),
        };
        let response = build_response_from_cache(cached);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("x-idempotency-replayed")
                .unwrap()
                .to_str()
                .unwrap(),
            "true"
        );
    }
}
