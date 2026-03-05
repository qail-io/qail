use super::*;
use axum::middleware::Next;
use std::sync::Arc;

/// RAII guard that removes an in-flight key when dropped.
/// Ensures cleanup even on panic or tokio task cancellation.
struct InFlightGuard {
    store_in_flight: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    key: String,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        lock_in_flight_set(&self.store_in_flight).remove(&self.key);
    }
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
    if !is_mutation_method(request.method()) {
        return next.run(request).await;
    }

    let Some(idempotency_key) = extract_idempotency_key(&request) else {
        return next.run(request).await;
    };

    let tenant_scope = extract_tenant_scope(state.as_ref(), request.headers().clone()).await;

    let (parts_req, body_req) = request.into_parts();
    let body_bytes_req = match axum::body::to_bytes(body_req, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return json_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                r#"{"error":"payload_too_large","message":"Request body exceeds 10MB limit for idempotent mutations"}"#,
            );
        }
    };
    let request_fingerprint = request_fingerprint(
        &parts_req.method,
        &parts_req.uri,
        parts_req
            .headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok()),
        &body_bytes_req,
    );
    let request = Request::from_parts(parts_req, Body::from(body_bytes_req));

    if let Some(cached) = state.idempotency_store.get(&tenant_scope, &idempotency_key) {
        if cached.request_fingerprint != request_fingerprint {
            tracing::warn!(
                tenant_scope = %tenant_scope,
                idempotency_key = %idempotency_key,
                stored = %cached.request_fingerprint,
                current = %request_fingerprint,
                "Idempotency key fingerprint mismatch — key reuse across routes"
            );
            return json_response(
                StatusCode::CONFLICT,
                r#"{"error":"idempotency_key_conflict","message":"This idempotency key was already used for a different request fingerprint. Use a new key."}"#,
            );
        }

        tracing::info!(
            tenant_scope = %tenant_scope,
            idempotency_key = %idempotency_key,
            "Idempotency key hit — replaying cached response"
        );
        crate::metrics::record_idempotency_hit();
        return build_response_from_cache(cached);
    }

    let cache_key = IdempotencyStore::cache_key(&tenant_scope, &idempotency_key);
    {
        let mut in_flight = lock_in_flight_set(&state.idempotency_store.in_flight);
        if !in_flight.insert(cache_key.clone()) {
            tracing::warn!(
                tenant_scope = %tenant_scope,
                idempotency_key = %idempotency_key,
                "Idempotency key in-flight — concurrent request rejected"
            );
            let mut response = json_response(
                StatusCode::CONFLICT,
                r#"{"error":"idempotency_key_in_flight","message":"A request with this idempotency key is already being processed. Retry after completion."}"#,
            );
            response
                .headers_mut()
                .insert("retry-after", HeaderValue::from_static("1"));
            return response;
        }
    }
    let _in_flight_guard = InFlightGuard {
        store_in_flight: Arc::clone(&state.idempotency_store.in_flight),
        key: cache_key,
    };

    let response = next.run(request).await;

    let (parts, body) = response.into_parts();
    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!(
                error = %e,
                idempotency_key = %idempotency_key,
                status = %parts.status,
                "Failed to capture response body for idempotency cache — returning original status without caching"
            );
            let mut resp = Response::from_parts(parts, Body::empty());
            resp.headers_mut().insert(
                "x-idempotency-body-truncated",
                HeaderValue::from_static("true"),
            );
            return resp;
        }
    };

    let content_type = parts
        .headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

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

    Response::from_parts(parts, Body::from(body_bytes))
}
