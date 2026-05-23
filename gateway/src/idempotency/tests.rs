use super::*;
use axum::http::HeaderMap;
use std::time::Duration;

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
            replay_headers: vec![],
            request_fingerprint: "POST:/test".to_string(),
        },
    );

    let cached = store.get("op-1", "key-abc").unwrap();
    assert_eq!(cached.status, 201);
    assert_eq!(cached.body, b"created");
    assert_eq!(cached.content_type, "application/json");

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
            replay_headers: vec![],
            request_fingerprint: "POST:/test".to_string(),
        },
    );

    assert!(store.get("op-2", "key-same").is_none());
    assert!(store.get("op-1", "key-same").is_some());
}

#[test]
fn store_cache_key_is_not_colon_ambiguous() {
    let store = IdempotencyStore::new(100, Duration::from_secs(60));

    store.insert(
        "tenant:user",
        "abc",
        CachedResponse {
            status: 201,
            body: b"tenant-user-response".to_vec(),
            content_type: "application/json".to_string(),
            replay_headers: vec![],
            request_fingerprint: "POST:/test".to_string(),
        },
    );

    assert_ne!(
        IdempotencyStore::cache_key("tenant:user", "abc"),
        IdempotencyStore::cache_key("tenant", "user:abc")
    );
    assert!(
        store.get("tenant", "user:abc").is_none(),
        "colon-bearing scope/key components must not collide"
    );
}

#[test]
fn idempotency_scope_includes_tenant_and_user() {
    let auth = crate::auth::AuthContext {
        user_id: "user-1".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-a".to_string()),
        claims: std::collections::HashMap::new(),
    };
    assert_eq!(idempotency_scope_from_auth(&auth), "tenant-a:user-1");
}

#[test]
fn idempotency_scope_keeps_users_isolated_without_tenant() {
    let auth_a = crate::auth::AuthContext {
        user_id: "user-a".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    let auth_b = crate::auth::AuthContext {
        user_id: "user-b".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    assert_eq!(idempotency_scope_from_auth(&auth_a), "_:user-a");
    assert_eq!(idempotency_scope_from_auth(&auth_b), "_:user-b");
}

#[test]
fn idempotency_scope_for_anonymous_is_shared_literal() {
    let auth = crate::auth::AuthContext::anonymous();
    assert_eq!(idempotency_scope_from_auth(&auth), "anonymous");
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
            replay_headers: vec![],
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
            replay_headers: vec![],
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
fn transaction_paths_bypass_idempotency() {
    for path in [
        "/txn",
        "/txn/begin",
        "/txn/query",
        "/txn/commit?trace=1",
        "/txn/rollback",
        "/txn/savepoint",
    ] {
        let uri = path.parse::<Uri>().expect("valid transaction uri");
        assert!(is_transaction_path(&uri), "{path} should bypass");
    }

    for path in ["/txns/begin", "/api/txn/begin", "/transaction/begin"] {
        let uri = path.parse::<Uri>().expect("valid non-transaction uri");
        assert!(!is_transaction_path(&uri), "{path} should not bypass");
    }
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

    let req2 = Request::builder()
        .method(Method::POST)
        .uri("/test")
        .body(Body::empty())
        .unwrap();
    assert_eq!(extract_idempotency_key(&req2), None);

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
        replay_headers: vec![],
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
        replay_headers: vec![],
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

#[test]
fn capture_replay_headers_preserves_safe_subset_only() {
    let mut headers = HeaderMap::new();
    headers.insert("location", "/v1/orders/1".parse().unwrap());
    headers.insert("etag", "\"v1\"".parse().unwrap());
    headers.insert("set-cookie", "session=abc".parse().unwrap());

    let captured = capture_replay_headers(&headers);
    assert!(
        captured
            .iter()
            .any(|(k, v)| k == "location" && v == "/v1/orders/1")
    );
    assert!(captured.iter().any(|(k, v)| k == "etag" && v == "\"v1\""));
    assert!(!captured.iter().any(|(k, _)| k == "set-cookie"));
}

#[test]
fn build_response_replays_safe_headers() {
    let response = build_response_from_cache(CachedResponse {
        status: 201,
        body: b"created".to_vec(),
        content_type: "application/json".to_string(),
        replay_headers: vec![
            ("location".to_string(), "/v1/orders/1".to_string()),
            ("etag".to_string(), "\"v1\"".to_string()),
        ],
        request_fingerprint: "POST:/v1/orders".to_string(),
    });

    assert_eq!(
        response
            .headers()
            .get("location")
            .and_then(|v| v.to_str().ok()),
        Some("/v1/orders/1")
    );
    assert_eq!(
        response.headers().get("etag").and_then(|v| v.to_str().ok()),
        Some("\"v1\"")
    );
}

#[test]
fn fingerprint_changes_with_body() {
    let headers = HeaderMap::new();
    let a = request_fingerprint(
        &Method::POST,
        &"/v1/orders?limit=10".parse::<Uri>().expect("valid uri"),
        &headers,
        Some("application/json"),
        br#"{"id":1}"#,
    );
    let b = request_fingerprint(
        &Method::POST,
        &"/v1/orders?limit=10".parse::<Uri>().expect("valid uri"),
        &headers,
        Some("application/json"),
        br#"{"id":2}"#,
    );
    assert_ne!(a, b);
}

#[test]
fn fingerprint_changes_with_content_type() {
    let uri = "/v1/orders".parse::<Uri>().expect("valid uri");
    let headers = HeaderMap::new();
    let json = request_fingerprint(
        &Method::POST,
        &uri,
        &headers,
        Some("application/json"),
        b"{}",
    );
    let form = request_fingerprint(
        &Method::POST,
        &uri,
        &headers,
        Some("application/x-www-form-urlencoded"),
        b"{}",
    );
    assert_ne!(json, form);
}

#[test]
fn fingerprint_canonicalizes_query_pair_order() {
    let headers = HeaderMap::new();
    let a = request_fingerprint(
        &Method::POST,
        &"/v1/orders?a=1&b=2".parse::<Uri>().expect("valid uri"),
        &headers,
        Some("application/json"),
        br#"{"id":1}"#,
    );
    let b = request_fingerprint(
        &Method::POST,
        &"/v1/orders?b=2&a=1".parse::<Uri>().expect("valid uri"),
        &headers,
        Some("application/json"),
        br#"{"id":1}"#,
    );
    assert_eq!(a, b);
}

#[test]
fn fingerprint_changes_with_query_value() {
    let headers = HeaderMap::new();
    let a = request_fingerprint(
        &Method::POST,
        &"/v1/orders?a=1".parse::<Uri>().expect("valid uri"),
        &headers,
        Some("application/json"),
        br#"{"id":1}"#,
    );
    let b = request_fingerprint(
        &Method::POST,
        &"/v1/orders?a=2".parse::<Uri>().expect("valid uri"),
        &headers,
        Some("application/json"),
        br#"{"id":1}"#,
    );
    assert_ne!(a, b);
}

#[test]
fn fingerprint_changes_with_transaction_id_header() {
    let uri = "/txn/commit".parse::<Uri>().expect("valid uri");
    let mut h1 = HeaderMap::new();
    let mut h2 = HeaderMap::new();
    h1.insert(
        "x-transaction-id",
        "11111111-1111-1111-1111-111111111111".parse().unwrap(),
    );
    h2.insert(
        "x-transaction-id",
        "22222222-2222-2222-2222-222222222222".parse().unwrap(),
    );
    let a = request_fingerprint(&Method::POST, &uri, &h1, None, b"");
    let b = request_fingerprint(&Method::POST, &uri, &h2, None, b"");
    assert_ne!(a, b);
}

#[test]
fn fingerprint_changes_with_branch_id_header() {
    let uri = "/api/orders/1".parse::<Uri>().expect("valid uri");
    let mut base = HeaderMap::new();
    base.insert("x-branch-id", "alpha".parse().unwrap());
    let mut other = HeaderMap::new();
    other.insert("x-branch-id", "beta".parse().unwrap());
    let a = request_fingerprint(&Method::PATCH, &uri, &base, Some("application/json"), b"{}");
    let b = request_fingerprint(
        &Method::PATCH,
        &uri,
        &other,
        Some("application/json"),
        b"{}",
    );
    assert_ne!(a, b);
}

#[test]
fn fingerprint_changes_with_impersonated_tenant_header() {
    let uri = "/api/orders".parse::<Uri>().expect("valid uri");
    let mut tenant_a = HeaderMap::new();
    tenant_a.insert("x-impersonate-tenant", "tenant-a".parse().unwrap());
    let mut tenant_b = HeaderMap::new();
    tenant_b.insert("x-impersonate-tenant", "tenant-b".parse().unwrap());

    let a = request_fingerprint(
        &Method::POST,
        &uri,
        &tenant_a,
        Some("application/json"),
        b"{}",
    );
    let b = request_fingerprint(
        &Method::POST,
        &uri,
        &tenant_b,
        Some("application/json"),
        b"{}",
    );
    assert_ne!(a, b);
}

#[test]
fn fingerprint_changes_with_prefer_header() {
    let uri = "/api/orders".parse::<Uri>().expect("valid uri");
    let mut merge = HeaderMap::new();
    merge.insert("prefer", "resolution=merge-duplicates".parse().unwrap());
    let mut ignore = HeaderMap::new();
    ignore.insert("prefer", "resolution=ignore-duplicates".parse().unwrap());
    let a = request_fingerprint(
        &Method::POST,
        &uri,
        &merge,
        Some("application/json"),
        br#"{"id":"1"}"#,
    );
    let b = request_fingerprint(
        &Method::POST,
        &uri,
        &ignore,
        Some("application/json"),
        br#"{"id":"1"}"#,
    );
    assert_ne!(a, b);
}

#[test]
fn auth_replay_fingerprint_changes_with_role_and_claims() {
    let mut claims = std::collections::HashMap::new();
    claims.insert("tier".to_string(), serde_json::json!("gold"));
    let mut admin = crate::auth::AuthContext {
        user_id: "user-1".to_string(),
        role: "admin".to_string(),
        tenant_id: Some("tenant-a".to_string()),
        claims: claims.clone(),
    };
    let mut viewer = admin.clone();
    viewer.role = "viewer".to_string();

    assert_ne!(
        auth_replay_fingerprint(&admin),
        auth_replay_fingerprint(&viewer)
    );

    admin
        .claims
        .insert("tier".to_string(), serde_json::json!("platinum"));
    assert_ne!(
        auth_replay_fingerprint(&admin),
        auth_replay_fingerprint(&crate::auth::AuthContext {
            user_id: "user-1".to_string(),
            role: "admin".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            claims,
        })
    );
}

#[test]
fn request_fingerprint_changes_with_auth_replay_fingerprint() {
    let uri = "/api/orders".parse::<Uri>().expect("valid uri");
    let headers = HeaderMap::new();
    let auth_a = "auth-a";
    let auth_b = "auth-b";

    let a = request_fingerprint_with_auth(
        &Method::POST,
        &uri,
        &headers,
        Some("application/json"),
        b"{}",
        auth_a,
    );
    let b = request_fingerprint_with_auth(
        &Method::POST,
        &uri,
        &headers,
        Some("application/json"),
        b"{}",
        auth_b,
    );

    assert_ne!(a, b);
}

#[test]
fn should_capture_response_requires_success_status() {
    let headers = HeaderMap::new();
    assert!(!should_capture_response_for_idempotency(
        StatusCode::BAD_REQUEST,
        &headers,
        1024
    ));
}

#[test]
fn should_capture_response_accepts_unknown_content_length() {
    let headers = HeaderMap::new();
    assert!(should_capture_response_for_idempotency(
        StatusCode::OK,
        &headers,
        1024
    ));
}

#[test]
fn should_capture_response_enforces_body_limit() {
    let mut headers = HeaderMap::new();
    headers.insert("content-length", "2048".parse().unwrap());
    assert!(!should_capture_response_for_idempotency(
        StatusCode::OK,
        &headers,
        1024
    ));
    assert!(response_exceeds_idempotency_body_limit(&headers, 1024));
}

#[test]
fn should_capture_response_accepts_bounded_success() {
    let mut headers = HeaderMap::new();
    headers.insert("content-length", "512".parse().unwrap());
    assert!(should_capture_response_for_idempotency(
        StatusCode::OK,
        &headers,
        1024
    ));
    assert!(!response_exceeds_idempotency_body_limit(&headers, 1024));
}

#[test]
fn parse_content_length_rejects_invalid_value() {
    let mut headers = HeaderMap::new();
    headers.insert("content-length", "not-a-number".parse().unwrap());
    assert_eq!(parse_content_length(&headers), None);
}
