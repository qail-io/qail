use super::*;
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
            request_fingerprint: "POST:/test".to_string(),
        },
    );

    assert!(store.get("op-2", "key-same").is_none());
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

#[test]
fn fingerprint_changes_with_body() {
    let a = request_fingerprint(
        &Method::POST,
        &"/v1/orders?limit=10".parse::<Uri>().expect("valid uri"),
        Some("application/json"),
        br#"{"id":1}"#,
    );
    let b = request_fingerprint(
        &Method::POST,
        &"/v1/orders?limit=10".parse::<Uri>().expect("valid uri"),
        Some("application/json"),
        br#"{"id":2}"#,
    );
    assert_ne!(a, b);
}

#[test]
fn fingerprint_changes_with_content_type() {
    let uri = "/v1/orders".parse::<Uri>().expect("valid uri");
    let json = request_fingerprint(&Method::POST, &uri, Some("application/json"), b"{}");
    let form = request_fingerprint(
        &Method::POST,
        &uri,
        Some("application/x-www-form-urlencoded"),
        b"{}",
    );
    assert_ne!(json, form);
}

#[test]
fn fingerprint_canonicalizes_query_pair_order() {
    let a = request_fingerprint(
        &Method::POST,
        &"/v1/orders?a=1&b=2".parse::<Uri>().expect("valid uri"),
        Some("application/json"),
        br#"{"id":1}"#,
    );
    let b = request_fingerprint(
        &Method::POST,
        &"/v1/orders?b=2&a=1".parse::<Uri>().expect("valid uri"),
        Some("application/json"),
        br#"{"id":1}"#,
    );
    assert_eq!(a, b);
}

#[test]
fn fingerprint_changes_with_query_value() {
    let a = request_fingerprint(
        &Method::POST,
        &"/v1/orders?a=1".parse::<Uri>().expect("valid uri"),
        Some("application/json"),
        br#"{"id":1}"#,
    );
    let b = request_fingerprint(
        &Method::POST,
        &"/v1/orders?a=2".parse::<Uri>().expect("valid uri"),
        Some("application/json"),
        br#"{"id":1}"#,
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
fn should_capture_response_requires_known_content_length() {
    let headers = HeaderMap::new();
    assert!(!should_capture_response_for_idempotency(
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
}

#[test]
fn parse_content_length_rejects_invalid_value() {
    let mut headers = HeaderMap::new();
    headers.insert("content-length", "not-a-number".parse().unwrap());
    assert_eq!(parse_content_length(&headers), None);
}
