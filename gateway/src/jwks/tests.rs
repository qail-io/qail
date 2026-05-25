use super::*;

#[test]
fn extract_kid_from_valid_jwt_header() {
    let token =
        "eyJhbGciOiJSUzI1NiIsImtpZCI6Im15LWtleS0xIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ0ZXN0In0.fake_sig";
    let kid = extract_kid_from_jwt(token).unwrap();
    assert_eq!(kid, "my-key-1");
}

#[test]
fn extract_kid_missing_returns_none() {
    let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0In0.fake";
    assert_eq!(extract_kid_from_jwt(token), None);
}

#[test]
fn extract_kid_malformed_token_returns_none() {
    assert_eq!(extract_kid_from_jwt("not-a-jwt"), None);
    assert_eq!(extract_kid_from_jwt(""), None);
    assert_eq!(extract_kid_from_jwt("a.b"), None);
}

#[test]
fn extract_kid_rejects_oversized_header_segment() {
    let token = format!("{}.payload.sig", "a".repeat(MAX_JWT_HEADER_B64_BYTES + 1));
    assert_eq!(extract_kid_from_jwt(&token), None);
}

#[test]
fn jwks_store_from_env_missing_returns_none() {
    if std::env::var("JWKS_URL").is_err() {
        assert!(JwksKeyStore::from_env().is_none());
    }
}

#[tokio::test]
async fn jwks_store_get_key_empty_returns_none() {
    let store = JwksKeyStore::new("https://example.com/jwks", Duration::from_secs(60));
    assert_eq!(store.key_count().await, 0);
    assert!(store.get_key("nonexistent").await.is_none());
}

#[test]
fn jwks_store_clamps_zero_refresh_interval() {
    let store = JwksKeyStore::new("https://example.com/jwks", Duration::ZERO);
    assert_eq!(store.refresh_interval, MIN_JWKS_REFRESH_INTERVAL);
}

#[test]
fn parse_jwks_body_accepts_valid_small_payload() {
    let jwks = parse_jwks_body(br#"{"keys":[]}"#).unwrap();
    assert!(jwks.keys.is_empty());
}

#[test]
fn parse_jwks_body_rejects_oversized_payload() {
    let oversized = vec![b' '; MAX_JWKS_BODY_BYTES + 1];
    let err = parse_jwks_body(&oversized).unwrap_err();
    assert!(err.contains("JWKS body too large"));
}
