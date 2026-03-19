use super::*;
use axum::http::HeaderMap;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};

use super::jwt::detect_jwt_algorithm;

#[test]
fn test_jwt_validation() {
    let secret = "test-secret-key-12345";
    let claims = JwtClaims {
        sub: "user123".to_string(),
        exp: (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize,
        role: Some("admin".to_string()),
        tenant_id: Some("tenant1".to_string()),
        operator_id: None,
        extra: HashMap::new(),
    };

    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap();

    let config = JwtConfig {
        secret: Some(secret.to_string()),
        algorithm: Algorithm::HS256,
        ..Default::default()
    };

    let auth = validate_jwt(&token, &config).unwrap();
    assert_eq!(auth.user_id, "user123");
    assert_eq!(auth.role, "admin");
    assert_eq!(auth.tenant_id, Some("tenant1".to_string()));
}

#[test]
fn test_engine_style_jwt() {
    // Engine JWT uses "user_id" instead of "sub" and may not have tenant_id
    let secret = "test-secret-key-12345";
    let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;

    // Simulate engine JWT payload: { "user_id": "...", "role": "SuperAdmin", "email": "..." }
    let payload = serde_json::json!({
        "user_id": "4fcc89a7-0753-4b8d-8457-71619533dbd8",
        "email": "scootsuperadmin@qail.io",
        "role": "SuperAdmin",
        "exp": exp,
        "iat": exp - 86400,
    });

    let token = encode(
        &Header::default(),
        &payload,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap();

    let config = JwtConfig {
        secret: Some(secret.to_string()),
        algorithm: Algorithm::HS256,
        ..Default::default()
    };

    let auth = validate_jwt(&token, &config).unwrap();
    assert_eq!(auth.user_id, "4fcc89a7-0753-4b8d-8457-71619533dbd8");
    assert_eq!(auth.role, "SuperAdmin");
    // No tenant_id in engine JWT — will be resolved via startup user→tenant map
    assert_eq!(auth.tenant_id, None);
}

#[test]
fn test_administrator_bypasses_rls() {
    // Platform-level "administrator" role bypasses RLS
    let auth = AuthContext {
        user_id: "master-user".to_string(),
        role: "administrator".to_string(),
        tenant_id: None,
        claims: HashMap::new(),
    };
    let rls = auth.to_rls_context();
    assert!(rls.bypasses_rls(), "administrator role should bypass RLS");
}

#[test]
fn test_administrator_pascal_case_bypasses_rls() {
    let auth = AuthContext {
        user_id: "master-user".to_string(),
        role: "Administrator".to_string(),
        tenant_id: None,
        claims: HashMap::new(),
    };
    let rls = auth.to_rls_context();
    assert!(
        rls.bypasses_rls(),
        "Administrator (PascalCase) role should bypass RLS"
    );
}

#[test]
fn test_super_admin_does_not_bypass_rls() {
    // Tenant-level super_admin should NOT bypass RLS
    let auth = AuthContext {
        user_id: "scoot-user".to_string(),
        role: "super_admin".to_string(),
        tenant_id: Some("operator-123".to_string()),
        claims: HashMap::new(),
    };
    let rls = auth.to_rls_context();
    assert!(!rls.bypasses_rls(), "super_admin should NOT bypass RLS");
    assert_eq!(rls.operator_id, "operator-123");
}

#[test]
fn test_operator_role_does_not_bypass_rls() {
    let auth = AuthContext {
        user_id: "test-user-001".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("op-test-001".to_string()),
        claims: HashMap::new(),
    };
    let rls = auth.to_rls_context();
    assert!(!rls.bypasses_rls(), "operator role should NOT bypass RLS");
    assert_eq!(rls.operator_id, "op-test-001");
}

// ══════════════════════════════════════════════════════════════════
// RED-TEAM: JWT Edge Cases (#6 from adversarial checklist)
// ══════════════════════════════════════════════════════════════════

#[test]
fn redteam_jwt_empty_sub_field() {
    let secret = "test-secret-key-12345";
    let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
    let payload = serde_json::json!({
        "sub": "",
        "exp": exp,
    });
    let token = encode(
        &Header::default(),
        &payload,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap();
    let config = JwtConfig {
        secret: Some(secret.to_string()),
        algorithm: Algorithm::HS256,
        ..Default::default()
    };
    let auth = validate_jwt(&token, &config).unwrap();
    assert_eq!(auth.user_id, "");
    // FIXED: empty sub now correctly fails is_authenticated().
    // Previously this was a documented finding where "" != "anonymous" passed.
    assert!(
        !auth.is_authenticated(),
        "Empty sub must not pass is_authenticated"
    );
}

#[test]
fn redteam_jwt_integer_tenant_id() {
    // tenant_id: 42 (integer instead of string) — must not silently coerce
    let secret = "test-secret-key-12345";
    let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
    let payload = serde_json::json!({
        "sub": "user-1",
        "exp": exp,
        "tenant_id": 42,
    });
    let token = encode(
        &Header::default(),
        &payload,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap();
    let config = JwtConfig {
        secret: Some(secret.to_string()),
        algorithm: Algorithm::HS256,
        ..Default::default()
    };
    let result = validate_jwt(&token, &config);
    // STRONG: serde rejects the entire JWT — integer tenant_id causes parse error
    assert!(
        result.is_err(),
        "Integer tenant_id must cause JWT parse failure (not silent coercion)"
    );
}

#[test]
fn redteam_jwt_is_super_admin_claim_no_rls_bypass() {
    // Attacker injects is_super_admin=true in JWT extra claims
    let auth = AuthContext {
        user_id: "attacker".to_string(),
        role: "user".to_string(),
        tenant_id: Some("tenant-x".to_string()),
        claims: {
            let mut m = HashMap::new();
            m.insert("is_super_admin".to_string(), serde_json::json!(true));
            m
        },
    };
    let rls = auth.to_rls_context();
    assert!(
        !rls.bypasses_rls(),
        "JWT is_super_admin claim must NOT grant RLS bypass"
    );
}

#[test]
fn redteam_jwt_operator_id_resolution() {
    // Engine JWT with operator_id claim
    let secret = "test-secret-key-12345";
    let exp = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize;
    let payload = serde_json::json!({
        "sub": "user-abc",
        "exp": exp,
        "operator_id": "op-123",
    });
    let token = encode(
        &Header::default(),
        &payload,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap();
    let config = JwtConfig {
        secret: Some(secret.to_string()),
        algorithm: Algorithm::HS256,
        ..Default::default()
    };
    let auth = validate_jwt(&token, &config).unwrap();
    assert_eq!(
        auth.tenant_id,
        Some("op-123".to_string()),
        "operator_id claim must resolve to tenant_id"
    );
}

#[tokio::test]
async fn redteam_enrich_fills_missing_tenant() {
    let mut auth = AuthContext {
        user_id: "user-abc".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: HashMap::new(),
    };
    let map = tokio::sync::RwLock::new({
        let mut m = std::collections::HashMap::new();
        m.insert("user-abc".to_string(), "operator-xyz".to_string());
        m
    });
    auth.enrich_with_tenant_map(&map).await;
    assert_eq!(auth.tenant_id, Some("operator-xyz".to_string()));
}

#[tokio::test]
async fn redteam_enrich_does_not_overwrite_existing_tenant() {
    let mut auth = AuthContext {
        user_id: "user-abc".to_string(),
        role: "user".to_string(),
        tenant_id: Some("already-set".to_string()),
        claims: HashMap::new(),
    };
    let map = tokio::sync::RwLock::new({
        let mut m = std::collections::HashMap::new();
        m.insert("user-abc".to_string(), "operator-xyz".to_string());
        m
    });
    auth.enrich_with_tenant_map(&map).await;
    assert_eq!(
        auth.tenant_id,
        Some("already-set".to_string()),
        "Must not overwrite existing tenant_id"
    );
}

#[tokio::test]
async fn redteam_enrich_skips_anonymous() {
    let mut auth = AuthContext {
        user_id: "anonymous".to_string(),
        role: "anon".to_string(),
        tenant_id: None,
        claims: HashMap::new(),
    };
    let map = tokio::sync::RwLock::new({
        let mut m = std::collections::HashMap::new();
        m.insert("anonymous".to_string(), "should-not-see".to_string());
        m
    });
    auth.enrich_with_tenant_map(&map).await;
    assert_eq!(
        auth.tenant_id, None,
        "Anonymous users must not get tenant_id enrichment"
    );
}

#[test]
fn redteam_finance_admin_does_not_bypass_rls() {
    let auth = AuthContext {
        user_id: "finance-user".to_string(),
        role: "FinanceAdmin".to_string(),
        tenant_id: Some("op-123".to_string()),
        claims: HashMap::new(),
    };
    let rls = auth.to_rls_context();
    assert!(!rls.bypasses_rls(), "FinanceAdmin must NOT bypass RLS");
}

// ══════════════════════════════════════════════════════════════════
// F1/F2: JWT hard-fail + JWKS wiring tests
// ══════════════════════════════════════════════════════════════════

#[test]
fn denied_context_is_not_authenticated() {
    let auth = AuthContext::denied();
    assert!(!auth.is_authenticated());
    assert!(auth.is_denied());
    assert_eq!(auth.user_id, "denied");
    assert_eq!(auth.role, "denied");
}

#[test]
fn anonymous_context_is_not_denied() {
    let auth = AuthContext::anonymous();
    assert!(!auth.is_authenticated());
    assert!(!auth.is_denied());
}

#[test]
fn invalid_jwt_returns_denied_not_anonymous() {
    // Test via extract_auth_from_headers_with_jwks without env mutation.
    // With no JWKS store and no JWT_SECRET, any Bearer token must be denied.
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer invalid.jwt.token".parse().unwrap());

    // No JWKS store, no JWT_SECRET env → denied
    let auth = extract_auth_from_headers_with_jwks(&headers, None, &[]);
    assert!(
        auth.is_denied(),
        "Invalid JWT must return denied, got: {:?}",
        auth
    );
    assert!(!auth.is_authenticated());
}

#[test]
fn bearer_without_any_config_returns_denied() {
    // Bearer present but neither JWKS store nor JWT_SECRET → denied
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "Bearer some.valid.token".parse().unwrap());

    let auth = extract_auth_from_headers_with_jwks(&headers, None, &[]);
    assert!(auth.is_denied(), "Bearer without any JWT config must deny");
}

#[test]
fn detect_jwt_algorithm_rs256() {
    // JWT header: {"alg":"RS256","typ":"JWT"}
    // base64url: eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9
    let token = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig";
    assert_eq!(detect_jwt_algorithm(token), Some(Algorithm::RS256));
}

#[test]
fn detect_jwt_algorithm_hs256() {
    // JWT header: {"alg":"HS256","typ":"JWT"}
    // base64url: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9
    let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig";
    assert_eq!(detect_jwt_algorithm(token), Some(Algorithm::HS256));
}

#[test]
fn detect_jwt_algorithm_malformed() {
    assert_eq!(detect_jwt_algorithm("not-jwt"), None);
    assert_eq!(detect_jwt_algorithm(""), None);
}

#[test]
fn lowercase_bearer_enters_jwt_path() {
    // "bearer" (lowercase) must NOT silently fall through to anonymous
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "bearer some.jwt.token".parse().unwrap());

    let auth = extract_auth_from_headers_with_jwks(&headers, None, &[]);
    assert!(
        auth.is_denied(),
        "lowercase 'bearer' must enter JWT path and deny (no config), got: {:?}",
        auth
    );
}

#[test]
fn uppercase_bearer_enters_jwt_path() {
    let mut headers = HeaderMap::new();
    headers.insert("authorization", "BEARER some.jwt.token".parse().unwrap());

    let auth = extract_auth_from_headers_with_jwks(&headers, None, &[]);
    assert!(
        auth.is_denied(),
        "uppercase 'BEARER' must enter JWT path and deny (no config), got: {:?}",
        auth
    );
}

#[test]
fn parse_allowed_algorithms_case_insensitive_dedup() {
    let parsed = parse_allowed_algorithms(&[
        " rs256 ".to_string(),
        "HS256".to_string(),
        "Rs256".to_string(),
    ])
    .unwrap();
    assert_eq!(parsed, vec![Algorithm::RS256, Algorithm::HS256]);
}

#[test]
fn parse_allowed_algorithms_rejects_unknown() {
    let err = parse_allowed_algorithms(&["FOO256".to_string()]).unwrap_err();
    assert!(err.to_string().contains("Unsupported JWT algorithm"));
}

// ══════════════════════════════════════════════════════════════════
// Tenant impersonation tests
// ══════════════════════════════════════════════════════════════════

#[test]
fn impersonation_downgrades_administrator_role() {
    // Simulate what extract_auth_for_state does for impersonation
    let mut auth = AuthContext {
        user_id: "master-user".to_string(),
        role: "administrator".to_string(),
        tenant_id: None,
        claims: HashMap::new(),
    };

    // Apply impersonation logic (same as in extract_auth_for_state)
    let target_tenant = "operator-abc-123";
    let is_platform_admin = matches!(auth.role.as_str(), "administrator" | "Administrator");
    assert!(is_platform_admin);
    auth.tenant_id = Some(target_tenant.to_string());
    auth.role = "operator".to_string();

    // After impersonation: tenant_id is set, role is downgraded
    assert_eq!(auth.tenant_id, Some("operator-abc-123".to_string()));
    assert_eq!(auth.role, "operator");

    // RLS must NOT bypass
    let rls = auth.to_rls_context();
    assert!(
        !rls.bypasses_rls(),
        "Impersonated admin must NOT bypass RLS"
    );
    assert_eq!(rls.operator_id, "operator-abc-123");
}

#[test]
fn impersonation_ignored_for_non_administrator() {
    // Non-administrator roles must not be able to impersonate
    let auth = AuthContext {
        user_id: "tenant-user".to_string(),
        role: "super_admin".to_string(),
        tenant_id: Some("original-tenant".to_string()),
        claims: HashMap::new(),
    };

    // Apply same check — should NOT match
    let is_platform_admin = matches!(auth.role.as_str(), "administrator" | "Administrator");
    assert!(!is_platform_admin, "super_admin is NOT platform admin");

    // tenant_id should remain unchanged
    assert_eq!(auth.tenant_id, Some("original-tenant".to_string()));
    assert_eq!(auth.role, "super_admin");
}
