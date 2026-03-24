use super::*;
use crate::auth::AuthContext;
use qail_core::ast::{CageKind, Expr, Qail, Value};

#[test]
fn test_policy_expands_user_id() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user123".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let result = engine.expand_filter("user_id = $user_id", &auth);
    assert_eq!(result, "user_id = 'user123'");
}

#[test]
fn test_policy_injects_filter() {
    let engine = PolicyEngine::new();
    let mut cmd = Qail::get("orders").columns(["id", "total"]);

    engine
        .inject_filter(&mut cmd, "user_id = 'user123'")
        .unwrap();

    assert_eq!(cmd.cages.len(), 1);
    assert!(matches!(cmd.cages[0].kind, CageKind::Filter));
    assert_eq!(cmd.cages[0].conditions.len(), 1);
}

#[test]
fn test_apply_policies_adds_filter() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "tenant_isolation".to_string(),
        table: "orders".to_string(),
        filter: Some("user_id = $user_id".to_string()),
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user456".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("orders").columns(["id"]);
    engine.apply_policies(&auth, &mut cmd).unwrap();

    // Check that filter was added
    assert_eq!(cmd.cages.len(), 1);
    let condition = &cmd.cages[0].conditions[0];
    assert_eq!(condition.left, Expr::Named("user_id".to_string()));
    assert_eq!(condition.value, Value::String("user456".to_string()));
}

#[test]
fn test_column_whitelist() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "hide_sensitive".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![],
        allowed_columns: vec!["id".into(), "name".into(), "email".into()],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    // SELECT * should be restricted
    let mut cmd = Qail::get("users");
    engine.apply_policies(&auth, &mut cmd).unwrap();
    assert_eq!(cmd.columns.len(), 3);
    assert!(cmd.columns.contains(&Expr::Named("id".to_string())));
    assert!(cmd.columns.contains(&Expr::Named("name".to_string())));
    assert!(cmd.columns.contains(&Expr::Named("email".to_string())));
}

#[test]
fn test_column_blacklist() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "hide_password".to_string(),
        table: "users".to_string(),
        filter: None,
        role: None,
        operations: vec![],
        allowed_columns: vec![],
        denied_columns: vec!["password_hash".into(), "secret_key".into()],
    });

    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("users").columns(["id", "name", "password_hash", "secret_key"]);
    engine.apply_policies(&auth, &mut cmd).unwrap();
    assert_eq!(cmd.columns.len(), 2);
    assert!(cmd.columns.contains(&Expr::Named("id".to_string())));
    assert!(cmd.columns.contains(&Expr::Named("name".to_string())));
}

// ══════════════════════════════════════════════════════════════════
// SECURITY: expand_filter SQL injection hardening (G1)
// ══════════════════════════════════════════════════════════════════

#[test]
fn security_expand_filter_escapes_user_id_quotes() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "x' OR 1=1 --".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    let result = engine.expand_filter("user_id = $user_id", &auth);
    assert_eq!(result, "user_id = 'x'' OR 1=1 --'");
    assert!(
        !result.contains("x' OR"),
        "Unescaped quote in user_id: {}",
        result
    );
}

#[test]
fn security_expand_filter_escapes_role_quotes() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "admin'; DROP TABLE users; --".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    let result = engine.expand_filter("role = $role", &auth);
    assert!(
        result.contains("admin''; DROP TABLE users; --"),
        "Unescaped quote in role: {}",
        result
    );
}

#[test]
fn security_expand_filter_escapes_claim_string_quotes() {
    let engine = PolicyEngine::new();
    let mut claims = std::collections::HashMap::new();
    claims.insert("org_name".to_string(), serde_json::json!("Acme' OR '1'='1"));
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims,
    };
    let result = engine.expand_filter("org = $org_name", &auth);
    assert!(
        result.contains("Acme'' OR ''1''=''1"),
        "Unescaped quote in claim: {}",
        result
    );
}

#[test]
fn security_expand_filter_numeric_claim_no_quotes() {
    let engine = PolicyEngine::new();
    let mut claims = std::collections::HashMap::new();
    claims.insert("age".to_string(), serde_json::json!(42));
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims,
    };
    let result = engine.expand_filter("age = $age", &auth);
    assert_eq!(result, "age = 42");
}

#[test]
fn security_expand_filter_bool_claim_no_quotes() {
    let engine = PolicyEngine::new();
    let mut claims = std::collections::HashMap::new();
    claims.insert("active".to_string(), serde_json::json!(true));
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims,
    };
    let result = engine.expand_filter("active = $active", &auth);
    assert_eq!(result, "active = true");
}

#[test]
fn security_expand_filter_safe_values_unchanged() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        role: "operator".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    let result = engine.expand_filter("user_id = $user_id AND role = $role", &auth);
    assert_eq!(
        result,
        "user_id = '550e8400-e29b-41d4-a716-446655440000' AND role = 'operator'"
    );
}

// ══════════════════════════════════════════════════════════════════
// SECURITY: $tenant_id expansion (H1)
// ══════════════════════════════════════════════════════════════════

#[test]
fn test_expand_filter_tenant_id() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: Some("tenant_abc".to_string()),
        claims: std::collections::HashMap::new(),
    };
    let result = engine.expand_filter("operator_id = $tenant_id", &auth);
    assert_eq!(result, "operator_id = 'tenant_abc'");
}

#[test]
fn test_expand_filter_tenant_id_sql_injection() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: Some("O'Brien".to_string()),
        claims: std::collections::HashMap::new(),
    };
    let result = engine.expand_filter("operator_id = $tenant_id", &auth);
    assert_eq!(result, "operator_id = 'O''Brien'");
    assert!(
        !result.contains("O'B"),
        "Unescaped quote in tenant_id: {}",
        result
    );
}

#[test]
fn test_expand_filter_tenant_id_missing() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "user1".to_string(),
        role: "user".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    // When tenant_id is None, $tenant_id should NOT be expanded (stays literal)
    let result = engine.expand_filter("operator_id = $tenant_id", &auth);
    assert_eq!(result, "operator_id = $tenant_id");
}

#[test]
fn test_apply_policies_does_not_early_deny_when_later_policy_allows_operation() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "harbors_public_read".to_string(),
        table: "harbors".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });
    engine.add_policy(PolicyDef {
        name: "harbors_operator_crud".to_string(),
        table: "harbors".to_string(),
        filter: None,
        role: Some("operator".to_string()),
        operations: vec![
            OperationType::Read,
            OperationType::Create,
            OperationType::Update,
            OperationType::Delete,
        ],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "user-op".to_string(),
        role: "operator".to_string(),
        tenant_id: Some("tenant-op".to_string()),
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::set("harbors");
    let result = engine.apply_policies(&auth, &mut cmd);
    assert!(
        result.is_ok(),
        "expected update to be allowed by operator CRUD policy, got: {:?}",
        result
    );
}

#[test]
fn test_apply_policies_denies_when_matching_policies_exist_but_none_allow_operation() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "harbors_public_read".to_string(),
        table: "harbors".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "anon".to_string(),
        role: "anonymous".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::set("harbors");
    let result = engine.apply_policies(&auth, &mut cmd);
    assert!(
        result.is_err(),
        "expected update deny when only read policy matches"
    );
}

#[test]
fn test_apply_policies_denies_when_no_policy_matches_table() {
    let mut engine = PolicyEngine::new();
    engine.add_policy(PolicyDef {
        name: "orders_read".to_string(),
        table: "orders".to_string(),
        filter: None,
        role: None,
        operations: vec![OperationType::Read],
        allowed_columns: vec![],
        denied_columns: vec![],
    });

    let auth = AuthContext {
        user_id: "anon".to_string(),
        role: "anonymous".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };

    let mut cmd = Qail::get("users");
    let result = engine.apply_policies(&auth, &mut cmd);
    assert!(
        result.is_err(),
        "expected deny when no policy matches target table"
    );
}

#[test]
fn test_apply_policies_allows_when_policy_engine_is_empty() {
    let engine = PolicyEngine::new();
    let auth = AuthContext {
        user_id: "anon".to_string(),
        role: "anonymous".to_string(),
        tenant_id: None,
        claims: std::collections::HashMap::new(),
    };
    let mut cmd = Qail::get("users");
    let result = engine.apply_policies(&auth, &mut cmd);
    assert!(
        result.is_ok(),
        "empty policy engine should preserve current behavior"
    );
}
