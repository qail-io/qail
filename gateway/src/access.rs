use std::collections::BTreeSet;

use qail_core::access::{AccessContext, AccessPolicy};
use qail_core::ast::Qail;
use qail_core::rls::SuperAdminToken;

use crate::GatewayState;
use crate::auth::AuthContext;
use crate::middleware::ApiError;

pub(crate) fn access_context_from_auth(auth: &AuthContext) -> AccessContext {
    if auth.is_platform_admin() {
        let token = SuperAdminToken::for_auth("gateway_access_policy");
        return AccessContext::super_admin(token);
    }

    let mut ctx = if auth.is_authenticated() {
        AccessContext::subject(auth.user_id.clone())
    } else {
        AccessContext::anonymous()
    };

    if !auth.role.trim().is_empty() {
        ctx = ctx.with_role(auth.role.clone());
    }
    if let Some(tenant_id) = auth.tenant_id.as_deref()
        && !tenant_id.trim().is_empty()
    {
        ctx = ctx.with_tenant(tenant_id.to_string());
    }

    ctx.with_scopes(scopes_from_claims(auth))
}

pub(crate) fn check_access_policy(
    state: &GatewayState,
    auth: &AuthContext,
    cmd: &Qail,
) -> Result<(), ApiError> {
    let Some(policy) = state.access_policy.as_ref() else {
        return Ok(());
    };

    check_policy_for_command(policy, auth, cmd).map_err(|message| {
        tracing::warn!(
            user_id = %auth.user_id,
            role = %auth.role,
            tenant_id = ?auth.tenant_id,
            table = %cmd.table,
            action = ?cmd.action,
            reason = %message,
            "Native access policy denied command"
        );
        ApiError::forbidden(format!("Access denied by native access policy: {message}"))
    })
}

fn check_policy_for_command(
    policy: &AccessPolicy,
    auth: &AuthContext,
    cmd: &Qail,
) -> Result<(), String> {
    let access_ctx = access_context_from_auth(auth);
    policy
        .check_command(&access_ctx, cmd)
        .map_err(|err| err.to_string())
}

fn scopes_from_claims(auth: &AuthContext) -> BTreeSet<String> {
    let mut scopes = BTreeSet::new();
    for key in ["scope", "scopes", "permissions"] {
        if let Some(value) = auth.claims.get(key) {
            collect_scope_value(value, &mut scopes);
        }
    }
    scopes
}

fn collect_scope_value(value: &serde_json::Value, scopes: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::String(raw) => {
            scopes.extend(
                raw.split(|ch: char| ch.is_ascii_whitespace() || ch == ',')
                    .map(str::trim)
                    .filter(|scope| !scope.is_empty())
                    .map(ToOwned::to_owned),
            );
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_scope_value(value, scopes);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use qail_core::access::{AccessDecision, AccessOperation, ColumnRule, TableAccessPolicy};
    use serde_json::json;

    use super::*;

    fn auth_with_claims(claims: HashMap<String, serde_json::Value>) -> AuthContext {
        AuthContext {
            user_id: "user-1".to_string(),
            role: "operator".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            claims,
        }
    }

    #[test]
    fn access_context_extracts_role_tenant_and_scope_claims() {
        let mut claims = HashMap::new();
        claims.insert(
            "scope".to_string(),
            json!("orders:read orders:write,profile:read"),
        );
        claims.insert(
            "permissions".to_string(),
            json!(["billing:read", ["reports:read"]]),
        );

        let ctx = access_context_from_auth(&auth_with_claims(claims));

        assert_eq!(ctx.subject_id.as_deref(), Some("user-1"));
        assert_eq!(ctx.tenant_id.as_deref(), Some("tenant-a"));
        assert!(ctx.roles.contains("operator"));
        assert!(ctx.scopes.contains("orders:read"));
        assert!(ctx.scopes.contains("orders:write"));
        assert!(ctx.scopes.contains("profile:read"));
        assert!(ctx.scopes.contains("billing:read"));
        assert!(ctx.scopes.contains("reports:read"));
    }

    #[test]
    fn access_policy_check_enforces_roles_scopes_and_columns() {
        let policy = AccessPolicy {
            default_decision: AccessDecision::Deny,
            tables: [(
                "orders".to_string(),
                TableAccessPolicy::new()
                    .allow_operations([AccessOperation::Read])
                    .read_columns(ColumnRule::only(["id", "status"]))
                    .require_any_role(["operator"])
                    .require_scopes(["orders:read"]),
            )]
            .into(),
        };
        let auth = auth_with_claims(HashMap::from([("scope".to_string(), json!("orders:read"))]));

        check_policy_for_command(
            &policy,
            &auth,
            &Qail::get("orders").columns(["id", "status"]),
        )
        .expect("matching access context should pass");

        let err = check_policy_for_command(
            &policy,
            &auth,
            &Qail::get("orders").columns(["id", "secret_note"]),
        )
        .expect_err("denied column should fail");
        assert!(err.contains("secret_note"));
    }
}
