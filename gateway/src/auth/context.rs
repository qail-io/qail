use super::AuthContext;
use std::collections::HashMap;

fn has_truthy_claim(claims: &HashMap<String, serde_json::Value>, key: &str) -> bool {
    let Some(value) = claims.get(key) else {
        return false;
    };
    match value {
        serde_json::Value::Bool(v) => *v,
        serde_json::Value::Number(v) => v.as_u64() == Some(1),
        serde_json::Value::String(v) => {
            let normalized = v.trim().to_ascii_lowercase();
            normalized == "true" || normalized == "1" || normalized == "yes"
        }
        _ => false,
    }
}

impl AuthContext {
    /// Create an unauthenticated anonymous context.
    pub fn anonymous() -> Self {
        Self {
            user_id: "anonymous".to_string(),
            role: "anonymous".to_string(),
            tenant_id: None,
            claims: HashMap::new(),
        }
    }

    /// Create a denied auth context for invalid credentials.
    ///
    /// Unlike `anonymous()`, this signals that the client *attempted*
    /// authentication but failed — downstream handlers / RLS should
    /// reject the request outright.
    pub fn denied() -> Self {
        Self {
            user_id: "denied".to_string(),
            role: "denied".to_string(),
            tenant_id: None,
            claims: HashMap::new(),
        }
    }

    /// Check whether the user holds the given role.
    pub fn has_role(&self, role: &str) -> bool {
        self.role == role
    }

    /// Returns `true` when the caller has an explicit platform-admin grant.
    ///
    /// This is intentionally fail-closed:
    /// - role must be `administrator` (case-insensitive)
    /// - tenant scope must be empty
    /// - JWT must explicitly assert platform admin via
    ///   `platform_admin=true`
    pub fn is_platform_admin(&self) -> bool {
        self.role.eq_ignore_ascii_case("administrator")
            && self.tenant_id.as_deref().is_none_or(str::is_empty)
            && self.has_platform_admin_claim()
    }

    /// Returns `true` when JWT claims explicitly grant platform-admin authority.
    pub fn has_platform_admin_claim(&self) -> bool {
        has_truthy_claim(&self.claims, "platform_admin")
    }

    /// Returns `true` when the caller can execute branch virtualization APIs.
    ///
    /// Branch metadata tables are global (not tenant-scoped), so this path is
    /// restricted to platform administrators only.
    pub fn can_use_branching(&self) -> bool {
        self.is_platform_admin()
    }

    /// Returns `true` when the caller can run EXPLAIN ANALYZE.
    ///
    /// Uses canonical platform-admin semantics only.
    pub fn can_run_explain_analyze(&self) -> bool {
        self.is_platform_admin()
    }

    /// Returns `true` if the context represents a real (non-anonymous) user.
    pub fn is_authenticated(&self) -> bool {
        !self.user_id.is_empty() && self.user_id != "anonymous" && self.user_id != "denied"
    }

    /// Returns `true` if the context represents a denied (invalid credentials) user.
    pub fn is_denied(&self) -> bool {
        self.role == "denied"
    }

    /// Extract JWT expiration (`exp`) Unix timestamp from claims when present.
    pub fn token_expiry_unix(&self) -> Option<i64> {
        let exp = self.claims.get("exp")?;
        match exp {
            serde_json::Value::Number(n) => n
                .as_i64()
                .or_else(|| n.as_u64().and_then(|v| i64::try_from(v).ok())),
            serde_json::Value::String(s) => s.trim().parse::<i64>().ok(),
            _ => None,
        }
    }

    /// Returns true when the JWT has expired according to `exp`.
    ///
    /// Contexts without `exp` are treated as non-expiring for compatibility
    /// (for example dev-mode header auth).
    pub fn is_token_expired_now(&self) -> bool {
        let Some(exp) = self.token_expiry_unix() else {
            return false;
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or(i64::MAX);
        now >= exp
    }

    /// Resolve tenant_id from the user→tenant cache when the JWT doesn't include it.
    ///
    /// Engine-style JWTs often only contain `user_id` and `role` — the tenant id
    /// must be looked up from the database. This method checks the startup-loaded
    /// cache and fills in `tenant_id` if missing.
    pub async fn enrich_with_tenant_map(
        &mut self,
        map: &tokio::sync::RwLock<std::collections::HashMap<String, String>>,
    ) {
        if self.tenant_id.is_none() && self.is_authenticated() {
            let guard = map.read().await;
            if let Some(tenant_id) = guard.get(&self.user_id) {
                self.tenant_id = Some(tenant_id.clone());
            }
        }
    }

    /// Convert gateway AuthContext to PgDriver's RlsContext for Postgres-native RLS.
    ///
    /// Mapping:
    /// - `tenant_id` → tenant scope
    /// - `claims["agent_id"]` → `agent_id`
    /// - explicit platform-admin claim + role `administrator` + empty tenant
    ///   scope → `is_super_admin`
    pub fn to_rls_context(&self) -> qail_core::rls::RlsContext {
        // Only platform-level administrators (no tenant scope) bypass RLS.
        // Tenant-scoped roles (including tenant-bound "administrator") use tenant filtering.
        let is_super_admin = self.is_platform_admin();

        // Audit log: super_admin activation is a high-privilege event
        if is_super_admin {
            tracing::warn!(
                user_id = %self.user_id,
                tenant_id = ?self.tenant_id,
                event = "super_admin_rls_bypass",
                "SUPER_ADMIN access activated — RLS bypass enabled"
            );
            let token = qail_core::rls::SuperAdminToken::for_auth("admin_rls_bypass");
            return qail_core::rls::RlsContext::super_admin(token);
        }

        let tenant_id = self.tenant_id.clone().unwrap_or_default();
        let agent_id = self
            .claims
            .get("agent_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if !agent_id.is_empty() && !tenant_id.is_empty() {
            qail_core::rls::RlsContext::tenant_and_agent(&tenant_id, &agent_id)
        } else if !agent_id.is_empty() {
            qail_core::rls::RlsContext::agent(&agent_id)
        } else {
            qail_core::rls::RlsContext::tenant(&tenant_id)
        }
    }
}
