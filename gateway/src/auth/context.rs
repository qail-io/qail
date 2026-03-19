use super::AuthContext;
use std::collections::HashMap;

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

    /// Returns `true` if the context represents a real (non-anonymous) user.
    pub fn is_authenticated(&self) -> bool {
        !self.user_id.is_empty() && self.user_id != "anonymous" && self.user_id != "denied"
    }

    /// Returns `true` if the context represents a denied (invalid credentials) user.
    pub fn is_denied(&self) -> bool {
        self.role == "denied"
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

    /// Legacy alias for `enrich_with_tenant_map`.
    pub async fn enrich_with_operator_map(
        &mut self,
        map: &tokio::sync::RwLock<std::collections::HashMap<String, String>>,
    ) {
        self.enrich_with_tenant_map(map).await;
    }

    /// Convert gateway AuthContext to PgDriver's RlsContext for Postgres-native RLS.
    ///
    /// Mapping:
    /// - `tenant_id` → tenant scope (`operator_id` kept internally for legacy compat)
    /// - `claims["agent_id"]` → `agent_id`
    /// - `role == "super_admin"` → `is_super_admin`
    pub fn to_rls_context(&self) -> qail_core::rls::RlsContext {
        // Only the platform-level "administrator" role bypasses RLS.
        // Tenant-scoped roles (operator, super_admin) use tenant filtering.
        let is_super_admin = matches!(self.role.as_str(), "administrator" | "Administrator");

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
