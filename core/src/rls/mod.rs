//! Row-Level Security (RLS) Context for Multi-Tenant SaaS
//!
//! Provides a shared tenant context that all Qail drivers can use
//! for data isolation. Each driver implements isolation differently:
//!
//! - **qail-pg**: `set_config('app.current_tenant_id', ...)` session variables
//! - **qail-qdrant**: metadata filter `{ tenant_id: "..." }` on vector search
//!
//! # Example
//!
//! ```
//! use qail_core::rls::{RlsContext, SuperAdminToken};
//!
//! // Tenant context — scopes data to a single tenant
//! let ctx = RlsContext::tenant("550e8400-e29b-41d4-a716-446655440000");
//! assert_eq!(ctx.tenant_id, "550e8400-e29b-41d4-a716-446655440000");
//!
//! // Super admin — bypasses tenant isolation (requires named constructor)
//! let token = SuperAdminToken::for_system_process("example");
//! let admin = RlsContext::super_admin(token);
//! assert!(admin.bypasses_rls());
//! ```

/// Tenant context for multi-tenant data isolation.
///
/// Each driver uses this context to scope operations to a specific tenant:
/// - **PostgreSQL**: Sets `app.current_tenant_id` session variable
/// - **Qdrant**: Filters vector searches by tenant metadata
pub mod tenant;

/// An opaque token that authorizes RLS bypass.
///
/// Create via one of the named constructors:
/// - [`SuperAdminToken::for_system_process`] — cron, startup, reference-data
/// - [`SuperAdminToken::for_webhook`] — inbound callbacks
/// - [`SuperAdminToken::for_auth`] — login, register, token refresh
///
/// External code cannot fabricate this token — it has a private field
/// and no public field constructor.
///
/// # Usage
/// ```ignore
/// let token = SuperAdminToken::for_system_process("cron::cleanup");
/// let ctx = RlsContext::super_admin(token);
/// assert!(ctx.bypasses_rls());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuperAdminToken {
    _private: (),
}

impl SuperAdminToken {
    /// Issue a token for a system/background process.
    ///
    /// Use for cron jobs, startup introspection, and public reference-data
    /// endpoints (vessel types, locations, currency) that need cross-tenant
    /// reads but have no user session.
    ///
    /// The `_reason` parameter documents intent at the call site
    /// (e.g. `"cron::check_expired_holds"`). Drivers like `qail-pg`
    /// may log it via tracing.
    pub fn for_system_process(_reason: &str) -> Self {
        Self { _private: () }
    }

    /// Issue a token for an inbound webhook or gateway trigger.
    ///
    /// Use for Meta WhatsApp callbacks, Xendit payment callbacks,
    /// and gateway event triggers that are authenticated via shared
    /// secret (`X-Trigger-Secret`) rather than JWT.
    pub fn for_webhook(_source: &str) -> Self {
        Self { _private: () }
    }

    /// Issue a token for an authentication operation.
    ///
    /// Use for login, register, token refresh, and admin-claims
    /// resolution — operations that necessarily run before (or
    /// outside) a tenant scope is known.
    pub fn for_auth(_operation: &str) -> Self {
        Self { _private: () }
    }
}

/// RLS context carrying tenant identity for data isolation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RlsContext {
    /// The unified tenant ID — the primary identity for data isolation.
    /// Empty string means no tenant scope.
    pub tenant_id: String,

    /// Legacy: The operator (vendor) this context is scoped to.
    /// Set to the same value as tenant_id during the transition period.
    pub operator_id: String,

    /// Legacy: The agent (reseller) this context is scoped to.
    /// Set to the same value as tenant_id during the transition period.
    pub agent_id: String,

    /// When true, the current user is a platform super admin
    /// and should bypass tenant isolation.
    ///
    /// This field is private — external code must use `bypasses_rls()`.
    /// Only `super_admin(token)` can set this to true, and that requires
    /// a `SuperAdminToken` which emits an audit log on creation.
    is_super_admin: bool,
}

impl RlsContext {
    /// Create a context scoped to a specific tenant (the unified identity).
    pub fn tenant(tenant_id: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            operator_id: tenant_id.to_string(), // backward compat
            agent_id: tenant_id.to_string(),    // backward compat
            is_super_admin: false,
        }
    }

    /// Create a context scoped to a specific operator.
    /// Legacy — use `tenant()` for new code.
    pub fn operator(operator_id: &str) -> Self {
        Self {
            tenant_id: operator_id.to_string(),
            operator_id: operator_id.to_string(),
            agent_id: String::new(),
            is_super_admin: false,
        }
    }

    /// Create a context scoped to a specific agent (reseller).
    /// Legacy — use `tenant()` for new code.
    pub fn agent(agent_id: &str) -> Self {
        Self {
            tenant_id: agent_id.to_string(),
            operator_id: String::new(),
            agent_id: agent_id.to_string(),
            is_super_admin: false,
        }
    }

    /// Create a context scoped to both operator and agent.
    /// Legacy — use `tenant()` for new code.
    pub fn operator_and_agent(operator_id: &str, agent_id: &str) -> Self {
        Self {
            tenant_id: operator_id.to_string(), // primary identity
            operator_id: operator_id.to_string(),
            agent_id: agent_id.to_string(),
            is_super_admin: false,
        }
    }

    /// Create a super admin context that bypasses tenant isolation.
    ///
    /// Requires a `SuperAdminToken` — which can only be created via
    /// named constructors (`for_system_process`, `for_webhook`, `for_auth`).
    ///
    /// Uses nil UUID for all IDs to avoid `''::uuid` cast errors
    /// in PostgreSQL RLS policies (PostgreSQL doesn't short-circuit OR).
    pub fn super_admin(_token: SuperAdminToken) -> Self {
        let nil = "00000000-0000-0000-0000-000000000000".to_string();
        Self {
            tenant_id: nil.clone(),
            operator_id: nil.clone(),
            agent_id: nil,
            is_super_admin: true,
        }
    }

    /// Create an empty context (no tenant, no super admin).
    ///
    /// Used for system-level operations that must not operate within
    /// any tenant scope (startup introspection, migrations, health checks).
    pub fn empty() -> Self {
        Self {
            tenant_id: String::new(),
            operator_id: String::new(),
            agent_id: String::new(),
            is_super_admin: false,
        }
    }

    /// Returns true if this context has a tenant scope.
    pub fn has_tenant(&self) -> bool {
        !self.tenant_id.is_empty()
    }

    /// Returns true if this context has an operator scope.
    pub fn has_operator(&self) -> bool {
        !self.operator_id.is_empty()
    }

    /// Returns true if this context has an agent scope.
    pub fn has_agent(&self) -> bool {
        !self.agent_id.is_empty()
    }

    /// Returns true if this context bypasses tenant isolation.
    pub fn bypasses_rls(&self) -> bool {
        self.is_super_admin
    }
}

impl std::fmt::Display for RlsContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_super_admin {
            write!(f, "RlsContext(super_admin)")
        } else if !self.tenant_id.is_empty() {
            write!(f, "RlsContext(tenant={})", self.tenant_id)
        } else {
            write!(f, "RlsContext(none)")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_context() {
        let ctx = RlsContext::tenant("t-123");
        assert_eq!(ctx.tenant_id, "t-123");
        assert_eq!(ctx.operator_id, "t-123"); // backward compat
        assert_eq!(ctx.agent_id, "t-123"); // backward compat
        assert!(!ctx.bypasses_rls());
        assert!(ctx.has_tenant());
    }

    #[test]
    fn test_operator_context_sets_tenant() {
        let ctx = RlsContext::operator("op-123");
        assert_eq!(ctx.tenant_id, "op-123");
        assert_eq!(ctx.operator_id, "op-123");
        assert!(ctx.agent_id.is_empty());
        assert!(!ctx.bypasses_rls());
        assert!(ctx.has_operator());
    }

    #[test]
    fn test_agent_context_sets_tenant() {
        let ctx = RlsContext::agent("ag-456");
        assert_eq!(ctx.tenant_id, "ag-456");
        assert!(ctx.operator_id.is_empty());
        assert_eq!(ctx.agent_id, "ag-456");
        assert!(ctx.has_agent());
    }

    #[test]
    fn test_super_admin_via_named_constructors() {
        let token = SuperAdminToken::for_system_process("test");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());

        let token = SuperAdminToken::for_webhook("test");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());

        let token = SuperAdminToken::for_auth("test");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());
    }

    #[test]
    fn test_operator_and_agent() {
        let ctx = RlsContext::operator_and_agent("op-1", "ag-2");
        assert_eq!(ctx.tenant_id, "op-1"); // primary identity = operator
        assert!(ctx.has_operator());
        assert!(ctx.has_agent());
        assert!(!ctx.bypasses_rls());
    }

    #[test]
    fn test_display() {
        let token = SuperAdminToken::for_system_process("test_display");
        assert_eq!(
            RlsContext::super_admin(token).to_string(),
            "RlsContext(super_admin)"
        );
        assert_eq!(RlsContext::tenant("x").to_string(), "RlsContext(tenant=x)");
        assert_eq!(
            RlsContext::operator("x").to_string(),
            "RlsContext(tenant=x)"
        );
    }

    #[test]
    fn test_equality() {
        let a = RlsContext::tenant("t-1");
        let b = RlsContext::tenant("t-1");
        let c = RlsContext::tenant("t-2");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_empty_context() {
        let ctx = RlsContext::empty();
        assert!(!ctx.has_tenant());
        assert!(!ctx.has_operator());
        assert!(!ctx.has_agent());
        assert!(!ctx.bypasses_rls());
    }

    #[test]
    fn test_for_system_process() {
        let token = SuperAdminToken::for_system_process("cron::check_expired_holds");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());
    }

    #[test]
    fn test_for_webhook() {
        let token = SuperAdminToken::for_webhook("xendit_callback");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());
    }

    #[test]
    fn test_for_auth() {
        let token = SuperAdminToken::for_auth("login");
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());
    }

    #[test]
    fn test_all_constructors_produce_equal_tokens() {
        let a = SuperAdminToken::for_system_process("a");
        let b = SuperAdminToken::for_webhook("b");
        let c = SuperAdminToken::for_auth("c");
        // All tokens are structurally identical
        assert_eq!(a, b);
        assert_eq!(b, c);
    }
}
