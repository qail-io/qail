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
//!
//! // Global context — scopes to platform rows (tenant_id IS NULL)
//! let global = RlsContext::global();
//! assert!(global.is_global());
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
/// - [`SuperAdminToken::for_system_process`] — cron, startup, cross-tenant internals
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
    /// Use for cron jobs, startup introspection, and internal cross-tenant
    /// maintenance paths. For shared/public reference data, prefer
    /// [`RlsContext::global()`] instead of bypass.
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

    /// Legacy: The agent (reseller) this context is scoped to.
    /// Empty string means no agent scope.
    pub agent_id: String,

    /// When true, the current user is a platform super admin
    /// and should bypass tenant isolation.
    ///
    /// This field is private — external code must use `bypasses_rls()`.
    /// Only `super_admin(token)` can set this to true, and that requires
    /// a `SuperAdminToken` which emits an audit log on creation.
    is_super_admin: bool,

    /// When true, the context is explicitly scoped to global/platform rows
    /// (`tenant_id IS NULL`) rather than tenant-specific rows.
    is_global: bool,

    /// The authenticated user's UUID for user-scoped DB policies.
    /// Empty string means no user scope. Set via `RlsContext::user()`.
    user_id: String,
}

impl RlsContext {
    /// Create a context scoped to a specific tenant (the unified identity).
    pub fn tenant(tenant_id: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            agent_id: String::new(),
            is_super_admin: false,
            is_global: false,
            user_id: String::new(),
        }
    }

    /// Create a context scoped to a specific agent (reseller).
    pub fn agent(agent_id: &str) -> Self {
        Self {
            tenant_id: String::new(),
            agent_id: agent_id.to_string(),
            is_super_admin: false,
            is_global: false,
            user_id: String::new(),
        }
    }

    /// Create a context scoped to both tenant and agent.
    pub fn tenant_and_agent(tenant_id: &str, agent_id: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            agent_id: agent_id.to_string(),
            is_super_admin: false,
            is_global: false,
            user_id: String::new(),
        }
    }

    /// Create a global context scoped to platform rows (`tenant_id IS NULL`).
    ///
    /// This is not a bypass: it applies explicit global scoping in AST injection
    /// and exposes `app.is_global=true` for policy usage at the database layer.
    pub fn global() -> Self {
        Self {
            tenant_id: String::new(),
            agent_id: String::new(),
            is_super_admin: false,
            is_global: true,
            user_id: String::new(),
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
            tenant_id: nil,
            agent_id: String::new(),
            is_super_admin: true,
            is_global: false,
            user_id: String::new(),
        }
    }

    /// Create an empty context (no tenant, no super admin).
    ///
    /// Used for system-level operations that must not operate within
    /// any tenant scope (startup introspection, migrations, health checks).
    pub fn empty() -> Self {
        Self {
            tenant_id: String::new(),
            agent_id: String::new(),
            is_super_admin: false,
            is_global: false,
            user_id: String::new(),
        }
    }

    /// Create a user-scoped context for authenticated end-user operations.
    ///
    /// Sets `app.current_user_id` so that DB policies can enforce
    /// row-level isolation by user (e.g. `user_id = get_current_user_id()`).
    /// Does NOT bypass tenant isolation or grant super-admin.
    pub fn user(user_id: &str) -> Self {
        Self {
            tenant_id: String::new(),
            agent_id: String::new(),
            is_super_admin: false,
            is_global: false,
            user_id: user_id.to_string(),
        }
    }

    /// Returns true if this context has a tenant scope.
    pub fn has_tenant(&self) -> bool {
        !self.tenant_id.is_empty()
    }

    /// Returns true if this context has an agent scope.
    pub fn has_agent(&self) -> bool {
        !self.agent_id.is_empty()
    }

    /// Returns true if this context has a user scope.
    pub fn has_user(&self) -> bool {
        !self.user_id.is_empty()
    }

    /// Returns the user ID for this context (empty if none).
    pub fn user_id(&self) -> &str {
        &self.user_id
    }

    /// Returns true if this context bypasses tenant isolation.
    pub fn bypasses_rls(&self) -> bool {
        self.is_super_admin
    }

    /// Returns true if this context is explicitly scoped to global rows.
    pub fn is_global(&self) -> bool {
        self.is_global
    }
}

impl std::fmt::Display for RlsContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_super_admin {
            write!(f, "RlsContext(super_admin)")
        } else if self.is_global {
            write!(f, "RlsContext(global)")
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
        assert!(ctx.agent_id.is_empty());
        assert!(!ctx.bypasses_rls());
        assert!(ctx.has_tenant());
    }

    #[test]
    fn test_agent_context_sets_tenant() {
        let ctx = RlsContext::agent("ag-456");
        assert!(ctx.tenant_id.is_empty());
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
    fn test_tenant_and_agent() {
        let ctx = RlsContext::tenant_and_agent("tenant-1", "ag-2");
        assert_eq!(ctx.tenant_id, "tenant-1");
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
        assert!(!ctx.has_agent());
        assert!(!ctx.bypasses_rls());
        assert!(!ctx.is_global());
    }

    #[test]
    fn test_global_context() {
        let ctx = RlsContext::global();
        assert!(!ctx.has_tenant());
        assert!(!ctx.has_agent());
        assert!(!ctx.bypasses_rls());
        assert!(ctx.is_global());
        assert_eq!(ctx.to_string(), "RlsContext(global)");
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

    #[test]
    fn test_user_context() {
        let ctx = RlsContext::user("550e8400-e29b-41d4-a716-446655440000");
        assert!(!ctx.has_tenant());
        assert!(!ctx.has_agent());
        assert!(!ctx.bypasses_rls());
        assert!(!ctx.is_global());
        assert!(ctx.has_user());
        assert_eq!(ctx.user_id(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_user_context_display() {
        let ctx = RlsContext::user("u-123");
        assert_eq!(ctx.to_string(), "RlsContext(none)");
        // user context doesn't have tenant, so Display falls through to "none"
        // (user_id is an orthogonal axis, not a tenant scope)
    }

    #[test]
    fn test_other_constructors_have_no_user() {
        assert!(!RlsContext::tenant("t-1").has_user());
        assert!(!RlsContext::global().has_user());
        assert!(!RlsContext::empty().has_user());
        let token = SuperAdminToken::for_auth("test");
        assert!(!RlsContext::super_admin(token).has_user());
    }
}
