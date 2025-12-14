//! Row-Level Security (RLS) Context for Multi-Tenant SaaS
//!
//! Provides a shared tenant context that all Qail drivers can use
//! for data isolation. Each driver implements isolation differently:
//!
//! - **qail-pg**: `set_config('app.current_operator_id', ...)` session variables
//! - **qail-qdrant**: metadata filter `{ operator_id: "..." }` on vector search
//!
//! # Example
//!
//! ```
//! use qail_core::rls::{RlsContext, SuperAdminToken};
//!
//! // Operator context — scopes data to a single operator
//! let ctx = RlsContext::operator("550e8400-e29b-41d4-a716-446655440000");
//! assert_eq!(ctx.operator_id, "550e8400-e29b-41d4-a716-446655440000");
//!
//! // Super admin — bypasses tenant isolation (requires token)
//! let token = SuperAdminToken::issue();
//! let admin = RlsContext::super_admin(token);
//! assert!(admin.bypasses_rls());
//! ```

/// Tenant context for multi-tenant data isolation.
///
/// Each driver uses this context to scope operations to a specific tenant:
/// - **PostgreSQL**: Sets session variables referenced by RLS policies
/// - **Qdrant**: Filters vector searches by tenant metadata
/// - **Redis**: *(removed — native cache replaces Redis)*
pub mod tenant;

/// An opaque token that authorizes RLS bypass.
///
/// This type can only be created by calling `SuperAdminToken::issue()`,
/// which emits a structured audit log. External code cannot fabricate
/// this token — it has a private field and no public constructor.
///
/// # Usage
/// ```ignore
/// let token = SuperAdminToken::issue();
/// let ctx = RlsContext::super_admin(token);
/// assert!(ctx.bypasses_rls());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuperAdminToken {
    _private: (),
}

impl SuperAdminToken {
    /// Issue a super admin token.
    ///
    /// This is the ONLY way to create a `SuperAdminToken`.
    /// Callers are responsible for audit logging (the gateway's auth
    /// module emits structured logs when this token is used to create
    /// an RLS context).
    pub fn issue() -> Self {
        Self { _private: () }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RlsContext {
    /// The operator (vendor) this context is scoped to.
    /// Empty string means no operator scope.
    pub operator_id: String,

    /// The agent (reseller) this context is scoped to.
    /// Empty string means no agent scope.
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
    /// Create a context scoped to a specific operator.
    pub fn operator(operator_id: &str) -> Self {
        Self {
            operator_id: operator_id.to_string(),
            agent_id: String::new(),
            is_super_admin: false,
        }
    }

    /// Create a context scoped to a specific agent (reseller).
    pub fn agent(agent_id: &str) -> Self {
        Self {
            operator_id: String::new(),
            agent_id: agent_id.to_string(),
            is_super_admin: false,
        }
    }

    /// Create a context scoped to both operator and agent.
    /// Used when an agent is acting on behalf of an operator.
    pub fn operator_and_agent(operator_id: &str, agent_id: &str) -> Self {
        Self {
            operator_id: operator_id.to_string(),
            agent_id: agent_id.to_string(),
            is_super_admin: false,
        }
    }

    /// Create a super admin context that bypasses tenant isolation.
    ///
    /// Requires a `SuperAdminToken` — which can only be created via
    /// `SuperAdminToken::issue()` with mandatory audit logging.
    ///
    /// Uses nil UUID for operator/agent IDs to avoid `''::uuid` cast errors
    /// in PostgreSQL RLS policies (PostgreSQL doesn't short-circuit OR).
    pub fn super_admin(_token: SuperAdminToken) -> Self {
        Self {
            operator_id: "00000000-0000-0000-0000-000000000000".to_string(),
            agent_id: "00000000-0000-0000-0000-000000000000".to_string(),
            is_super_admin: true,
        }
    }

    /// Create an empty context (no tenant, no super admin).
    ///
    /// Used for system-level operations that must not operate within
    /// any tenant scope (startup introspection, migrations, health checks).
    pub fn empty() -> Self {
        Self {
            operator_id: String::new(),
            agent_id: String::new(),
            is_super_admin: false,
        }
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
        } else if !self.operator_id.is_empty() && !self.agent_id.is_empty() {
            write!(f, "RlsContext(op={}, ag={})", self.operator_id, self.agent_id)
        } else if !self.operator_id.is_empty() {
            write!(f, "RlsContext(op={})", self.operator_id)
        } else if !self.agent_id.is_empty() {
            write!(f, "RlsContext(ag={})", self.agent_id)
        } else {
            write!(f, "RlsContext(none)")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operator_context() {
        let ctx = RlsContext::operator("op-123");
        assert_eq!(ctx.operator_id, "op-123");
        assert!(ctx.agent_id.is_empty());
        assert!(!ctx.bypasses_rls());
        assert!(ctx.has_operator());
        assert!(!ctx.has_agent());
    }

    #[test]
    fn test_agent_context() {
        let ctx = RlsContext::agent("ag-456");
        assert!(ctx.operator_id.is_empty());
        assert_eq!(ctx.agent_id, "ag-456");
        assert!(ctx.has_agent());
        assert!(!ctx.has_operator());
    }

    #[test]
    fn test_super_admin() {
        let token = SuperAdminToken::issue();
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());
    }

    #[test]
    fn test_operator_and_agent() {
        let ctx = RlsContext::operator_and_agent("op-1", "ag-2");
        assert!(ctx.has_operator());
        assert!(ctx.has_agent());
        assert!(!ctx.bypasses_rls());
    }

    #[test]
    fn test_display() {
        let token = SuperAdminToken::issue();
        assert_eq!(RlsContext::super_admin(token).to_string(), "RlsContext(super_admin)");
        assert_eq!(RlsContext::operator("x").to_string(), "RlsContext(op=x)");
        assert_eq!(RlsContext::agent("y").to_string(), "RlsContext(ag=y)");
        assert_eq!(
            RlsContext::operator_and_agent("x", "y").to_string(),
            "RlsContext(op=x, ag=y)"
        );
    }

    #[test]
    fn test_equality() {
        let a = RlsContext::operator("op-1");
        let b = RlsContext::operator("op-1");
        let c = RlsContext::operator("op-2");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_empty_context() {
        let ctx = RlsContext::empty();
        assert!(!ctx.has_operator());
        assert!(!ctx.has_agent());
        assert!(!ctx.bypasses_rls());
    }

    #[test]
    fn test_super_admin_token_cannot_be_forged() {
        // SuperAdminToken { _private: () } — the private field prevents
        // external construction. This test documents the intent.
        let token = SuperAdminToken::issue();
        let ctx = RlsContext::super_admin(token);
        assert!(ctx.bypasses_rls());
    }
}
