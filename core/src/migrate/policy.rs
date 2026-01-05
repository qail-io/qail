//! RLS Policy Definition (AST-native)
//!
//! Defines PostgreSQL Row-Level Security policies using typed AST
//! expressions — not raw SQL strings. QAIL speaks AST.
//!
//! # Example
//! ```
//! use qail_core::migrate::policy::{RlsPolicy, PolicyTarget};
//! use qail_core::ast::{Expr, BinaryOp, Value};
//!
//! // operator_id = current_setting('app.current_operator_id')::uuid
//! let tenant_check = Expr::Binary {
//!     left: Box::new(Expr::Named("operator_id".into())),
//!     op: BinaryOp::Eq,
//!     right: Box::new(Expr::Cast {
//!         expr: Box::new(Expr::FunctionCall {
//!             name: "current_setting".into(),
//!             args: vec![Expr::Literal(Value::String("app.current_operator_id".into()))],
//!             alias: None,
//!         }),
//!         target_type: "uuid".into(),
//!         alias: None,
//!     }),
//!     alias: None,
//! };
//!
//! let policy = RlsPolicy::create("orders_operator_isolation", "orders")
//!     .for_all()
//!     .using(tenant_check.clone())
//!     .with_check(tenant_check);
//! ```

use crate::ast::Expr;
use serde::{Serialize, Deserialize};

/// What the policy applies to (SELECT, INSERT, UPDATE, DELETE, or ALL).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyTarget {
    /// Applies to all operations.
    All,
    /// Applies to SELECT queries.
    Select,
    /// Applies to INSERT operations.
    Insert,
    /// Applies to UPDATE operations.
    Update,
    /// Applies to DELETE operations.
    Delete,
}

impl std::fmt::Display for PolicyTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyTarget::All => write!(f, "ALL"),
            PolicyTarget::Select => write!(f, "SELECT"),
            PolicyTarget::Insert => write!(f, "INSERT"),
            PolicyTarget::Update => write!(f, "UPDATE"),
            PolicyTarget::Delete => write!(f, "DELETE"),
        }
    }
}

/// Whether this is permissive (default) or restrictive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyPermissiveness {
    /// Rows matching ANY permissive policy are visible (OR).
    Permissive,
    /// Rows must also match ALL restrictive policies (AND).
    Restrictive,
}

impl std::fmt::Display for PolicyPermissiveness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolicyPermissiveness::Permissive => write!(f, "PERMISSIVE"),
            PolicyPermissiveness::Restrictive => write!(f, "RESTRICTIVE"),
        }
    }
}

/// AST-native RLS policy definition.
///
/// All expressions use typed `Expr` nodes — no raw SQL strings.
/// The transpiler converts these to `CREATE POLICY ... USING (...) WITH CHECK (...)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RlsPolicy {
    /// Policy name (e.g., "orders_operator_isolation")
    pub name: String,
    /// Table this policy applies to
    pub table: String,
    /// Target command(s): ALL, SELECT, INSERT, UPDATE, DELETE
    pub target: PolicyTarget,
    /// Permissive (default) or Restrictive
    pub permissiveness: PolicyPermissiveness,
    /// USING expression — controls which existing rows are visible.
    /// Applied to SELECT, UPDATE (read), DELETE.
    pub using: Option<Expr>,
    /// WITH CHECK expression — controls which new rows can be written.
    /// Applied to INSERT, UPDATE (write).
    pub with_check: Option<Expr>,
    /// Role this policy applies to (default: PUBLIC)
    pub role: Option<String>,
}

impl RlsPolicy {
    /// Create a new policy builder.
    ///
    /// ```
    /// use qail_core::migrate::policy::RlsPolicy;
    /// let policy = RlsPolicy::create("tenant_isolation", "orders");
    /// ```
    pub fn create(name: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            table: table.into(),
            target: PolicyTarget::All,
            permissiveness: PolicyPermissiveness::Permissive,
            using: None,
            with_check: None,
            role: None,
        }
    }

    /// Set policy target to ALL (SELECT + INSERT + UPDATE + DELETE).
    pub fn for_all(mut self) -> Self {
        self.target = PolicyTarget::All;
        self
    }

    /// Set policy target to SELECT only.
    pub fn for_select(mut self) -> Self {
        self.target = PolicyTarget::Select;
        self
    }

    /// Set policy target to INSERT only.
    pub fn for_insert(mut self) -> Self {
        self.target = PolicyTarget::Insert;
        self
    }

    /// Set policy target to UPDATE only.
    pub fn for_update(mut self) -> Self {
        self.target = PolicyTarget::Update;
        self
    }

    /// Set policy target to DELETE only.
    pub fn for_delete(mut self) -> Self {
        self.target = PolicyTarget::Delete;
        self
    }

    /// Make this policy restrictive (AND with other policies).
    pub fn restrictive(mut self) -> Self {
        self.permissiveness = PolicyPermissiveness::Restrictive;
        self
    }

    /// Set the USING expression (visibility filter for existing rows).
    /// This is an AST expression, not a raw SQL string.
    pub fn using(mut self, expr: Expr) -> Self {
        self.using = Some(expr);
        self
    }

    /// Set the WITH CHECK expression (write filter for new rows).
    /// This is an AST expression, not a raw SQL string.
    pub fn with_check(mut self, expr: Expr) -> Self {
        self.with_check = Some(expr);
        self
    }

    /// Restrict policy to a specific role.
    pub fn to_role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }
}

/// Helper: build the standard tenant isolation expression.
///
/// Generates: `column = current_setting('app.session_var')::cast_type`
///
/// This is the most common RLS pattern and deserves a first-class helper.
///
/// # Example
/// ```
/// use qail_core::migrate::policy::tenant_check;
///
/// let expr = tenant_check("operator_id", "app.current_operator_id", "uuid");
/// // Equivalent to: operator_id = current_setting('app.current_operator_id')::uuid
/// ```
pub fn tenant_check(
    column: impl Into<String>,
    session_var: impl Into<String>,
    cast_type: impl Into<String>,
) -> Expr {
    use crate::ast::{BinaryOp, Value};

    Expr::Binary {
        left: Box::new(Expr::Named(column.into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Cast {
            expr: Box::new(Expr::FunctionCall {
                name: "current_setting".into(),
                args: vec![Expr::Literal(Value::String(session_var.into()))],
                alias: None,
            }),
            target_type: cast_type.into(),
            alias: None,
        }),
        alias: None,
    }
}

/// Helper: build a boolean session variable check.
///
/// Generates: `current_setting('app.session_var')::boolean = true`
///
/// Used for super admin bypass policies.
///
/// # Example
/// ```
/// use qail_core::migrate::policy::session_bool_check;
///
/// let expr = session_bool_check("app.is_super_admin");
/// // Equivalent to: current_setting('app.is_super_admin')::boolean = true
/// ```
pub fn session_bool_check(session_var: impl Into<String>) -> Expr {
    use crate::ast::{BinaryOp, Value};

    Expr::Binary {
        left: Box::new(Expr::Cast {
            expr: Box::new(Expr::FunctionCall {
                name: "current_setting".into(),
                args: vec![Expr::Literal(Value::String(session_var.into()))],
                alias: None,
            }),
            target_type: "boolean".into(),
            alias: None,
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Literal(Value::Bool(true))),
        alias: None,
    }
}

/// Helper: combine two expressions with OR.
///
/// Useful for: `tenant_check OR super_admin_bypass`
pub fn or(left: Expr, right: Expr) -> Expr {
    use crate::ast::BinaryOp;

    Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::Or,
        right: Box::new(right),
        alias: None,
    }
}

/// Helper: combine two expressions with AND.
pub fn and(left: Expr, right: Expr) -> Expr {
    use crate::ast::BinaryOp;

    Expr::Binary {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
        alias: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::BinaryOp;

    #[test]
    fn test_policy_builder() {
        let policy = RlsPolicy::create("orders_isolation", "orders")
            .for_all()
            .using(tenant_check("operator_id", "app.current_operator_id", "uuid"))
            .with_check(tenant_check("operator_id", "app.current_operator_id", "uuid"));

        assert_eq!(policy.name, "orders_isolation");
        assert_eq!(policy.table, "orders");
        assert_eq!(policy.target, PolicyTarget::All);
        assert!(policy.using.is_some());
        assert!(policy.with_check.is_some());
    }

    #[test]
    fn test_policy_restrictive() {
        let policy = RlsPolicy::create("admin_only", "secrets")
            .for_select()
            .restrictive()
            .to_role("app_user");

        assert_eq!(policy.target, PolicyTarget::Select);
        assert_eq!(policy.permissiveness, PolicyPermissiveness::Restrictive);
        assert_eq!(policy.role.as_deref(), Some("app_user"));
    }

    #[test]
    fn test_tenant_check_helper() {
        let expr = tenant_check("operator_id", "app.current_operator_id", "uuid");

        match &expr {
            Expr::Binary { left, op, right, .. } => {
                assert_eq!(*op, BinaryOp::Eq);
                match left.as_ref() {
                    Expr::Named(n) => assert_eq!(n, "operator_id"),
                    _ => panic!("Expected Named"),
                }
                match right.as_ref() {
                    Expr::Cast { expr, target_type, .. } => {
                        assert_eq!(target_type, "uuid");
                        match expr.as_ref() {
                            Expr::FunctionCall { name, args, .. } => {
                                assert_eq!(name, "current_setting");
                                assert_eq!(args.len(), 1);
                            }
                            _ => panic!("Expected FunctionCall"),
                        }
                    }
                    _ => panic!("Expected Cast"),
                }
            }
            _ => panic!("Expected Binary"),
        }
    }

    #[test]
    fn test_super_admin_bypass() {
        let expr = or(
            tenant_check("operator_id", "app.current_operator_id", "uuid"),
            session_bool_check("app.is_super_admin"),
        );

        match &expr {
            Expr::Binary { op, .. } => assert_eq!(*op, BinaryOp::Or),
            _ => panic!("Expected Binary OR"),
        }
    }

    #[test]
    fn test_and_combinator() {
        let expr = and(
            tenant_check("operator_id", "app.current_operator_id", "uuid"),
            tenant_check("agent_id", "app.current_agent_id", "uuid"),
        );

        match &expr {
            Expr::Binary { op, .. } => assert_eq!(*op, BinaryOp::And),
            _ => panic!("Expected Binary AND"),
        }
    }

    #[test]
    fn test_policy_target_display() {
        assert_eq!(PolicyTarget::All.to_string(), "ALL");
        assert_eq!(PolicyTarget::Select.to_string(), "SELECT");
        assert_eq!(PolicyTarget::Insert.to_string(), "INSERT");
        assert_eq!(PolicyTarget::Update.to_string(), "UPDATE");
        assert_eq!(PolicyTarget::Delete.to_string(), "DELETE");
    }
}
