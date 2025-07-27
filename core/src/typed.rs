//! Type-safe table and column types for compile-time validation.
//!
//! This module provides traits and types that enable compile-time type checking
//! for table/column references and value types.
//!
//! # Example (from generated code)
//! ```ignore
//! use qail_core::prelude::*;
//! use schema::users;
//!
//! Qail::get(users::Users)
//!     .typed_eq(users::age(), 25)  // Compile-time type check
//! ```

use std::marker::PhantomData;

/// Trait for type-safe table references.
/// 
/// Generated table structs implement this trait.
pub trait Table {
    /// The table name as a static string.
    fn table_name() -> &'static str;
    
    /// Get table name (instance method for convenience)
    fn name(&self) -> &'static str {
        Self::table_name()
    }
}

// =============================================================================
// Compile-Time Policy Safety (Scenario C)
// =============================================================================

/// Marker trait for data access policies.
pub trait Policy {}

/// Public data, accessible by default.
#[derive(Debug, Clone, Copy)]
pub struct Public;
impl Policy for Public {}

/// Protected data, requires `AdminCap` witness.
#[derive(Debug, Clone, Copy)]
pub struct Protected;
impl Policy for Protected {}

/// Restricted data, requires `SystemCap` witness (highest privilege).
#[derive(Debug, Clone, Copy)]
pub struct Restricted;
impl Policy for Restricted {}

// =============================================================================
// Capability Witnesses (Compile-Time Authorization)
// =============================================================================

/// Marker trait for capability witnesses.
/// A capability witness proves authorization to access protected data.
/// 
/// **Root of Trust:** Capabilities have private constructors and can only
/// be created via `CapabilityProvider::mint_*()` methods. This ensures
/// that only your authentication layer (middleware, AuthService) can
/// issue capabilities after verifying credentials.
pub trait Capability: 'static + Clone {}

/// No capability - cannot access protected data.
/// This is the only capability that can be freely constructed.
#[derive(Debug, Clone, Copy)]
pub struct NoCap;
impl Capability for NoCap {}

/// Admin capability - can access Protected data.
/// 
/// **Sealed:** Cannot be constructed directly. Must be obtained from
/// `CapabilityProvider::mint_admin()` in your authentication layer.
/// 
/// # Example
/// ```ignore
/// // In your auth middleware:
/// impl AuthService {
///     pub fn verify_admin(&self, token: &str) -> Result<AdminCap, AuthError> {
///         let claims = self.verify_jwt(token)?;
///         if claims.role == "admin" {
///             Ok(CapabilityProvider::mint_admin())
///         } else {
///             Err(AuthError::Forbidden)
///         }
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct AdminCap(());  // Private field = cannot be constructed outside this module
impl Capability for AdminCap {}

/// System capability - can access Restricted data (highest privilege).
/// 
/// **Sealed:** Cannot be constructed directly. Must be obtained from
/// `CapabilityProvider::mint_system()` in your authentication layer.
#[derive(Debug, Clone, Copy)]
pub struct SystemCap(());  // Private field = cannot be constructed outside this module
impl Capability for SystemCap {}

/// Provider for minting sealed capabilities.
/// 
/// Only use this in your authentication layer after verifying credentials.
/// 
/// # Security
/// This is the **Root of Trust** for your data governance system.
/// Place this in a single, auditable location (e.g., auth middleware).
pub struct CapabilityProvider;

impl CapabilityProvider {
    /// Mint an AdminCap after verifying admin privileges.
    /// 
    /// **Warning:** Only call this after JWT/session verification confirms admin role.
    #[inline]
    pub fn mint_admin() -> AdminCap {
        AdminCap(())
    }
    
    /// Mint a SystemCap after verifying system privileges.
    /// 
    /// **Warning:** Only call this for internal system operations or superadmin roles.
    #[inline]
    pub fn mint_system() -> SystemCap {
        SystemCap(())
    }
}

/// Trait for compile-time policy enforcement.
/// 
/// `P: PolicyAllowedBy<C>` means "Policy P can be accessed with Capability C".
pub trait PolicyAllowedBy<C: Capability> {}

// Public is always allowed (by any capability)
impl<C: Capability> PolicyAllowedBy<C> for Public {}

// Protected requires AdminCap or SystemCap
impl PolicyAllowedBy<AdminCap> for Protected {}
impl PolicyAllowedBy<SystemCap> for Protected {}

// Restricted requires SystemCap only
impl PolicyAllowedBy<SystemCap> for Restricted {}

/// A typed column reference with compile-time type AND policy information.
/// 
/// - `T`: Rust type (e.g., `i32`, `String`)
/// - `P`: Access Policy (default: `Public`)
#[derive(Debug, Clone, Copy)]
pub struct TypedColumn<T, P: Policy = Public> {
    table: &'static str,
    name: &'static str,
    _phantom: PhantomData<(T, P)>,
}

impl<T, P: Policy> TypedColumn<T, P> {
    /// Create a new typed column.
    pub const fn new(table: &'static str, name: &'static str) -> Self {
        Self {
            table,
            name,
            _phantom: PhantomData,
        }
    }
    
    /// Get the table name.
    pub const fn table(&self) -> &'static str {
        self.table
    }
    
    /// Get the column name.
    pub const fn name(&self) -> &'static str {
        self.name
    }
    
    /// Get qualified name (table.column).
    pub fn qualified(&self) -> String {
        format!("{}.{}", self.table, self.name)
    }

    /// Cast visibility (unsafe, use with caution)
    pub const fn cast_policy<NewP: Policy>(self) -> TypedColumn<T, NewP> {
        TypedColumn {
            table: self.table,
            name: self.name,
            _phantom: PhantomData,
        }
    }
    
    /// Unlock a protected column with a capability witness.
    /// 
    /// This allows accessing protected data by proving authorization:
    /// ```ignore
    /// // Protected column requires AdminCap to unlock
    /// let unlocked = users::password_hash.unlock(&AdminCap);
    /// query.column(unlocked)  // Now compiles!
    /// ```
    /// 
    /// Compile-time check ensures capability is sufficient:
    /// - `Public` columns: No unlock needed (but allowed)
    /// - `Protected` columns: Require `AdminCap` or `SystemCap`
    /// - `Restricted` columns: Require `SystemCap`
    pub fn unlock<C: Capability>(self, _witness: &C) -> TypedColumn<T, Public> 
    where 
        P: PolicyAllowedBy<C>
    {
        TypedColumn {
            table: self.table,
            name: self.name,
            _phantom: PhantomData,
        }
    }
}

/// Allow TypedColumn to be used where &str is expected.
impl<T, P: Policy> AsRef<str> for TypedColumn<T, P> {
    fn as_ref(&self) -> &str {
        self.name
    }
}

/// Allow TypedColumn to be converted to String.
impl<T, P: Policy> From<TypedColumn<T, P>> for String {
    fn from(col: TypedColumn<T, P>) -> String {
        col.name.to_string()
    }
}

/// Allow &TypedColumn to be converted to String.
impl<T, P: Policy> From<&TypedColumn<T, P>> for String {
    fn from(col: &TypedColumn<T, P>) -> String {
        col.name.to_string()
    }
}

/// Trait for types that can be used as column references.
pub trait IntoColumn {
    fn column_name(&self) -> &str;
}

impl IntoColumn for &str {
    fn column_name(&self) -> &str {
        self
    }
}

impl IntoColumn for String {
    fn column_name(&self) -> &str {
        self
    }
}

impl<T, P: Policy> IntoColumn for TypedColumn<T, P> {
    fn column_name(&self) -> &str {
        self.name
    }
}

impl<T, P: Policy> IntoColumn for &TypedColumn<T, P> {
    fn column_name(&self) -> &str {
        self.name
    }
}

// =============================================================================
// Compile-Time Relationship Safety (Scenario B)
// =============================================================================

/// Trait for compile-time relationship checking between tables.
/// 
/// When codegen finds `ref:` annotations in schema.qail, it generates:
/// ```ignore
/// impl RelatedTo<posts::Posts> for users::Users {
///     fn join_columns() -> (&'static str, &'static str) {
///         ("id", "user_id")  // users.id = posts.user_id
///     }
/// }
/// ```
/// 
/// This enables compile-time checked joins:
/// ```ignore
/// // Compiles ✓
/// Qail::get(users::table).join_related(posts::table)
/// 
/// // Compile ERROR: "Users: RelatedTo<Products> is not satisfied"
/// Qail::get(users::table).join_related(products::table)
/// ```
pub trait RelatedTo<Target: Table> {
    /// Returns (self_column, target_column) for the join condition.
    fn join_columns() -> (&'static str, &'static str);
}

/// Marker trait for value types that match a column type.
pub trait ColumnValue<C> {}

// Implement ColumnValue for matching types
impl ColumnValue<i64> for i64 {}
impl ColumnValue<i64> for i32 {}
impl ColumnValue<i64> for &i64 {}
impl ColumnValue<i32> for i32 {}
impl ColumnValue<i32> for &i32 {}

impl ColumnValue<f64> for f64 {}
impl ColumnValue<f64> for f32 {}
impl ColumnValue<f64> for &f64 {}

impl ColumnValue<String> for String {}
impl ColumnValue<String> for &str {}
impl ColumnValue<String> for &String {}

impl ColumnValue<bool> for bool {}
impl ColumnValue<bool> for &bool {}

impl ColumnValue<uuid::Uuid> for uuid::Uuid {}
impl ColumnValue<uuid::Uuid> for &uuid::Uuid {}

// JSON accepts many types
impl<T> ColumnValue<serde_json::Value> for T {}

impl ColumnValue<chrono::DateTime<chrono::Utc>> for chrono::DateTime<chrono::Utc> {}
impl ColumnValue<chrono::DateTime<chrono::Utc>> for &str {} // String dates
impl ColumnValue<chrono::DateTime<chrono::Utc>> for String {}

// =============================================================================
// CapQuery: Capability-Aware Query Builder
// =============================================================================

use crate::ast::Qail;

/// A capability-aware query wrapper.
/// 
/// This wraps a `Qail` query with a capability level, enabling compile-time
/// enforcement of data access policies.
/// 
/// # Example
/// ```ignore
/// use qail_core::typed::{CapQuery, AdminCap};
/// 
/// // Create a capability-aware query
/// let query = CapQuery::new(Qail::get("users"))
///     .with_cap(&AdminCap)              // Upgrade to AdminCap
///     .column_protected(users::password_hash)  // Now allowed!
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct CapQuery<C: Capability = NoCap> {
    inner: Qail,
    _cap: PhantomData<C>,
}

impl CapQuery<NoCap> {
    /// Create a new capability-aware query from a Qail instance.
    pub fn new(qail: Qail) -> Self {
        Self {
            inner: qail,
            _cap: PhantomData,
        }
    }
}

impl<C: Capability> CapQuery<C> {
    /// Upgrade capability level with a witness.
    /// 
    /// This enables accessing protected columns:
    /// ```ignore
    /// let query = CapQuery::new(Qail::get("users"))
    ///     .with_cap(&AdminCap)  // Now can access Protected columns
    ///     .column_protected(users::password_hash);
    /// ```
    pub fn with_cap<NewC: Capability>(self, _witness: &NewC) -> CapQuery<NewC> {
        CapQuery {
            inner: self.inner,
            _cap: PhantomData,
        }
    }
    
    /// Add a column that requires policy check.
    /// 
    /// Only compiles if the capability is sufficient for the column's policy.
    pub fn column_protected<T, P: Policy + PolicyAllowedBy<C>>(
        mut self, 
        col: TypedColumn<T, P>
    ) -> Self {
        use crate::ast::Expr;
        self.inner.columns.push(Expr::Named(col.name().to_string()));
        self
    }
    
    /// Add a public column (always allowed).
    pub fn column<T>(mut self, col: TypedColumn<T, Public>) -> Self {
        use crate::ast::Expr;
        self.inner.columns.push(Expr::Named(col.name().to_string()));
        self
    }
    
    /// Finish building and return the inner Qail.
    pub fn build(self) -> Qail {
        self.inner
    }
    
    /// Get a reference to the inner Qail.
    pub fn inner(&self) -> &Qail {
        &self.inner
    }
}

/// Extension trait to add `with_cap` to Qail.
pub trait WithCap {
    /// Start building a capability-aware query.
    fn with_cap<C: Capability>(self, cap: &C) -> CapQuery<C>;
}

impl WithCap for Qail {
    fn with_cap<C: Capability>(self, _cap: &C) -> CapQuery<C> {
        CapQuery {
            inner: self,
            _cap: PhantomData,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    struct TestTable;
    impl Table for TestTable {
        fn table_name() -> &'static str { "test_table" }
    }
    impl From<TestTable> for String {
        fn from(_: TestTable) -> String { "test_table".to_string() }
    }
    
    #[test]
    fn test_table_into_string() {
        let name: String = TestTable.into();
        assert_eq!(name, "test_table");
    }
    
    #[test]
    fn test_typed_column() {
        let col: TypedColumn<i64> = TypedColumn::new("users", "age");
        assert_eq!(col.name(), "age");
        assert_eq!(col.table(), "users");
    }
    
    #[test]
    fn test_unlock_protected_with_admin_cap() {
        // Protected column
        let password: TypedColumn<String, Protected> = TypedColumn::new("users", "password_hash");
        
        // Can unlock with AdminCap (minted via CapabilityProvider)
        let admin_cap = CapabilityProvider::mint_admin();
        let unlocked = password.unlock(&admin_cap);
        assert_eq!(unlocked.name(), "password_hash");
        
        // Verify it's now Public
        let _public: TypedColumn<String, Public> = unlocked;
    }
    
    #[test]
    fn test_unlock_restricted_with_system_cap() {
        // Restricted column
        let audit: TypedColumn<String, Restricted> = TypedColumn::new("audit", "raw_data");
        
        // Can unlock with SystemCap (minted via CapabilityProvider)
        let system_cap = CapabilityProvider::mint_system();
        let unlocked = audit.unlock(&system_cap);
        assert_eq!(unlocked.name(), "raw_data");
    }
    
    #[test]
    fn test_public_always_allowed() {
        // Public column doesn't need unlock but can accept any cap
        let email: TypedColumn<String, Public> = TypedColumn::new("users", "email");
        
        let _with_no_cap = email.clone().unlock(&NoCap);
        let _with_admin = email.clone().unlock(&CapabilityProvider::mint_admin());
        let _with_system = email.unlock(&CapabilityProvider::mint_system());
    }
    
    #[test]
    fn test_cap_query_builder() {
        use crate::transpiler::ToSql;
        
        // Simulated typed columns
        let email: TypedColumn<String, Public> = TypedColumn::new("users", "email");
        let password: TypedColumn<String, Protected> = TypedColumn::new("users", "password_hash");
        
        // Build query with AdminCap (minted by auth layer)
        let admin_cap = CapabilityProvider::mint_admin();
        let query = Qail::get("users")
            .with_cap(&admin_cap)
            .column(email)
            .column_protected(password)
            .build();
        
        let sql = query.to_sql();
        assert!(sql.contains("email"), "Should have email column");
        assert!(sql.contains("password_hash"), "Should have password_hash column");
    }
}


