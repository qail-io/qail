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
    /// Table this column belongs to.
    table: &'static str,
    /// Column name.
    name: &'static str,
    /// PhantomData for type and policy params.
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
    /// Get the column name as a string slice.
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

// =============================================================================
// TypedQail: Table-Typed Query Builder (Compile-Time Relation Safety)
// =============================================================================

/// A table-typed query wrapper that carries the source table type `T`.
/// 
/// This enables compile-time relationship checking via `RelatedTo<Target>`
/// trait bounds, preventing invalid joins at the type level.
/// 
/// # Example
/// ```ignore
/// use schema::{users, posts, products};
/// 
/// // Compiles ✓ — users RelatedTo posts (via ref:users.id)
/// Qail::typed(users::table).join_related(posts::table).build()
/// 
/// // Compile ERROR — "Users: RelatedTo<Products> is not satisfied"
/// Qail::typed(users::table).join_related(products::table).build()
/// ```
#[derive(Debug, Clone)]
pub struct TypedQail<T: Table> {
    inner: Qail,
    _table: PhantomData<T>,
}

impl Qail {
    /// Create a typed query builder that carries the table type.
    /// 
    /// This enables `join_related()` with compile-time relationship checking.
    /// 
    /// ```ignore
    /// Qail::typed(users::table)
    ///     .join_related(posts::table)     // Compiles ✓
    ///     .typed_eq(users::age(), 25)
    ///     .build()
    /// ```
    pub fn typed<T: Table + Into<String>>(table: T) -> TypedQail<T> {
        TypedQail {
            inner: Qail::get(table),
            _table: PhantomData,
        }
    }
}

impl<T: Table> TypedQail<T> {
    /// Join a related table with compile-time relationship checking.
    /// 
    /// Only compiles if `T: RelatedTo<U>` is satisfied.
    /// The `RelatedTo` impls are generated from `ref:` annotations in schema.qail.
    /// 
    /// ```ignore
    /// // posts has: user_id UUID ref:users.id
    /// // Generated: impl RelatedTo<posts::Posts> for users::Users
    /// 
    /// Qail::typed(users::table)
    ///     .join_related(posts::table)   // Compiles ✓
    ///     .build()
    /// ```
    pub fn join_related<U: Table>(mut self, _target: U) -> Self
    where
        T: RelatedTo<U>,
    {
        let (from_col, to_col) = T::join_columns();
        self.inner = self.inner.left_join(U::table_name(), from_col, to_col);
        self
    }
    
    /// Add a typed column to the query.
    pub fn typed_column<C>(mut self, col: TypedColumn<C>) -> Self {
        use crate::ast::Expr;
        self.inner.columns.push(Expr::Named(col.name().to_string()));
        self
    }
    
    /// Add multiple typed columns to the query.
    pub fn typed_columns<C>(mut self, cols: impl IntoIterator<Item = TypedColumn<C>>) -> Self {
        use crate::ast::Expr;
        for col in cols {
            self.inner.columns.push(Expr::Named(col.name().to_string()));
        }
        self
    }
    
    /// Type-safe equality filter.
    ///
    /// # Arguments
    ///
    /// * `col` — Typed column descriptor.
    /// * `value` — Value whose type must match the column's type marker.
    pub fn typed_eq<C, V>(mut self, col: TypedColumn<C>, value: V) -> Self
    where
        V: Into<crate::ast::Value> + ColumnValue<C>,
    {
        self.inner = self.inner.typed_eq(col, value);
        self
    }
    
    /// Type-safe filter with custom operator.
    ///
    /// # Arguments
    ///
    /// * `col` — Typed column descriptor.
    /// * `op` — Comparison operator.
    /// * `value` — Value whose type must match the column's type marker.
    pub fn typed_filter<C, V>(mut self, col: TypedColumn<C>, op: crate::ast::Operator, value: V) -> Self
    where
        V: Into<crate::ast::Value> + ColumnValue<C>,
    {
        self.inner = self.inner.typed_filter(col, op, value);
        self
    }
    
    /// Add a string-based filter (untyped).
    ///
    /// # Arguments
    ///
    /// * `column` — Column name.
    /// * `op` — Comparison operator.
    /// * `value` — Filter value.
    pub fn filter(mut self, column: impl AsRef<str>, op: crate::ast::Operator, value: impl Into<crate::ast::Value>) -> Self {
        self.inner = self.inner.filter(column, op, value);
        self
    }
    
    /// Add a string-based column (untyped).
    pub fn column(mut self, name: impl AsRef<str>) -> Self {
        self.inner = self.inner.column(name);
        self
    }

    /// Set limit.
    pub fn limit(mut self, n: i64) -> Self {
        self.inner = self.inner.limit(n);
        self
    }
    
    /// Set offset.
    pub fn offset(mut self, n: i64) -> Self {
        self.inner = self.inner.offset(n);
        self
    }
    
    /// Add ordering.
    pub fn order_by(mut self, column: impl AsRef<str>, order: crate::ast::SortOrder) -> Self {
        self.inner = self.inner.order_by(column, order);
        self
    }
    
    /// Upgrade to capability-aware query.
    pub fn with_cap<C: Capability>(self, cap: &C) -> CapQuery<C> {
        self.inner.with_cap(cap)
    }
    
    /// Apply RLS context (tenant scoping) — available on all typed queries.
    /// For tables that `impl RequiresRls`, prefer the dedicated `.with_rls()` → `RlsQuery<T>` path.
    pub fn rls(mut self, ctx: &crate::rls::RlsContext) -> Self {
        self.inner = self.inner.with_rls(ctx);
        self
    }
    
    /// Get a reference to the inner Qail.
    pub fn inner(&self) -> &Qail {
        &self.inner
    }
}

// =============================================================================
// DirectBuild: Non-RLS tables can .build() directly
// =============================================================================

/// Marker trait for tables that do NOT require RLS.
/// 
/// Tables without `operator_id` get this trait from codegen,
/// allowing `TypedQail<T>.build()` directly.
pub trait DirectBuild: Table {}

impl<T: Table + DirectBuild> TypedQail<T> {
    /// Finish building and return the inner Qail.
    /// 
    /// Only available for tables that do NOT require RLS.
    /// RLS-protected tables must go through `.with_rls(ctx).build()` instead.
    pub fn build(self) -> Qail {
        self.inner
    }
}

// =============================================================================
// RLS Proof Witness (Compile-Time Tenant Enforcement)
// =============================================================================

/// Marker trait for tables that require tenant isolation.
/// 
/// Tables with `operator_id` column get this from codegen.
/// When a table implements `RequiresRls`, its `TypedQail<T>` can only
/// produce a `Qail` via `.with_rls(ctx)` → `RlsQuery<T>` → `.build()`.
/// 
/// This makes data leakage a **compile error**, not a runtime bug.
/// 
/// # Example
/// ```ignore
/// // Codegen for a table with operator_id:
/// pub struct Orders;
/// impl Table for Orders { ... }
/// impl RequiresRls for Orders {}
/// 
/// // ✗ Compile error — Orders does not impl DirectBuild
/// Qail::typed(Orders).build()
/// 
/// // ✓ Compiles — RLS proof provided
/// let ctx = RlsContext::operator("op-uuid");
/// Qail::typed(Orders).with_rls(&ctx).build()
/// ```
pub trait RequiresRls: Table {}

// =============================================================================
// Infrastructure Resource Traits (Section 10)
// =============================================================================

/// Trait for type-safe S3/object storage bucket references.
///
/// Generated bucket structs implement this trait.
pub trait Bucket {
    /// The bucket name as a static string.
    fn bucket_name() -> &'static str;
}

/// Trait for type-safe message queue references (SQS, etc).
///
/// Generated queue structs implement this trait.
pub trait Queue {
    /// The queue name as a static string.
    fn queue_name() -> &'static str;
}

/// Trait for type-safe event topic references (SNS, Kafka, etc).
///
/// Generated topic structs implement this trait.
pub trait Topic {
    /// The topic name as a static string.
    fn topic_name() -> &'static str;
}

/// Proof that RLS context has been applied to a query.
/// 
/// Sealed constructor — only created internally by `TypedQail::with_rls()`.
#[derive(Debug, Clone)]
pub struct RlsProof(());

/// A query builder that carries proof of tenant isolation.
/// 
/// This is the ONLY way to `.build()` a query on an RLS-protected table.
/// Created by `TypedQail<T: RequiresRls>::with_rls(ctx)`.
#[derive(Debug, Clone)]
pub struct RlsQuery<T: Table> {
    inner: Qail,
    _proof: RlsProof,
    _table: PhantomData<T>,
}

impl<T: Table + RequiresRls> TypedQail<T> {
    /// Apply RLS context and produce a proven query.
    /// 
    /// This is *required* for tables with `RequiresRls` — without it,
    /// there's no `.build()` method available.
    /// 
    /// ```ignore
    /// let ctx = RlsContext::operator("op-uuid");
    /// let query = Qail::typed(Orders)
    ///     .column("id")
    ///     .with_rls(&ctx)   // returns RlsQuery<Orders>
    ///     .build();         // now .build() is available
    /// ```
    pub fn with_rls(mut self, ctx: &crate::rls::RlsContext) -> RlsQuery<T> {
        self.inner = self.inner.with_rls(ctx);
        RlsQuery {
            inner: self.inner,
            _proof: RlsProof(()),
            _table: PhantomData,
        }
    }
}

impl<T: Table> RlsQuery<T> {
    /// Finish building and return the inner Qail (RLS already applied).
    pub fn build(self) -> Qail {
        self.inner
    }
    
    /// Get a reference to the inner Qail.
    pub fn inner(&self) -> &Qail {
        &self.inner
    }
    
    /// Add a column to the query.
    pub fn column(mut self, name: impl AsRef<str>) -> Self {
        self.inner = self.inner.column(name);
        self
    }
    
    /// Add a typed column.
    pub fn typed_column<C>(mut self, col: TypedColumn<C>) -> Self {
        use crate::ast::Expr;
        self.inner.columns.push(Expr::Named(col.name().to_string()));
        self
    }
    
    /// Type-safe equality filter.
    ///
    /// # Arguments
    ///
    /// * `col` — Typed column descriptor.
    /// * `value` — Value whose type must match the column's type marker.
    pub fn typed_eq<C, V>(mut self, col: TypedColumn<C>, value: V) -> Self
    where
        V: Into<crate::ast::Value> + ColumnValue<C>,
    {
        self.inner = self.inner.typed_eq(col, value);
        self
    }
    
    /// String-based filter (untyped).
    ///
    /// # Arguments
    ///
    /// * `column` — Column name.
    /// * `op` — Comparison operator.
    /// * `value` — Filter value.
    pub fn filter(mut self, column: impl AsRef<str>, op: crate::ast::Operator, value: impl Into<crate::ast::Value>) -> Self {
        self.inner = self.inner.filter(column, op, value);
        self
    }
    
    /// Set limit.
    pub fn limit(mut self, n: i64) -> Self {
        self.inner = self.inner.limit(n);
        self
    }
    
    /// Set offset.
    pub fn offset(mut self, n: i64) -> Self {
        self.inner = self.inner.offset(n);
        self
    }
    
    /// Add ordering.
    pub fn order_by(mut self, column: impl AsRef<str>, order: crate::ast::SortOrder) -> Self {
        self.inner = self.inner.order_by(column, order);
        self
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
    
    // =========================================================================
    // TypedQail + join_related tests
    // =========================================================================
    
    // Non-RLS tables
    struct Users;
    impl Table for Users { fn table_name() -> &'static str { "users" } }
    impl AsRef<str> for Users { fn as_ref(&self) -> &str { "users" } }
    impl From<Users> for String { fn from(_: Users) -> String { "users".into() } }
    impl DirectBuild for Users {}
    
    struct Posts;
    impl Table for Posts { fn table_name() -> &'static str { "posts" } }
    impl AsRef<str> for Posts { fn as_ref(&self) -> &str { "posts" } }
    impl From<Posts> for String { fn from(_: Posts) -> String { "posts".into() } }
    impl DirectBuild for Posts {}
    
    struct Products;
    impl Table for Products { fn table_name() -> &'static str { "products" } }
    impl AsRef<str> for Products { fn as_ref(&self) -> &str { "products" } }
    impl From<Products> for String { fn from(_: Products) -> String { "products".into() } }
    impl DirectBuild for Products {}
    
    // RLS-protected table (has operator_id)
    struct Orders;
    impl Table for Orders { fn table_name() -> &'static str { "orders" } }
    impl AsRef<str> for Orders { fn as_ref(&self) -> &str { "orders" } }
    impl From<Orders> for String { fn from(_: Orders) -> String { "orders".into() } }
    impl RequiresRls for Orders {}
    
    // Users has many Posts (users.id -> posts.user_id)
    impl RelatedTo<Posts> for Users {
        fn join_columns() -> (&'static str, &'static str) { ("id", "user_id") }
    }
    // Reverse: Posts belongs to Users
    impl RelatedTo<Users> for Posts {
        fn join_columns() -> (&'static str, &'static str) { ("user_id", "id") }
    }
    // Note: NO RelatedTo<Products> for Users — join_related(Products) should NOT compile
    
    #[test]
    fn test_typed_qail_entry_point() {
        use crate::transpiler::ToSql;
        
        let query = Qail::typed(Users).build();
        let sql = query.to_sql();
        assert!(sql.contains("users"), "Should query users table");
    }
    
    #[test]
    fn test_typed_qail_join_related() {
        use crate::transpiler::ToSql;
        
        let query = Qail::typed(Users)
            .join_related(Posts)
            .build();
        
        let sql = query.to_sql();
        assert!(sql.contains("LEFT JOIN posts"), "Should have LEFT JOIN posts");
        assert!(sql.contains("id"), "Should have join column");
        assert!(sql.contains("user_id"), "Should have FK column");
    }
    
    #[test]
    fn test_typed_qail_reverse_join() {
        use crate::transpiler::ToSql;
        
        // Posts -> Users (reverse direction)
        let query = Qail::typed(Posts)
            .join_related(Users)
            .build();
        
        let sql = query.to_sql();
        assert!(sql.contains("LEFT JOIN users"), "Should have LEFT JOIN users");
    }
    
    #[test]
    fn test_typed_qail_with_columns_and_filter() {
        use crate::transpiler::ToSql;
        
        let email: TypedColumn<String> = TypedColumn::new("users", "email");
        let age: TypedColumn<i64> = TypedColumn::new("users", "age");
        
        let query = Qail::typed(Users)
            .typed_column(email)
            .typed_eq(age, 25i64)
            .build();
        
        let sql = query.to_sql();
        assert!(sql.contains("email"), "Should have email column");
        assert!(sql.contains("age"), "Should have age filter");
    }
    
    #[test]
    fn test_typed_qail_typed_columns_batch() {
        let col1: TypedColumn<String> = TypedColumn::new("users", "name");
        let col2: TypedColumn<String> = TypedColumn::new("users", "email");
        
        let query = Qail::typed(Users)
            .typed_columns(vec![col1, col2])
            .build();
        
        assert_eq!(query.columns.len(), 2, "Should have 2 columns");
    }
    
    #[test]
    fn test_typed_qail_full_chain() {
        use crate::transpiler::ToSql;
        use crate::ast::SortOrder;
        
        let query = Qail::typed(Users)
            .join_related(Posts)
            .column("email")
            .filter("age", crate::ast::Operator::Gt, 18)
            .order_by("created_at", SortOrder::Desc)
            .limit(10)
            .build();
        
        let sql = query.to_sql();
        assert!(sql.contains("LEFT JOIN posts"), "join");
        assert!(sql.contains("email"), "column");
        assert!(sql.contains("age"), "filter");
        assert!(sql.contains("LIMIT 10"), "limit");
    }
    
    // =========================================================================
    // RLS Proof Witness Tests
    // =========================================================================
    
    #[test]
    fn test_rls_query_with_proof() {
        use crate::transpiler::ToSql;
        use crate::rls::RlsContext;
        
        let ctx = RlsContext::operator("op-123");
        // The real test is that this COMPILES — Orders requires RLS proof
        let query = Qail::typed(Orders)
            .column("id")
            .column("total")
            .with_rls(&ctx)
            .build();
        
        let sql = query.to_sql();
        assert!(sql.contains("orders"), "Should query orders table");
        assert!(sql.contains("id"), "Should have id column");
    }
    
    #[test]
    fn test_rls_query_chaining_after_proof() {
        use crate::transpiler::ToSql;
        use crate::rls::RlsContext;
        use crate::ast::SortOrder;
        
        let ctx = RlsContext::operator("op-456");
        // The real test: chaining after .with_rls() still works
        let query = Qail::typed(Orders)
            .column("id")
            .with_rls(&ctx)
            .column("status")
            .filter("status", crate::ast::Operator::Eq, "active")
            .order_by("created_at", SortOrder::Desc)
            .limit(10)
            .build();
        
        let sql = query.to_sql();
        assert!(sql.contains("orders"), "table");
        assert!(sql.contains("status"), "filter");
        assert!(sql.contains("LIMIT 10"), "limit");
    }
    
    #[test]
    fn test_non_rls_table_builds_directly() {
        use crate::transpiler::ToSql;
        
        // Users has DirectBuild — .build() works without RLS
        let query = Qail::typed(Users)
            .column("email")
            .build();
        
        let sql = query.to_sql();
        assert!(sql.contains("users"), "Should build directly");
    }
    
    #[test]
    fn test_super_admin_bypasses_rls() {
        use crate::transpiler::ToSql;
        use crate::rls::RlsContext;
        
        // Super admin — RLS injection is a no-op but proof still required
        let token = crate::rls::SuperAdminToken::for_system_process("test_super_admin_bypass");
        let ctx = RlsContext::super_admin(token);
        let query = Qail::typed(Orders)
            .column("id")
            .with_rls(&ctx)
            .build();
        
        let sql = query.to_sql();
        assert!(sql.contains("orders"), "Should query orders");
        // Super admin should NOT inject operator_id filter
    }
    
    // NOTE: The following should NOT compile — proving compile-time safety:
    // Qail::typed(Users).join_related(Products)  // ERROR: Users: RelatedTo<Products> not satisfied
    // Qail::typed(Orders).build()                // ERROR: Orders does not impl DirectBuild
}


