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

/// A typed column reference with compile-time type information.
/// 
/// The type parameter `T` represents the Rust type for this column.
#[derive(Debug, Clone, Copy)]
pub struct TypedColumn<T> {
    table: &'static str,
    name: &'static str,
    _phantom: PhantomData<T>,
}

impl<T> TypedColumn<T> {
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
}

/// Allow TypedColumn to be used where &str is expected.
impl<T> AsRef<str> for TypedColumn<T> {
    fn as_ref(&self) -> &str {
        self.name
    }
}

/// Allow TypedColumn to be converted to String.
impl<T> From<TypedColumn<T>> for String {
    fn from(col: TypedColumn<T>) -> String {
        col.name.to_string()
    }
}

/// Allow &TypedColumn to be converted to String.
impl<T> From<&TypedColumn<T>> for String {
    fn from(col: &TypedColumn<T>) -> String {
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

impl<T> IntoColumn for TypedColumn<T> {
    fn column_name(&self) -> &str {
        self.name
    }
}

impl<T> IntoColumn for &TypedColumn<T> {
    fn column_name(&self) -> &str {
        self.name
    }
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

// DateTime
impl ColumnValue<chrono::DateTime<chrono::Utc>> for chrono::DateTime<chrono::Utc> {}
impl ColumnValue<chrono::DateTime<chrono::Utc>> for &str {} // String dates
impl ColumnValue<chrono::DateTime<chrono::Utc>> for String {}

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
}
