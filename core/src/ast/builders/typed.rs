//! Type-safe builder methods for compile-time type checking.
//!
//! These methods use TypedColumn<T> and ColumnValue<C> to enforce
//! that values match column types at compile time.
//!
//! # Example
//! ```ignore
//! use schema::users;
//!
//! // This compiles - i64 matches INT column
//! Qail::get(users::Users).typed_eq(users::age(), 25)
//!
//! // This fails at compile time - &str doesn't match INT column  
//! Qail::get(users::Users).typed_eq(users::age(), "string")
//! ```

use crate::ast::{Cage, Condition, Expr, Operator, Qail, Value};
use crate::typed::{ColumnValue, TypedColumn};

impl Qail {
    /// Type-safe equality condition.
    /// 
    /// Enforces at compile time that the value type matches the column type.
    pub fn typed_eq<T, V>(mut self, col: TypedColumn<T>, value: V) -> Self
    where
        V: Into<Value> + ColumnValue<T>,
    {
        let condition = Condition {
            left: Expr::Named(col.name().to_string()),
            op: Operator::Eq,
            value: value.into(),
            is_array_unnest: false,
        };
        self.add_condition(condition);
        self
    }
    
    /// Type-safe not-equal condition.
    pub fn typed_ne<T, V>(mut self, col: TypedColumn<T>, value: V) -> Self
    where
        V: Into<Value> + ColumnValue<T>,
    {
        let condition = Condition {
            left: Expr::Named(col.name().to_string()),
            op: Operator::Ne,
            value: value.into(),
            is_array_unnest: false,
        };
        self.add_condition(condition);
        self
    }
    
    /// Type-safe greater-than condition.
    pub fn typed_gt<T, V>(mut self, col: TypedColumn<T>, value: V) -> Self
    where
        V: Into<Value> + ColumnValue<T>,
    {
        let condition = Condition {
            left: Expr::Named(col.name().to_string()),
            op: Operator::Gt,
            value: value.into(),
            is_array_unnest: false,
        };
        self.add_condition(condition);
        self
    }
    
    /// Type-safe less-than condition.
    pub fn typed_lt<T, V>(mut self, col: TypedColumn<T>, value: V) -> Self
    where
        V: Into<Value> + ColumnValue<T>,
    {
        let condition = Condition {
            left: Expr::Named(col.name().to_string()),
            op: Operator::Lt,
            value: value.into(),
            is_array_unnest: false,
        };
        self.add_condition(condition);
        self
    }
    
    /// Type-safe greater-than-or-equal condition.
    pub fn typed_gte<T, V>(mut self, col: TypedColumn<T>, value: V) -> Self
    where
        V: Into<Value> + ColumnValue<T>,
    {
        let condition = Condition {
            left: Expr::Named(col.name().to_string()),
            op: Operator::Gte,
            value: value.into(),
            is_array_unnest: false,
        };
        self.add_condition(condition);
        self
    }
    
    /// Type-safe less-than-or-equal condition.
    pub fn typed_lte<T, V>(mut self, col: TypedColumn<T>, value: V) -> Self
    where
        V: Into<Value> + ColumnValue<T>,
    {
        let condition = Condition {
            left: Expr::Named(col.name().to_string()),
            op: Operator::Lte,
            value: value.into(),
            is_array_unnest: false,
        };
        self.add_condition(condition);
        self
    }
    
    /// Type-safe column selection.
    pub fn typed_column<T>(mut self, col: TypedColumn<T>) -> Self {
        self.columns.push(Expr::Named(col.name().to_string()));
        self
    }
    
    /// Type-safe filter with custom operator.
    pub fn typed_filter<T, V>(mut self, col: TypedColumn<T>, op: Operator, value: V) -> Self
    where
        V: Into<Value> + ColumnValue<T>,
    {
        let condition = Condition {
            left: Expr::Named(col.name().to_string()),
            op,
            value: value.into(),
            is_array_unnest: false,
        };
        self.add_condition(condition);
        self
    }
    
    /// Helper to add condition to appropriate cage
    fn add_condition(&mut self, condition: Condition) {
        use crate::ast::cages::CageKind;
        use crate::ast::LogicalOp;
        
        if self.cages.is_empty() {
            self.cages.push(Cage {
                kind: CageKind::Filter,
                conditions: Vec::new(),
                logical_op: LogicalOp::And,
            });
        }
        if let Some(cage) = self.cages.last_mut() {
            cage.conditions.push(condition);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed::TypedColumn;
    
    #[test]
    fn test_typed_eq_compiles() {
        // This should compile - i64 value for i64 column
        let col: TypedColumn<i64> = TypedColumn::new("users", "age");
        let query = Qail::get("users").typed_eq(col, 25i64);
        assert!(!query.cages.is_empty());
    }
    
    #[test]
    fn test_typed_column() {
        let col: TypedColumn<String> = TypedColumn::new("users", "name");
        let query = Qail::get("users").typed_column(col);
        assert_eq!(query.columns.len(), 1);
    }
}
