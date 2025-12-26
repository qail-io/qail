//! Ergonomic builder functions for QAIL AST expressions.
//!
//! This module provides convenient helper functions to construct AST nodes
//! without the verbosity of creating structs directly.
//!
//! # Example
//! ```ignore
//! use qail_core::ast::builders::*;
//!
//! let expr = count_filter(vec![
//!     eq("direction", "outbound"),
//!     gt("created_at", now_minus("24 hours")),
//! ]).alias("messages_sent_24h");
//! ```

use crate::ast::{AggregateFunc, BinaryOp, Condition, Expr, Operator, Value};

// ==================== Column Reference ====================

/// Create a column reference expression
pub fn col(name: &str) -> Expr {
    Expr::Named(name.to_string())
}

/// Create a star (*) expression for SELECT *
pub fn star() -> Expr {
    Expr::Star
}

// ==================== Aggregate Functions ====================

/// COUNT(*) aggregate
pub fn count() -> AggregateBuilder {
    AggregateBuilder {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: None,
        alias: None,
    }
}

/// COUNT(DISTINCT column) aggregate
pub fn count_distinct(column: &str) -> AggregateBuilder {
    AggregateBuilder {
        col: column.to_string(),
        func: AggregateFunc::Count,
        distinct: true,
        filter: None,
        alias: None,
    }
}

/// COUNT(*) FILTER (WHERE conditions) aggregate
pub fn count_filter(conditions: Vec<Condition>) -> AggregateBuilder {
    AggregateBuilder {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: Some(conditions),
        alias: None,
    }
}

/// SUM(column) aggregate
pub fn sum(column: &str) -> AggregateBuilder {
    AggregateBuilder {
        col: column.to_string(),
        func: AggregateFunc::Sum,
        distinct: false,
        filter: None,
        alias: None,
    }
}

/// AVG(column) aggregate
pub fn avg(column: &str) -> AggregateBuilder {
    AggregateBuilder {
        col: column.to_string(),
        func: AggregateFunc::Avg,
        distinct: false,
        filter: None,
        alias: None,
    }
}

/// MIN(column) aggregate
pub fn min(column: &str) -> AggregateBuilder {
    AggregateBuilder {
        col: column.to_string(),
        func: AggregateFunc::Min,
        distinct: false,
        filter: None,
        alias: None,
    }
}

/// MAX(column) aggregate
pub fn max(column: &str) -> AggregateBuilder {
    AggregateBuilder {
        col: column.to_string(),
        func: AggregateFunc::Max,
        distinct: false,
        filter: None,
        alias: None,
    }
}

/// Builder for aggregate expressions
#[derive(Debug, Clone)]
pub struct AggregateBuilder {
    col: String,
    func: AggregateFunc,
    distinct: bool,
    filter: Option<Vec<Condition>>,
    alias: Option<String>,
}

impl AggregateBuilder {
    /// Add DISTINCT modifier
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Add FILTER (WHERE ...) clause
    pub fn filter(mut self, conditions: Vec<Condition>) -> Self {
        self.filter = Some(conditions);
        self
    }

    /// Add alias (AS name)
    pub fn alias(mut self, name: &str) -> Expr {
        self.alias = Some(name.to_string());
        self.build()
    }

    /// Build the final Expr
    pub fn build(self) -> Expr {
        Expr::Aggregate {
            col: self.col,
            func: self.func,
            distinct: self.distinct,
            filter: self.filter,
            alias: self.alias,
        }
    }
}

impl From<AggregateBuilder> for Expr {
    fn from(builder: AggregateBuilder) -> Self {
        builder.build()
    }
}

// ==================== Time Functions ====================

/// NOW() function
pub fn now() -> Expr {
    Expr::FunctionCall {
        name: "NOW".to_string(),
        args: vec![],
        alias: None,
    }
}

/// INTERVAL 'duration' expression
pub fn interval(duration: &str) -> Expr {
    Expr::SpecialFunction {
        name: "INTERVAL".to_string(),
        args: vec![(None, Box::new(Expr::Named(format!("'{}'", duration))))],
        alias: None,
    }
}

/// NOW() - INTERVAL 'duration' helper
pub fn now_minus(duration: &str) -> Expr {
    Expr::Binary {
        left: Box::new(now()),
        op: BinaryOp::Sub,
        right: Box::new(interval(duration)),
        alias: None,
    }
}

/// NOW() + INTERVAL 'duration' helper
pub fn now_plus(duration: &str) -> Expr {
    Expr::Binary {
        left: Box::new(now()),
        op: BinaryOp::Add,
        right: Box::new(interval(duration)),
        alias: None,
    }
}

// ==================== Type Casting ====================

/// Cast expression to target type (expr::type)
pub fn cast(expr: impl Into<Expr>, target_type: &str) -> CastBuilder {
    CastBuilder {
        expr: expr.into(),
        target_type: target_type.to_string(),
        alias: None,
    }
}

/// Builder for cast expressions
#[derive(Debug, Clone)]
pub struct CastBuilder {
    expr: Expr,
    target_type: String,
    alias: Option<String>,
}

impl CastBuilder {
    /// Add alias (AS name)
    pub fn alias(mut self, name: &str) -> Expr {
        self.alias = Some(name.to_string());
        self.build()
    }

    /// Build the final Expr
    pub fn build(self) -> Expr {
        Expr::Cast {
            expr: Box::new(self.expr),
            target_type: self.target_type,
            alias: self.alias,
        }
    }
}

impl From<CastBuilder> for Expr {
    fn from(builder: CastBuilder) -> Self {
        builder.build()
    }
}

// ==================== CASE WHEN ====================

/// Start a CASE WHEN expression
pub fn case_when(condition: Condition, then_expr: impl Into<Expr>) -> CaseBuilder {
    CaseBuilder {
        when_clauses: vec![(condition, Box::new(then_expr.into()))],
        else_value: None,
        alias: None,
    }
}

/// Builder for CASE expressions
#[derive(Debug, Clone)]
pub struct CaseBuilder {
    when_clauses: Vec<(Condition, Box<Expr>)>,
    else_value: Option<Box<Expr>>,
    alias: Option<String>,
}

impl CaseBuilder {
    /// Add another WHEN clause
    pub fn when(mut self, condition: Condition, then_expr: impl Into<Expr>) -> Self {
        self.when_clauses.push((condition, Box::new(then_expr.into())));
        self
    }

    /// Add ELSE clause
    pub fn otherwise(mut self, else_expr: impl Into<Expr>) -> Self {
        self.else_value = Some(Box::new(else_expr.into()));
        self
    }

    /// Add alias (AS name)
    pub fn alias(mut self, name: &str) -> Expr {
        self.alias = Some(name.to_string());
        self.build()
    }

    /// Build the final Expr
    pub fn build(self) -> Expr {
        Expr::Case {
            when_clauses: self.when_clauses,
            else_value: self.else_value,
            alias: self.alias,
        }
    }
}

impl From<CaseBuilder> for Expr {
    fn from(builder: CaseBuilder) -> Self {
        builder.build()
    }
}

// ==================== Binary Expressions ====================

/// Create a binary expression (left op right)
pub fn binary(left: impl Into<Expr>, op: BinaryOp, right: impl Into<Expr>) -> BinaryBuilder {
    BinaryBuilder {
        left: left.into(),
        op,
        right: right.into(),
        alias: None,
    }
}

/// Builder for binary expressions
#[derive(Debug, Clone)]
pub struct BinaryBuilder {
    left: Expr,
    op: BinaryOp,
    right: Expr,
    alias: Option<String>,
}

impl BinaryBuilder {
    /// Add alias (AS name)
    pub fn alias(mut self, name: &str) -> Expr {
        self.alias = Some(name.to_string());
        self.build()
    }

    /// Build the final Expr
    pub fn build(self) -> Expr {
        Expr::Binary {
            left: Box::new(self.left),
            op: self.op,
            right: Box::new(self.right),
            alias: self.alias,
        }
    }
}

impl From<BinaryBuilder> for Expr {
    fn from(builder: BinaryBuilder) -> Self {
        builder.build()
    }
}

// ==================== Condition Helpers ====================

/// Helper to create a condition
fn make_condition(column: &str, op: Operator, value: Value) -> Condition {
    Condition {
        left: Expr::Named(column.to_string()),
        op,
        value,
        is_array_unnest: false,
    }
}

/// Create an equality condition (column = value)
pub fn eq(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Eq, value.into())
}

/// Create a not-equal condition (column != value)
pub fn ne(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Ne, value.into())
}

/// Create a greater-than condition (column > value)
pub fn gt(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Gt, value.into())
}

/// Create a greater-than-or-equal condition (column >= value)
pub fn gte(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Gte, value.into())
}

/// Create a less-than condition (column < value)
pub fn lt(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Lt, value.into())
}

/// Create a less-than-or-equal condition (column <= value)
pub fn lte(column: &str, value: impl Into<Value>) -> Condition {
    make_condition(column, Operator::Lte, value.into())
}

/// Create an IN condition (column IN (values))
pub fn is_in<V: Into<Value>>(column: &str, values: impl IntoIterator<Item = V>) -> Condition {
    let vals: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
    make_condition(column, Operator::In, Value::Array(vals))
}

/// Create a NOT IN condition (column NOT IN (values))
pub fn not_in<V: Into<Value>>(column: &str, values: impl IntoIterator<Item = V>) -> Condition {
    let vals: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
    make_condition(column, Operator::NotIn, Value::Array(vals))
}

/// Create an IS NULL condition
pub fn is_null(column: &str) -> Condition {
    make_condition(column, Operator::IsNull, Value::Null)
}

/// Create an IS NOT NULL condition
pub fn is_not_null(column: &str) -> Condition {
    make_condition(column, Operator::IsNotNull, Value::Null)
}

/// Create a LIKE condition (column LIKE pattern)
pub fn like(column: &str, pattern: &str) -> Condition {
    make_condition(column, Operator::Like, Value::String(pattern.to_string()))
}

/// Create an ILIKE condition (case-insensitive LIKE)
pub fn ilike(column: &str, pattern: &str) -> Condition {
    make_condition(column, Operator::ILike, Value::String(pattern.to_string()))
}

// ==================== Function Calls ====================

/// Create a function call expression
pub fn func(name: &str, args: Vec<Expr>) -> FunctionBuilder {
    FunctionBuilder {
        name: name.to_string(),
        args,
        alias: None,
    }
}

/// COALESCE(args...) function
pub fn coalesce(args: Vec<Expr>) -> FunctionBuilder {
    func("COALESCE", args)
}

/// NULLIF(a, b) function
pub fn nullif(a: impl Into<Expr>, b: impl Into<Expr>) -> FunctionBuilder {
    func("NULLIF", vec![a.into(), b.into()])
}

/// Builder for function call expressions
#[derive(Debug, Clone)]
pub struct FunctionBuilder {
    name: String,
    args: Vec<Expr>,
    alias: Option<String>,
}

impl FunctionBuilder {
    /// Add alias (AS name)
    pub fn alias(mut self, name: &str) -> Expr {
        self.alias = Some(name.to_string());
        self.build()
    }

    /// Build the final Expr
    pub fn build(self) -> Expr {
        Expr::FunctionCall {
            name: self.name,
            args: self.args,
            alias: self.alias,
        }
    }
}

impl From<FunctionBuilder> for Expr {
    fn from(builder: FunctionBuilder) -> Self {
        builder.build()
    }
}

// ==================== Literal Values ====================

/// Create an integer literal expression
pub fn int(value: i64) -> Expr {
    Expr::Named(value.to_string())
}

/// Create a float literal expression  
pub fn float(value: f64) -> Expr {
    Expr::Named(value.to_string())
}

/// Create a string literal expression
pub fn text(value: &str) -> Expr {
    Expr::Named(format!("'{}'", value))
}

// ==================== Extension Trait for Expr ====================

/// Extension trait to add fluent methods to Expr
pub trait ExprExt {
    /// Add an alias to this expression
    fn as_alias(self, alias: &str) -> Expr;
}

impl ExprExt for Expr {
    fn as_alias(self, alias: &str) -> Expr {
        match self {
            Expr::Named(name) => Expr::Aliased { name, alias: alias.to_string() },
            Expr::Aggregate { col, func, distinct, filter, .. } => {
                Expr::Aggregate { col, func, distinct, filter, alias: Some(alias.to_string()) }
            }
            Expr::Cast { expr, target_type, .. } => {
                Expr::Cast { expr, target_type, alias: Some(alias.to_string()) }
            }
            Expr::Case { when_clauses, else_value, .. } => {
                Expr::Case { when_clauses, else_value, alias: Some(alias.to_string()) }
            }
            Expr::FunctionCall { name, args, .. } => {
                Expr::FunctionCall { name, args, alias: Some(alias.to_string()) }
            }
            Expr::Binary { left, op, right, .. } => {
                Expr::Binary { left, op, right, alias: Some(alias.to_string()) }
            }
            other => other,  // Star, Aliased, etc. - return as-is
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_filter() {
        let expr = count_filter(vec![
            eq("direction", "outbound"),
        ]).alias("sent_count");
        
        assert!(matches!(expr, Expr::Aggregate { alias: Some(a), .. } if a == "sent_count"));
    }

    #[test]
    fn test_now_minus() {
        let expr = now_minus("24 hours");
        assert!(matches!(expr, Expr::Binary { op: BinaryOp::Sub, .. }));
    }

    #[test]
    fn test_case_when() {
        let expr = case_when(gt("x", 0), int(1))
            .otherwise(int(0))
            .alias("result");
        
        assert!(matches!(expr, Expr::Case { alias: Some(a), .. } if a == "result"));
    }

    #[test]
    fn test_cast() {
        let expr = cast(col("value"), "float8").alias("value_f");
        assert!(matches!(expr, Expr::Cast { target_type, .. } if target_type == "float8"));
    }
}
