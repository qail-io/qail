//! CASE WHEN expression builders.

use crate::ast::{Condition, Expr};

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
    pub(crate) when_clauses: Vec<(Condition, Box<Expr>)>,
    pub(crate) else_value: Option<Box<Expr>>,
    pub(crate) alias: Option<String>,
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
