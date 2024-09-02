//! Type casting builder.

use crate::ast::Expr;

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
    pub(crate) expr: Expr,
    pub(crate) target_type: String,
    pub(crate) alias: Option<String>,
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
