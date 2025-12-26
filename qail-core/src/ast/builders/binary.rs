//! Binary expression builders.

use crate::ast::{BinaryOp, Expr};

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
    pub(crate) left: Expr,
    pub(crate) op: BinaryOp,
    pub(crate) right: Expr,
    pub(crate) alias: Option<String>,
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
