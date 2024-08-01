//! Function call builders (COALESCE, REPLACE, SUBSTRING, etc.)

use crate::ast::{BinaryOp, Expr};
use super::literals::int;

/// Create a function call expression
pub fn func(name: &str, args: Vec<Expr>) -> FunctionBuilder {
    FunctionBuilder {
        name: name.to_string(),
        args,
        alias: None,
    }
}

/// COALESCE(args...) function
pub fn coalesce<E: Into<Expr>>(args: impl IntoIterator<Item = E>) -> FunctionBuilder {
    func("COALESCE", args.into_iter().map(|e| e.into()).collect())
}

/// NULLIF(a, b) function
pub fn nullif(a: impl Into<Expr>, b: impl Into<Expr>) -> FunctionBuilder {
    func("NULLIF", vec![a.into(), b.into()])
}

/// REPLACE(source, from, to) function
/// 
/// # Example
/// ```ignore
/// replace(col("phone"), text("+"), text(""))  // REPLACE(phone, '+', '')
/// ```
pub fn replace(source: impl Into<Expr>, from: impl Into<Expr>, to: impl Into<Expr>) -> FunctionBuilder {
    func("REPLACE", vec![source.into(), from.into(), to.into()])
}

/// Builder for function call expressions
#[derive(Debug, Clone)]
pub struct FunctionBuilder {
    pub(crate) name: String,
    pub(crate) args: Vec<Expr>,
    pub(crate) alias: Option<String>,
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

/// SUBSTRING(source FROM start [FOR length])
/// 
/// # Example
/// ```ignore
/// substring(col("phone"), 2)       // SUBSTRING(phone FROM 2)
/// substring_for(col("name"), 1, 5) // SUBSTRING(name FROM 1 FOR 5)
/// ```
pub fn substring(source: impl Into<Expr>, from: i32) -> Expr {
    Expr::SpecialFunction {
        name: "SUBSTRING".to_string(),
        args: vec![
            (None, Box::new(source.into())),
            (Some("FROM".to_string()), Box::new(int(from as i64))),
        ],
        alias: None,
    }
}

/// SUBSTRING(source FROM start FOR length)
pub fn substring_for(source: impl Into<Expr>, from: i32, length: i32) -> Expr {
    Expr::SpecialFunction {
        name: "SUBSTRING".to_string(),
        args: vec![
            (None, Box::new(source.into())),
            (Some("FROM".to_string()), Box::new(int(from as i64))),
            (Some("FOR".to_string()), Box::new(int(length as i64))),
        ],
        alias: None,
    }
}

/// String concatenation (a || b || c)
/// 
/// # Example
/// ```ignore
/// concat([col("first_name"), text(" "), col("last_name")])
/// ```
pub fn concat<E: Into<Expr>>(exprs: impl IntoIterator<Item = E>) -> ConcatBuilder {
    let exprs: Vec<Expr> = exprs.into_iter().map(|e| e.into()).collect();
    ConcatBuilder { exprs, alias: None }
}

/// Builder for concat expressions
#[derive(Debug, Clone)]
pub struct ConcatBuilder {
    pub(crate) exprs: Vec<Expr>,
    pub(crate) alias: Option<String>,
}

impl ConcatBuilder {
    /// Add alias (AS name)
    pub fn alias(mut self, name: &str) -> Expr {
        self.alias = Some(name.to_string());
        self.build()
    }
    
    /// Build the final Expr
    pub fn build(self) -> Expr {
        use super::literals::text;
        
        if self.exprs.is_empty() {
            return text("");
        }
        
        let mut iter = self.exprs.into_iter();
        let first = iter.next().unwrap();
        
        let result = iter.fold(first, |acc, expr| {
            Expr::Binary {
                left: Box::new(acc),
                op: BinaryOp::Concat,
                right: Box::new(expr),
                alias: None,
            }
        });
        
        // Apply alias to the final result
        if let Some(alias) = self.alias {
            match result {
                Expr::Binary { left, op, right, .. } => {
                    Expr::Binary { left, op, right, alias: Some(alias) }
                }
                other => other,
            }
        } else {
            result
        }
    }
}

impl From<ConcatBuilder> for Expr {
    fn from(builder: ConcatBuilder) -> Self {
        builder.build()
    }
}
