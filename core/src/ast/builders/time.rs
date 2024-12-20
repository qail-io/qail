//! Time function builders (NOW, INTERVAL, etc.)

use crate::ast::{BinaryOp, Expr};

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
    use crate::ast::Value;
    Expr::SpecialFunction {
        name: "INTERVAL".to_string(),
        // Use Expr::Literal so the encoder wraps in quotes properly
        args: vec![(None, Box::new(Expr::Literal(Value::String(duration.to_string()))))],
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
