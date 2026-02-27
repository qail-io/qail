//! Policy SQL expression parser.
//!
//! Converts raw SQL policy expressions (from `pg_policies.qual` / `.with_check`)
//! into typed `Expr` AST nodes that can be rendered back to QAIL format.
//!
//! This lives in `qail-core` so all downstream crates (`qail-pg`, CLI, etc.)
//! can reuse it.

use crate::ast::expr::Expr;
use crate::ast::{BinaryOp, Value};

/// Parse a raw SQL policy expression from `pg_policies` into a typed `Expr` AST.
///
/// Handles the common RLS patterns:
/// - `col = current_setting('var', true)::type`  (tenant check)
/// - `(current_setting('var', true))::boolean = true`  (session bool check)
/// - `expr1 OR expr2` / `expr1 AND expr2` (combinators)
///
/// Falls back to `Expr::Raw(String)` for anything else.
pub fn parse_policy_expr(sql: &str) -> Expr {
    let s = sql.trim();

    // Strip outer parens if the entire expression is wrapped
    let s = strip_outer_parens(s);

    // Try: expr OR expr
    if let Some(pos) = find_top_level_op(s, " OR ") {
        let left = parse_policy_expr(&s[..pos]);
        let right = parse_policy_expr(&s[pos + 4..]);
        return Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::Or,
            right: Box::new(right),
            alias: None,
        };
    }

    // Try: expr AND expr
    if let Some(pos) = find_top_level_op(s, " AND ") {
        let left = parse_policy_expr(&s[..pos]);
        let right = parse_policy_expr(&s[pos + 5..]);
        return Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
            alias: None,
        };
    }

    // Try: col = current_setting('var', true)::type
    // or:  (current_setting('var', true))::type = 'true'
    if let Some(eq_pos) = find_top_level_op(s, " = ") {
        let lhs = s[..eq_pos].trim();
        let rhs = s[eq_pos + 3..].trim();

        // Pattern 1: col = current_setting(...)::type
        if let Some(expr) = try_parse_tenant_check(lhs, rhs) {
            return expr;
        }
        // Pattern 2: current_setting(...)::type = value (swapped)
        if let Some(expr) = try_parse_tenant_check(rhs, lhs) {
            // Swap back so col is on the left
            return expr;
        }
    }

    // Fallback: raw SQL
    Expr::Raw(s.to_string())
}

/// Try to parse `lhs = rhs` where lhs is a column name and rhs is `current_setting('var', ...)::type`
fn try_parse_tenant_check(col_side: &str, setting_side: &str) -> Option<Expr> {
    let setting_side = strip_outer_parens(setting_side);

    // Check if setting_side is current_setting(...)::type
    if let Some(rest) = setting_side.strip_prefix("current_setting(") {
        // Find the matching closing paren
        if let Some(close) = rest.find(')') {
            let args_str = &rest[..close];
            let after_paren = &rest[close + 1..];

            // Extract session variable from first argument: 'var_name' or 'var_name'::text
            let session_var = args_str
                .split(',')
                .next()?
                .trim()
                .trim_matches('\'')
                .to_string();

            // Extract cast type from ::type
            let cast_type = if let Some(cast) = after_paren.strip_prefix("::") {
                cast.trim().to_string()
            } else {
                "text".to_string()
            };

            let col_side = strip_outer_parens(col_side);

            // Check if col_side is a simple column name or a literal
            let left = if col_side == "'true'" || col_side == "true" {
                Expr::Literal(Value::Bool(true))
            } else if col_side == "'false'" || col_side == "false" {
                Expr::Literal(Value::Bool(false))
            } else {
                Expr::Named(col_side.to_string())
            };

            return Some(Expr::Binary {
                left: Box::new(left),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Cast {
                    expr: Box::new(Expr::FunctionCall {
                        name: "current_setting".into(),
                        args: vec![Expr::Literal(Value::String(session_var))],
                        alias: None,
                    }),
                    target_type: cast_type,
                    alias: None,
                }),
                alias: None,
            });
        }
    }

    None
}

/// Strip balanced outer parentheses from an expression.
pub fn strip_outer_parens(s: &str) -> &str {
    let s = s.trim();
    if s.starts_with('(') && s.ends_with(')') {
        // Check that parens are balanced (the opening matches the closing)
        let mut depth = 0;
        let bytes = s.as_bytes();
        for (i, &b) in bytes.iter().enumerate() {
            match b {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 && i < bytes.len() - 1 {
                        // The opening paren closes before the end — not a wrapper
                        return s;
                    }
                }
                _ => {}
            }
        }
        if depth == 0 {
            return strip_outer_parens(&s[1..s.len() - 1]);
        }
    }
    s
}

/// Find a top-level (not inside parentheses) occurrence of `op` in `s`.
pub fn find_top_level_op(s: &str, op: &str) -> Option<usize> {
    let mut depth = 0;
    let bytes = s.as_bytes();
    let op_bytes = op.as_bytes();
    if bytes.len() < op_bytes.len() {
        return None;
    }
    for i in 0..=bytes.len() - op_bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && &bytes[i..i + op_bytes.len()] == op_bytes {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_tenant_check() {
        let expr = parse_policy_expr("id = current_setting('app.current_operator_id', true)::uuid");
        match &expr {
            Expr::Binary {
                left, op, right, ..
            } => {
                assert!(matches!(op, BinaryOp::Eq));
                assert!(matches!(left.as_ref(), Expr::Named(n) if n == "id"));
                assert!(
                    matches!(right.as_ref(), Expr::Cast { target_type, .. } if target_type == "uuid")
                );
            }
            _ => panic!("Expected Binary, got {:?}", expr),
        }
    }

    #[test]
    fn test_or_combinator() {
        let expr = parse_policy_expr(
            "id = current_setting('app.op', true)::uuid OR current_setting('app.admin', true)::boolean = true",
        );
        assert!(matches!(
            &expr,
            Expr::Binary {
                op: BinaryOp::Or,
                ..
            }
        ));
    }

    #[test]
    fn test_raw_fallback() {
        let expr = parse_policy_expr("status = 'cancelled'::text");
        assert!(matches!(&expr, Expr::Raw(_)));
    }

    #[test]
    fn test_strip_outer_parens() {
        assert_eq!(strip_outer_parens("(foo)"), "foo");
        assert_eq!(strip_outer_parens("((foo))"), "foo");
        assert_eq!(strip_outer_parens("(a) AND (b)"), "(a) AND (b)");
    }
}
