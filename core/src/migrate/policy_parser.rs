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
/// Returns an error for unsupported expressions.
pub fn parse_policy_expr(sql: &str) -> Result<Expr, String> {
    let s = sql.trim();

    // Strip outer parens if the entire expression is wrapped
    let s = strip_outer_parens(s);

    // Try: expr OR expr
    if let Some(pos) = find_top_level_op(s, " OR ") {
        let left = parse_policy_expr(&s[..pos])?;
        let right = parse_policy_expr(&s[pos + 4..])?;
        return Ok(Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::Or,
            right: Box::new(right),
            alias: None,
        });
    }

    // Try: expr AND expr
    if let Some(pos) = find_top_level_op(s, " AND ") {
        let left = parse_policy_expr(&s[..pos])?;
        let right = parse_policy_expr(&s[pos + 5..])?;
        return Ok(Expr::Binary {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
            alias: None,
        });
    }

    // Try: col = current_setting('var', true)::type
    // or:  (current_setting('var', true))::type = 'true'
    if let Some(eq_pos) = find_top_level_op(s, " = ") {
        let lhs = s[..eq_pos].trim();
        let rhs = s[eq_pos + 3..].trim();

        // Pattern 1: col = current_setting(...)::type
        if let Some(expr) = try_parse_tenant_check(lhs, rhs) {
            return Ok(expr);
        }
        // Pattern 2: current_setting(...)::type = value (swapped)
        if let Some(expr) = try_parse_tenant_check(rhs, lhs) {
            // Swap back so col is on the left
            return Ok(expr);
        }
    }

    Err(format!("unsupported policy expression: {}", s))
}

/// Try to parse `lhs = rhs` where lhs is a column name and rhs is `current_setting('var', ...)::type`
fn try_parse_tenant_check(col_side: &str, setting_side: &str) -> Option<Expr> {
    let (session_var, cast_type) = parse_setting_expr(setting_side)?;
    let left = parse_policy_lhs(col_side);

    Some(Expr::Binary {
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
    })
}

fn parse_policy_lhs(col_side: &str) -> Expr {
    let lhs = strip_outer_parens(col_side).trim();
    if is_sql_true_literal(lhs) {
        return Expr::Literal(Value::Bool(true));
    }
    if is_sql_false_literal(lhs) {
        return Expr::Literal(Value::Bool(false));
    }
    Expr::Named(lhs.to_string())
}

fn parse_setting_expr(setting_side: &str) -> Option<(String, String)> {
    let mut normalized = strip_outer_parens(setting_side).trim().to_string();
    // Normalize pg_dump-style wrappers like: (NULLIF(...))::uuid -> NULLIF(...)::uuid
    loop {
        let candidate = normalized.trim();
        if !candidate.starts_with('(') {
            break;
        }
        let Some(close_idx) = find_matching_paren(candidate, 0) else {
            break;
        };
        let rest = candidate[close_idx + 1..].trim();
        if !rest.starts_with("::") {
            break;
        }
        let inner = candidate[1..close_idx].trim();
        normalized = format!("{inner}{rest}");
    }
    let s = normalized.trim();

    // Direct: current_setting('app.current_tenant_id', true)::uuid
    if let Some((session_var, rest)) = parse_current_setting_call(s) {
        let cast = parse_cast_suffix(rest).unwrap_or_else(|| "text".to_string());
        return Some((session_var, cast));
    }

    // Wrapped: NULLIF(current_setting(...), ''::text)::uuid
    if let Some((args, rest)) = parse_function_args_and_rest_ci(s, "NULLIF") {
        let (arg1, _arg2) = split_args2(args)?;
        let (session_var, mut cast) = parse_setting_expr(arg1.trim())?;
        if let Some(parsed_cast) = parse_cast_suffix(rest) {
            cast = parsed_cast;
        }
        return Some((session_var, cast));
    }

    // Wrapped: COALESCE(current_setting(...), 'false'::text)
    if let Some((args, rest)) = parse_function_args_and_rest_ci(s, "COALESCE") {
        let (arg1, arg2) = split_args2(args)?;
        let (session_var, mut cast) = parse_setting_expr(arg1.trim())?;
        if let Some(parsed_cast) = parse_cast_suffix(rest) {
            cast = parsed_cast;
        } else if is_sql_bool_string_literal(arg2.trim()) {
            // COALESCE(..., 'false'::text) is used for boolean context in pg_dump output
            cast = "boolean".to_string();
        }
        return Some((session_var, cast));
    }

    None
}

fn parse_cast_suffix(rest: &str) -> Option<String> {
    let tail = strip_outer_parens(rest).trim();
    tail.strip_prefix("::").map(|s| s.trim().to_string())
}

fn split_args2(args: &str) -> Option<(&str, &str)> {
    let idx = find_top_level_char(args, ',')?;
    Some((&args[..idx], &args[idx + 1..]))
}

fn parse_function_args_and_rest_ci<'a>(s: &'a str, fn_name: &str) -> Option<(&'a str, &'a str)> {
    let s = s.trim();
    let prefix = format!("{fn_name}(");
    if !starts_with_ci(s, &prefix) {
        return None;
    }
    let open_idx = fn_name.len();
    let close_idx = find_matching_paren(s, open_idx)?;
    let args = &s[open_idx + 1..close_idx];
    let rest = &s[close_idx + 1..];
    Some((args, rest))
}

fn parse_current_setting_call(expr: &str) -> Option<(String, &str)> {
    let (args, rest) = parse_function_args_and_rest_ci(expr, "current_setting")?;
    let session_var = extract_first_string_literal(args)?;
    Some((session_var, rest))
}

fn starts_with_ci(s: &str, prefix: &str) -> bool {
    s.get(..prefix.len())
        .is_some_and(|h| h.eq_ignore_ascii_case(prefix))
}

fn find_matching_paren(s: &str, open_idx: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    if *bytes.get(open_idx)? != b'(' {
        return None;
    }
    let mut depth = 0usize;
    let mut i = open_idx;
    let mut in_string = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            if b == b'\'' {
                // SQL escaped quote: ''
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => in_string = true,
            b'(' => depth += 1,
            b')' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

fn find_top_level_char(s: &str, needle: char) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_string = false;
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_string = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' => in_string = true,
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {
                if depth == 0 && (b as char) == needle {
                    return Some(i);
                }
            }
        }
        i += 1;
    }
    None
}

fn extract_first_string_literal(s: &str) -> Option<String> {
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.first().copied()? != b'\'' {
        return None;
    }
    let mut out = String::new();
    let mut i = 1usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                out.push('\'');
                i += 2;
                continue;
            }
            return Some(out);
        }
        out.push(b as char);
        i += 1;
    }
    None
}

fn is_sql_true_literal(s: &str) -> bool {
    matches!(
        s.trim().to_ascii_lowercase().as_str(),
        "true" | "'true'" | "'true'::text" | "'true'::varchar"
    )
}

fn is_sql_false_literal(s: &str) -> bool {
    matches!(
        s.trim().to_ascii_lowercase().as_str(),
        "false" | "'false'" | "'false'::text" | "'false'::varchar"
    )
}

fn is_sql_bool_string_literal(s: &str) -> bool {
    is_sql_true_literal(s) || is_sql_false_literal(s)
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
        let expr = parse_policy_expr("id = current_setting('app.current_operator_id', true)::uuid")
            .expect("expected tenant check parse");
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
        )
        .expect("expected OR parse");
        assert!(matches!(
            &expr,
            Expr::Binary {
                op: BinaryOp::Or,
                ..
            }
        ));
    }

    #[test]
    fn test_unsupported_expr_returns_error() {
        let expr = parse_policy_expr("status = 'cancelled'::text");
        assert!(expr.is_err());
    }

    #[test]
    fn test_coalesce_current_setting_boolean_eq_true() {
        let expr = parse_policy_expr(
            "COALESCE(current_setting('app.is_super_admin'::text, true), 'false'::text) = 'true'::text",
        )
        .expect("expected COALESCE(current_setting(...)) parse");
        match &expr {
            Expr::Binary {
                left, op, right, ..
            } => {
                assert!(matches!(op, BinaryOp::Eq));
                assert!(matches!(left.as_ref(), Expr::Literal(Value::Bool(true))));
                assert!(
                    matches!(right.as_ref(), Expr::Cast { target_type, .. } if target_type == "boolean")
                );
            }
            _ => panic!("Expected Binary, got {:?}", expr),
        }
    }

    #[test]
    fn test_nullif_current_setting_cast_uuid() {
        let expr = parse_policy_expr(
            "tenant_id = (NULLIF(current_setting('app.current_tenant_id'::text, true), ''::text))::uuid",
        )
        .expect("expected NULLIF(current_setting(...)) parse");
        match &expr {
            Expr::Binary {
                left, op, right, ..
            } => {
                assert!(matches!(op, BinaryOp::Eq));
                assert!(matches!(left.as_ref(), Expr::Named(n) if n == "tenant_id"));
                assert!(
                    matches!(right.as_ref(), Expr::Cast { target_type, .. } if target_type == "uuid")
                );
            }
            _ => panic!("Expected Binary, got {:?}", expr),
        }
    }

    #[test]
    fn test_strip_outer_parens() {
        assert_eq!(strip_outer_parens("(foo)"), "foo");
        assert_eq!(strip_outer_parens("((foo))"), "foo");
        assert_eq!(strip_outer_parens("(a) AND (b)"), "(a) AND (b)");
    }
}
