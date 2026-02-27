//! AST structural sanitization for untrusted input.
//!
//! The text parser enforces identifier constraints (alphanumeric + `_` + `.`),
//! but the binary/postcard path deserializes directly into `Qail` — an attacker
//! can craft identifiers that inject SQL fragments.
//!
//! Call [`validate_ast`] on any `Qail` obtained from an untrusted source
//! (binary endpoint, external API, etc.) before execution.

use crate::ast::{Action, Expr, Qail, Value};
use std::fmt;

/// Error returned when AST structural validation fails.
#[derive(Debug, Clone)]
pub struct SanitizeError {
    pub field: String,
    pub value: String,
    pub reason: String,
}

impl fmt::Display for SanitizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AST validation failed: {} '{}' — {}",
            self.field, self.value, self.reason
        )
    }
}

impl std::error::Error for SanitizeError {}

/// Maximum identifier length (PostgreSQL NAMEDATALEN - 1).
const MAX_IDENT_LEN: usize = 63;

/// Validate that an identifier matches the parser grammar: `[a-zA-Z0-9_.]`.
///
/// Also rejects empty identifiers and those exceeding PostgreSQL's 63-char limit.
fn is_safe_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MAX_IDENT_LEN
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
}

/// Validate an identifier, returning a `SanitizeError` if invalid.
fn check_ident(field: &str, value: &str) -> Result<(), SanitizeError> {
    if is_safe_identifier(value) {
        Ok(())
    } else {
        Err(SanitizeError {
            field: field.to_string(),
            value: value.chars().take(40).collect(),
            reason: "identifiers must match [a-zA-Z0-9_.] and be ≤63 chars".to_string(),
        })
    }
}

/// Validate an `Expr` node for unsafe patterns.
///
/// - `Expr::Named` must be a safe identifier.
/// - `Expr::Raw` is rejected outright (binary path must not carry raw SQL).
/// - Recursive variants (Cast, Binary, etc.) are validated recursively.
fn check_expr(field: &str, expr: &Expr) -> Result<(), SanitizeError> {
    match expr {
        Expr::Star => Ok(()),
        Expr::Named(name) => check_ident(field, name),
        Expr::Aliased { name, alias } => {
            check_ident(field, name)?;
            check_ident(&format!("{field}.alias"), alias)
        }
        Expr::Aggregate {
            col, alias, filter, ..
        } => {
            check_ident(field, col)?;
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            if let Some(conditions) = filter {
                for cond in conditions {
                    check_expr(&format!("{field}.filter"), &cond.left)?;
                }
            }
            Ok(())
        }
        Expr::FunctionCall { name, args, alias } => {
            check_ident(field, name)?;
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            for arg in args {
                check_expr(&format!("{field}.arg"), arg)?;
            }
            Ok(())
        }
        Expr::Cast {
            expr,
            target_type,
            alias,
        } => {
            check_expr(field, expr)?;
            check_ident(&format!("{field}.cast_type"), target_type)?;
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        Expr::Binary {
            left, right, alias, ..
        } => {
            check_expr(field, left)?;
            check_expr(field, right)?;
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        Expr::Raw(_) => Err(SanitizeError {
            field: field.to_string(),
            value: "(raw SQL)".to_string(),
            reason: "Expr::Raw is not allowed in binary AST".to_string(),
        }),
        Expr::Literal(_) => Ok(()),
        Expr::JsonAccess {
            column, alias, path_segments, ..
        } => {
            check_ident(field, column)?;
            for (key, _) in path_segments {
                // Integer indices are fine; string keys must be safe identifiers
                if key.parse::<i64>().is_err() && !is_safe_identifier(key) {
                    return Err(SanitizeError {
                        field: format!("{field}.json_path"),
                        value: key.chars().take(40).collect(),
                        reason: "JSON path key must be a safe identifier or integer"
                            .to_string(),
                    });
                }
            }
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        Expr::Subquery { query, alias } => {
            validate_ast(query)?;
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        Expr::Exists {
            query, alias, ..
        } => {
            validate_ast(query)?;
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        // For all other complex Expr variants, validate aliases where present
        Expr::Window { name, func, partition, params, order, .. } => {
            if !name.is_empty() {
                check_ident(&format!("{field}.window_alias"), name)?;
            }
            check_ident(&format!("{field}.window_func"), func)?;
            for p in partition {
                check_ident(&format!("{field}.partition"), p)?;
            }
            for p in params {
                check_expr(&format!("{field}.window_param"), p)?;
            }
            for cage in order {
                for cond in &cage.conditions {
                    check_expr(&format!("{field}.window_order"), &cond.left)?;
                    check_value(&format!("{field}.window_order"), &cond.value)?;
                }
            }
            Ok(())
        }
        Expr::Case { when_clauses, else_value, alias } => {
            for (cond, val) in when_clauses {
                check_expr(&format!("{field}.case_when"), &Expr::Named(cond.left.to_string()))?;
                check_expr(&format!("{field}.case_then"), val)?;
            }
            if let Some(e) = else_value {
                check_expr(&format!("{field}.case_else"), e)?;
            }
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        Expr::SpecialFunction { args, alias, name } => {
            check_ident(&format!("{field}.special_func"), name)?;
            for (_, arg) in args {
                check_expr(&format!("{field}.special_func_arg"), arg)?;
            }
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        Expr::ArrayConstructor { elements, alias } | Expr::RowConstructor { elements, alias } => {
            for elem in elements {
                check_expr(&format!("{field}.element"), elem)?;
            }
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        Expr::Subscript { expr, index, alias } => {
            check_expr(&format!("{field}.subscript_expr"), expr)?;
            check_expr(&format!("{field}.subscript_index"), index)?;
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        Expr::Collate { expr, collation, alias } => {
            check_expr(&format!("{field}.collate_expr"), expr)?;
            check_ident(&format!("{field}.collation"), collation)?;
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        Expr::FieldAccess { expr, field: f, alias } => {
            check_expr(&format!("{field}.field_access_expr"), expr)?;
            check_ident(&format!("{field}.field"), f)?;
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        Expr::Def { name, .. } => check_ident(field, name),
        Expr::Mod { col, .. } => check_expr(field, col),
    }
}

/// Check a `Value` for embedded subqueries.
fn check_value(field: &str, value: &Value) -> Result<(), SanitizeError> {
    match value {
        Value::Subquery(q) => validate_ast(q),
        Value::Array(vals) => {
            for v in vals {
                check_value(field, v)?;
            }
            Ok(())
        }
        Value::Expr(expr) => check_expr(field, expr),
        _ => Ok(()),
    }
}

/// Validate a `Qail` AST from an untrusted source.
///
/// Checks all identifier fields against the parser grammar (`[a-zA-Z0-9_.]`)
/// and rejects dangerous constructs like `Expr::Raw` and procedural actions.
///
/// # Errors
///
/// Returns `SanitizeError` on the first violation found.
pub fn validate_ast(cmd: &Qail) -> Result<(), SanitizeError> {
    // ── Block dangerous actions from binary path ─────────────────────
    match cmd.action {
        Action::Call | Action::Do | Action::SessionSet | Action::SessionReset => {
            return Err(SanitizeError {
                field: "action".to_string(),
                value: format!("{:?}", cmd.action),
                reason: "procedural/session actions are not allowed via binary AST".to_string(),
            });
        }
        _ => {}
    }

    // ── Raw SQL pass-through ─────────────────────────────────────────
    if cmd.is_raw_sql() {
        return Err(SanitizeError {
            field: "table".to_string(),
            value: "(raw SQL)".to_string(),
            reason: "raw SQL pass-through is not allowed via binary AST".to_string(),
        });
    }

    // ── Table name ───────────────────────────────────────────────────
    if !cmd.table.is_empty() {
        check_ident("table", &cmd.table)?;
    }

    // ── Columns ──────────────────────────────────────────────────────
    for (i, col) in cmd.columns.iter().enumerate() {
        check_expr(&format!("columns[{i}]"), col)?;
    }

    // ── Joins ────────────────────────────────────────────────────────
    for (i, join) in cmd.joins.iter().enumerate() {
        // Join table may include alias: "users u"
        // Validate each space-separated token
        for token in join.table.split_whitespace() {
            check_ident(&format!("joins[{i}].table"), token)?;
        }
        if let Some(ref conditions) = join.on {
            for cond in conditions {
                check_expr(&format!("joins[{i}].on"), &cond.left)?;
                check_value(&format!("joins[{i}].on"), &cond.value)?;
            }
        }
    }

    // ── Cages (filters, sorts, etc.) ─────────────────────────────────
    for cage in &cmd.cages {
        for cond in &cage.conditions {
            check_expr("cage.condition.left", &cond.left)?;
            check_value("cage.condition.value", &cond.value)?;
        }
    }

    // ── CTEs ─────────────────────────────────────────────────────────
    for cte in &cmd.ctes {
        check_ident("cte.name", &cte.name)?;
        for col in &cte.columns {
            check_ident("cte.column", col)?;
        }
        validate_ast(&cte.base_query)?;
        if let Some(ref rq) = cte.recursive_query {
            validate_ast(rq)?;
        }
    }

    // ── DISTINCT ON ──────────────────────────────────────────────────
    for expr in &cmd.distinct_on {
        check_expr("distinct_on", expr)?;
    }

    // ── RETURNING ────────────────────────────────────────────────────
    if let Some(ref cols) = cmd.returning {
        for col in cols {
            check_expr("returning", col)?;
        }
    }

    // ── ON CONFLICT ──────────────────────────────────────────────────
    if let Some(ref oc) = cmd.on_conflict {
        for col in &oc.columns {
            check_ident("on_conflict.column", col)?;
        }
    }

    // ── FROM / USING tables ──────────────────────────────────────────
    for t in &cmd.from_tables {
        check_ident("from_tables", t)?;
    }
    for t in &cmd.using_tables {
        check_ident("using_tables", t)?;
    }

    // ── SET ops ──────────────────────────────────────────────────────
    for (_, sub) in &cmd.set_ops {
        validate_ast(sub)?;
    }

    // ── Source query (INSERT … SELECT) ───────────────────────────────
    if let Some(ref sq) = cmd.source_query {
        validate_ast(sq)?;
    }

    // ── HAVING ───────────────────────────────────────────────────────
    for cond in &cmd.having {
        check_expr("having", &cond.left)?;
        check_value("having", &cond.value)?;
    }

    // ── Channel (LISTEN/NOTIFY) ──────────────────────────────────────
    if let Some(ref ch) = cmd.channel {
        check_ident("channel", ch)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Qail;

    #[test]
    fn valid_simple_query_passes() {
        let cmd = Qail::get("users").columns(["id", "name"]);
        assert!(validate_ast(&cmd).is_ok());
    }

    #[test]
    fn sql_injection_in_table_rejected() {
        let cmd = Qail::get("users; DROP TABLE users; --");
        let err = validate_ast(&cmd).unwrap_err();
        assert_eq!(err.field, "table");
    }

    #[test]
    fn raw_sql_rejected() {
        let cmd = Qail::raw_sql("SELECT 1");
        let err = validate_ast(&cmd).unwrap_err();
        assert_eq!(err.field, "table");
    }

    #[test]
    fn raw_expr_rejected() {
        let cmd = Qail::get("users").columns_expr(vec![Expr::Raw("NOW()".to_string())]);
        let err = validate_ast(&cmd).unwrap_err();
        assert!(err.reason.contains("Raw"));
    }

    #[test]
    fn call_action_rejected() {
        let cmd = Qail {
            action: Action::Call,
            table: "my_proc()".to_string(),
            ..Default::default()
        };
        let err = validate_ast(&cmd).unwrap_err();
        assert_eq!(err.field, "action");
    }

    #[test]
    fn do_action_rejected() {
        let cmd = Qail {
            action: Action::Do,
            table: "plpgsql".to_string(),
            ..Default::default()
        };
        let err = validate_ast(&cmd).unwrap_err();
        assert_eq!(err.field, "action");
    }

    #[test]
    fn valid_qualified_name_passes() {
        let cmd = Qail::get("public.users").columns(["users.id", "users.name"]);
        assert!(validate_ast(&cmd).is_ok());
    }

    #[test]
    fn injection_in_join_table_rejected() {
        use crate::ast::JoinKind;
        let cmd = Qail::get("users").join(JoinKind::Left, "orders; DROP TABLE x", "users.id", "orders.user_id");
        let err = validate_ast(&cmd).unwrap_err();
        assert!(err.field.contains("joins"));
    }

    #[test]
    fn injection_in_column_rejected() {
        let cmd = Qail::get("users").columns(["id", "name; DROP TABLE x"]);
        let err = validate_ast(&cmd).unwrap_err();
        assert!(err.field.contains("columns"));
    }

    #[test]
    fn empty_table_name_passes() {
        // Some actions like TxnStart have empty table
        let cmd = Qail {
            action: Action::TxnStart,
            table: String::new(),
            ..Default::default()
        };
        assert!(validate_ast(&cmd).is_ok());
    }

    #[test]
    fn oversized_identifier_rejected() {
        let long_name = "a".repeat(64);
        let cmd = Qail::get(&long_name);
        let err = validate_ast(&cmd).unwrap_err();
        assert!(err.reason.contains("63"));
    }
}
