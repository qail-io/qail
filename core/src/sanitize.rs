//! AST structural sanitization for untrusted input.
//!
//! The text parser enforces identifier constraints (alphanumeric + `_` + `.`),
//! but externally provided ASTs (binary endpoints, APIs, generated payloads, etc.)
//! may bypass parser-level identifier checks and still reach execution.
//!
//! Call [`validate_ast`](crate::sanitize::validate_ast) on any `Qail` obtained from an untrusted source
//! (binary endpoint, external API, etc.) before execution.

use crate::ast::{
    Action, ConflictAction, Expr, MergeAction, MergeSource, Qail, TableConstraint, Value,
};
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
        && s.split('.').all(|part| {
            !part.is_empty()
                && part.len() <= MAX_IDENT_LEN
                && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        })
}

/// Validate an identifier, returning a `SanitizeError` if invalid.
fn check_ident(field: &str, value: &str) -> Result<(), SanitizeError> {
    if is_safe_identifier(value) {
        Ok(())
    } else {
        Err(SanitizeError {
            field: field.to_string(),
            value: value.chars().take(40).collect(),
            reason: "identifier parts must match [a-zA-Z0-9_] and be ≤63 chars".to_string(),
        })
    }
}

fn table_ref_error(field: &str, value: &str) -> SanitizeError {
    SanitizeError {
        field: field.to_string(),
        value: value.chars().take(40).collect(),
        reason: "table references must be identifier or identifier [AS] alias".to_string(),
    }
}

fn check_table_ref(field: &str, value: &str) -> Result<(), SanitizeError> {
    let parts = value.split_whitespace().collect::<Vec<_>>();
    match parts.as_slice() {
        [table] => check_ident(field, table),
        [table, alias] => {
            check_ident(field, table)?;
            check_ident(field, alias)
        }
        [table, as_keyword, alias] if as_keyword.eq_ignore_ascii_case("as") => {
            check_ident(field, table)?;
            check_ident(field, alias)
        }
        _ => Err(table_ref_error(field, value)),
    }
}

fn action_allows_table_alias(action: Action) -> bool {
    matches!(
        action,
        Action::Get
            | Action::Cnt
            | Action::Set
            | Action::Del
            | Action::Export
            | Action::Explain
            | Action::ExplainAnalyze
            | Action::Over
    )
}

fn check_fk_action(field: &str, value: &str) -> Result<(), SanitizeError> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', " ");
    if matches!(
        normalized.as_str(),
        "cascade" | "restrict" | "no action" | "set null" | "set default"
    ) {
        Ok(())
    } else {
        Err(SanitizeError {
            field: field.to_string(),
            value: value.chars().take(40).collect(),
            reason:
                "foreign key action must be cascade, restrict, no_action, set_null, or set_default"
                    .to_string(),
        })
    }
}

fn check_fk_deferrable(field: &str, value: &str) -> Result<(), SanitizeError> {
    let normalized = value.trim().to_ascii_lowercase().replace('_', " ");
    if matches!(
        normalized.as_str(),
        "deferrable"
            | "initially deferred"
            | "initially immediate"
            | "deferrable initially deferred"
            | "deferrable initially immediate"
    ) {
        Ok(())
    } else {
        Err(SanitizeError {
            field: field.to_string(),
            value: value.chars().take(40).collect(),
            reason: "foreign key deferrable clause must be deferrable, initially_deferred, or initially_immediate".to_string(),
        })
    }
}

/// Validate an `Expr` node for unsafe patterns.
///
/// - `Expr::Named` must be a safe identifier.
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
            if col != "*" {
                check_ident(field, col)?;
            }
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            if let Some(conditions) = filter {
                for cond in conditions {
                    check_expr(&format!("{field}.filter"), &cond.left)?;
                    check_value(&format!("{field}.filter"), &cond.value)?;
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
        Expr::Literal(_) => Ok(()),
        Expr::JsonAccess {
            column,
            alias,
            path_segments,
            ..
        } => {
            check_ident(field, column)?;
            for (key, _) in path_segments {
                // Integer indices are fine; string keys must be safe identifiers
                if key.parse::<i64>().is_err() && !is_safe_identifier(key) {
                    return Err(SanitizeError {
                        field: format!("{field}.json_path"),
                        value: key.chars().take(40).collect(),
                        reason: "JSON path key must be a safe identifier or integer".to_string(),
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
        Expr::Exists { query, alias, .. } => {
            validate_ast(query)?;
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        // For all other complex Expr variants, validate aliases where present
        Expr::Window {
            name,
            func,
            partition,
            params,
            order,
            ..
        } => {
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
        Expr::Case {
            when_clauses,
            else_value,
            alias,
        } => {
            for (cond, val) in when_clauses {
                check_expr(&format!("{field}.case_when"), &cond.left)?;
                check_value(&format!("{field}.case_when"), &cond.value)?;
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
        Expr::Collate {
            expr,
            collation,
            alias,
        } => {
            check_expr(&format!("{field}.collate_expr"), expr)?;
            check_ident(&format!("{field}.collation"), collation)?;
            if let Some(a) = alias {
                check_ident(&format!("{field}.alias"), a)?;
            }
            Ok(())
        }
        Expr::FieldAccess {
            expr,
            field: f,
            alias,
        } => {
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
/// and rejects dangerous procedural actions.
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

    // ── Table name ───────────────────────────────────────────────────
    if !cmd.table.is_empty() {
        if action_allows_table_alias(cmd.action) {
            check_table_ref("table", &cmd.table)?;
        } else {
            check_ident("table", &cmd.table)?;
        }
    }

    // ── Columns ──────────────────────────────────────────────────────
    for (i, col) in cmd.columns.iter().enumerate() {
        check_expr(&format!("columns[{i}]"), col)?;
    }

    // ── Table Constraints ────────────────────────────────────────────
    for (i, constraint) in cmd.table_constraints.iter().enumerate() {
        match constraint {
            TableConstraint::Unique(cols) | TableConstraint::PrimaryKey(cols) => {
                for col in cols {
                    check_ident(&format!("table_constraints[{i}].column"), col)?;
                }
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
                deferrable,
            } => {
                if let Some(name) = name {
                    check_ident(&format!("table_constraints[{i}].name"), name)?;
                }
                for col in columns {
                    check_ident(&format!("table_constraints[{i}].column"), col)?;
                }
                check_ident(&format!("table_constraints[{i}].ref_table"), ref_table)?;
                for col in ref_columns {
                    check_ident(&format!("table_constraints[{i}].ref_column"), col)?;
                }
                if let Some(action) = on_delete {
                    check_fk_action(&format!("table_constraints[{i}].on_delete"), action)?;
                }
                if let Some(action) = on_update {
                    check_fk_action(&format!("table_constraints[{i}].on_update"), action)?;
                }
                if let Some(clause) = deferrable {
                    check_fk_deferrable(&format!("table_constraints[{i}].deferrable"), clause)?;
                }
            }
        }
    }

    // ── Joins ────────────────────────────────────────────────────────
    for (i, join) in cmd.joins.iter().enumerate() {
        check_table_ref(&format!("joins[{i}].table"), &join.table)?;
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
        if let ConflictAction::DoUpdate { assignments } = &oc.action {
            for (col, expr) in assignments {
                check_ident("on_conflict.assignment.column", col)?;
                check_expr("on_conflict.assignment.expr", expr)?;
            }
        }
    }

    // ── MERGE ────────────────────────────────────────────────────────
    if let Some(ref merge) = cmd.merge {
        if let Some(alias) = &merge.target_alias {
            check_ident("merge.target_alias", alias)?;
        }
        match &merge.source {
            MergeSource::Table { name, alias } => {
                if let Some(alias) = alias {
                    check_ident("merge.source.table", name)?;
                    check_ident("merge.source.alias", alias)?;
                } else {
                    check_table_ref("merge.source.table", name)?;
                }
            }
            MergeSource::Query { query, alias } => {
                validate_ast(query)?;
                if let Some(alias) = alias {
                    check_ident("merge.source.alias", alias)?;
                }
            }
        }
        for cond in &merge.on {
            check_expr("merge.on.left", &cond.left)?;
            check_value("merge.on.value", &cond.value)?;
        }
        for clause in &merge.clauses {
            for cond in &clause.condition {
                check_expr("merge.clause.condition.left", &cond.left)?;
                check_value("merge.clause.condition.value", &cond.value)?;
            }
            match &clause.action {
                MergeAction::Update { assignments } => {
                    for (col, expr) in assignments {
                        check_ident("merge.update.column", col)?;
                        check_expr("merge.update.expr", expr)?;
                    }
                }
                MergeAction::Insert { columns, values } => {
                    for col in columns {
                        check_ident("merge.insert.column", col)?;
                    }
                    for expr in values {
                        check_expr("merge.insert.expr", expr)?;
                    }
                }
                MergeAction::Delete | MergeAction::DoNothing => {}
            }
        }
    }

    // ── FROM / USING tables ──────────────────────────────────────────
    for t in &cmd.from_tables {
        check_table_ref("from_tables", t)?;
    }
    for t in &cmd.using_tables {
        check_table_ref("using_tables", t)?;
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
    use crate::ast::{Operator, Qail};

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
    fn valid_long_qualified_identifier_parts_pass() {
        let schema = "s".repeat(MAX_IDENT_LEN);
        let table = "t".repeat(MAX_IDENT_LEN);
        let cmd = Qail::get(format!("{schema}.{table}")).columns(["id"]);

        assert!(validate_ast(&cmd).is_ok());
    }

    #[test]
    fn empty_qualified_identifier_part_is_rejected() {
        let err = validate_ast(&Qail::get("public..users")).unwrap_err();

        assert_eq!(err.field, "table");
    }

    #[test]
    fn query_and_mutation_table_aliases_pass_sanitizer() {
        assert!(validate_ast(&Qail::get("public.users u")).is_ok());
        assert!(validate_ast(&Qail::set("public.users AS u").set_value("active", true)).is_ok());
        assert!(validate_ast(&Qail::del("public.users u")).is_ok());
    }

    #[test]
    fn ddl_table_alias_shape_is_rejected() {
        let err = validate_ast(&Qail::make("public.users u")).unwrap_err();
        assert_eq!(err.field, "table");
    }

    #[test]
    fn injection_in_join_table_rejected() {
        use crate::ast::JoinKind;
        let cmd = Qail::get("users").join(
            JoinKind::Left,
            "orders; DROP TABLE x",
            "users.id",
            "orders.user_id",
        );
        let err = validate_ast(&cmd).unwrap_err();
        assert!(err.field.contains("joins"));
    }

    #[test]
    fn malformed_join_table_reference_rejected() {
        use crate::ast::JoinKind;
        let cmd = Qail::get("users").join(
            JoinKind::Left,
            "orders DROP TABLE x",
            "users.id",
            "orders.user_id",
        );
        let err = validate_ast(&cmd).unwrap_err();
        assert!(err.field.contains("joins"));
    }

    #[test]
    fn update_from_and_delete_using_aliases_pass_sanitizer() {
        let update = Qail::set("orders")
            .set_value("status", "paid")
            .update_from(["accounts a"]);
        assert!(validate_ast(&update).is_ok());

        let delete = Qail::del("orders").delete_using(["accounts a"]);
        assert!(validate_ast(&delete).is_ok());
    }

    #[test]
    fn merge_inline_source_alias_passes_sanitizer() {
        let cmd = Qail::merge_into("orders")
            .target_alias("o")
            .using_table("stage_orders s")
            .merge_on_column("o.id", Operator::Eq, "s.order_id")
            .when_matched_do_nothing();

        assert!(validate_ast(&cmd).is_ok());
    }

    #[test]
    fn malformed_merge_source_table_reference_rejected() {
        let cmd = Qail::merge_into("orders")
            .using_table("stage_orders DROP TABLE x")
            .merge_on_column("orders.id", Operator::Eq, "stage_orders.order_id")
            .when_matched_do_nothing();

        let err = validate_ast(&cmd).unwrap_err();
        assert_eq!(err.field, "merge.source.table");
    }

    #[test]
    fn injection_in_update_from_and_delete_using_rejected() {
        let update = Qail::set("orders")
            .set_value("status", "paid")
            .update_from(["accounts; DROP TABLE accounts"]);
        let err = validate_ast(&update).unwrap_err();
        assert_eq!(err.field, "from_tables");

        let delete = Qail::del("orders").delete_using(["accounts; DROP TABLE accounts"]);
        let err = validate_ast(&delete).unwrap_err();
        assert_eq!(err.field, "using_tables");
    }

    #[test]
    fn injection_in_column_rejected() {
        let cmd = Qail::get("users").columns(["id", "name; DROP TABLE x"]);
        let err = validate_ast(&cmd).unwrap_err();
        assert!(err.field.contains("columns"));
    }

    #[test]
    fn on_conflict_update_assignment_expression_injection_rejected() {
        let cmd = Qail::add("users")
            .set_value("id", 1)
            .set_value("name", "Alice")
            .on_conflict_update(
                &["id"],
                &[(
                    "name",
                    Expr::Named("EXCLUDED.name, is_admin = true".to_string()),
                )],
            );

        let err = validate_ast(&cmd).unwrap_err();

        assert_eq!(err.field, "on_conflict.assignment.expr");
        assert!(
            err.value.contains("EXCLUDED.name"),
            "unexpected rejected value: {}",
            err.value
        );
    }

    #[test]
    fn on_conflict_update_assignment_column_injection_rejected() {
        let cmd = Qail::add("users")
            .set_value("id", 1)
            .set_value("name", "Alice")
            .on_conflict_update(
                &["id"],
                &[("name, is_admin", Expr::Named("EXCLUDED.name".to_string()))],
            );

        let err = validate_ast(&cmd).unwrap_err();

        assert_eq!(err.field, "on_conflict.assignment.column");
    }

    #[test]
    fn aggregate_filter_value_expression_injection_rejected() {
        use crate::ast::{AggregateFunc, Condition, Operator, Value};

        let mut cmd = Qail::get("events");
        cmd.columns.push(Expr::Aggregate {
            col: "id".to_string(),
            func: AggregateFunc::Count,
            distinct: false,
            filter: Some(vec![Condition {
                left: Expr::Named("direction".to_string()),
                op: Operator::Eq,
                value: Value::Expr(Box::new(Expr::Named("bad;DROP".to_string()))),
                is_array_unnest: false,
            }]),
            alias: None,
        });

        let err = validate_ast(&cmd).unwrap_err();
        assert_eq!(err.field, "columns[0].filter");
    }

    #[test]
    fn count_star_aggregate_passes_sanitizer() {
        use crate::ast::AggregateFunc;

        let mut cmd = Qail::get("events");
        cmd.columns.push(Expr::Aggregate {
            col: "*".to_string(),
            func: AggregateFunc::Count,
            distinct: false,
            filter: None,
            alias: Some("total".to_string()),
        });

        assert!(validate_ast(&cmd).is_ok());
    }

    #[test]
    fn case_when_complex_condition_expression_passes_sanitizer() {
        use crate::ast::{Condition, Operator, Value};

        let mut cmd = Qail::get("users");
        cmd.columns.push(Expr::Case {
            when_clauses: vec![(
                Condition {
                    left: Expr::Cast {
                        expr: Box::new(Expr::JsonAccess {
                            column: "profile".to_string(),
                            path_segments: vec![("active".to_string(), true)],
                            alias: None,
                        }),
                        target_type: "integer".to_string(),
                        alias: None,
                    },
                    op: Operator::Gt,
                    value: Value::Int(0),
                    is_array_unnest: false,
                },
                Box::new(Expr::Literal(Value::String("active".to_string()))),
            )],
            else_value: Some(Box::new(Expr::Literal(Value::String(
                "inactive".to_string(),
            )))),
            alias: Some("status_label".to_string()),
        });

        assert!(validate_ast(&cmd).is_ok());
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
