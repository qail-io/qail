//! INSERT SQL generation.

use crate::ast::*;
use crate::transpiler::SqlGenerator;
use crate::transpiler::conditions::ConditionToSql;
use crate::transpiler::dialect::Dialect;

/// Generate INSERT INTO SQL with VALUES, ON CONFLICT, and RETURNING clauses.
pub fn build_insert(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();
    let mut sql = String::from("INSERT INTO ");
    sql.push_str(&generator.quote_identifier(&cmd.table));

    // For ADD queries, we use columns and first cage contains values
    let cols: Vec<String> = cmd
        .columns
        .iter()
        .map(|c| render_insert_column(c, generator.as_ref()))
        .collect();

    if !cols.is_empty() {
        sql.push_str(" (");
        sql.push_str(&cols.join(", "));
        sql.push(')');
    }

    // OVERRIDING clause for GENERATED columns
    if let Some(ref overriding) = cmd.overriding {
        match overriding {
            OverridingKind::SystemValue => sql.push_str(" OVERRIDING SYSTEM VALUE"),
            OverridingKind::UserValue => sql.push_str(" OVERRIDING USER VALUE"),
        }
    }

    // DEFAULT VALUES - insert a row with all defaults
    if cmd.default_values {
        sql.push_str(" DEFAULT VALUES");
    } else if let Some(ref source_query) = cmd.source_query {
        // INSERT...SELECT: use source_query if present
        use crate::transpiler::ToSql;
        sql.push(' ');
        sql.push_str(&source_query.to_sql_with_dialect(dialect));
    } else if let Some(cage) = cmd.cages.first() {
        // Traditional INSERT with VALUES
        let values: Vec<String> = cage
            .conditions
            .iter()
            .map(|c| c.to_value_sql(generator.as_ref()))
            .collect();

        if !values.is_empty() {
            sql.push_str(" VALUES (");
            sql.push_str(&values.join(", "));
            sql.push(')');
        }
    }

    // ON CONFLICT clause
    if let Some(on_conflict) = &cmd.on_conflict {
        sql.push_str(&build_on_conflict(
            on_conflict,
            &dialect,
            generator.as_ref(),
        ));
    }

    match &cmd.returning {
        None => sql.push_str(" RETURNING *"), // Default: return all
        Some(cols) if cols.is_empty() => {}   // Explicitly no RETURNING
        Some(cols) => {
            let col_strs: Vec<String> = cols
                .iter()
                .map(|e| render_sql_expr(e, generator.as_ref()))
                .collect();
            sql.push_str(" RETURNING ");
            sql.push_str(&col_strs.join(", "));
        }
    }

    sql
}

/// Build ON CONFLICT clause (Postgres style)
fn build_on_conflict(
    on_conflict: &OnConflict,
    _dialect: &Dialect,
    generator: &dyn SqlGenerator,
) -> String {
    // Postgres supports ON CONFLICT.
    build_on_conflict_postgres(on_conflict, generator)
}

/// PostgreSQL style: ON CONFLICT (cols) DO UPDATE SET ... or DO NOTHING
fn build_on_conflict_postgres(on_conflict: &OnConflict, generator: &dyn SqlGenerator) -> String {
    let mut sql = String::from(" ON CONFLICT (");
    let cols: Vec<String> = on_conflict
        .columns
        .iter()
        .map(|c| generator.quote_identifier(c))
        .collect();
    sql.push_str(&cols.join(", "));
    sql.push(')');

    match &on_conflict.action {
        ConflictAction::DoNothing => {
            sql.push_str(" DO NOTHING");
        }
        ConflictAction::DoUpdate { assignments } => {
            sql.push_str(" DO UPDATE SET ");
            let sets: Vec<String> = assignments
                .iter()
                .map(|(col, expr)| {
                    format!(
                        "{} = {}",
                        generator.quote_identifier(col),
                        render_sql_expr(expr, generator)
                    )
                })
                .collect();
            sql.push_str(&sets.join(", "));
        }
    }

    sql
}

fn render_insert_column(expr: &Expr, generator: &dyn SqlGenerator) -> String {
    match expr {
        Expr::Named(name) => generator.quote_identifier(name),
        _ => "/* ERROR: Invalid insert column */".to_string(),
    }
}

fn render_sql_expr(expr: &Expr, generator: &dyn SqlGenerator) -> String {
    match expr {
        Expr::Star => "*".to_string(),
        Expr::Named(name) => render_named_expr(name, generator),
        Expr::Literal(value) => value.to_string(),
        Expr::Binary {
            left, op, right, ..
        } => match op {
            BinaryOp::IsNull => format!("({} IS NULL)", render_sql_expr(left, generator)),
            BinaryOp::IsNotNull => format!("({} IS NOT NULL)", render_sql_expr(left, generator)),
            _ => format!(
                "({} {} {})",
                render_sql_expr(left, generator),
                op,
                render_sql_expr(right, generator)
            ),
        },
        Expr::FunctionCall { name, args, .. } => {
            let Some(function) = render_function_name(name) else {
                return "/* ERROR: Invalid function name */".to_string();
            };
            let args = args
                .iter()
                .map(|arg| render_sql_expr(arg, generator))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{function}({args})")
        }
        Expr::Cast {
            expr, target_type, ..
        } => {
            let Some(target_type) = checked_sql_type_fragment(target_type) else {
                return "/* ERROR: Invalid cast target type */".to_string();
            };
            format!("{}::{}", render_sql_expr(expr, generator), target_type)
        }
        Expr::JsonAccess {
            column,
            path_segments,
            ..
        } => render_json_access(column, path_segments, generator),
        Expr::Collate {
            expr, collation, ..
        } => format!(
            "{} COLLATE {}",
            render_sql_expr(expr, generator),
            render_qualified_identifier(collation, generator)
        ),
        Expr::FieldAccess { expr, field, .. } => format!(
            "({}).{}",
            render_sql_expr(expr, generator),
            render_qualified_identifier(field, generator)
        ),
        _ => "/* ERROR: Invalid expression */".to_string(),
    }
}

fn render_named_expr(name: &str, generator: &dyn SqlGenerator) -> String {
    if name == "*"
        || name.starts_with('\'')
        || name.starts_with('"')
        || name.starts_with(':')
        || name.starts_with('$')
        || name.parse::<f64>().is_ok()
        || name.eq_ignore_ascii_case("NULL")
        || name.eq_ignore_ascii_case("TRUE")
        || name.eq_ignore_ascii_case("FALSE")
    {
        name.to_string()
    } else {
        generator.quote_identifier(name)
    }
}

fn render_function_name(name: &str) -> Option<String> {
    if name.is_empty()
        || name.contains('\0')
        || name.split('.').any(str::is_empty)
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.')
    {
        None
    } else {
        Some(name.to_uppercase())
    }
}

fn checked_sql_type_fragment(fragment: &str) -> Option<String> {
    let fragment = fragment.trim();
    if fragment.is_empty()
        || fragment.contains('\0')
        || fragment.contains(';')
        || fragment.contains('\'')
        || fragment.contains('"')
        || fragment.contains("--")
        || fragment.contains("/*")
        || fragment.contains("*/")
        || !fragment.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'_' | b'.' | b' ' | b'(' | b')' | b',' | b'[' | b']' | b'%' | b'+' | b'-'
                )
        })
    {
        None
    } else {
        Some(fragment.to_string())
    }
}

fn render_qualified_identifier(value: &str, generator: &dyn SqlGenerator) -> String {
    if value.is_empty() || value.as_bytes().contains(&0) || value.split('.').any(str::is_empty) {
        "/* ERROR: Invalid identifier */".to_string()
    } else {
        generator.quote_identifier(value)
    }
}

fn render_json_access(
    column: &str,
    path_segments: &[(String, bool)],
    generator: &dyn SqlGenerator,
) -> String {
    let mut sql = generator.quote_identifier(column);
    for (path, as_text) in path_segments {
        let op = if *as_text { "->>" } else { "->" };
        if path.parse::<i64>().is_ok() {
            sql.push_str(&format!("{}{}", op, path));
        } else {
            sql.push_str(&format!(
                "{}'{}'",
                op,
                crate::transpiler::escape_sql_string_literal(path)
            ));
        }
    }
    sql
}
