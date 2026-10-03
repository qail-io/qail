//! `Expr::Aggregate` rendering shared by the SELECT, ORDER BY and MERGE paths.

use crate::ast::{Condition, Expr, aggregate_sort_key, check_aggregate_shape, sort_order_sql};

/// `FUNC([DISTINCT] args [ORDER BY ...]) [WITHIN GROUP (ORDER BY ...)]
/// [FILTER (WHERE ...)]` without the alias, or a `/* ERROR: ... */` marker
/// for a shape the fields cannot render faithfully.
///
/// `render_col` receives a non-`*` `col`; `render_expr` renders arguments and
/// sort keys; `render_filter` renders the non-empty FILTER conditions.
pub(crate) fn aggregate_call_sql(
    expr: &Expr,
    render_col: &dyn Fn(&str) -> String,
    render_expr: &dyn Fn(&Expr) -> String,
    render_filter: &dyn Fn(&[Condition]) -> String,
) -> String {
    let Expr::Aggregate {
        col,
        func,
        distinct,
        filter,
        args,
        order_by,
        within_group,
        ..
    } = expr
    else {
        return "/* ERROR: not an aggregate */".to_string();
    };
    if let Err(message) = check_aggregate_shape(*func, col, *distinct, args, order_by, within_group)
    {
        return format!("/* ERROR: {} */", message.replace("*/", "* /"));
    }

    let mut sql = format!("{func}(");
    if *distinct {
        sql.push_str("DISTINCT ");
    }
    if args.is_empty() {
        if col == "*" {
            sql.push('*');
        } else {
            sql.push_str(&render_col(col));
        }
    } else {
        let args: Vec<String> = args.iter().map(render_expr).collect();
        sql.push_str(&args.join(", "));
    }
    if !order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&sort_keys_sql(order_by, render_expr));
    }
    sql.push(')');
    if !within_group.is_empty() {
        sql.push_str(" WITHIN GROUP (ORDER BY ");
        sql.push_str(&sort_keys_sql(within_group, render_expr));
        sql.push(')');
    }
    if let Some(conditions) = filter
        && !conditions.is_empty()
    {
        sql.push_str(" FILTER (WHERE ");
        sql.push_str(&render_filter(conditions));
        sql.push(')');
    }
    sql
}

fn sort_keys_sql(cages: &[crate::ast::Cage], render_expr: &dyn Fn(&Expr) -> String) -> String {
    cages
        .iter()
        .filter_map(|cage| aggregate_sort_key(cage).ok())
        .map(|(key, order)| format!("{}{}", render_expr(key), sort_order_sql(order)))
        .collect::<Vec<_>>()
        .join(", ")
}
