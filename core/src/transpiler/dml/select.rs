//! SELECT SQL generation.

use crate::ast::*;
use crate::transpiler::conditions::{ConditionToSql, read_only_subquery_sql};
use crate::transpiler::dialect::Dialect;
use crate::transpiler::identifier::{
    render_table_reference, table_reference_base, table_reference_sql_qualifier,
};
use crate::transpiler::traits::{SqlGenerator, escape_sql_string_literal};

/// Generate SELECT SQL from a QAIL command, including CTEs, joins, filtering, grouping, and ordering.
pub fn build_select(cmd: &Qail, dialect: Dialect) -> String {
    let generator = dialect.generator();

    // CTE prefix: WITH cte1 AS (...), cte2 AS (...)
    let cte_prefix = if !cmd.ctes.is_empty() {
        let has_recursive = cmd.ctes.iter().any(|c| c.recursive);
        let cte_parts: Vec<String> = cmd
            .ctes
            .iter()
            .map(|cte| super::cte::build_single_cte(cte, dialect))
            .collect();
        if has_recursive {
            format!("WITH RECURSIVE {} ", cte_parts.join(", "))
        } else {
            format!("WITH {} ", cte_parts.join(", "))
        }
    } else {
        String::new()
    };

    let mut sql = if !cmd.distinct_on.is_empty() {
        let exprs: Vec<String> = cmd
            .distinct_on
            .iter()
            .map(|e| render_expr_for_orderby(e, generator.as_ref(), cmd))
            .collect();
        format!("{}SELECT DISTINCT ON ({}) ", cte_prefix, exprs.join(", "))
    } else if cmd.distinct {
        format!("{}SELECT DISTINCT ", cte_prefix)
    } else {
        format!("{}SELECT ", cte_prefix)
    };

    if cmd.columns.is_empty() {
        sql.push('*');
    } else {
        let cols: Vec<String> = cmd
            .columns
            .iter()
            .map(|c| {
                match c {
                    Expr::Star => "*".to_string(),
                    Expr::Named(name) => generator.quote_identifier(name),
                    Expr::Aliased { name, alias } => format!(
                        "{} AS {}",
                        generator.quote_identifier(name),
                        generator.quote_identifier(alias)
                    ),
                    Expr::Case {
                        when_clauses,
                        else_value,
                        alias,
                    } => {
                        let mut case_sql = String::from("CASE");
                        for (cond, val) in when_clauses {
                            case_sql.push_str(&format!(
                                " WHEN {} THEN {}",
                                cond.to_sql(generator.as_ref(), Some(cmd)),
                                render_expr_for_orderby(val, generator.as_ref(), cmd)
                            ));
                        }
                        if let Some(e) = else_value {
                            case_sql.push_str(&format!(
                                " ELSE {}",
                                render_expr_for_orderby(e, generator.as_ref(), cmd)
                            ));
                        }
                        case_sql.push_str(" END");
                        if let Some(a) = alias {
                            format!("{} AS {}", case_sql, generator.quote_identifier(a))
                        } else {
                            case_sql
                        }
                    }
                    Expr::JsonAccess {
                        column,
                        path_segments,
                        alias,
                    } => {
                        let expr = render_json_access(column, path_segments, generator.as_ref());
                        if let Some(a) = alias {
                            format!("{} AS {}", expr, generator.quote_identifier(a))
                        } else {
                            expr
                        }
                    }
                    Expr::FunctionCall { name, args, alias } => {
                        if name.eq_ignore_ascii_case("case") {
                            // case(when_cond, then_val, else_val) -> CASE WHEN ... THEN ... ELSE ... END
                            if args.len() >= 3 {
                                let cond_str = args[0].to_string();
                                let then_str = args[1].to_string();
                                let else_str = args[2].to_string();

                                // Arg 0: WHEN condition (Raw preferred)
                                let cond_sql =
                                    if cond_str.starts_with('{') && cond_str.ends_with('}') {
                                        cond_str[1..cond_str.len() - 1].to_string()
                                    } else {
                                        cond_str
                                    };

                                // Arg 1: THEN value (Quoted unless raw)
                                let then_sql =
                                    if then_str.starts_with('{') && then_str.ends_with('}') {
                                        then_str[1..then_str.len() - 1].to_string()
                                    } else {
                                        generator.quote_identifier(&then_str)
                                    };

                                // Arg 2: ELSE value (Quoted unless raw)
                                let else_sql =
                                    if else_str.starts_with('{') && else_str.ends_with('}') {
                                        else_str[1..else_str.len() - 1].to_string()
                                    } else {
                                        generator.quote_identifier(&else_str)
                                    };

                                let expr = format!(
                                    "CASE WHEN {} THEN {} ELSE {} END",
                                    cond_sql, then_sql, else_sql
                                );
                                if let Some(a) = alias {
                                    format!("{} AS {}", expr, generator.quote_identifier(a))
                                } else {
                                    expr
                                }
                            } else {
                                // Invalid case call, fallback to standard function
                                let args_sql: Vec<String> =
                                    args.iter().map(|a| a.to_string()).collect();
                                let expr = format!("CASE({})", args_sql.join(", "));
                                if let Some(a) = alias {
                                    format!("{} AS {}", expr, generator.quote_identifier(a))
                                } else {
                                    expr
                                }
                            }
                        } else {
                            // Standard Function - transpile each arg expression
                            let Some(function) = render_function_name(name) else {
                                return "/* ERROR: Invalid function name */".to_string();
                            };
                            let args_sql: Vec<String> = args
                                .iter()
                                .map(|a| {
                                    let arg_str = a.to_string();
                                    if arg_str.starts_with('{') && arg_str.ends_with('}') {
                                        // Raw SQL block: {content} -> content
                                        arg_str[1..arg_str.len() - 1].to_string()
                                    } else {
                                        // For expressions (especially binary), don't quote
                                        match a {
                                            Expr::Named(n) => {
                                                // Don't quote if already quoted, is a param, or is numeric
                                                if n.starts_with('\'')
                                                    || n.starts_with('"')
                                                    || n.starts_with(':')
                                                    || n.starts_with('$')
                                                    || n.parse::<f64>().is_ok()
                                                    || n.eq_ignore_ascii_case("NULL")
                                                    || n.eq_ignore_ascii_case("TRUE")
                                                    || n.eq_ignore_ascii_case("FALSE")
                                                {
                                                    n.clone()
                                                } else {
                                                    generator.quote_identifier(n)
                                                }
                                            }
                                            Expr::Star => "*".to_string(),
                                            _ => {
                                                render_expr_for_orderby(a, generator.as_ref(), cmd)
                                            }
                                        }
                                    }
                                })
                                .collect();
                            let expr = format!("{}({})", function, args_sql.join(", "));
                            if let Some(a) = alias {
                                format!("{} AS {}", expr, generator.quote_identifier(a))
                            } else {
                                expr
                            }
                        }
                    }
                    Expr::Aggregate {
                        col,
                        func,
                        distinct,
                        filter,
                        alias,
                    } => {
                        // Render aggregate function: COUNT(*), COUNT(DISTINCT col), SUM(col), etc.
                        let col_expr = if col == "*" {
                            "*".to_string()
                        } else {
                            generator.quote_identifier(col)
                        };
                        let mut expr = if *distinct {
                            format!("{}(DISTINCT {})", func, col_expr)
                        } else {
                            format!("{}({})", func, col_expr)
                        };

                        if let Some(conditions) = filter
                            && !conditions.is_empty()
                        {
                            let filter_parts: Vec<String> = conditions
                                .iter()
                                .map(|c| c.to_sql(generator.as_ref(), Some(cmd)))
                                .collect();
                            expr.push_str(&format!(
                                " FILTER (WHERE {})",
                                filter_parts.join(" AND ")
                            ));
                        }

                        if let Some(a) = alias {
                            format!("{} AS {}", expr, generator.quote_identifier(a))
                        } else {
                            expr
                        }
                    }
                    Expr::Cast {
                        expr,
                        target_type,
                        alias,
                    } => {
                        let Some(target_type) = checked_sql_type_fragment(target_type) else {
                            return "/* ERROR: Invalid cast target type */".to_string();
                        };
                        let cast_expr = format!(
                            "{}::{}",
                            render_expr_for_orderby(expr, generator.as_ref(), cmd),
                            target_type
                        );
                        if let Some(a) = alias {
                            format!("{} AS {}", cast_expr, generator.quote_identifier(a))
                        } else {
                            cast_expr
                        }
                    }
                    Expr::Collate {
                        expr,
                        collation,
                        alias,
                    } => {
                        let expr = render_expr_for_orderby(expr, generator.as_ref(), cmd);
                        let collation = render_qualified_identifier(collation, generator.as_ref());
                        let collate_expr = format!("{expr} COLLATE {collation}");
                        if let Some(a) = alias {
                            format!("{} AS {}", collate_expr, generator.quote_identifier(a))
                        } else {
                            collate_expr
                        }
                    }
                    Expr::FieldAccess { expr, field, alias } => {
                        let field_expr = format!(
                            "({}).{}",
                            render_expr_for_orderby(expr, generator.as_ref(), cmd),
                            render_qualified_identifier(field, generator.as_ref())
                        );
                        if let Some(a) = alias {
                            format!("{} AS {}", field_expr, generator.quote_identifier(a))
                        } else {
                            field_expr
                        }
                    }
                    Expr::Binary { alias, .. }
                    | Expr::SpecialFunction { alias, .. }
                    | Expr::ArrayConstructor { alias, .. }
                    | Expr::RowConstructor { alias, .. }
                    | Expr::Subscript { alias, .. } => append_alias(
                        render_expr_for_orderby(c, generator.as_ref(), cmd),
                        alias,
                        generator.as_ref(),
                    ),
                    Expr::Literal(value) => {
                        render_value_for_expression(value, generator.as_ref(), cmd)
                    }
                    Expr::Subquery { query, alias } => append_alias(
                        format!("({})", read_only_subquery_sql(query)),
                        alias,
                        generator.as_ref(),
                    ),
                    Expr::Exists {
                        query,
                        negated,
                        alias,
                    } => {
                        let exists_sql = if *negated {
                            format!("NOT EXISTS ({})", read_only_subquery_sql(query))
                        } else {
                            format!("EXISTS ({})", read_only_subquery_sql(query))
                        };
                        append_alias(exists_sql, alias, generator.as_ref())
                    }
                    Expr::Window {
                        name,
                        func,
                        params,
                        partition,
                        order,
                        frame,
                    } => {
                        // Window function: FUNC(args) OVER (PARTITION BY x ORDER BY y) AS alias
                        let params_str = if params.is_empty() {
                            String::new()
                        } else {
                            params
                                .iter()
                                .map(|v| render_expr_for_orderby(v, generator.as_ref(), cmd))
                                .collect::<Vec<_>>()
                                .join(", ")
                        };
                        let Some(function) = render_function_name(func) else {
                            return "/* ERROR: Invalid window function name */".to_string();
                        };

                        let mut over_clause = String::from("OVER (");
                        if !partition.is_empty() {
                            over_clause.push_str("PARTITION BY ");
                            let quoted_partition: Vec<String> = partition
                                .iter()
                                .map(|p| generator.quote_identifier(p))
                                .collect();
                            over_clause.push_str(&quoted_partition.join(", "));
                            if !order.is_empty() {
                                over_clause.push(' ');
                            }
                        }
                        if !order.is_empty() {
                            over_clause.push_str("ORDER BY ");
                            let order_parts: Vec<String> = order
                                .iter()
                                .map(|cage| {
                                    let col_str = if let Some(cond) = cage.conditions.first() {
                                        match &cond.left {
                                            Expr::Named(n) => generator.quote_identifier(n),
                                            expr => render_expr_for_orderby(
                                                expr,
                                                generator.as_ref(),
                                                cmd,
                                            ),
                                        }
                                    } else {
                                        return String::new();
                                    };
                                    match &cage.kind {
                                        CageKind::Sort(SortOrder::Asc) => {
                                            format!("{} ASC", col_str)
                                        }
                                        CageKind::Sort(SortOrder::Desc) => {
                                            format!("{} DESC", col_str)
                                        }
                                        CageKind::Sort(SortOrder::AscNullsFirst) => {
                                            format!("{} ASC NULLS FIRST", col_str)
                                        }
                                        CageKind::Sort(SortOrder::AscNullsLast) => {
                                            format!("{} ASC NULLS LAST", col_str)
                                        }
                                        CageKind::Sort(SortOrder::DescNullsFirst) => {
                                            format!("{} DESC NULLS FIRST", col_str)
                                        }
                                        CageKind::Sort(SortOrder::DescNullsLast) => {
                                            format!("{} DESC NULLS LAST", col_str)
                                        }
                                        _ => String::new(),
                                    }
                                })
                                .filter(|s| !s.is_empty())
                                .collect();
                            over_clause.push_str(&order_parts.join(", "));
                        }

                        if let Some(fr) = frame {
                            over_clause.push(' ');
                            match fr {
                                WindowFrame::Rows { start, end } => {
                                    over_clause.push_str(&format!(
                                        "ROWS BETWEEN {} AND {}",
                                        bound_to_sql(start),
                                        bound_to_sql(end)
                                    ));
                                }
                                WindowFrame::Range { start, end } => {
                                    over_clause.push_str(&format!(
                                        "RANGE BETWEEN {} AND {}",
                                        bound_to_sql(start),
                                        bound_to_sql(end)
                                    ));
                                }
                            }
                        }

                        over_clause.push(')');
                        format!(
                            "{}({}) {} AS {}",
                            function,
                            params_str,
                            over_clause,
                            generator.quote_identifier(name)
                        )
                    }
                    Expr::Def { .. } | Expr::Mod { .. } => {
                        "/* ERROR: Invalid select expression */".to_string()
                    }
                }
            })
            .collect();
        sql.push_str(&cols.join(", "));
    }

    // FROM (with optional ONLY for inheritance control)
    if cmd.only_table {
        sql.push_str(" FROM ONLY ");
    } else {
        sql.push_str(" FROM ");
    }
    sql.push_str(&render_table_reference(&cmd.table, generator.as_ref()));

    // TABLESAMPLE
    let sample = cmd.sample.or_else(|| {
        cmd.cages.iter().find_map(|cage| match &cage.kind {
            CageKind::Sample(percent) => Some((SampleMethod::Bernoulli, *percent as f64, None)),
            _ => None,
        })
    });

    if let Some((method, percent, seed)) = sample {
        let method_str = match method {
            SampleMethod::Bernoulli => "BERNOULLI",
            SampleMethod::System => "SYSTEM",
        };
        sql.push_str(&format!(" TABLESAMPLE {}({})", method_str, percent));
        if let Some(s) = seed {
            sql.push_str(&format!(" REPEATABLE({})", s));
        }
    }

    // JOINS
    for join in &cmd.joins {
        let (kind, needs_on) = match join.kind {
            JoinKind::Inner => ("INNER", true),
            JoinKind::Left => ("LEFT", true),
            JoinKind::Right => ("RIGHT", true),
            JoinKind::Lateral => ("LATERAL", true),
            JoinKind::Full => ("FULL OUTER", true),
            JoinKind::Cross => ("CROSS", false),
        };
        // Join: target.source_singular_id = source.id
        let source_base = table_reference_base(&cmd.table);
        let source_singular = source_base
            .rsplit('.')
            .next()
            .unwrap_or(source_base)
            .trim_end_matches('s');

        let target_table = render_table_reference(&join.table, generator.as_ref());
        let target_qualifier = table_reference_sql_qualifier(&join.table)
            .map(|qualifier| generator.quote_identifier(qualifier))
            .unwrap_or_else(|| generator.quote_identifier(&join.table));
        let source_fk = format!("{}_id", source_singular);
        let source_table = table_reference_sql_qualifier(&cmd.table)
            .map(|qualifier| generator.quote_identifier(qualifier))
            .unwrap_or_else(|| generator.quote_identifier(&cmd.table));

        if let Some(on_conds) = &join.on {
            let on_sql: Vec<String> = on_conds
                .iter()
                .map(|c| c.to_sql(generator.as_ref(), Some(cmd)))
                .collect();
            sql.push_str(&format!(
                " {} JOIN {} ON {}",
                kind,
                target_table,
                on_sql.join(" AND ")
            ));
        } else if join.on_true {
            // Explicit ON TRUE (unconditional join, used for CTE joins)
            sql.push_str(&format!(" {} JOIN {} ON TRUE", kind, target_table));
        } else if needs_on {
            sql.push_str(&format!(
                " {} JOIN {} ON {}.{} = {}.id",
                kind,
                target_table,
                target_qualifier,
                generator.quote_identifier(&source_fk),
                source_table
            ));
        } else {
            sql.push_str(&format!(" {} JOIN {}", kind, target_table));
        }
    }

    // Prepare for GROUP BY check
    let has_aggregates = cmd
        .columns
        .iter()
        .any(|c| matches!(c, Expr::Aggregate { .. }));
    let mut non_aggregated_cols = Vec::new();
    if has_aggregates {
        for col in &cmd.columns {
            match col {
                Expr::Named(name) => {
                    non_aggregated_cols.push(generator.quote_identifier(name));
                }
                Expr::Aliased { name, .. } => {
                    // Use the base column name for GROUP BY (before AS alias)
                    non_aggregated_cols.push(generator.quote_identifier(name));
                }
                Expr::JsonAccess {
                    column,
                    path_segments,
                    ..
                } => {
                    // Include JSON access expression in GROUP BY
                    non_aggregated_cols.push(render_json_access(
                        column,
                        path_segments,
                        generator.as_ref(),
                    ));
                }
                _ => {} // Aggregates and other expressions not added to GROUP BY
            }
        }
    }

    // Process cages
    let mut where_groups: Vec<String> = Vec::new();
    let mut order_by_clauses: Vec<String> = Vec::new();
    let mut limit: Option<usize> = None;
    let mut offset: Option<usize> = None;

    for cage in &cmd.cages {
        match &cage.kind {
            CageKind::Filter => {
                if !cage.conditions.is_empty() {
                    let joiner = match cage.logical_op {
                        LogicalOp::And => " AND ",
                        LogicalOp::Or => " OR ",
                    };
                    let conditions: Vec<String> = cage
                        .conditions
                        .iter()
                        .map(|c| c.to_sql(generator.as_ref(), Some(cmd)))
                        .collect();
                    let group = conditions.join(joiner);
                    // Wrap OR groups in parentheses for correct precedence
                    if cage.logical_op == LogicalOp::Or && cage.conditions.len() > 1 {
                        where_groups.push(format!("({})", group));
                    } else {
                        where_groups.push(group);
                    }
                }
            }
            CageKind::Sort(order) => {
                if let Some(cond) = cage.conditions.first() {
                    let dir = match order {
                        SortOrder::Asc => "ASC",
                        SortOrder::Desc => "DESC",
                        SortOrder::AscNullsFirst => "ASC NULLS FIRST",
                        SortOrder::AscNullsLast => "ASC NULLS LAST",
                        SortOrder::DescNullsFirst => "DESC NULLS FIRST",
                        SortOrder::DescNullsLast => "DESC NULLS LAST",
                    };
                    let col_sql = render_expr_for_orderby(&cond.left, generator.as_ref(), cmd);
                    order_by_clauses.push(format!("{} {}", col_sql, dir));
                }
            }
            CageKind::Limit(n) => {
                limit = Some(*n);
            }
            CageKind::Offset(n) => {
                offset = Some(*n);
            }
            CageKind::Payload => {
                // Not used in SELECT
            }
            CageKind::Sample(_) => {
                // Handled separately after FROM clause
            }
            CageKind::Qualify => {
                // Will be processed separately after ORDER BY for QUALIFY clause
            }
            CageKind::Partition => {
                // Handled in window function OVER clause
            }
        }
    }

    // WHERE - each cage group is joined with AND
    if !where_groups.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_groups.join(" AND "));
    }

    // GROUP BY (with ROLLUP/CUBE support)
    if !non_aggregated_cols.is_empty() {
        sql.push_str(" GROUP BY ");
        match cmd.group_by_mode {
            GroupByMode::Simple => sql.push_str(&non_aggregated_cols.join(", ")),
            GroupByMode::Rollup => {
                sql.push_str(&format!("ROLLUP({})", non_aggregated_cols.join(", ")))
            }
            GroupByMode::Cube => sql.push_str(&format!("CUBE({})", non_aggregated_cols.join(", "))),
            GroupByMode::GroupingSets(ref sets) => {
                let sets_str: Vec<String> =
                    sets.iter().map(|s| format!("({})", s.join(", "))).collect();
                sql.push_str(&format!("GROUPING SETS ({})", sets_str.join(", ")));
            }
        }
    }

    // HAVING (filter on aggregates)
    if !cmd.having.is_empty() {
        let having_conds: Vec<String> = cmd
            .having
            .iter()
            .map(|c| c.to_sql(generator.as_ref(), Some(cmd)))
            .collect();
        sql.push_str(" HAVING ");
        sql.push_str(&having_conds.join(" AND "));
    }

    if !order_by_clauses.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_by_clauses.join(", "));
    }

    // QUALIFY (Snowflake, BigQuery, Databricks) - filter on window function results
    // Appears after ORDER BY, before LIMIT
    for cage in &cmd.cages {
        if let CageKind::Qualify = cage.kind
            && !cage.conditions.is_empty()
        {
            let qualify_conds: Vec<String> = cage
                .conditions
                .iter()
                .map(|c| c.to_sql(generator.as_ref(), Some(cmd)))
                .collect();
            sql.push_str(" QUALIFY ");
            sql.push_str(&qualify_conds.join(" AND "));
        }
    }

    sql.push_str(&generator.limit_offset(limit, offset));
    append_fetch_clause(&mut sql, cmd.fetch);

    if !cmd.set_ops.is_empty() && set_operand_has_branch_clauses(cmd) {
        sql = wrap_set_operand_sql(sql, dialect);
    }

    // SET OPERATIONS (UNION, INTERSECT, EXCEPT)
    for (set_op, other_cmd) in &cmd.set_ops {
        let op_str = match set_op {
            SetOp::Union => "UNION",
            SetOp::UnionAll => "UNION ALL",
            SetOp::Intersect => "INTERSECT",
            SetOp::Except => "EXCEPT",
        };
        sql.push_str(&format!(
            " {} {}",
            op_str,
            build_set_operand(other_cmd, dialect)
        ));
    }

    // FOR UPDATE/SHARE (row locking)
    if let Some(lock) = &cmd.lock_mode {
        match lock {
            LockMode::Update => sql.push_str(" FOR UPDATE"),
            LockMode::NoKeyUpdate => sql.push_str(" FOR NO KEY UPDATE"),
            LockMode::Share => sql.push_str(" FOR SHARE"),
            LockMode::KeyShare => sql.push_str(" FOR KEY SHARE"),
        }
        if cmd.skip_locked {
            sql.push_str(" SKIP LOCKED");
        }
    }

    sql
}

pub(super) fn build_set_operand(cmd: &Qail, dialect: Dialect) -> String {
    let sql = build_select(cmd, dialect);
    if set_operand_needs_wrapper(cmd) {
        wrap_set_operand_sql(sql, dialect)
    } else {
        sql
    }
}

fn set_operand_needs_wrapper(cmd: &Qail) -> bool {
    !cmd.set_ops.is_empty() || set_operand_has_branch_clauses(cmd)
}

fn set_operand_has_branch_clauses(cmd: &Qail) -> bool {
    cmd.fetch.is_some()
        || cmd.cages.iter().any(|cage| {
            matches!(
                cage.kind,
                CageKind::Sort(_) | CageKind::Limit(_) | CageKind::Offset(_)
            )
        })
}

fn wrap_set_operand_sql(sql: String, dialect: Dialect) -> String {
    match dialect {
        Dialect::Postgres | Dialect::SQLite => format!("({sql})"),
    }
}

fn append_fetch_clause(sql: &mut String, fetch: Option<(u64, bool)>) {
    if let Some((count, with_ties)) = fetch {
        if with_ties {
            sql.push_str(&format!(" FETCH FIRST {} ROWS WITH TIES", count));
        } else {
            sql.push_str(&format!(" FETCH FIRST {} ROWS ONLY", count));
        }
    }
}

/// Render an expression for ORDER BY (and potentially other contexts).
/// Handles CASE, Binary, FunctionCall, SpecialFunction, and Named expressions.
fn render_expr_for_orderby(
    expr: &Expr,
    generator: &dyn crate::transpiler::SqlGenerator,
    cmd: &Qail,
) -> String {
    match expr {
        Expr::Star => "*".to_string(),
        Expr::Named(name) => {
            // Don't quote if already quoted, is a param, or is numeric
            if name.starts_with('\'')
                || name.starts_with('"')
                || name.starts_with(':')
                || name.starts_with('$')
                || name.parse::<f64>().is_ok()
                || name.eq_ignore_ascii_case("NULL")
                || name.eq_ignore_ascii_case("TRUE")
                || name.eq_ignore_ascii_case("FALSE")
            {
                name.clone()
            } else {
                generator.quote_identifier(name)
            }
        }
        Expr::Aliased { name, .. } => generator.quote_identifier(name),
        Expr::Literal(value) => render_value_for_expression(value, generator, cmd),
        Expr::Aggregate {
            col,
            func,
            distinct,
            filter,
            ..
        } => {
            let col_expr = if col == "*" {
                "*".to_string()
            } else {
                generator.quote_identifier(col)
            };
            let mut expr = if *distinct {
                format!("{}(DISTINCT {})", func, col_expr)
            } else {
                format!("{}({})", func, col_expr)
            };
            if let Some(conditions) = filter
                && !conditions.is_empty()
            {
                let filter_parts = conditions
                    .iter()
                    .map(|condition| condition.to_sql(generator, Some(cmd)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                expr.push_str(&format!(" FILTER (WHERE {filter_parts})"));
            }
            expr
        }
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            let mut case_sql = String::from("CASE");
            for (cond, val) in when_clauses {
                case_sql.push_str(&format!(
                    " WHEN {} THEN {}",
                    cond.to_sql(generator, Some(cmd)),
                    render_expr_for_orderby(val, generator, cmd)
                ));
            }
            if let Some(e) = else_value {
                case_sql.push_str(&format!(
                    " ELSE {}",
                    render_expr_for_orderby(e, generator, cmd)
                ));
            }
            case_sql.push_str(" END");
            case_sql
        }
        Expr::Binary {
            left, op, right, ..
        } => {
            let left_sql = render_expr_for_orderby(left, generator, cmd);
            let right_sql = render_expr_for_orderby(right, generator, cmd);
            match op {
                BinaryOp::IsNull => format!("({} IS NULL)", left_sql),
                BinaryOp::IsNotNull => format!("({} IS NOT NULL)", left_sql),
                _ => format!("({} {} {})", left_sql, op, right_sql),
            }
        }
        Expr::FunctionCall { name, args, .. } => {
            let Some(function) = render_function_name(name) else {
                return "/* ERROR: Invalid function name */".to_string();
            };
            let args_sql: Vec<String> = args
                .iter()
                .map(|a| render_expr_for_orderby(a, generator, cmd))
                .collect();
            format!("{}({})", function, args_sql.join(", "))
        }
        Expr::SpecialFunction { name, args, .. } => {
            let Some(function) = render_function_name(name) else {
                return "/* ERROR: Invalid function name */".to_string();
            };
            let mut parts = Vec::new();
            for (keyword, expr) in args {
                let expr = render_expr_for_orderby(expr, generator, cmd);
                if let Some(keyword) = keyword {
                    let Some(keyword) = render_sql_keyword(keyword) else {
                        return "/* ERROR: Invalid function keyword */".to_string();
                    };
                    parts.push(format!("{keyword} {expr}"));
                } else {
                    parts.push(expr);
                }
            }
            format!("{function}({})", parts.join(" "))
        }
        Expr::JsonAccess {
            column,
            path_segments,
            ..
        } => render_json_access(column, path_segments, generator),
        Expr::Cast {
            expr, target_type, ..
        } => {
            let Some(target_type) = checked_sql_type_fragment(target_type) else {
                return "/* ERROR: Invalid cast target type */".to_string();
            };
            format!(
                "{}::{}",
                render_expr_for_orderby(expr, generator, cmd),
                target_type
            )
        }
        Expr::Collate {
            expr, collation, ..
        } => format!(
            "{} COLLATE {}",
            render_expr_for_orderby(expr, generator, cmd),
            render_qualified_identifier(collation, generator)
        ),
        Expr::FieldAccess { expr, field, .. } => format!(
            "({}).{}",
            render_expr_for_orderby(expr, generator, cmd),
            render_qualified_identifier(field, generator)
        ),
        Expr::ArrayConstructor { elements, .. } => {
            let elements = elements
                .iter()
                .map(|element| render_expr_for_orderby(element, generator, cmd))
                .collect::<Vec<_>>()
                .join(", ");
            format!("ARRAY[{elements}]")
        }
        Expr::RowConstructor { elements, .. } => {
            let elements = elements
                .iter()
                .map(|element| render_expr_for_orderby(element, generator, cmd))
                .collect::<Vec<_>>()
                .join(", ");
            format!("ROW({elements})")
        }
        Expr::Subscript { expr, index, .. } => format!(
            "{}[{}]",
            render_expr_for_orderby(expr, generator, cmd),
            render_expr_for_orderby(index, generator, cmd)
        ),
        Expr::Subquery { query, .. } => format!("({})", read_only_subquery_sql(query)),
        Expr::Exists { query, negated, .. } => {
            if *negated {
                format!("NOT EXISTS ({})", read_only_subquery_sql(query))
            } else {
                format!("EXISTS ({})", read_only_subquery_sql(query))
            }
        }
        Expr::Def { .. } | Expr::Mod { .. } | Expr::Window { .. } => {
            "/* ERROR: Invalid select expression */".to_string()
        }
    }
}

fn append_alias(
    expr_sql: String,
    alias: &Option<String>,
    generator: &dyn crate::transpiler::SqlGenerator,
) -> String {
    if let Some(alias) = alias {
        format!("{} AS {}", expr_sql, generator.quote_identifier(alias))
    } else {
        expr_sql
    }
}

fn render_value_for_expression(
    value: &Value,
    generator: &dyn crate::transpiler::SqlGenerator,
    cmd: &Qail,
) -> String {
    match value {
        Value::Column(column) => generator.quote_identifier(column),
        Value::Expr(expr) => render_expr_for_orderby(expr, generator, cmd),
        Value::Subquery(query) => format!("({})", read_only_subquery_sql(query)),
        Value::Function(function) => render_raw_function_value(function),
        Value::NamedParam(name) => render_named_param(name),
        Value::Array(values) => {
            let values = values
                .iter()
                .map(|value| render_value_for_expression(value, generator, cmd))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({values})")
        }
        _ => value.to_string(),
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

fn render_sql_keyword(keyword: &str) -> Option<String> {
    if keyword.is_empty()
        || keyword.contains('\0')
        || !keyword
            .bytes()
            .all(|b| b.is_ascii_alphabetic() || b == b'_')
    {
        None
    } else {
        Some(keyword.to_uppercase())
    }
}

fn render_named_param(name: &str) -> String {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return "/* ERROR: Invalid parameter name */".to_string();
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return "/* ERROR: Invalid parameter name */".to_string();
    }
    format!(":{}", name)
}

fn render_raw_function_value(value: &str) -> String {
    if value.len() > 1024
        || value.contains('\0')
        || value.contains(';')
        || value.contains("--")
        || value.contains("/*")
        || value.contains("*/")
    {
        "/* ERROR: Invalid function expression */".to_string()
    } else {
        value.to_string()
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
    let mut result = generator.quote_identifier(column);
    for (path, as_text) in path_segments {
        let op = if *as_text { "->>" } else { "->" };
        if path.parse::<i64>().is_ok() {
            result.push_str(&format!("{}{}", op, path));
        } else {
            result.push_str(&format!("{}'{}'", op, escape_sql_string_literal(path)));
        }
    }
    result
}

/// Convert FrameBound to SQL string for window functions
fn bound_to_sql(bound: &FrameBound) -> String {
    match bound {
        FrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
        FrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
        FrameBound::CurrentRow => "CURRENT ROW".to_string(),
        FrameBound::Preceding(n) => format!("{} PRECEDING", n),
        FrameBound::Following(n) => format!("{} FOLLOWING", n),
    }
}
