//! SELECT SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::conditions::ConditionToSql;

/// Generate SELECT SQL.
pub fn build_select(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    
    // CTE prefix: WITH cte1 AS (...), cte2 AS (...) 
    let cte_prefix = if !cmd.ctes.is_empty() {
        let has_recursive = cmd.ctes.iter().any(|c| c.recursive);
        let cte_parts: Vec<String> = cmd.ctes.iter()
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
    
    // Handle DISTINCT ON (Postgres-specific) - supports expressions
    let mut sql = if !cmd.distinct_on.is_empty() {
        let exprs: Vec<String> = cmd.distinct_on.iter()
            .map(|e| render_expr_for_orderby(e, &generator, cmd))
            .collect();
        format!("{}SELECT DISTINCT ON ({}) ", cte_prefix, exprs.join(", "))
    } else if cmd.distinct {
        format!("{}SELECT DISTINCT ", cte_prefix)
    } else {
        format!("{}SELECT ", cte_prefix)
    };

    // Columns
    if cmd.columns.is_empty() {
        sql.push('*');
    } else {
        let cols: Vec<String> = cmd.columns.iter().map(|c| {
             match c {
                Expr::Named(name) => generator.quote_identifier(name),
                Expr::Case { when_clauses, else_value, alias } => {
                    let mut case_sql = String::from("CASE");
                    for (cond, val) in when_clauses {
                        case_sql.push_str(&format!(" WHEN {} THEN {}", 
                            cond.to_sql(&generator, Some(cmd)), 
                            render_expr_for_orderby(val, &generator, cmd)));
                    }
                    if let Some(e) = else_value {
                        case_sql.push_str(&format!(" ELSE {}", render_expr_for_orderby(e, &generator, cmd)));
                    }
                    case_sql.push_str(" END");
                    if let Some(a) = alias {
                        format!("{} AS {}", case_sql, generator.quote_identifier(a))
                    } else {
                        case_sql
                    }
                }
                Expr::JsonAccess { column, path_segments, alias } => {
                    let mut expr = generator.quote_identifier(column);
                    for (path, as_text) in path_segments {
                        let op = if *as_text { "->>" } else { "->" };
                        if path.parse::<i64>().is_ok() { expr.push_str(&format!("{}{}", op, path)); } else { expr.push_str(&format!("{}'{}'", op, path)); }
                    }
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
                            // Convert args to strings for processing
                            let cond_str = args[0].to_string();
                            let then_str = args[1].to_string();
                            let else_str = args[2].to_string();
                            
                            // Arg 0: WHEN condition (Raw preferred)
                            let cond_sql = if cond_str.starts_with('{') && cond_str.ends_with('}') {
                                cond_str[1..cond_str.len()-1].to_string()
                            } else {
                                cond_str
                            };
                            
                            // Arg 1: THEN value (Quoted unless raw)
                            let then_sql = if then_str.starts_with('{') && then_str.ends_with('}') {
                                then_str[1..then_str.len()-1].to_string()
                            } else {
                                generator.quote_identifier(&then_str)
                            };

                            // Arg 2: ELSE value (Quoted unless raw)
                            let else_sql = if else_str.starts_with('{') && else_str.ends_with('}') {
                                else_str[1..else_str.len()-1].to_string()
                            } else {
                                generator.quote_identifier(&else_str)
                            };
                            
                            let expr = format!("CASE WHEN {} THEN {} ELSE {} END", cond_sql, then_sql, else_sql);
                            if let Some(a) = alias {
                                format!("{} AS {}", expr, generator.quote_identifier(a))
                            } else {
                                expr
                            }
                        } else {
                            // Invalid case call, fallback to standard function
                            let args_sql: Vec<String> = args.iter()
                                .map(|a| a.to_string())
                                .collect();
                            let expr = format!("CASE({})", args_sql.join(", "));
                             if let Some(a) = alias {
                                format!("{} AS {}", expr, generator.quote_identifier(a))
                            } else {
                                expr
                            }
                        }
                    } else {
                        // Standard Function - transpile each arg expression
                        let args_sql: Vec<String> = args.iter()
                            .map(|a| {
                                let arg_str = a.to_string();
                                if arg_str.starts_with('{') && arg_str.ends_with('}') {
                                    // Raw SQL block: {content} -> content
                                    arg_str[1..arg_str.len()-1].to_string()
                                } else {
                                   // For expressions (especially binary), don't quote
                                   match a {
                                       Expr::Named(n) => {
                                           // Don't quote if already quoted, is a param, or is numeric
                                           if n.starts_with('\'') || n.starts_with('"') 
                                               || n.starts_with(':') || n.starts_with('$')
                                               || n.parse::<f64>().is_ok()
                                               || n.eq_ignore_ascii_case("NULL")
                                               || n.eq_ignore_ascii_case("TRUE")
                                               || n.eq_ignore_ascii_case("FALSE") {
                                               n.clone()
                                           } else {
                                               generator.quote_identifier(n)
                                           }
                                       },
                                       Expr::Star => "*".to_string(),
                                       _ => arg_str, // Binary, FunctionCall etc - use as-is
                                   }
                                }
                            })
                            .collect();
                        let expr = format!("{}({})", name.to_uppercase(), args_sql.join(", "));
                        if let Some(a) = alias {
                            format!("{} AS {}", expr, generator.quote_identifier(a))
                        } else {
                            expr
                        }
                    }
                }
                Expr::Aggregate { col, func, distinct, filter, alias } => {
                    // Render aggregate function: COUNT(*), COUNT(DISTINCT col), SUM(col), etc.
                    let col_expr = if col == "*" { "*".to_string() } else { generator.quote_identifier(col) };
                    let mut expr = if *distinct {
                        format!("{}(DISTINCT {})", func, col_expr)
                    } else {
                        format!("{}({})", func, col_expr)
                    };
                    
                    // Add FILTER clause if present (PostgreSQL extension)
                    if let Some(conditions) = filter {
                        if !conditions.is_empty() {
                            let filter_parts: Vec<String> = conditions.iter()
                                .map(|c| c.to_string())
                                .collect();
                            expr.push_str(&format!(" FILTER (WHERE {})", filter_parts.join(" AND ")));
                        }
                    }
                    
                    if let Some(a) = alias {
                        format!("{} AS {}", expr, generator.quote_identifier(a))
                    } else {
                        expr
                    }
                }
                Expr::Cast { expr, target_type, alias } => {
                    let cast_expr = format!("{}::{}", expr, target_type);
                    if let Some(a) = alias {
                        format!("{} AS {}", cast_expr, generator.quote_identifier(a))
                    } else {
                        cast_expr
                    }
                }
                Expr::Window { name, func, params, partition, order, frame } => {
                    // Window function: FUNC(args) OVER (PARTITION BY x ORDER BY y) AS alias
                    let params_str = if params.is_empty() {
                        String::new()
                    } else {
                        params.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(", ")
                    };
                    
                    let mut over_clause = String::from("OVER (");
                    if !partition.is_empty() {
                        over_clause.push_str("PARTITION BY ");
                        let quoted_partition: Vec<String> = partition.iter().map(|p| generator.quote_identifier(p)).collect();
                        over_clause.push_str(&quoted_partition.join(", "));
                        if !order.is_empty() {
                            over_clause.push(' ');
                        }
                    }
                    if !order.is_empty() {
                        over_clause.push_str("ORDER BY ");
                        let order_parts: Vec<String> = order.iter().map(|cage| {
                            let col_str = if let Some(cond) = cage.conditions.first() {
                                match &cond.left {
                                    Expr::Named(n) => generator.quote_identifier(n),
                                    expr => expr.to_string(),
                                }
                            } else {
                                return String::new();
                            };
                            match &cage.kind {
                                CageKind::Sort(SortOrder::Asc) => format!("{} ASC", col_str),
                                CageKind::Sort(SortOrder::Desc) => format!("{} DESC", col_str),
                                CageKind::Sort(SortOrder::AscNullsFirst) => format!("{} ASC NULLS FIRST", col_str),
                                CageKind::Sort(SortOrder::AscNullsLast) => format!("{} ASC NULLS LAST", col_str),
                                CageKind::Sort(SortOrder::DescNullsFirst) => format!("{} DESC NULLS FIRST", col_str),
                                CageKind::Sort(SortOrder::DescNullsLast) => format!("{} DESC NULLS LAST", col_str),
                                _ => String::new(),
                            }
                        }).filter(|s| !s.is_empty()).collect();
                        over_clause.push_str(&order_parts.join(", "));
                    }
                    
                    // Add frame if present
                    if let Some(fr) = frame {
                        over_clause.push(' ');
                        match fr {
                            WindowFrame::Rows { start, end } => {
                                over_clause.push_str(&format!("ROWS BETWEEN {} AND {}", 
                                    bound_to_sql(start), bound_to_sql(end)));
                            }
                            WindowFrame::Range { start, end } => {
                                over_clause.push_str(&format!("RANGE BETWEEN {} AND {}", 
                                    bound_to_sql(start), bound_to_sql(end)));
                            }
                        }
                    }
                    
                    over_clause.push(')');
                    format!("{}({}) {} AS {}", func.to_uppercase(), params_str, over_clause, generator.quote_identifier(name))
                }
                _ => c.to_string(), // Fallback for complex cols if any remaining
            }
        }).collect();
        sql.push_str(&cols.join(", "));
    }

    // FROM
    sql.push_str(" FROM ");
    sql.push_str(&generator.quote_identifier(&cmd.table));
    
    // Check for TABLESAMPLE (Postgres/Standard SQL) - handle early before joins
    let sample_percent = cmd.cages.iter().find_map(|c| {
        if let CageKind::Sample(pct) = c.kind {
            Some(pct)
        } else {
            None
        }
    });
    if let Some(pct) = sample_percent {
        sql.push_str(&format!(" TABLESAMPLE BERNOULLI({})", pct));
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
        let source_singular = cmd.table.trim_end_matches('s');
        
        let target_table = generator.quote_identifier(&join.table);
        let source_fk = format!("{}_id", source_singular);
        let source_table = generator.quote_identifier(&cmd.table);
        
        if let Some(on_conds) = &join.on {
            let on_sql: Vec<String> = on_conds.iter()
                .map(|c| c.to_sql(&generator, Some(cmd)))
                .collect();
            sql.push_str(&format!(
                " {} JOIN {} ON {}",
                kind,
                target_table,
                on_sql.join(" AND ")
            ));
        } else if join.on_true {
            // Explicit ON TRUE (unconditional join, used for CTE joins)
            sql.push_str(&format!(
                " {} JOIN {} ON TRUE",
                kind,
                target_table
            ));
        } else if needs_on {
            sql.push_str(&format!(
                " {} JOIN {} ON {}.{} = {}.id",
                kind, 
                target_table, 
                target_table, 
                generator.quote_identifier(&source_fk), 
                source_table
            ));
        } else {
            sql.push_str(&format!(
                " {} JOIN {}",
                kind,
                target_table
            ));
        }
    }
    
    // Prepare for GROUP BY check
    let has_aggregates = cmd.columns.iter().any(|c| matches!(c, Expr::Aggregate { .. }));
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
                 Expr::JsonAccess { column, path_segments, .. } => {
                     // Include JSON access expression in GROUP BY
                     let mut expr = generator.quote_identifier(column);
                     for (path, as_text) in path_segments {
                         let op = if *as_text { "->>" } else { "->" };
                         if path.parse::<i64>().is_ok() { expr.push_str(&format!("{}{}", op, path)); } else { expr.push_str(&format!("{}'{}'", op, path)); }
                     }
                     non_aggregated_cols.push(expr);
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
                    let conditions: Vec<String> = cage.conditions.iter().map(|c| c.to_sql(&generator, Some(cmd))).collect();
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
                    let col_sql = render_expr_for_orderby(&cond.left, &generator, cmd);
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
            GroupByMode::Rollup => sql.push_str(&format!("ROLLUP({})", non_aggregated_cols.join(", "))),
            GroupByMode::Cube => sql.push_str(&format!("CUBE({})", non_aggregated_cols.join(", "))),
        }
    }

    // HAVING (filter on aggregates)
    if !cmd.having.is_empty() {
        let having_conds: Vec<String> = cmd.having.iter()
            .map(|c| c.to_sql(&generator, Some(cmd)))
            .collect();
        sql.push_str(" HAVING ");
        sql.push_str(&having_conds.join(" AND "));
    }

    // ORDER BY
    if !order_by_clauses.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_by_clauses.join(", "));
    }

    // QUALIFY (Snowflake, BigQuery, Databricks) - filter on window function results
    // Appears after ORDER BY, before LIMIT
    for cage in &cmd.cages {
        if let CageKind::Qualify = cage.kind {
            if !cage.conditions.is_empty() {
                let qualify_conds: Vec<String> = cage.conditions.iter()
                    .map(|c| c.to_sql(&generator, Some(cmd)))
                    .collect();
                sql.push_str(" QUALIFY ");
                sql.push_str(&qualify_conds.join(" AND "));
            }
        }
    }

    // LIMIT / OFFSET
    sql.push_str(&generator.limit_offset(limit, offset));

    // SET OPERATIONS (UNION, INTERSECT, EXCEPT)
    for (set_op, other_cmd) in &cmd.set_ops {
        let op_str = match set_op {
            SetOp::Union => "UNION",
            SetOp::UnionAll => "UNION ALL",
            SetOp::Intersect => "INTERSECT",
            SetOp::Except => "EXCEPT",
        };
        sql.push_str(&format!(" {} {}", op_str, build_select(other_cmd, dialect)));
    }

    sql
}

/// Render an expression for ORDER BY (and potentially other contexts).
/// Handles CASE, Binary, FunctionCall, SpecialFunction, and Named expressions.
fn render_expr_for_orderby(expr: &Expr, generator: &Box<dyn crate::transpiler::SqlGenerator>, cmd: &QailCmd) -> String {
    match expr {
        Expr::Named(name) => {
            // Don't quote if already quoted, is a param, or is numeric
            if name.starts_with('\'') || name.starts_with('"') 
                || name.starts_with(':') || name.starts_with('$')
                || name.parse::<f64>().is_ok()
                || name.eq_ignore_ascii_case("NULL")
                || name.eq_ignore_ascii_case("TRUE")
                || name.eq_ignore_ascii_case("FALSE") {
                name.clone()
            } else {
                generator.quote_identifier(name)
            }
        }
        Expr::Case { when_clauses, else_value, .. } => {
            let mut case_sql = String::from("CASE");
            for (cond, val) in when_clauses {
                case_sql.push_str(&format!(" WHEN {} THEN {}", 
                    cond.to_sql(generator, Some(cmd)),
                    render_expr_for_orderby(val, generator, cmd)));
            }
            if let Some(e) = else_value {
                case_sql.push_str(&format!(" ELSE {}", render_expr_for_orderby(e, generator, cmd)));
            }
            case_sql.push_str(" END");
            case_sql
        }
        Expr::Binary { left, op, right, .. } => {
            let left_sql = render_expr_for_orderby(left, generator, cmd);
            let right_sql = render_expr_for_orderby(right, generator, cmd);
            let op_str = match op {
                BinaryOp::Add => "+",
                BinaryOp::Sub => "-",
                BinaryOp::Mul => "*",
                BinaryOp::Div => "/",
                BinaryOp::Rem => "%",
                BinaryOp::Concat => "||",
            };
            format!("({} {} {})", left_sql, op_str, right_sql)
        }
        Expr::FunctionCall { name, args, .. } => {
            let args_sql: Vec<String> = args.iter()
                .map(|a| render_expr_for_orderby(a, generator, cmd))
                .collect();
            format!("{}({})", name.to_uppercase(), args_sql.join(", "))
        }
        Expr::SpecialFunction { name, args, .. } => {
            // Handle SUBSTRING(x FROM y [FOR z]), EXTRACT, etc.
            match name.as_str() {
                "SUBSTRING" => {
                    let mut parts = Vec::new();
                    for (kw, arg) in args {
                        let arg_sql = render_expr_for_orderby(arg, generator, cmd);
                        if let Some(keyword) = kw {
                            parts.push(format!("{} {}", keyword, arg_sql));
                        } else {
                            parts.push(arg_sql);
                        }
                    }
                    format!("SUBSTRING({})", parts.join(" "))
                }
                "EXTRACT" => {
                    let field = args.first().map(|(_, e)| render_expr_for_orderby(e, generator, cmd)).unwrap_or_default();
                    let source = args.get(1).map(|(_, e)| render_expr_for_orderby(e, generator, cmd)).unwrap_or_default();
                    format!("EXTRACT({} FROM {})", field, source)
                }
                _ => format!("{}(...)", name)
            }
        }
        Expr::JsonAccess { column, path_segments, .. } => {
            let mut result = generator.quote_identifier(column);
            for (path, as_text) in path_segments {
                let op = if *as_text { "->>" } else { "->" };
                if path.parse::<i64>().is_ok() { result.push_str(&format!("{}{}", op, path)); } else { result.push_str(&format!("{}'{}'", op, path)); }
            }
            result
        }
        _ => expr.to_string(), // Fallback for Star, Aliased, etc.
    }
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
