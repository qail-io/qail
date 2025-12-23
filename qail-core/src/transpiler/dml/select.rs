//! SELECT SQL generation.

use crate::ast::*;
use crate::transpiler::dialect::Dialect;
use crate::transpiler::conditions::ConditionToSql;

/// Generate SELECT SQL.
pub fn build_select(cmd: &QailCmd, dialect: Dialect) -> String {
    let generator = dialect.generator();
    
    // Handle DISTINCT ON (Postgres-specific)
    let mut sql = if !cmd.distinct_on.is_empty() {
        let cols: Vec<String> = cmd.distinct_on.iter()
            .map(|c| generator.quote_identifier(c))
            .collect();
        format!("SELECT DISTINCT ON ({}) ", cols.join(", "))
    } else if cmd.distinct {
        String::from("SELECT DISTINCT ")
    } else {
        String::from("SELECT ")
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
                        case_sql.push_str(&format!(" WHEN {} THEN {}", cond.to_sql(&generator, Some(cmd)), val));
                    }
                    if let Some(e) = else_value {
                        case_sql.push_str(&format!(" ELSE {}", e));
                    }
                    case_sql.push_str(" END");
                    if let Some(a) = alias {
                        format!("{} AS {}", case_sql, generator.quote_identifier(a))
                    } else {
                        case_sql
                    }
                }
                Expr::JsonAccess { column, path, as_text, alias } => {
                    let op = if *as_text { "->>" } else { "->" };
                    let expr = format!("{}{}'{}'" , generator.quote_identifier(column), op, path);
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
                                       Expr::Named(n) => generator.quote_identifier(n),
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
             if let Expr::Named(name) = col {
                 non_aggregated_cols.push(generator.quote_identifier(name));
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
                    let col_sql = match &cond.left {
                        Expr::Named(name) => generator.quote_identifier(name),
                        expr => expr.to_string(),
                    };
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
