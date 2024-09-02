//! SQL to QAIL transformation using sqlparser.
//!
//! This module provides clean AST-to-code transformation by parsing SQL
//! with `sqlparser-rs` and mapping AST nodes to QAIL builder calls.

use sqlparser::ast::{
    Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use super::utils::to_pascal_case;

/// Transform SQL string to QAIL builder code.
pub fn sql_to_qail(sql: &str) -> Result<String, String> {
    let dialect = PostgreSqlDialect {};
    let ast = Parser::parse_sql(&dialect, sql).map_err(|e| format!("Parse error: {}", e))?;

    if ast.is_empty() {
        return Err("Empty SQL".to_string());
    }

    Ok(transform_statement(&ast[0]))
}

/// Transform a SQL statement to QAIL code.
fn transform_statement(stmt: &Statement) -> String {
    match stmt {
        Statement::Query(query) => transform_query(query),
        Statement::Insert(insert) => {
            let table_name = insert.table.to_string();
            let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();
            transform_insert(&table_name, &columns)
        }
        Statement::Update(update) => {
            let table_name = extract_table_factor(&update.table.relation);
            let assignments: Vec<(String, String)> = update.assignments.iter()
                .map(|a| {
                    let col = a.target.to_string();
                    let val = format!("{}", a.value);
                    (col, val)
                })
                .collect();
            transform_update(&table_name, &assignments)
        }
        Statement::Delete(delete) => {
            let table_name = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                    tables.first()
                        .map(|t| extract_table_factor(&t.relation))
                        .unwrap_or_else(|| "table".to_string())
                }
                sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                    tables.first()
                        .map(|t| extract_table_factor(&t.relation))
                        .unwrap_or_else(|| "table".to_string())
                }
            };
            transform_delete(&table_name)
        }
        Statement::CreateTable(create) => {
            transform_create_table(&create.name.to_string())
        }
        Statement::Drop { object_type, names, .. } => {
            transform_drop(object_type, names)
        }
        Statement::Truncate(truncate) => {
            let table = truncate.table.to_string();
            transform_truncate(&table)
        }
        Statement::Explain { statement, analyze, .. } => {
            transform_explain(statement, *analyze)
        }
        _ => {
            let stmt_type = format!("{:?}", stmt).split('(').next().unwrap_or("Unknown").to_string();
            format!("// SQL statement type '{}' not yet mapped to QAIL", stmt_type)
        }
    }
}

/// Transform a SELECT query to QAIL code.
fn transform_query(query: &Query) -> String {
    let mut result = String::new();
    result.push_str("use qail_core::ast::{QailCmd, Operator, Order};\n\n");

    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let cte_name = &cte.alias.name;
            result.push_str(&format!(
                "// CTE '{}': define as separate query and use .as_cte(\"{}\")\n",
                cte_name, cte_name
            ));
            // Transform the inner CTE query
            let inner_code = transform_query(&cte.query)
                .replace("use qail_core::ast::{QailCmd, Operator, Order};\n\n", "")
                .replace("// Execute with qail-pg driver:\n// let rows = driver.fetch(&cmd).await?;", "")
                .replace(";\n\n", "")
                .trim()
                .to_string();
            result.push_str(&format!("let {}_cte = {};\n\n", cte_name, inner_code));
        }
        result.push_str("// Then reference CTE in main query using the alias\n\n");
    }

    if let SetExpr::Select(select) = query.body.as_ref() {
        let table = extract_table(select);
        let columns = extract_columns(select);

        result.push_str(&format!("let cmd = QailCmd::get(\"{}\")\n", table));
        result.push_str(&format!("    .columns([{}])", columns));

        if let Some(selection) = &select.selection {
            result.push_str(&format!("\n    {}", transform_where(selection)));
        }

        if let Some(order_by) = &query.order_by
            && let sqlparser::ast::OrderByKind::Expressions(order_exprs) = &order_by.kind
            && !order_exprs.is_empty()
        {
            result.push_str(&format!("\n    {}", transform_order_by(order_exprs)));
        }

        if let Some(sqlparser::ast::LimitClause::LimitOffset { limit: Some(limit_expr), .. }) = &query.limit_clause {
            result.push_str(&format!("\n    .limit({})", expr_to_string(limit_expr)));
        }

        result.push_str(";\n\n");
        result.push_str("// Execute with qail-pg driver:\n");
        result.push_str(&format!(
            "let rows: Vec<{}Row> = driver.query_as(&cmd).await?;",
            to_pascal_case(&table)
        ));
    }

    result
}

/// Extract table name from SELECT.
fn extract_table(select: &Select) -> String {
    if let Some(from) = select.from.first() {
        extract_table_factor(&from.relation)
    } else {
        "table".to_string()
    }
}

/// Extract table name from TableFactor.
fn extract_table_factor(table: &TableFactor) -> String {
    match table {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => "table".to_string(),
    }
}

/// Extract columns from SELECT projection.
fn extract_columns(select: &Select) -> String {
    let cols: Vec<String> = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => format!("\"{}\"", expr_to_string(expr)),
            SelectItem::ExprWithAlias { alias, .. } => {
                format!("\"{}\"", alias.value)
            }
            SelectItem::Wildcard(_) => "\"*\"".to_string(),
            SelectItem::QualifiedWildcard(name, _) => format!("\"{}.*\"", name),
        })
        .collect();
    cols.join(", ")
}

/// Transform WHERE clause to QAIL filter.
fn transform_where(expr: &Expr) -> String {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let col = expr_to_string(left);
            let val = expr_to_string(right);
            let op_str = match op {
                sqlparser::ast::BinaryOperator::Eq => "Operator::Eq",
                sqlparser::ast::BinaryOperator::NotEq => "Operator::Ne",
                sqlparser::ast::BinaryOperator::Lt => "Operator::Lt",
                sqlparser::ast::BinaryOperator::LtEq => "Operator::Lte",
                sqlparser::ast::BinaryOperator::Gt => "Operator::Gt",
                sqlparser::ast::BinaryOperator::GtEq => "Operator::Gte",
                sqlparser::ast::BinaryOperator::And => {
                    let left_filter = transform_where(left);
                    let right_filter = transform_where(right);
                    return format!("{}\n    {}", left_filter, right_filter);
                }
                sqlparser::ast::BinaryOperator::Or => {
                    let left_filter = transform_where(left);
                    let right_filter = transform_where(right).replace(".filter(", ".or_filter(");
                    return format!("{}\n    {}", left_filter, right_filter);
                }
                _ => "Operator::Eq",
            };
            format!(".filter(\"{}\", {}, {})", col, op_str, val)
        }
        Expr::InList { expr, list, negated } => {
            let col = expr_to_string(expr);
            let vals: Vec<String> = list.iter().map(expr_to_string).collect();
            let op = if *negated { "Operator::NotIn" } else { "Operator::In" };
            format!(".filter(\"{}\", {}, vec![{}])", col, op, vals.join(", "))
        }
        Expr::IsNull(e) => {
            format!(".filter(\"{}\", Operator::IsNull, Value::Null)", expr_to_string(e))
        }
        Expr::IsNotNull(e) => {
            format!(".filter(\"{}\", Operator::IsNotNull, Value::Null)", expr_to_string(e))
        }
        Expr::Between { expr, low, high, negated } => {
            let col = expr_to_string(expr);
            let op = if *negated { "Operator::NotBetween" } else { "Operator::Between" };
            format!(".filter(\"{}\", {}, ({}, {}))", col, op, expr_to_string(low), expr_to_string(high))
        }
        Expr::Nested(inner) => transform_where(inner),
        Expr::Like { expr, pattern, .. } => {
            format!(".filter(\"{}\", Operator::Like, {})", expr_to_string(expr), expr_to_string(pattern))
        }
        Expr::ILike { expr, pattern, .. } => {
            format!(".filter(\"{}\", Operator::ILike, {})", expr_to_string(expr), expr_to_string(pattern))
        }
        _ => format!("// Complex WHERE: {}", expr),
    }
}

/// Transform ORDER BY to QAIL.
fn transform_order_by(order_by: &[sqlparser::ast::OrderByExpr]) -> String {
    if let Some(first) = order_by.first() {
        let col = expr_to_string(&first.expr);
        let dir = if first.options.asc.unwrap_or(true) { "Asc" } else { "Desc" };
        format!(".order_by(\"{}\", Order::{})", col, dir)
    } else {
        String::new()
    }
}

/// Transform INSERT to QAIL.
fn transform_insert(table_name: &str, columns: &[String]) -> String {
    let mut set_values = String::new();
    for col in columns {
        set_values.push_str(&format!("    .set_value(\"{}\", {}_value)\n", col, col));
    }
    if set_values.is_empty() {
        set_values = "    // Add .set_value(col, val) for each column\n".to_string();
    }
    format!(
        "use qail_core::ast::QailCmd;\n\n\
         let cmd = QailCmd::add(\"{}\")\n{};

\
         let result = driver.execute(&cmd).await?;",
        table_name, set_values.trim_end()
    )
}

/// Transform UPDATE to QAIL.
fn transform_update(table_name: &str, assignments: &[(String, String)]) -> String {
    let mut set_values = String::new();
    for (col, val) in assignments {
        set_values.push_str(&format!("    .set_value(\"{}\", {})\n", col, val));
    }
    if set_values.is_empty() {
        set_values = "    // Add .set_value(col, val) for each column\n".to_string();
    }
    format!(
        "use qail_core::ast::{{QailCmd, Operator}};\n\n\
         let cmd = QailCmd::set(\"{}\")\n{}    .filter(\"id\", Operator::Eq, id);\n\n\
         let result = driver.execute(&cmd).await?;",
        table_name, set_values
    )
}

/// Transform DELETE to QAIL.
fn transform_delete(table_name: &str) -> String {
    format!(
        "use qail_core::ast::{{QailCmd, Operator}};\n\n\
         let cmd = QailCmd::del(\"{}\")\n    \
         .filter(\"id\", Operator::Eq, id);\n\n\
         let result = driver.execute(&cmd).await?;",
        table_name
    )
}

/// Convert expression to string representation.
fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::Value(val) => {
            let s = format!("{}", val);
            if let Some(stripped) = s.strip_prefix('$')
                && let Ok(n) = stripped.parse::<u32>()
            {
                return format!("param_{} /* replace with actual value */", n);
            }
            s
        }
        Expr::CompoundIdentifier(parts) => parts.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."),
        _ => format!("{}", expr),
    }
}

/// Transform CREATE TABLE to QAIL.
fn transform_create_table(table_name: &str) -> String {
    format!(
        "use qail_core::ast::QailCmd;\n\n\
         let cmd = QailCmd::make(\"{}\")\n    \
         // Add column definitions with .column_def(name, type, constraints)\n;\n\n\
         let result = driver.execute(&cmd).await?;",
        table_name
    )
}

/// Transform DROP to QAIL.
fn transform_drop(object_type: &sqlparser::ast::ObjectType, names: &[sqlparser::ast::ObjectName]) -> String {
    use sqlparser::ast::ObjectType;
    
    let table = names.first()
        .map(|n| n.to_string())
        .unwrap_or_else(|| "table".to_string());
    
    match object_type {
        ObjectType::Table => format!(
            "use qail_core::ast::{{QailCmd, Action}};\n\n\
             let cmd = QailCmd {{ action: Action::Drop, table: \"{}\".into(), ..Default::default() }};\n\n\
             let result = driver.execute(&cmd).await?;",
            table
        ),
        ObjectType::Index => format!(
            "use qail_core::ast::{{QailCmd, Action}};\n\n\
             let cmd = QailCmd {{ action: Action::DropIndex, table: \"{}\".into(), ..Default::default() }};\n\n\
             let result = driver.execute(&cmd).await?;",
            table
        ),
        _ => format!("// DROP {:?} not yet mapped to QAIL", object_type),
    }
}

/// Transform TRUNCATE to QAIL.
fn transform_truncate(table_name: &str) -> String {
    format!(
        "use qail_core::ast::QailCmd;\n\n\
         let cmd = QailCmd::truncate(\"{}\");\n\n\
         let result = driver.execute(&cmd).await?;",
        table_name
    )
}

/// Transform EXPLAIN to QAIL.
fn transform_explain(statement: &Statement, analyze: bool) -> String {
    // Recursively transform the inner statement and wrap it
    let inner = transform_statement(statement);
    
    if analyze {
        format!(
            "// EXPLAIN ANALYZE wrapper:\n\
             // Use QailCmd::explain_analyze(table) instead of QailCmd::get(table)\n\n\
             {}", inner
        )
    } else {
        format!(
            "// EXPLAIN wrapper:\n\
             // Use QailCmd::explain(table) instead of QailCmd::get(table)\n\n\
             {}", inner
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let sql = "SELECT id, name FROM users WHERE active = true ORDER BY name ASC";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("QailCmd::get(\"users\")"));
        assert!(result.contains(".columns"));
        assert!(result.contains(".filter"));
        assert!(result.contains(".order_by"));
    }

    #[test]
    fn test_select_with_limit() {
        let sql = "SELECT * FROM users LIMIT 10";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains(".limit(10)"));
    }

    #[test]
    fn test_insert() {
        let sql = "INSERT INTO users (name, email) VALUES ('test', 'test@example.com')";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("QailCmd::add"));
    }

    #[test]
    fn test_update() {
        let sql = "UPDATE users SET name = 'new' WHERE id = 1";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("QailCmd::set"));
    }

    #[test]
    fn test_delete() {
        let sql = "DELETE FROM users WHERE id = 1";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("QailCmd::del"));
    }

    #[test]
    fn test_cte() {
        let sql = "WITH stats AS (SELECT COUNT(*) FROM orders) SELECT * FROM stats";
        let result = sql_to_qail(sql).unwrap();
        assert!(result.contains("CTE"));
    }
}
