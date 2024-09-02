//! SQL clause extractors

use sqlparser::ast::{Expr, OrderByExpr, Select, SelectItem, TableFactor};

use crate::transformer::traits::*;

/// Extract table name from SELECT
pub fn extract_table(select: &Select) -> Result<String, ExtractError> {
    select
        .from
        .first()
        .map(|f| extract_table_from_factor(&f.relation))
        .transpose()?
        .ok_or_else(|| ExtractError {
            message: "No FROM clause found".to_string(),
        })
}

/// Extract table name from TableFactor
pub fn extract_table_from_factor(factor: &TableFactor) -> Result<String, ExtractError> {
    match factor {
        TableFactor::Table { name, .. } => Ok(name.to_string()),
        _ => Err(ExtractError {
            message: "Complex table expression not supported".to_string(),
        }),
    }
}

/// Extract column names from SELECT
pub fn extract_columns(select: &Select) -> Vec<String> {
    select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => expr_to_string(expr),
            SelectItem::ExprWithAlias { alias, .. } => alias.value.clone(),
            SelectItem::Wildcard(_) => "*".to_string(),
            SelectItem::QualifiedWildcard(name, _) => format!("{}.*", name),
        })
        .collect()
}

/// Extract filter from WHERE clause
pub fn extract_filter(expr: &Expr) -> Result<FilterData, ExtractError> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let column = expr_to_string(left);
            let operator = match op {
                sqlparser::ast::BinaryOperator::Eq => "Operator::Eq",
                sqlparser::ast::BinaryOperator::NotEq => "Operator::NotEq",
                sqlparser::ast::BinaryOperator::Lt => "Operator::Lt",
                sqlparser::ast::BinaryOperator::LtEq => "Operator::LtEq",
                sqlparser::ast::BinaryOperator::Gt => "Operator::Gt",
                sqlparser::ast::BinaryOperator::GtEq => "Operator::GtEq",
                _ => "Operator::Eq",
            };
            let value = expr_to_value(right);

            Ok(FilterData {
                column,
                operator: operator.to_string(),
                value,
            })
        }
        _ => Err(ExtractError {
            message: "Complex filter expression not supported".to_string(),
        }),
    }
}

/// Extract ORDER BY clause
pub fn extract_order_by(exprs: &[OrderByExpr]) -> Vec<OrderByData> {
    exprs
        .iter()
        .map(|o| OrderByData {
            column: expr_to_string(&o.expr),
            descending: o.options.asc == Some(false),
        })
        .collect()
}

/// Extract LIMIT value
pub fn extract_limit(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Value(value_with_span) => {
            let s = value_with_span.to_string();
            s.parse().ok()
        }
        _ => None,
    }
}

/// Convert expression to string
pub fn expr_to_string(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => parts.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join("."),
        _ => expr.to_string(),
    }
}

/// Convert expression to ValueData
pub fn expr_to_value(expr: &Expr) -> ValueData {
    match expr {
        Expr::Value(v) => {
            let s = v.to_string();
            if let Some(stripped) = s.strip_prefix('$')
                && let Ok(n) = stripped.parse::<usize>()
            {
                return ValueData::Param(n);
            }
            if s == "NULL" {
                ValueData::Null
            } else {
                ValueData::Literal(s)
            }
        }
        Expr::Identifier(ident) => ValueData::Column(ident.value.clone()),
        _ => ValueData::Literal(expr.to_string()),
    }
}
