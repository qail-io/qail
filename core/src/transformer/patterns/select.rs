//! SELECT pattern implementation

use sqlparser::ast::{SetExpr, Statement};

use crate::transformer::traits::*;
use crate::transformer::clauses::*;

/// SELECT query pattern
pub struct SelectPattern;

impl SqlPattern for SelectPattern {
    fn id(&self) -> &'static str {
        "select"
    }

    fn priority(&self) -> u32 {
        100
    }

    fn matches(&self, stmt: &Statement, _ctx: &MatchContext) -> bool {
        matches!(stmt, Statement::Query(q) if matches!(q.body.as_ref(), SetExpr::Select(_)))
    }

    fn extract(&self, stmt: &Statement, _ctx: &MatchContext) -> Result<PatternData, ExtractError> {
        let Statement::Query(query) = stmt else {
            return Err(ExtractError {
                message: "Expected Query statement".to_string(),
            });
        };

        let SetExpr::Select(select) = query.body.as_ref() else {
            return Err(ExtractError {
                message: "Expected SELECT".to_string(),
            });
        };

        let table = extract_table(select)?;
        let columns = extract_columns(select);
        let filter = select
            .selection
            .as_ref()
            .map(extract_filter)
            .transpose()?;

        let order_by = query.order_by.as_ref().and_then(|ob| {
            match &ob.kind {
                sqlparser::ast::OrderByKind::All(_) => None,
                sqlparser::ast::OrderByKind::Expressions(exprs) => {
                    if exprs.is_empty() {
                        None
                    } else {
                        Some(extract_order_by(exprs))
                    }
                }
            }
        });

        let limit = query.limit_clause.as_ref().and_then(|lc| {
            match lc {
                sqlparser::ast::LimitClause::LimitOffset { limit, .. } => {
                    limit.as_ref().and_then(extract_limit)
                }
                _ => None,
            }
        });

        Ok(PatternData::Select {
            table,
            columns,
            filter,
            order_by,
            limit,
            joins: Vec::new(),
        })
    }

    fn transform(&self, data: &PatternData, ctx: &TransformContext) -> Result<String, TransformError> {
        let PatternData::Select {
            table,
            columns,
            filter,
            order_by,
            limit,
            ..
        } = data
        else {
            return Err(TransformError {
                message: "Expected Select data".to_string(),
            });
        };

        let mut lines = Vec::new();

        if ctx.include_imports {
            lines.push("use qail_core::ast::{Qail, Operator, Order};".to_string());
            lines.push(String::new());
        }

        let mut chain = format!("let cmd = Qail::get(\"{}\")", table);

        if !columns.is_empty() && columns[0] != "*" {
            let cols: Vec<String> = columns.iter().map(|c| format!("\"{}\"", c)).collect();
            chain.push_str(&format!("\n    .columns([{}])", cols.join(", ")));
        }

        if let Some(f) = filter {
            let value = match &f.value {
                ValueData::Param(n) => {
                    ctx.binds
                        .get(*n - 1)
                        .cloned()
                        .unwrap_or_else(|| format!("param_{}", n))
                }
                ValueData::Literal(s) => s.clone(),
                ValueData::Column(c) => format!("\"{}\"", c),
                ValueData::Null => "None".to_string(),
            };
            chain.push_str(&format!(
                "\n    .filter(\"{}\", {}, {})",
                f.column, f.operator, value
            ));
        }

        if let Some(orders) = order_by {
            for o in orders {
                let dir = if o.descending { "Order::Desc" } else { "Order::Asc" };
                chain.push_str(&format!("\n    .order_by(\"{}\", {})", o.column, dir));
            }
        }

        if let Some(l) = limit {
            chain.push_str(&format!("\n    .limit({})", l));
        }

        chain.push(';');
        lines.push(chain);

        lines.push(String::new());
        let default_row_type = format!("{}Row", to_pascal_case(table));
        let row_type = ctx.return_type.as_deref().unwrap_or(&default_row_type);
        lines.push(format!(
            "let rows: Vec<{}> = driver.query_as(&cmd).await?;",
            row_type
        ));

        Ok(lines.join("\n"))
    }
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().chain(chars).collect::<String>(),
                None => String::new(),
            }
        })
        .collect()
}
