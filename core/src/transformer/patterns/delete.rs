//! DELETE pattern implementation

use sqlparser::ast::Statement;

use crate::transformer::traits::*;
use crate::transformer::clauses::*;

/// DELETE query pattern
pub struct DeletePattern;

impl SqlPattern for DeletePattern {
    fn id(&self) -> &'static str {
        "delete"
    }

    fn priority(&self) -> u32 {
        100
    }

    fn matches(&self, stmt: &Statement, _ctx: &MatchContext) -> bool {
        matches!(stmt, Statement::Delete(_))
    }

    fn extract(&self, stmt: &Statement, _ctx: &MatchContext) -> Result<PatternData, ExtractError> {
        let Statement::Delete(delete) = stmt else {
            return Err(ExtractError {
                message: "Expected DELETE statement".to_string(),
            });
        };

        let table = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                tables.first()
                    .map(|t| extract_table_from_factor(&t.relation))
                    .transpose()?
                    .unwrap_or_else(|| "table".to_string())
            }
            sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                tables.first()
                    .map(|t| extract_table_from_factor(&t.relation))
                    .transpose()?
                    .unwrap_or_else(|| "table".to_string())
            }
        };

        let filter = delete
            .selection
            .as_ref()
            .map(extract_filter)
            .transpose()?;

        let returning = delete.returning.as_ref().map(|items| {
            items
                .iter()
                .map(|item| match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(e) => e.to_string(),
                    sqlparser::ast::SelectItem::Wildcard(_) => "*".to_string(),
                    _ => item.to_string(),
                })
                .collect()
        });

        Ok(PatternData::Delete {
            table,
            filter,
            returning,
        })
    }

    fn transform(&self, data: &PatternData, ctx: &TransformContext) -> Result<String, TransformError> {
        let PatternData::Delete {
            table,
            filter,
            returning,
        } = data
        else {
            return Err(TransformError {
                message: "Expected Delete data".to_string(),
            });
        };

        let mut lines = Vec::new();

        if ctx.include_imports {
            lines.push("use qail_core::ast::{Qail, Operator};".to_string());
            lines.push(String::new());
        }

        let mut chain = format!("let cmd = Qail::del(\"{}\")", table);

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

        if let Some(ret) = returning {
            if ret.contains(&"*".to_string()) {
                chain.push_str("\n    .returning([\"*\"])");
            } else {
                let cols: Vec<String> = ret.iter().map(|c| format!("\"{}\"", c)).collect();
                chain.push_str(&format!("\n    .returning([{}])", cols.join(", ")));
            }
        }

        chain.push(';');
        lines.push(chain);

        lines.push(String::new());
        if returning.is_some() {
            let default_row_type = format!("{}Row", to_pascal_case(table));
            let row_type = ctx.return_type.as_deref().unwrap_or(&default_row_type);
            lines.push(format!(
                "let row: {} = driver.query_one(&cmd).await?;",
                row_type
            ));
        } else {
            lines.push("driver.execute(&cmd).await?;".to_string());
        }

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
