//! INSERT pattern implementation

use sqlparser::ast::{SetExpr, Statement};

use crate::transformer::traits::*;

/// INSERT query pattern
pub struct InsertPattern;

impl SqlPattern for InsertPattern {
    fn id(&self) -> &'static str {
        "insert"
    }

    fn priority(&self) -> u32 {
        100
    }

    fn matches(&self, stmt: &Statement, _ctx: &MatchContext) -> bool {
        matches!(stmt, Statement::Insert(_))
    }

    fn extract(&self, stmt: &Statement, ctx: &MatchContext) -> Result<PatternData, ExtractError> {
        let Statement::Insert(insert) = stmt else {
            return Err(ExtractError {
                message: "Expected INSERT statement".to_string(),
            });
        };

        let table = insert.table.to_string();
        let columns: Vec<String> = insert.columns.iter().map(|c| c.value.clone()).collect();

        let mut values = Vec::new();
        if let Some(source) = &insert.source
            && let SetExpr::Values(v) = source.body.as_ref()
        {
            for row in &v.rows {
                for expr in row.iter() {
                    let value = crate::transformer::clauses::expr_to_value(expr);
                    if let ValueData::Param(n) = &value
                        && let Some(bind) = ctx.binds.get(*n - 1)
                    {
                        values.push(ValueData::Literal(bind.clone()));
                        continue;
                    }
                    values.push(value);
                }
            }
        }

        let returning = insert.returning.as_ref().map(|items| {
            items
                .iter()
                .map(|item| match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(e) => e.to_string(),
                    sqlparser::ast::SelectItem::Wildcard(_) => "*".to_string(),
                    _ => item.to_string(),
                })
                .collect()
        });

        Ok(PatternData::Insert {
            table,
            columns,
            values,
            returning,
        })
    }

    fn transform(&self, data: &PatternData, ctx: &TransformContext) -> Result<String, TransformError> {
        let PatternData::Insert {
            table,
            columns,
            values,
            returning,
        } = data
        else {
            return Err(TransformError {
                message: "Expected Insert data".to_string(),
            });
        };

        let mut lines = Vec::new();

        if ctx.include_imports {
            lines.push("use qail_core::ast::Qail;".to_string());
            lines.push(String::new());
        }

        let mut chain = format!("let cmd = Qail::add(\"{}\")", table);

        for (i, col) in columns.iter().enumerate() {
            let value = values.get(i).map(|v| format_value(v, &ctx.binds)).unwrap_or_else(|| "None".to_string());
            chain.push_str(&format!("\n    .set_value(\"{}\", {})", col, value));
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

fn format_value(value: &ValueData, binds: &[String]) -> String {
    match value {
        ValueData::Param(n) => binds
            .get(*n - 1)
            .cloned()
            .unwrap_or_else(|| format!("param_{}", n)),
        ValueData::Literal(s) => s.clone(),
        ValueData::Column(c) => format!("\"{}\"", c),
        ValueData::Null => "None".to_string(),
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
