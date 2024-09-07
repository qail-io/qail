//! UPDATE pattern implementation

use sqlparser::ast::Statement;

use crate::transformer::traits::*;
use crate::transformer::clauses::*;

/// UPDATE query pattern
pub struct UpdatePattern;

impl SqlPattern for UpdatePattern {
    fn id(&self) -> &'static str {
        "update"
    }

    fn priority(&self) -> u32 {
        100
    }

    fn matches(&self, stmt: &Statement, _ctx: &MatchContext) -> bool {
        matches!(stmt, Statement::Update(_))
    }

    fn extract(&self, stmt: &Statement, _ctx: &MatchContext) -> Result<PatternData, ExtractError> {
        let Statement::Update(update) = stmt else {
            return Err(ExtractError {
                message: "Expected UPDATE statement".to_string(),
            });
        };

        let table = extract_table_from_factor(&update.table.relation)?;

        let set_values: Vec<SetValueData> = update
            .assignments
            .iter()
            .map(|a| {
                let column = a.target.to_string();
                let value = expr_to_value(&a.value);
                SetValueData { column, value }
            })
            .collect();

        let filter = update
            .selection
            .as_ref()
            .map(extract_filter)
            .transpose()?;

        let returning = update.returning.as_ref().map(|items| {
            items
                .iter()
                .map(|item| match item {
                    sqlparser::ast::SelectItem::UnnamedExpr(e) => e.to_string(),
                    sqlparser::ast::SelectItem::Wildcard(_) => "*".to_string(),
                    _ => item.to_string(),
                })
                .collect()
        });

        Ok(PatternData::Update {
            table,
            set_values,
            filter,
            returning,
        })
    }

    fn transform(&self, data: &PatternData, ctx: &TransformContext) -> Result<String, TransformError> {
        let PatternData::Update {
            table,
            set_values,
            filter,
            returning,
        } = data
        else {
            return Err(TransformError {
                message: "Expected Update data".to_string(),
            });
        };

        let mut lines = Vec::new();

        if ctx.include_imports {
            lines.push("use qail_core::ast::{Qail, Operator};".to_string());
            lines.push(String::new());
        }

        let mut chain = format!("let cmd = Qail::set(\"{}\")", table);

        for sv in set_values {
            let value = format_value(&sv.value, &ctx.binds);
            chain.push_str(&format!("\n    .set_value(\"{}\", {})", sv.column, value));
        }

        if let Some(f) = filter {
            let value = format_value(&f.value, &ctx.binds);
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
