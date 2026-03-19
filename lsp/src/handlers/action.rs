//! Code Action Handler - SQL to QAIL Migration

use qail_core::analyzer::rust_ast::transformer::sql_to_qail;
use qail_core::analyzer::{QueryCall, detect_query_calls};
use std::collections::HashMap;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use crate::server::QailLanguageServer;

/// Detect fetch method from semantic query call span.
fn detect_fetch_method(lines: &[&str], query: &QueryCall, sql_type: &str) -> &'static str {
    let start = query.start_line.saturating_sub(1);
    let end = query
        .end_line
        .saturating_sub(1)
        .min(lines.len().saturating_sub(1));

    if start <= end {
        for i in start..=end {
            if let Some(line) = lines.get(i) {
                if line.contains(".fetch_optional") {
                    return "fetch_optional";
                }
                if line.contains(".fetch_one") {
                    return "fetch_one";
                }
                if line.contains(".fetch_all") {
                    return "fetch_all";
                }
                if line.contains(".execute") {
                    return "execute";
                }
            }
        }
    }

    if sql_type == "SELECT" {
        "fetch_all"
    } else {
        "execute"
    }
}

/// Map fetch method + SQL type to driver method
fn get_driver_method(fetch_method: &str, sql_type: &str) -> &'static str {
    match (fetch_method, sql_type) {
        ("fetch_optional", _) => "query_optional",
        ("fetch_one", _) => "query_one",
        ("execute", _) => "execute",
        (_, "SELECT") => "query_as",
        (_, "INSERT") => "query_one",
        (_, "UPDATE") => "execute",
        (_, "DELETE") => "execute",
        _ => "query_as",
    }
}

fn classify_sql_type(sql: &str) -> Option<&'static str> {
    let upper = sql.to_ascii_uppercase();

    if upper.contains("SELECT") && upper.contains("FROM") {
        Some("SELECT")
    } else if upper.contains("INSERT INTO") {
        Some("INSERT")
    } else if upper.contains("UPDATE") && upper.contains("SET") {
        Some("UPDATE")
    } else if upper.contains("DELETE FROM") {
        Some("DELETE")
    } else {
        None
    }
}

fn selection_overlaps_query(selection: &Range, query: &QueryCall) -> bool {
    let start = query.start_line.saturating_sub(1) as u32;
    let end = query.end_line.saturating_sub(1) as u32;
    selection.start.line <= end && selection.end.line >= start
}

fn query_block_start(lines: &[&str], query: &QueryCall) -> usize {
    let mut start = query.start_line.saturating_sub(1);
    if lines.is_empty() {
        return 0;
    }
    start = start.min(lines.len().saturating_sub(1));

    while start > 0 {
        let prev = lines[start - 1].trim_end();
        if prev.ends_with('=') {
            start -= 1;
            continue;
        }
        break;
    }

    start
}

fn expand_end_column(line: &str, end_column: usize) -> usize {
    let bytes = line.as_bytes();
    let mut col = end_column.min(bytes.len());

    while col < bytes.len() && bytes[col].is_ascii_whitespace() {
        col += 1;
    }

    if col < bytes.len() && bytes[col] == b'?' {
        col += 1;
    }

    while col < bytes.len() && bytes[col].is_ascii_whitespace() {
        col += 1;
    }

    if col < bytes.len() && bytes[col] == b';' {
        col += 1;
    }

    col
}

fn query_edit_range(lines: &[&str], query: &QueryCall) -> Range {
    let start_line = query_block_start(lines, query);
    let end_line = query
        .end_line
        .saturating_sub(1)
        .min(lines.len().saturating_sub(1));

    let end_col = lines
        .get(end_line)
        .map(|line| expand_end_column(line, query.end_column))
        .unwrap_or(query.end_column);

    Range {
        start: Position {
            line: start_line as u32,
            character: 0,
        },
        end: Position {
            line: end_line as u32,
            character: end_col as u32,
        },
    }
}

/// Apply indentation to generated code
fn apply_indentation(code: &str, target_indent: usize) -> String {
    let min_indent = code
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);

    code.lines()
        .map(|line| {
            if line.trim().is_empty() {
                String::new()
            } else {
                let line_indent = line.len() - line.trim_start().len();
                let relative = line_indent.saturating_sub(min_indent);
                format!(
                    "{}{}",
                    " ".repeat(target_indent + relative),
                    line.trim_start()
                )
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Transform QAIL code with proper return types
fn transform_qail_code(
    mut code: String,
    binds: &[String],
    return_type: Option<&str>,
    driver_method: &str,
) -> String {
    // Replace param placeholders with actual bind values (for SELECT/WHERE)
    for (i, bind) in binds.iter().enumerate() {
        let placeholder = format!("param_{} /* replace with actual value */", i + 1);
        code = code.replace(&placeholder, bind);
    }

    // For INSERT statements: replace {col}_value placeholders with bind values
    // Extract column names from .set_value("col_name", col_name_value) patterns using simple string parsing
    let columns: Vec<String> = code
        .match_indices(".set_value(\"")
        .filter_map(|(idx, _)| {
            let rest = &code[idx + 12..]; // Skip `.set_value("`
            rest.find('"').map(|end| rest[..end].to_string())
        })
        .collect();

    // Replace each {col}_value placeholder with corresponding bind value
    for (i, col) in columns.iter().enumerate() {
        if let Some(bind) = binds.get(i) {
            let placeholder = format!("{}_value", col);
            code = code.replace(&placeholder, bind);
        }
    }

    // Replace return type
    if let Some(rt) = return_type
        && let Some(start) = code.find("Vec<")
        && let Some(end) = code[start..].find('>')
    {
        let before = &code[..start + 4];
        let after = &code[start + end..];
        code = format!("{}{}{}", before, rt, after);
    }

    // Replace driver method
    code = code.replace("driver.query_as", &format!("driver.{}", driver_method));

    // Adjust for execute (no return type)
    if driver_method == "execute"
        && let Some(let_start) = code.find("let rows:")
        && let Some(eq_pos) = code[let_start..].find(" = ")
    {
        let before = &code[..let_start];
        let after = &code[let_start + eq_pos + 3..];
        code = format!("{}{}", before, after);
    }

    // Adjust for query_optional (Option<T>)
    if driver_method == "query_optional" {
        code = code.replace("Vec<", "Option<");
        code = code.replace("let rows:", "let row:");
    }

    // Adjust for query_one (T)
    if driver_method == "query_one"
        && let Some(vec_start) = code.find("Vec<")
        && let Some(end) = code[vec_start..].find('>')
    {
        let type_name = &code[vec_start + 4..vec_start + end];
        code = code.replace(&format!("Vec<{}>", type_name), type_name);
        code = code.replace("let rows:", "let row:");
    } else if driver_method == "query_one" {
        code = code.replace("let rows:", "let row:");
    }

    code
}

impl QailLanguageServer {
    /// Handle code action request
    pub async fn handle_code_action(
        &self,
        params: CodeActionParams,
    ) -> Result<Option<Vec<CodeActionOrCommand>>> {
        let mut actions = Vec::new();
        let uri = params.text_document.uri.clone();

        // Only process .rs files
        if !uri.as_str().ends_with(".rs") {
            return Ok(Some(actions));
        }

        let docs = self
            .documents
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(content) = docs.get(uri.as_str()) else {
            return Ok(Some(actions));
        };

        let query_calls = detect_query_calls(content);
        let lines: Vec<&str> = content.lines().collect();

        for query in &query_calls {
            if !selection_overlaps_query(&params.range, query) {
                continue;
            }

            let Some(sql_type) = classify_sql_type(&query.sql) else {
                continue;
            };

            let suggested_qail =
                sql_to_qail(&query.sql).unwrap_or_else(|_| "// Could not parse SQL".to_string());
            let fetch_method = detect_fetch_method(&lines, query, sql_type);
            let driver_method = get_driver_method(fetch_method, sql_type);

            let qail_code = transform_qail_code(
                suggested_qail,
                &query.binds,
                query.return_type.as_deref(),
                driver_method,
            );

            let range = query_edit_range(&lines, query);
            let target_indent = lines
                .get(range.start.line as usize)
                .map(|l| l.len() - l.trim_start().len())
                .unwrap_or(0);
            let indented_code = apply_indentation(&qail_code, target_indent);

            let mut changes = HashMap::new();
            changes.insert(
                uri.clone(),
                vec![TextEdit {
                    range,
                    new_text: indented_code,
                }],
            );

            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                title: format!("🚀 Migrate {} to QAIL", sql_type),
                kind: Some(CodeActionKind::REFACTOR),
                edit: Some(WorkspaceEdit {
                    changes: Some(changes),
                    ..Default::default()
                }),
                is_preferred: Some(true),
                ..Default::default()
            }));
        }

        Ok(Some(actions))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_sql_type_detects_select() {
        assert_eq!(
            classify_sql_type("SELECT id FROM users WHERE id = $1"),
            Some("SELECT")
        );
    }

    #[test]
    fn query_range_extends_through_optional_suffix() {
        let lines = vec!["let rows = sqlx::query(\"SELECT 1\").fetch_all(&pool).await?;"];
        let await_end = lines[0]
            .find(".await")
            .map(|idx| idx + ".await".len())
            .expect("await token expected");

        let query = QueryCall {
            start_line: 1,
            start_column: 11,
            end_line: 1,
            end_column: await_end,
            sql: "SELECT 1".to_string(),
            binds: vec![],
            return_type: None,
            query_fn: "query".to_string(),
        };

        let range = query_edit_range(&lines, &query);
        assert_eq!(range.start.line, 0);
        assert_eq!(range.start.character, 0);
        assert_eq!(range.end.line, 0);
        assert_eq!(range.end.character as usize, lines[0].len());
    }
}
