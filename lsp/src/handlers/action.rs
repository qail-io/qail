//! Code Action Handler - SQL to QAIL Migration

use std::collections::HashMap;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use qail_core::analyzer::detect_raw_sql;

use crate::server::QailLanguageServer;

/// Detect fetch method from sqlx query chain
fn detect_fetch_method(lines: &[&str], start: usize, end: usize) -> &'static str {
    for i in start..=end.min(lines.len().saturating_sub(1)) {
        if let Some(line) = lines.get(i) {
            if line.contains(".fetch_optional") { return "fetch_optional"; }
            if line.contains(".fetch_one") { return "fetch_one"; }
            if line.contains(".fetch_all") { return "fetch_all"; }
            if line.contains(".execute") { return "execute"; }
        }
    }
    "fetch_all"
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

/// Extract bind parameters from code block
fn extract_binds(lines: &[&str], start: usize, end: usize) -> Vec<String> {
    let mut binds = Vec::new();
    for i in start..=end {
        if let Some(line) = lines.get(i)
            && line.contains(".bind(")
            && let Some(start_idx) = line.find(".bind(")
        {
            let rest = &line[start_idx + 6..];
            if let Some(end_idx) = rest.find(')') {
                binds.push(rest[..end_idx].trim().to_string());
            }
        }
    }
    binds
}

/// Extract return type from query_as::<_, Type>
fn extract_return_type(lines: &[&str], start: usize, end: usize) -> Option<String> {
    for i in start..=end {
        if let Some(line) = lines.get(i)
            && line.contains("query_as::<")
            && let Some(start_idx) = line.find("query_as::<_, ")
        {
            let rest = &line[start_idx + 14..];
            if let Some(end_idx) = rest.find('>') {
                return Some(rest[..end_idx].trim().to_string());
            }
        }
    }
    None
}

/// Find block boundaries (let query = ... .await?)
fn find_block_range(lines: &[&str], sql_start: usize, sql_end: usize) -> (usize, usize) {
    let mut block_start = sql_start;
    for i in (0..=sql_start).rev() {
        if let Some(line) = lines.get(i)
            && line.trim_start().starts_with("let ") 
            && (line.contains("= r\"") || line.contains("= \"") || line.contains("query"))
        {
            block_start = i;
            break;
        }
    }

    let mut block_end = sql_end;
    for i in sql_end..lines.len().min(sql_end + 15) {
        if let Some(line) = lines.get(i)
            && line.contains(".await")
        {
            block_end = i;
            break;
        }
    }

    (block_start, block_end)
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
                format!("{}{}", " ".repeat(target_indent + relative), line.trim_start())
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
            let rest = &code[idx + 12..];  // Skip `.set_value("`
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

        let docs = self.documents.read().unwrap();
        let Some(content) = docs.get(uri.as_str()) else {
            return Ok(Some(actions));
        };

        // Detect raw SQL
        let sql_matches = detect_raw_sql(content);
        let lines: Vec<&str> = content.lines().collect();

        for sql_match in &sql_matches {
            let sql_start = sql_match.line - 1;
            let sql_end = sql_match.end_line - 1;

            if params.range.start.line as usize <= sql_end
                && params.range.end.line as usize >= sql_start
            {
                // Find block boundaries
                let (block_start, block_end) = find_block_range(&lines, sql_start, sql_end);

                let binds = extract_binds(&lines, sql_end, block_end);
                let return_type = extract_return_type(&lines, sql_end, block_end);
                let fetch_method = detect_fetch_method(&lines, sql_end, block_end);
                let driver_method = get_driver_method(fetch_method, &sql_match.sql_type);

                let qail_code = transform_qail_code(
                    sql_match.suggested_qail.clone(),
                    &binds,
                    return_type.as_deref(),
                    driver_method,
                );

                // Apply indentation
                let target_indent = lines.get(block_start)
                    .map(|l| l.len() - l.trim_start().len())
                    .unwrap_or(0);
                let indented_code = apply_indentation(&qail_code, target_indent);

                let end_col = lines.get(block_end)
                    .map(|l| l.find(';').map(|p| p + 1).unwrap_or(l.len()))
                    .unwrap_or(0);

                let range = Range {
                    start: Position { line: block_start as u32, character: 0 },
                    end: Position { line: block_end as u32, character: end_col as u32 },
                };

                let mut changes = HashMap::new();
                changes.insert(uri.clone(), vec![TextEdit {
                    range,
                    new_text: indented_code,
                }]);

                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: format!("🚀 Migrate {} to QAIL", sql_match.sql_type),
                    kind: Some(CodeActionKind::REFACTOR),
                    edit: Some(WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    }),
                    is_preferred: Some(true),
                    ..Default::default()
                }));
            }
        }

        Ok(Some(actions))
    }
}
