//! Code Action Handler - SQL to QAIL Migration

use qail_core::analyzer::rust_ast::transformer::sql_to_qail;
use qail_core::analyzer::{FetchMethod, QueryCall, SqlType, detect_query_calls};
use std::collections::HashMap;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use crate::server::QailLanguageServer;
use crate::utf16::Utf16Index;

/// Map fetch method + SQL type to driver method
fn get_driver_method(fetch_method: FetchMethod, sql_type: SqlType) -> &'static str {
    match (fetch_method, sql_type) {
        (FetchMethod::FetchOptional, _) => "query_optional",
        (FetchMethod::FetchOne, _) => "query_one",
        (FetchMethod::Execute, _) => "execute",
        (_, SqlType::Select) => "query_as",
        (_, SqlType::Insert) => "query_one",
        (_, SqlType::Update) => "execute",
        (_, SqlType::Delete) => "execute",
        _ => "query_as",
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

fn query_edit_range(content: &str, lines: &[&str], query: &QueryCall) -> Range {
    let start_line = query_block_start(lines, query);
    let end_line = query
        .end_line
        .saturating_sub(1)
        .min(lines.len().saturating_sub(1));
    let index = Utf16Index::new(content);

    let end_col_bytes = lines
        .get(end_line)
        .map(|line| expand_end_column(line, query.end_column))
        .unwrap_or(query.end_column);
    let end_col_utf16 = index
        .byte_col_to_utf16(end_line, end_col_bytes)
        .unwrap_or(end_col_bytes);

    Range {
        start: Position {
            line: start_line as u32,
            character: 0,
        },
        end: Position {
            line: end_line as u32,
            character: end_col_utf16 as u32,
        },
    }
}

fn diagnostic_code(diag: &Diagnostic) -> Option<&str> {
    match diag.code.as_ref()? {
        NumberOrString::String(code) => Some(code.as_str()),
        NumberOrString::Number(_) => None,
    }
}

fn parse_did_you_mean_replacement(message: &str) -> Option<(String, String)> {
    let marker = "Did you mean '";
    let marker_idx = message.find(marker)?;

    let missing = extract_first_single_quoted(message.get(..marker_idx)?)?;
    let suggestion = extract_single_quoted_after(message, marker)?;

    if missing.is_empty() || suggestion.is_empty() || missing == suggestion {
        return None;
    }

    Some((missing, suggestion))
}

fn extract_first_single_quoted(input: &str) -> Option<String> {
    let start = input.find('\'')? + 1;
    let end = start + input.get(start..)?.find('\'')?;
    Some(input.get(start..end)?.to_string())
}

fn extract_single_quoted_after(input: &str, marker: &str) -> Option<String> {
    let start = input.find(marker)? + marker.len();
    let end = start + input.get(start..)?.find('\'')?;
    Some(input.get(start..end)?.to_string())
}

fn range_to_offsets(index: &Utf16Index<'_>, range: &Range) -> Option<(usize, usize)> {
    let start = index.position_to_offset(range.start)?;
    let end = index.position_to_offset(range.end)?;
    (start <= end).then_some((start, end))
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn find_identifier_occurrence(haystack: &str, needle: &str) -> Option<(usize, usize)> {
    if needle.is_empty() {
        return None;
    }

    let bytes = haystack.as_bytes();
    let mut cursor = 0usize;

    while let Some(found) = haystack.get(cursor..).and_then(|s| s.find(needle)) {
        let start = cursor + found;
        let end = start + needle.len();
        let prev_is_ident = start
            .checked_sub(1)
            .and_then(|idx| bytes.get(idx))
            .is_some_and(|b| is_ident_byte(*b));
        let next_is_ident = bytes.get(end).is_some_and(|b| is_ident_byte(*b));

        if !prev_is_ident && !next_is_ident {
            return Some((start, end));
        }

        cursor = end;
    }

    None
}

fn replacement_edit_for_diagnostic(
    content: &str,
    diagnostic: &Diagnostic,
    from: &str,
    to: &str,
) -> Option<TextEdit> {
    let index = Utf16Index::new(content);
    let (range_start, range_end) = range_to_offsets(&index, &diagnostic.range)?;
    let snippet = content.get(range_start..range_end)?;
    let (local_start, local_end) = find_identifier_occurrence(snippet, from)?;
    let abs_start = range_start + local_start;
    let abs_end = range_start + local_end;

    Some(TextEdit {
        range: Range {
            start: index.offset_to_position(abs_start),
            end: index.offset_to_position(abs_end),
        },
        new_text: to.to_string(),
    })
}

fn with_rls_insertion_rel(snippet: &str) -> Option<usize> {
    if let Some(build_idx) = snippet.find(".build(") {
        return Some(build_idx);
    }

    let trimmed_len = snippet.trim_end_matches(char::is_whitespace).len();
    if trimmed_len == 0 {
        return None;
    }

    if snippet.as_bytes().get(trimmed_len.saturating_sub(1)) == Some(&b';') {
        return Some(trimmed_len.saturating_sub(1));
    }

    Some(trimmed_len)
}

fn with_rls_edit_for_diagnostic(content: &str, diagnostic: &Diagnostic) -> Option<TextEdit> {
    let index = Utf16Index::new(content);
    let (range_start, range_end) = range_to_offsets(&index, &diagnostic.range)?;
    let snippet = content.get(range_start..range_end)?;

    if !snippet.contains("Qail::") || snippet.contains(".with_rls(") || snippet.contains(".rls(") {
        return None;
    }

    let insertion_rel = with_rls_insertion_rel(snippet)?;
    let insertion_offset = range_start + insertion_rel;
    let pos = index.offset_to_position(insertion_offset);

    Some(TextEdit {
        range: Range {
            start: pos,
            end: pos,
        },
        new_text: ".with_rls(&ctx)".to_string(),
    })
}

fn missing_tenant_scope_edit(
    content: &str,
    diagnostic: &Diagnostic,
    new_text: &str,
) -> Option<TextEdit> {
    let index = Utf16Index::new(content);
    let (range_start, range_end) = range_to_offsets(&index, &diagnostic.range)?;
    let snippet = content.get(range_start..range_end)?;

    if !snippet.contains("Qail::") {
        return None;
    }
    if snippet.contains(".eq(\"tenant_id\"")
        || snippet.contains(".where_eq(\"tenant_id\"")
        || snippet.contains(".is_null(\"tenant_id\"")
    {
        return None;
    }

    let insertion_rel = with_rls_insertion_rel(snippet)?;
    let insertion_offset = range_start + insertion_rel;
    let pos = index.offset_to_position(insertion_offset);

    Some(TextEdit {
        range: Range {
            start: pos,
            end: pos,
        },
        new_text: new_text.to_string(),
    })
}

fn semantic_quickfix_actions(
    uri: &Url,
    content: &str,
    diagnostics: &[Diagnostic],
) -> Vec<CodeActionOrCommand> {
    let mut actions = Vec::new();

    for diagnostic in diagnostics {
        match diagnostic_code(diagnostic) {
            Some("QAIL-SCHEMA") => {
                let Some((from, to)) = parse_did_you_mean_replacement(&diagnostic.message) else {
                    continue;
                };
                let Some(edit) = replacement_edit_for_diagnostic(content, diagnostic, &from, &to)
                else {
                    continue;
                };

                let mut changes = HashMap::new();
                changes.insert(uri.clone(), vec![edit]);

                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: format!("Replace '{}' with '{}'", from, to),
                    kind: Some(CodeActionKind::QUICKFIX),
                    diagnostics: Some(vec![diagnostic.clone()]),
                    edit: Some(WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    }),
                    is_preferred: Some(true),
                    ..Default::default()
                }));
            }
            Some("QAIL-RLS") => {
                if !diagnostic.message.contains("has no .with_rls()") {
                    if diagnostic.message.contains("no explicit tenant scope") {
                        if let Some(edit) = missing_tenant_scope_edit(
                            content,
                            diagnostic,
                            ".eq(\"tenant_id\", tenant_id)",
                        ) {
                            let mut changes = HashMap::new();
                            changes.insert(uri.clone(), vec![edit]);
                            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                                title: "Add tenant scope .eq(\"tenant_id\", tenant_id)".to_string(),
                                kind: Some(CodeActionKind::QUICKFIX),
                                diagnostics: Some(vec![diagnostic.clone()]),
                                edit: Some(WorkspaceEdit {
                                    changes: Some(changes),
                                    ..Default::default()
                                }),
                                is_preferred: Some(true),
                                ..Default::default()
                            }));
                        }

                        if let Some(edit) = missing_tenant_scope_edit(
                            content,
                            diagnostic,
                            ".is_null(\"tenant_id\")",
                        ) {
                            let mut changes = HashMap::new();
                            changes.insert(uri.clone(), vec![edit]);
                            actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                                title: "Add global scope .is_null(\"tenant_id\")".to_string(),
                                kind: Some(CodeActionKind::QUICKFIX),
                                diagnostics: Some(vec![diagnostic.clone()]),
                                edit: Some(WorkspaceEdit {
                                    changes: Some(changes),
                                    ..Default::default()
                                }),
                                is_preferred: Some(false),
                                ..Default::default()
                            }));
                        }
                    }
                    continue;
                }
                let Some(edit) = with_rls_edit_for_diagnostic(content, diagnostic) else {
                    continue;
                };

                let mut changes = HashMap::new();
                changes.insert(uri.clone(), vec![edit]);

                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                    title: "Add .with_rls(&ctx)".to_string(),
                    kind: Some(CodeActionKind::QUICKFIX),
                    diagnostics: Some(vec![diagnostic.clone()]),
                    edit: Some(WorkspaceEdit {
                        changes: Some(changes),
                        ..Default::default()
                    }),
                    is_preferred: Some(true),
                    ..Default::default()
                }));
            }
            _ => {}
        }
    }

    actions
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

fn sql_migration_actions(uri: &Url, content: &str, selection: &Range) -> Vec<CodeActionOrCommand> {
    let mut actions = Vec::new();
    let query_calls = detect_query_calls(content);
    let lines: Vec<&str> = content.lines().collect();

    for query in &query_calls {
        if !selection_overlaps_query(selection, query) {
            continue;
        }

        if query.sql_type == SqlType::Unknown {
            continue;
        }

        let suggested_qail =
            sql_to_qail(&query.sql).unwrap_or_else(|_| "// Could not parse SQL".to_string());
        let driver_method = get_driver_method(query.fetch_method, query.sql_type);

        let qail_code = transform_qail_code(
            suggested_qail,
            &query.binds,
            query.return_type.as_deref(),
            driver_method,
        );

        let range = query_edit_range(content, &lines, query);
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
            title: format!("🚀 Migrate {} to QAIL", query.sql_type.as_str()),
            kind: Some(CodeActionKind::REFACTOR),
            edit: Some(WorkspaceEdit {
                changes: Some(changes),
                ..Default::default()
            }),
            is_preferred: Some(true),
            ..Default::default()
        }));
    }

    actions
}

impl QailLanguageServer {
    /// Handle code action request
    pub async fn handle_code_action(
        &self,
        params: CodeActionParams,
    ) -> Result<Option<Vec<CodeActionOrCommand>>> {
        let mut actions = Vec::new();
        let uri = params.text_document.uri.clone();

        let docs = self
            .documents
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(content) = docs.get(uri.as_str()) else {
            return Ok(Some(actions));
        };

        actions.extend(semantic_quickfix_actions(
            &uri,
            content,
            &params.context.diagnostics,
        ));

        if uri.as_str().ends_with(".rs") {
            actions.extend(sql_migration_actions(&uri, content, &params.range));
        }

        Ok(Some(actions))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply_edit(content: &str, edit: &TextEdit) -> String {
        let index = Utf16Index::new(content);
        let (start, end) = range_to_offsets(&index, &edit.range).expect("valid range");
        format!("{}{}{}", &content[..start], edit.new_text, &content[end..])
    }

    #[test]
    fn driver_method_uses_semantic_fields() {
        assert_eq!(
            get_driver_method(FetchMethod::FetchOptional, SqlType::Select),
            "query_optional"
        );
        assert_eq!(
            get_driver_method(FetchMethod::Unknown, SqlType::Update),
            "execute"
        );
    }

    #[test]
    fn query_range_extends_through_optional_suffix() {
        let lines = vec!["let rows = query(\"SELECT 1\").fetch_all(&pool).await?;"];
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
            sql_type: SqlType::Select,
            binds: vec![],
            fetch_method: FetchMethod::FetchAll,
            return_type: None,
            query_fn: "query".to_string(),
        };

        let content = lines.join("\n");
        let range = query_edit_range(&content, &lines, &query);
        assert_eq!(range.start.line, 0);
        assert_eq!(range.start.character, 0);
        assert_eq!(range.end.line, 0);
        assert_eq!(
            range.end.character as usize,
            lines[0].encode_utf16().count()
        );
    }

    #[test]
    fn query_range_uses_utf16_columns() {
        let lines = vec!["🙂 let rows = query(\"SELECT 1\").fetch_all(&pool).await?;"];
        let await_end = lines[0]
            .find(".await")
            .map(|idx| idx + ".await".len())
            .expect("await token expected");

        let query = QueryCall {
            start_line: 1,
            start_column: 6,
            end_line: 1,
            end_column: await_end,
            sql: "SELECT 1".to_string(),
            sql_type: SqlType::Select,
            binds: vec![],
            fetch_method: FetchMethod::FetchAll,
            return_type: None,
            query_fn: "query".to_string(),
        };

        let content = lines.join("\n");
        let range = query_edit_range(&content, &lines, &query);
        let expected_utf16 = lines[0].encode_utf16().count();
        assert_eq!(range.end.character as usize, expected_utf16);
    }

    #[test]
    fn parse_did_you_mean_handles_table_and_column_messages() {
        let table = "Table 'usrs' not found. Did you mean 'users'?";
        assert_eq!(
            parse_did_you_mean_replacement(table),
            Some(("usrs".to_string(), "users".to_string()))
        );

        let column = "Column 'emial' not found in table 'users'. Did you mean 'email'?";
        assert_eq!(
            parse_did_you_mean_replacement(column),
            Some(("emial".to_string(), "email".to_string()))
        );
    }

    #[test]
    fn schema_quickfix_replaces_token_in_diagnostic_range() {
        let src = r#"let cmd = Qail::get("usrs").columns(["id"]);"#;
        let diag = Diagnostic {
            range: Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end: Position {
                    line: 0,
                    character: src.len() as u32,
                },
            },
            code: Some(NumberOrString::String("QAIL-SCHEMA".to_string())),
            message: "Table 'usrs' not found. Did you mean 'users'?".to_string(),
            ..Default::default()
        };

        let edit = replacement_edit_for_diagnostic(src, &diag, "usrs", "users")
            .expect("replacement should exist");
        let rewritten = apply_edit(src, &edit);
        assert_eq!(
            rewritten,
            r#"let cmd = Qail::get("users").columns(["id"]);"#
        );
    }

    #[test]
    fn schema_quickfix_respects_identifier_boundaries() {
        let src = r#"let cmd = Qail::get("users_backup");"#;
        let diag = Diagnostic {
            range: Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end: Position {
                    line: 0,
                    character: src.len() as u32,
                },
            },
            ..Default::default()
        };

        let edit = replacement_edit_for_diagnostic(src, &diag, "users", "accounts");
        assert!(edit.is_none(), "partial token should not be replaced");
    }

    #[test]
    fn rls_quickfix_inserts_before_semicolon() {
        let src = r#"let cmd = Qail::get("orders");"#;
        let diag = Diagnostic {
            range: Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end: Position {
                    line: 0,
                    character: src.len() as u32,
                },
            },
            code: Some(NumberOrString::String("QAIL-RLS".to_string())),
            message: "⚠️ RLS AUDIT: Qail::get(\"orders\") has no .with_rls()".to_string(),
            ..Default::default()
        };

        let edit = with_rls_edit_for_diagnostic(src, &diag).expect("rls edit expected");
        let rewritten = apply_edit(src, &edit);
        assert_eq!(
            rewritten,
            r#"let cmd = Qail::get("orders").with_rls(&ctx);"#
        );
    }

    #[test]
    fn rls_quickfix_inserts_before_build_call() {
        let src = r#"let cmd = Qail::typed(Orders).build();"#;
        let diag = Diagnostic {
            range: Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end: Position {
                    line: 0,
                    character: src.len() as u32,
                },
            },
            code: Some(NumberOrString::String("QAIL-RLS".to_string())),
            message: "⚠️ RLS AUDIT: Qail::get(\"orders\") has no .with_rls()".to_string(),
            ..Default::default()
        };

        let edit = with_rls_edit_for_diagnostic(src, &diag).expect("rls edit expected");
        let rewritten = apply_edit(src, &edit);
        assert_eq!(
            rewritten,
            r#"let cmd = Qail::typed(Orders).with_rls(&ctx).build();"#
        );
    }

    #[test]
    fn rls_quickfix_adds_tenant_scope_eq_scaffold() {
        let src = r#"let _q = Qail::get("orders").columns(["id"]);"#;
        let diag = Diagnostic {
            range: Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end: Position {
                    line: 0,
                    character: src.len() as u32,
                },
            },
            code: Some(NumberOrString::String("QAIL-RLS".to_string())),
            message: "⚠️ RLS AUDIT: no explicit tenant scope".to_string(),
            ..Default::default()
        };

        let edit = missing_tenant_scope_edit(src, &diag, ".eq(\"tenant_id\", tenant_id)")
            .expect("tenant scope edit expected");
        let rewritten = apply_edit(src, &edit);
        assert_eq!(
            rewritten,
            r#"let _q = Qail::get("orders").columns(["id"]).eq("tenant_id", tenant_id);"#
        );
    }

    #[test]
    fn rls_quickfix_adds_tenant_scope_is_null_scaffold() {
        let src = r#"let _q = Qail::typed(Orders).build();"#;
        let diag = Diagnostic {
            range: Range {
                start: Position {
                    line: 0,
                    character: 0,
                },
                end: Position {
                    line: 0,
                    character: src.len() as u32,
                },
            },
            code: Some(NumberOrString::String("QAIL-RLS".to_string())),
            message: "⚠️ RLS AUDIT: no explicit tenant scope".to_string(),
            ..Default::default()
        };

        let edit = missing_tenant_scope_edit(src, &diag, ".is_null(\"tenant_id\")")
            .expect("tenant scope edit expected");
        let rewritten = apply_edit(src, &edit);
        assert_eq!(
            rewritten,
            r#"let _q = Qail::typed(Orders).is_null("tenant_id").build();"#
        );
    }
}
