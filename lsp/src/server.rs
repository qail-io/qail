//! QAIL Language Server Core

use qail_core::analyzer::{
    QueryCall, TextLiteral, detect_query_calls, extract_text_literals, literal_offset_to_line_col,
    looks_like_qail_query, looks_like_sql_query, trim_query_bounds,
};
use qail_core::ast::{Condition, Expr, Qail, Value};
use qail_core::build::{
    QailUsage, Schema as BuildSchema, ValidationDiagnosticKind, scan_source_text,
    validate_against_schema_diagnostics,
};
use qail_core::parse;
use qail_core::schema::Schema;
use qail_core::validator::Validator;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

use crate::utf16::Utf16Index;

/// QAIL Language Server
#[derive(Debug)]
pub struct QailLanguageServer {
    pub client: Client,
    pub documents: RwLock<HashMap<String, String>>,
    pub schema: RwLock<Option<Validator>>,
    pub build_schema: RwLock<Option<BuildSchema>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EmbeddedQueryKind {
    Qail,
    Sql,
}

#[derive(Debug, Clone)]
pub(crate) struct EmbeddedQuery {
    pub kind: EmbeddedQueryKind,
    pub text: String,
    pub start_line: usize,
    pub start_column: usize,
    pub end_line: usize,
    pub end_column: usize,
}

impl QailLanguageServer {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            documents: RwLock::new(HashMap::new()),
            schema: RwLock::new(None),
            build_schema: RwLock::new(None),
        }
    }

    pub fn try_load_schema_from_uri(&self, uri: &str) {
        if let Ok(schema) = self.schema.read()
            && schema.is_some()
        {
            return;
        }

        let Some(file_path) = uri_to_file_path(uri) else {
            return;
        };

        for candidate_dir in schema_probe_dirs(&file_path) {
            if self.load_schema_from_dir(&candidate_dir) {
                break;
            }
        }
    }

    fn load_schema_from_dir(&self, workspace_root: &Path) -> bool {
        let qail_path = workspace_root.join("schema.qail");
        let Ok(content) = qail_core::schema_source::read_qail_schema_source(&qail_path) else {
            return false;
        };

        let mut loaded_any = false;

        if let Ok(schema) = Schema::from_qail_schema(&content)
            && let Ok(mut s) = self.schema.write()
        {
            *s = Some(schema.to_validator());
            loaded_any = true;
        }

        if let Ok(schema) = BuildSchema::parse(&content)
            && let Ok(mut s) = self.build_schema.write()
        {
            *s = Some(schema);
            loaded_any = true;
        }

        loaded_any
    }

    pub(crate) fn extract_query_at_line(&self, uri: &str, line: usize) -> Option<EmbeddedQuery> {
        let docs = self.documents.read().ok()?;
        let content = docs.get(uri)?;

        if uri.ends_with(".rs") {
            return extract_rust_query_at_line(content, line);
        }

        extract_text_query_at_line(content, line)
    }

    /// Get diagnostics for a document. Pass the file URI to enable N+1 detection for `.rs` files.
    pub fn get_diagnostics(&self, text: &str, uri: &str) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        // ── QAIL query syntax diagnostics ──
        if uri.ends_with(".rs") {
            diagnostics.extend(collect_rust_qail_diagnostics(text));
        } else {
            diagnostics.extend(collect_text_qail_diagnostics(text));
        }

        // ── N+1 detection for Rust files ──
        if uri.ends_with(".rs") {
            let file_path = uri.strip_prefix("file://").unwrap_or(uri);
            let nplus1_diags = qail_core::analyzer::detect_n_plus_one_in_file(file_path, text);
            let utf16 = Utf16Index::new(text);

            for diag in nplus1_diags {
                let severity = match diag.severity {
                    qail_core::analyzer::NPlusOneSeverity::Error => DiagnosticSeverity::ERROR,
                    qail_core::analyzer::NPlusOneSeverity::Warning => DiagnosticSeverity::WARNING,
                };
                let start_char = utf16
                    .one_based_byte_col_to_utf16(diag.line, diag.column)
                    .unwrap_or(diag.column.saturating_sub(1));
                let end_char = utf16
                    .one_based_byte_col_to_utf16(diag.line, diag.end_column)
                    .unwrap_or(diag.end_column.saturating_sub(1));

                let mut message = diag.message.clone();
                if let Some(ref hint) = diag.hint {
                    message.push_str(&format!("\nHint: {}", hint));
                }

                diagnostics.push(Diagnostic {
                    range: Range {
                        start: Position {
                            line: (diag.line.saturating_sub(1)) as u32,
                            character: start_char as u32,
                        },
                        end: Position {
                            line: (diag.line.saturating_sub(1)) as u32,
                            character: end_char as u32,
                        },
                    },
                    severity: Some(severity),
                    code: Some(NumberOrString::String(diag.code.as_str().to_string())),
                    source: Some("qail-nplus1".to_string()),
                    message,
                    ..Default::default()
                });
            }
        }

        // ── Schema + RLS semantic diagnostics (matches build/CLI pipeline) ──
        if let Ok(schema) = self.build_schema.read()
            && let Some(build_schema) = schema.as_ref()
        {
            diagnostics.extend(collect_semantic_qail_diagnostics(text, uri, build_schema));
        }

        diagnostics
    }
}

fn uri_to_file_path(uri: &str) -> Option<PathBuf> {
    Url::parse(uri).ok()?.to_file_path().ok()
}

fn schema_probe_dirs(file_path: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut current = file_path.parent();

    while let Some(dir) = current {
        out.push(dir.to_path_buf());
        current = dir.parent();
    }

    out
}

fn collect_text_qail_diagnostics(text: &str) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let mut saw_embedded_query = false;

    for literal in extract_text_literals(text) {
        let Some((kind, query_text, start_line, start_col, end_line, end_col)) =
            literal_query_span(text, &literal)
        else {
            continue;
        };
        saw_embedded_query = true;

        if kind == EmbeddedQueryKind::Qail
            && let Err(e) = parse(query_text)
        {
            diagnostics.push(diagnostic_from_parse_error(
                start_line.saturating_sub(1),
                start_col.saturating_sub(1),
                end_line.saturating_sub(1),
                end_col.saturating_sub(1),
                e.to_string(),
            ));
        }
    }

    if !saw_embedded_query
        && let Some((kind, query_text, start_line, start_col, end_line, end_col)) =
            full_document_query_span(text)
        && kind == EmbeddedQueryKind::Qail
        && let Err(e) = parse(query_text)
    {
        diagnostics.push(diagnostic_from_parse_error(
            start_line.saturating_sub(1),
            start_col.saturating_sub(1),
            end_line.saturating_sub(1),
            end_col.saturating_sub(1),
            e.to_string(),
        ));
    }

    diagnostics
}

fn collect_rust_qail_diagnostics(text: &str) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let utf16 = Utf16Index::new(text);

    for query in detect_query_calls(text) {
        if !looks_like_qail_query(&query.sql) {
            continue;
        }

        if let Err(e) = parse(&query.sql) {
            let start_col = utf16
                .byte_col_to_utf16(query.start_line.saturating_sub(1), query.start_column)
                .unwrap_or(query.start_column);
            let end_col = utf16
                .byte_col_to_utf16(query.end_line.saturating_sub(1), query.end_column)
                .unwrap_or(query.end_column);
            diagnostics.push(diagnostic_from_parse_error(
                query.start_line.saturating_sub(1),
                start_col,
                query.end_line.saturating_sub(1),
                end_col,
                e.to_string(),
            ));
        }
    }

    diagnostics
}

fn collect_semantic_qail_diagnostics(
    text: &str,
    uri: &str,
    schema: &BuildSchema,
) -> Vec<Diagnostic> {
    let (usages, usage_ranges) = collect_document_usages(text, uri);
    if usages.is_empty() {
        return Vec::new();
    }

    let semantic = validate_against_schema_diagnostics(schema, &usages);
    let line_to_range: HashMap<usize, Range> = usage_ranges
        .into_iter()
        .map(|(line, range)| (line, range))
        .collect();

    semantic
        .into_iter()
        .map(|diag| {
            let line = extract_line_from_validation_message(&diag.message).unwrap_or(1);
            let range = line_to_range.get(&line).cloned().unwrap_or_else(|| Range {
                start: Position {
                    line: line.saturating_sub(1) as u32,
                    character: 0,
                },
                end: Position {
                    line: line.saturating_sub(1) as u32,
                    character: 1,
                },
            });

            let (severity, source, code) = match diag.kind {
                ValidationDiagnosticKind::SchemaError => {
                    (DiagnosticSeverity::ERROR, "qail-schema", "QAIL-SCHEMA")
                }
                ValidationDiagnosticKind::RlsWarning => {
                    (DiagnosticSeverity::WARNING, "qail-rls", "QAIL-RLS")
                }
            };

            Diagnostic {
                range,
                severity: Some(severity),
                code: Some(NumberOrString::String(code.to_string())),
                source: Some(source.to_string()),
                message: strip_file_line_prefix(&diag.message),
                ..Default::default()
            }
        })
        .collect()
}

fn collect_document_usages(text: &str, uri: &str) -> (Vec<QailUsage>, Vec<(usize, Range)>) {
    let file = uri.strip_prefix("file://").unwrap_or(uri).to_string();
    let file_uses_super_admin =
        text.contains("for_system_process(") && !text.contains("qail:allow(super_admin)");

    if uri.ends_with(".rs") {
        return collect_rust_document_usages(&file, text, file_uses_super_admin);
    }

    collect_text_document_usages(&file, text, file_uses_super_admin)
}

fn collect_rust_document_usages(
    file: &str,
    text: &str,
    file_uses_super_admin: bool,
) -> (Vec<QailUsage>, Vec<(usize, Range)>) {
    let mut usages = Vec::new();
    let mut ranges = Vec::new();
    let mut seen = HashSet::new();
    let utf16 = Utf16Index::new(text);

    for usage in scan_source_text(file, text) {
        let key = usage_dedupe_key(&usage);
        if !seen.insert(key) {
            continue;
        }

        let range = rust_usage_line_range_with_index(&utf16, usage.line, usage.column);
        ranges.push((usage.line, range));
        usages.push(usage);
    }

    for query in detect_query_calls(text) {
        if !looks_like_qail_query(&query.sql) {
            continue;
        }

        let Ok(cmd) = parse(&query.sql) else {
            continue;
        };

        let line = query.start_line;
        let start_char = utf16
            .byte_col_to_utf16(query.start_line.saturating_sub(1), query.start_column)
            .unwrap_or(query.start_column);
        let end_char = utf16
            .byte_col_to_utf16(query.end_line.saturating_sub(1), query.end_column)
            .unwrap_or(query.end_column);
        let range = Range {
            start: Position {
                line: query.start_line.saturating_sub(1) as u32,
                character: start_char as u32,
            },
            end: Position {
                line: query.end_line.saturating_sub(1) as u32,
                character: end_char as u32,
            },
        };

        let usage = QailUsage {
            file: file.to_string(),
            line,
            column: query.start_column + 1,
            table: cmd.table.clone(),
            is_dynamic_table: false,
            columns: collect_usage_columns(&cmd),
            action: action_to_usage_tag(&cmd).to_string(),
            is_cte_ref: false,
            has_rls: rust_query_chain_has_rls(text, &query),
            has_explicit_tenant_scope: cmd_has_explicit_tenant_scope(&cmd),
            file_uses_super_admin,
        };

        let key = usage_dedupe_key(&usage);
        if !seen.insert(key) {
            continue;
        }

        usages.push(usage);
        ranges.push((line, range));
    }

    (usages, ranges)
}

fn usage_dedupe_key(usage: &QailUsage) -> String {
    format!(
        "{}:{}:{}:{}",
        usage.line, usage.column, usage.action, usage.table
    )
}

fn rust_usage_line_range_with_index(utf16: &Utf16Index<'_>, line_1: usize, col_1: usize) -> Range {
    let line_idx = line_1.saturating_sub(1);
    let start_col = utf16
        .one_based_byte_col_to_utf16(line_1, col_1)
        .unwrap_or(col_1.saturating_sub(1));
    let line_len = utf16
        .line_len_utf16(line_idx)
        .unwrap_or(start_col.saturating_add(1));
    let end_col = line_len.max(start_col.saturating_add(1));

    Range {
        start: Position {
            line: line_idx as u32,
            character: start_col as u32,
        },
        end: Position {
            line: line_idx as u32,
            character: end_col as u32,
        },
    }
}

fn collect_text_document_usages(
    file: &str,
    text: &str,
    file_uses_super_admin: bool,
) -> (Vec<QailUsage>, Vec<(usize, Range)>) {
    let mut usages = Vec::new();
    let mut ranges = Vec::new();

    for literal in extract_text_literals(text) {
        let Some((kind, query_text, start_line, start_col, end_line, end_col)) =
            literal_query_span(text, &literal)
        else {
            continue;
        };
        if kind != EmbeddedQueryKind::Qail {
            continue;
        }

        let Ok(cmd) = parse(query_text) else {
            continue;
        };

        let range = Range {
            start: Position {
                line: start_line.saturating_sub(1) as u32,
                character: start_col.saturating_sub(1) as u32,
            },
            end: Position {
                line: end_line.saturating_sub(1) as u32,
                character: end_col.saturating_sub(1) as u32,
            },
        };

        usages.push(QailUsage {
            file: file.to_string(),
            line: start_line,
            column: start_col,
            table: cmd.table.clone(),
            is_dynamic_table: false,
            columns: collect_usage_columns(&cmd),
            action: action_to_usage_tag(&cmd).to_string(),
            is_cte_ref: false,
            has_rls: false,
            has_explicit_tenant_scope: cmd_has_explicit_tenant_scope(&cmd),
            file_uses_super_admin,
        });
        ranges.push((start_line, range));
    }

    if usages.is_empty()
        && let Some((kind, query_text, start_line, start_col, end_line, end_col)) =
            full_document_query_span(text)
        && kind == EmbeddedQueryKind::Qail
        && let Ok(cmd) = parse(query_text)
    {
        let range = Range {
            start: Position {
                line: start_line.saturating_sub(1) as u32,
                character: start_col.saturating_sub(1) as u32,
            },
            end: Position {
                line: end_line.saturating_sub(1) as u32,
                character: end_col.saturating_sub(1) as u32,
            },
        };

        usages.push(QailUsage {
            file: file.to_string(),
            line: start_line,
            column: start_col,
            table: cmd.table.clone(),
            is_dynamic_table: false,
            columns: collect_usage_columns(&cmd),
            action: action_to_usage_tag(&cmd).to_string(),
            is_cte_ref: false,
            has_rls: false,
            has_explicit_tenant_scope: cmd_has_explicit_tenant_scope(&cmd),
            file_uses_super_admin,
        });
        ranges.push((start_line, range));
    }

    (usages, ranges)
}

fn action_to_usage_tag(cmd: &Qail) -> &'static str {
    match cmd.action {
        qail_core::ast::Action::Get => "GET",
        qail_core::ast::Action::Add => "ADD",
        qail_core::ast::Action::Set => "SET",
        qail_core::ast::Action::Del => "DEL",
        qail_core::ast::Action::Put => "PUT",
        _ => "GET",
    }
}

fn collect_usage_columns(cmd: &Qail) -> Vec<String> {
    let mut columns = Vec::new();
    let mut seen = HashSet::new();
    let mut push = |name: &str| {
        if name.trim().is_empty() {
            return;
        }
        if seen.insert(name.to_string()) {
            columns.push(name.to_string());
        }
    };

    for expr in &cmd.columns {
        collect_columns_from_expr(expr, &mut push);
    }

    for cage in &cmd.cages {
        for cond in &cage.conditions {
            collect_columns_from_condition(cond, &mut push);
        }
    }

    for cond in &cmd.having {
        collect_columns_from_condition(cond, &mut push);
    }

    for join in &cmd.joins {
        if let Some(on) = &join.on {
            for cond in on {
                collect_columns_from_condition(cond, &mut push);
            }
        }
    }

    if let Some(returning) = &cmd.returning {
        for expr in returning {
            collect_columns_from_expr(expr, &mut push);
        }
    }

    columns
}

fn collect_columns_from_condition(cond: &Condition, push: &mut dyn FnMut(&str)) {
    collect_columns_from_expr(&cond.left, push);
    if let Value::Column(col) = &cond.value {
        push(col);
    }
}

fn collect_columns_from_expr(expr: &Expr, push: &mut dyn FnMut(&str)) {
    match expr {
        Expr::Named(name) => push(name),
        Expr::Aliased { name, .. } => push(name),
        Expr::Aggregate { col, .. } => push(col),
        Expr::Cast { expr, .. } => collect_columns_from_expr(expr, push),
        Expr::JsonAccess { column, .. } => push(column),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_columns_from_expr(arg, push);
            }
        }
        Expr::SpecialFunction { args, .. } => {
            for (_, arg) in args {
                collect_columns_from_expr(arg, push);
            }
        }
        Expr::Binary { left, right, .. } => {
            collect_columns_from_expr(left, push);
            collect_columns_from_expr(right, push);
        }
        Expr::ArrayConstructor { elements, .. } | Expr::RowConstructor { elements, .. } => {
            for element in elements {
                collect_columns_from_expr(element, push);
            }
        }
        Expr::Subscript { expr, index, .. } => {
            collect_columns_from_expr(expr, push);
            collect_columns_from_expr(index, push);
        }
        Expr::Collate { expr, .. } => collect_columns_from_expr(expr, push),
        _ => {}
    }
}

fn cmd_has_explicit_tenant_scope(cmd: &Qail) -> bool {
    cmd.cages.iter().any(|cage| {
        cage.conditions
            .iter()
            .any(is_explicit_tenant_scope_condition)
    })
}

fn is_explicit_tenant_scope_condition(cond: &Condition) -> bool {
    let Expr::Named(raw_left) = &cond.left else {
        return false;
    };
    if !is_tenant_identifier(raw_left) {
        return false;
    }
    matches!(
        cond.op,
        qail_core::ast::Operator::Eq | qail_core::ast::Operator::IsNull
    )
}

fn is_tenant_identifier(raw_ident: &str) -> bool {
    let without_cast = raw_ident.split("::").next().unwrap_or(raw_ident).trim();
    let last_segment = without_cast.rsplit('.').next().unwrap_or(without_cast);
    let normalized = last_segment
        .trim_matches('"')
        .trim_matches('`')
        .to_ascii_lowercase();
    normalized == "tenant_id"
}

fn rust_query_chain_has_rls(text: &str, query: &QueryCall) -> bool {
    let start = query.start_line.saturating_sub(1);
    let end = query.end_line.saturating_sub(1);
    let snippet = text
        .lines()
        .enumerate()
        .filter_map(|(idx, line)| ((start..=end).contains(&idx)).then_some(line))
        .collect::<Vec<_>>()
        .join("\n");

    snippet.contains(".with_rls(") || snippet.contains(".rls(")
}

fn extract_line_from_validation_message(message: &str) -> Option<usize> {
    let (_, rest) = message.split_once(':')?;
    let (line, _) = rest.split_once(':')?;
    line.trim().parse::<usize>().ok()
}

fn strip_file_line_prefix(message: &str) -> String {
    if let Some((_, rest)) = message.split_once(':')
        && let Some((line, tail)) = rest.split_once(':')
        && line.trim().parse::<usize>().is_ok()
    {
        return tail.trim_start().to_string();
    }

    message.to_string()
}

fn diagnostic_from_parse_error(
    start_line: usize,
    start_col: usize,
    end_line: usize,
    end_col: usize,
    message: String,
) -> Diagnostic {
    Diagnostic {
        range: Range {
            start: Position {
                line: start_line as u32,
                character: start_col as u32,
            },
            end: Position {
                line: end_line as u32,
                character: end_col as u32,
            },
        },
        severity: Some(DiagnosticSeverity::ERROR),
        source: Some("qail".to_string()),
        message,
        ..Default::default()
    }
}

fn extract_rust_query_at_line(content: &str, line: usize) -> Option<EmbeddedQuery> {
    detect_query_calls(content)
        .into_iter()
        .find(|query| is_line_in_query_call(query, line))
        .map(|query| query_call_to_embedded(content, query))
}

fn query_call_to_embedded(content: &str, query: QueryCall) -> EmbeddedQuery {
    let kind = if looks_like_qail_query(&query.sql) {
        EmbeddedQueryKind::Qail
    } else {
        EmbeddedQueryKind::Sql
    };
    let utf16 = Utf16Index::new(content);
    let start_col = utf16
        .byte_col_to_utf16(query.start_line.saturating_sub(1), query.start_column)
        .unwrap_or(query.start_column);
    let end_col = utf16
        .byte_col_to_utf16(query.end_line.saturating_sub(1), query.end_column)
        .unwrap_or(query.end_column);

    EmbeddedQuery {
        kind,
        text: query.sql,
        start_line: query.start_line.saturating_sub(1),
        start_column: start_col,
        end_line: query.end_line.saturating_sub(1),
        end_column: end_col,
    }
}

fn is_line_in_query_call(query: &QueryCall, line: usize) -> bool {
    let start = query.start_line.saturating_sub(1);
    let end = query.end_line.saturating_sub(1);
    (start..=end).contains(&line)
}

fn extract_text_query_at_line(content: &str, line: usize) -> Option<EmbeddedQuery> {
    for literal in extract_text_literals(content) {
        if !is_line_in_literal(&literal, line) {
            continue;
        }

        let Some((kind, query_text, start_line, start_col, end_line, end_col)) =
            literal_query_span(content, &literal)
        else {
            continue;
        };

        return Some(EmbeddedQuery {
            kind,
            text: query_text.to_string(),
            start_line: start_line.saturating_sub(1),
            start_column: start_col.saturating_sub(1),
            end_line: end_line.saturating_sub(1),
            end_column: end_col.saturating_sub(1),
        });
    }

    if let Some((kind, query_text, start_line, start_col, end_line, end_col)) =
        full_document_query_span(content)
    {
        let start = start_line.saturating_sub(1);
        let end = end_line.saturating_sub(1);
        if (start..=end).contains(&line) {
            return Some(EmbeddedQuery {
                kind,
                text: query_text.to_string(),
                start_line: start,
                start_column: start_col.saturating_sub(1),
                end_line: end,
                end_column: end_col.saturating_sub(1),
            });
        }
    }

    None
}

fn is_line_in_literal(literal: &TextLiteral, zero_based_line: usize) -> bool {
    let line = zero_based_line + 1;
    (literal.start_line..=literal.end_line).contains(&line)
}

fn literal_query_span<'a>(
    content: &str,
    literal: &'a TextLiteral,
) -> Option<(EmbeddedQueryKind, &'a str, usize, usize, usize, usize)> {
    let (start, end) = trim_query_bounds(&literal.text)?;
    let query_text = literal.text.get(start..end)?;
    let kind = if looks_like_qail_query(query_text) {
        EmbeddedQueryKind::Qail
    } else if looks_like_sql_query(query_text) {
        EmbeddedQueryKind::Sql
    } else {
        return None;
    };

    let (start_line, start_col_byte) = literal_offset_to_line_col(literal, start);
    let (end_line, end_col_byte) = literal_offset_to_line_col(literal, end);
    let utf16 = Utf16Index::new(content);
    let start_col = utf16
        .one_based_byte_col_to_utf16(start_line, start_col_byte)
        .unwrap_or(start_col_byte);
    let end_col = utf16
        .one_based_byte_col_to_utf16(end_line, end_col_byte)
        .unwrap_or(end_col_byte);
    Some((kind, query_text, start_line, start_col, end_line, end_col))
}

pub(crate) fn full_document_query_span(
    content: &str,
) -> Option<(EmbeddedQueryKind, &str, usize, usize, usize, usize)> {
    let (start, end) = trim_query_bounds(content)?;
    let query_text = content.get(start..end)?;
    let kind = if looks_like_qail_query(query_text) {
        EmbeddedQueryKind::Qail
    } else if looks_like_sql_query(query_text) {
        EmbeddedQueryKind::Sql
    } else {
        return None;
    };

    let utf16 = Utf16Index::new(content);
    let start_pos = utf16.offset_to_position(start);
    let end_pos = utf16.offset_to_position(end);
    Some((
        kind,
        query_text,
        start_pos.line as usize + 1,
        start_pos.character as usize + 1,
        end_pos.line as usize + 1,
        end_pos.character as usize + 1,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rust_query_span_detection_covers_chain_lines() {
        let src = r#"async fn run(pool: &Pool) {
    let rows = query("SELECT * FROM users")
        .fetch_all(pool)
        .await;
}"#;

        let query = extract_rust_query_at_line(src, 2).expect("query should match span");
        assert_eq!(query.kind, EmbeddedQueryKind::Sql);
        assert_eq!(query.text, "SELECT * FROM users");
    }

    #[test]
    fn rust_query_kind_marks_qail_text() {
        let src = r#"async fn run(pool: &Pool) {
    let rows = query("get users fields id")
        .fetch_all(pool)
        .await;
}"#;

        let query = extract_rust_query_at_line(src, 1).expect("query expected");
        assert_eq!(query.kind, EmbeddedQueryKind::Qail);
    }

    #[test]
    fn qail_classifier_rejects_sql_prefix() {
        assert!(!looks_like_qail_query("SELECT id FROM users"));
        assert!(looks_like_qail_query("get users fields id"));
    }

    #[test]
    fn text_query_extraction_supports_multiline_literals() {
        let src = r#"const q = `
get users
fields id, email
where active = true
`;"#;

        let query = extract_text_query_at_line(src, 2).expect("query expected");
        assert_eq!(query.kind, EmbeddedQueryKind::Qail);
        assert_eq!(
            query.text,
            "get users\nfields id, email\nwhere active = true"
        );
    }

    #[test]
    fn text_query_extraction_marks_sql_literals() {
        let src = r#"const sql = "
SELECT id, email
FROM users
WHERE active = true
";"#;

        let query = extract_text_query_at_line(src, 2).expect("sql query expected");
        assert_eq!(query.kind, EmbeddedQueryKind::Sql);
        assert_eq!(
            query.text,
            "SELECT id, email\nFROM users\nWHERE active = true"
        );
    }

    #[test]
    fn text_diagnostics_ignore_comment_literals() {
        let src = r#"
// "get users fields id where"
const msg = "hello";
"#;

        let diags = collect_text_qail_diagnostics(src);
        assert!(diags.is_empty(), "{diags:?}");
    }

    #[test]
    fn text_diagnostics_cover_raw_qail_documents() {
        let src = "get users fields id where";
        let diags = collect_text_qail_diagnostics(src);
        assert!(!diags.is_empty(), "raw qail file should be validated");
    }

    #[test]
    fn text_query_extraction_supports_raw_qail_document() {
        let src = "get users fields id where active = true";
        let query = extract_text_query_at_line(src, 0).expect("raw query expected");
        assert_eq!(query.kind, EmbeddedQueryKind::Qail);
        assert_eq!(query.text, "get users fields id where active = true");
    }

    #[test]
    fn rust_semantic_usages_include_qail_builder_chain() {
        let src = r#"fn demo(ctx: &RlsContext) {
    let _q = Qail::get("orders")
        .columns(["id"])
        .with_rls(&ctx);
}"#;

        let (usages, ranges) = collect_rust_document_usages("test.rs", src, false);
        assert_eq!(usages.len(), 1);
        assert_eq!(usages[0].table, "orders");
        assert_eq!(usages[0].action, "GET");
        assert!(usages[0].has_rls);
        assert_eq!(ranges.len(), 1);
    }

    #[test]
    fn rust_semantic_usages_include_typed_builder_chain() {
        let src = r#"fn demo(ctx: &RlsContext) {
    let _q = Qail::typed(users::table)
        .column("id")
        .with_rls(&ctx);
}"#;

        let (usages, _) = collect_rust_document_usages("test.rs", src, false);
        assert_eq!(usages.len(), 1);
        assert_eq!(usages[0].table, "users");
        assert_eq!(usages[0].action, "TYPED");
        assert!(usages[0].columns.iter().any(|c| c == "id"));
    }

    #[test]
    fn rust_semantic_diagnostics_cover_qail_builder_chain() {
        let schema = BuildSchema::parse(
            r#"
table orders {
  id UUID
  tenant_id UUID
}
"#,
        )
        .expect("schema should parse");
        let src = r#"fn demo() {
    let _q = Qail::get("orders").columns(["id"]);
}"#;

        let diags = collect_semantic_qail_diagnostics(src, "file:///tmp/demo.rs", &schema);
        assert!(
            diags.iter().any(|d| matches!(
                &d.code,
                Some(NumberOrString::String(code)) if code == "QAIL-RLS"
            )),
            "expected RLS warning for builder chain without with_rls: {diags:?}"
        );
    }

    #[test]
    fn file_uri_maps_to_path() {
        let uri = "file:///tmp/qail/src/main.rs";
        let path = uri_to_file_path(uri).expect("file uri should parse");
        assert_eq!(path, PathBuf::from("/tmp/qail/src/main.rs"));
    }

    #[test]
    fn schema_probe_dirs_walks_upward() {
        let dirs = schema_probe_dirs(Path::new("/tmp/qail/src/main.rs"));
        assert_eq!(dirs.first(), Some(&PathBuf::from("/tmp/qail/src")));
        assert!(dirs.contains(&PathBuf::from("/tmp/qail")));
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for QailLanguageServer {
    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::FULL,
                )),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                completion_provider: Some(CompletionOptions {
                    trigger_characters: Some(vec![
                        ":".to_string(),
                        ".".to_string(),
                        "?".to_string(),
                    ]),
                    ..Default::default()
                }),
                code_action_provider: Some(CodeActionProviderCapability::Simple(true)),
                document_formatting_provider: Some(OneOf::Left(true)),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "QAIL LSP initialized")
            .await;
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        self.handle_did_open(params).await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        self.handle_did_change(params).await;
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        self.handle_hover(params).await
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        self.handle_completion(params).await
    }

    async fn code_action(
        &self,
        params: CodeActionParams,
    ) -> Result<Option<Vec<CodeActionOrCommand>>> {
        self.handle_code_action(params).await
    }

    async fn formatting(&self, params: DocumentFormattingParams) -> Result<Option<Vec<TextEdit>>> {
        self.handle_formatting(params).await
    }
}
