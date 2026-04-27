//! QAIL Language Server Core

use qail_core::analyzer::{
    QueryCall, TextLiteral, detect_query_calls, extract_text_literals, looks_like_qail_query,
    looks_like_sql_query, trim_query_bounds,
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
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::SystemTime;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

use crate::utf16::Utf16Index;

/// QAIL Language Server
#[derive(Debug)]
pub struct QailLanguageServer {
    pub client: Client,
    pub documents: RwLock<HashMap<String, OpenDocument>>,
    pub schemas: RwLock<HashMap<PathBuf, WorkspaceSchemaCache>>,
}

#[derive(Debug)]
pub struct OpenDocument {
    pub text: String,
    pub version: i32,
}

#[derive(Debug)]
pub struct WorkspaceSchemaCache {
    pub schema_path: PathBuf,
    pub schema_mtime: Option<SystemTime>,
    pub validator: Option<Validator>,
    pub build_schema: Option<BuildSchema>,
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
            schemas: RwLock::new(HashMap::new()),
        }
    }

    pub fn try_load_schema_from_uri(&self, uri: &str) -> Option<PathBuf> {
        let file_path = uri_to_file_path(uri)?;

        for candidate_dir in schema_probe_dirs(&file_path) {
            let schema_path = candidate_dir.join("schema.qail");
            let Ok(metadata) = fs::metadata(&schema_path) else {
                continue;
            };
            if !metadata.is_file() {
                continue;
            }

            let schema_mtime = metadata.modified().ok();
            let needs_reload = self
                .schemas
                .read()
                .ok()
                .and_then(|schemas| {
                    schemas.get(&candidate_dir).map(|cached| {
                        cached.schema_path != schema_path || cached.schema_mtime != schema_mtime
                    })
                })
                .unwrap_or(true);

            if needs_reload {
                self.load_schema_from_dir(&candidate_dir, schema_path, schema_mtime);
            }

            return Some(candidate_dir);
        }

        None
    }

    fn load_schema_from_dir(
        &self,
        workspace_root: &Path,
        schema_path: PathBuf,
        schema_mtime: Option<SystemTime>,
    ) {
        let Ok(content) = qail_core::schema_source::read_qail_schema_source(&schema_path) else {
            if let Ok(mut schemas) = self.schemas.write() {
                schemas.remove(workspace_root);
            }
            return;
        };

        let validator = Schema::from_qail_schema(&content)
            .ok()
            .map(|schema| schema.to_validator());
        let build_schema = BuildSchema::parse(&content).ok();

        if let Ok(mut schemas) = self.schemas.write() {
            schemas.insert(
                workspace_root.to_path_buf(),
                WorkspaceSchemaCache {
                    schema_path,
                    schema_mtime,
                    validator,
                    build_schema,
                },
            );
        }
    }

    pub(crate) fn schema_validator_for_uri(&self, uri: &str) -> Option<Validator> {
        let root = self.try_load_schema_from_uri(uri)?;
        self.schemas.read().ok()?.get(&root)?.validator.clone()
    }

    pub(crate) fn extract_query_at_position(
        &self,
        uri: &str,
        position: Position,
    ) -> Option<EmbeddedQuery> {
        let docs = self.documents.read().ok()?;
        let content = &docs.get(uri)?.text;

        if uri.ends_with(".rs") {
            return extract_rust_query_at_position(content, position);
        }

        extract_text_query_at_position(content, position)
    }

    pub(crate) fn get_document(&self, uri: &str) -> Option<String> {
        self.documents
            .read()
            .ok()?
            .get(uri)
            .map(|doc| doc.text.clone())
    }

    /// Get diagnostics for a document. Pass the file URI to enable N+1 detection for `.rs` files.
    pub fn get_diagnostics(&self, text: &str, uri: &str) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let schema_root = self.try_load_schema_from_uri(uri);

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
        if let Some(root) = schema_root
            && let Ok(schemas) = self.schemas.read()
            && let Some(cache) = schemas.get(&root)
            && let Some(build_schema) = cache.build_schema.as_ref()
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
    let line_to_range: HashMap<usize, Range> = usage_ranges.into_iter().collect();

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

fn split_validation_message(message: &str) -> Option<(usize, &str)> {
    let mut search_start = 0usize;

    while let Some(rel_sep_idx) = message.get(search_start..)?.find(": ") {
        let sep_idx = search_start + rel_sep_idx;
        let prefix = message.get(..sep_idx)?;
        if let Some((_, line_part)) = prefix.rsplit_once(':')
            && let Ok(line) = line_part.trim().parse::<usize>()
        {
            let body = message.get(sep_idx + 2..)?;
            return Some((line, body));
        }

        search_start = sep_idx + 2;
    }

    None
}

fn extract_line_from_validation_message(message: &str) -> Option<usize> {
    split_validation_message(message).map(|(line, _)| line)
}

fn strip_file_line_prefix(message: &str) -> String {
    split_validation_message(message)
        .map(|(_, body)| body.to_string())
        .unwrap_or_else(|| message.to_string())
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

#[cfg(test)]
fn extract_rust_query_at_line(content: &str, line: usize) -> Option<EmbeddedQuery> {
    detect_query_calls(content)
        .into_iter()
        .find(|query| is_line_in_query_call(query, line))
        .map(|query| query_call_to_embedded(content, query))
}

fn extract_rust_query_at_position(content: &str, position: Position) -> Option<EmbeddedQuery> {
    detect_query_calls(content)
        .into_iter()
        .map(|query| query_call_to_embedded(content, query))
        .find(|query| embedded_query_contains_position(query, position))
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

#[cfg(test)]
fn is_line_in_query_call(query: &QueryCall, line: usize) -> bool {
    let start = query.start_line.saturating_sub(1);
    let end = query.end_line.saturating_sub(1);
    (start..=end).contains(&line)
}

#[cfg(test)]
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

fn extract_text_query_at_position(content: &str, position: Position) -> Option<EmbeddedQuery> {
    for literal in extract_text_literals(content) {
        let Some((kind, query_text, start_line, start_col, end_line, end_col)) =
            literal_query_span(content, &literal)
        else {
            continue;
        };

        let query = EmbeddedQuery {
            kind,
            text: query_text.to_string(),
            start_line: start_line.saturating_sub(1),
            start_column: start_col.saturating_sub(1),
            end_line: end_line.saturating_sub(1),
            end_column: end_col.saturating_sub(1),
        };

        if embedded_query_contains_position(&query, position) {
            return Some(query);
        }
    }

    if let Some((kind, query_text, start_line, start_col, end_line, end_col)) =
        full_document_query_span(content)
    {
        let query = EmbeddedQuery {
            kind,
            text: query_text.to_string(),
            start_line: start_line.saturating_sub(1),
            start_column: start_col.saturating_sub(1),
            end_line: end_line.saturating_sub(1),
            end_column: end_col.saturating_sub(1),
        };

        if embedded_query_contains_position(&query, position) {
            return Some(query);
        }
    }

    None
}

#[cfg(test)]
fn is_line_in_literal(literal: &TextLiteral, zero_based_line: usize) -> bool {
    let line = zero_based_line + 1;
    (literal.start_line..=literal.end_line).contains(&line)
}

pub(crate) fn embedded_query_contains_position(query: &EmbeddedQuery, position: Position) -> bool {
    let line = position.line as usize;
    let character = position.character as usize;

    if line < query.start_line || line > query.end_line {
        return false;
    }

    if query.start_line == query.end_line {
        return character >= query.start_column && character <= query.end_column;
    }

    if line == query.start_line {
        return character >= query.start_column;
    }
    if line == query.end_line {
        return character <= query.end_column;
    }

    true
}

fn literal_byte_offset_to_line_col(literal: &TextLiteral, offset: usize) -> (usize, usize) {
    let capped = offset.min(literal.text.len());
    let mut rel_line = 0usize;
    let mut rel_col_bytes = 0usize;

    for b in literal.text.as_bytes().iter().take(capped) {
        if *b == b'\n' {
            rel_line += 1;
            rel_col_bytes = 0;
        } else {
            rel_col_bytes += 1;
        }
    }

    let line = literal.start_line + rel_line;
    let column = if rel_line == 0 {
        literal.start_column + rel_col_bytes
    } else {
        rel_col_bytes + 1
    };

    (line, column)
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

    let (start_line, start_col_byte) = literal_byte_offset_to_line_col(literal, start);
    let (end_line, end_col_byte) = literal_byte_offset_to_line_col(literal, end);
    let utf16 = Utf16Index::new(content);
    let start_col = utf16
        .one_based_byte_col_to_utf16(start_line, start_col_byte)
        .map(|col| col.saturating_add(1))
        .unwrap_or(start_col_byte);
    let end_col = utf16
        .one_based_byte_col_to_utf16(end_line, end_col_byte)
        .map(|col| col.saturating_add(1))
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

#[tower_lsp::async_trait]
impl LanguageServer for QailLanguageServer {
    async fn initialize(&self, _: InitializeParams) -> Result<InitializeResult> {
        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Options(
                    TextDocumentSyncOptions {
                        open_close: Some(true),
                        change: Some(TextDocumentSyncKind::FULL),
                        save: Some(TextDocumentSyncSaveOptions::Supported(true)),
                        ..Default::default()
                    },
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

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        self.handle_did_close(params).await;
    }

    async fn did_change_watched_files(&self, params: DidChangeWatchedFilesParams) {
        self.handle_did_change_watched_files(params).await;
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

#[cfg(test)]
mod tests;
