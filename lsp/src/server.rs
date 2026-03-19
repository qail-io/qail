//! QAIL Language Server Core

use qail_core::analyzer::{QueryCall, detect_query_calls};
use qail_core::parse;
use qail_core::schema::Schema;
use qail_core::validator::Validator;
use std::collections::HashMap;
use std::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

/// QAIL Language Server
#[derive(Debug)]
pub struct QailLanguageServer {
    pub client: Client,
    pub documents: RwLock<HashMap<String, String>>,
    pub schema: RwLock<Option<Validator>>,
    schema_loaded: RwLock<bool>,
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
            schema_loaded: RwLock::new(false),
        }
    }

    pub fn try_load_schema_from_uri(&self, uri: &str) {
        if let Ok(loaded) = self.schema_loaded.read()
            && *loaded
        {
            return;
        }

        if let Some(workspace_root) = uri.strip_prefix("file://").and_then(|p| {
            std::path::Path::new(p)
                .parent()
                .map(|p| p.to_string_lossy().to_string())
        }) {
            self.load_schema(&workspace_root);
            if let Ok(mut loaded) = self.schema_loaded.write() {
                *loaded = true;
            }
        }
    }

    fn load_schema(&self, workspace_root: &str) {
        let qail_path = std::path::Path::new(workspace_root).join("schema.qail");
        if let Ok(content) = qail_core::schema_source::read_qail_schema_source(&qail_path)
            && let Ok(schema) = Schema::from_qail_schema(&content)
            && let Ok(mut s) = self.schema.write()
        {
            *s = Some(schema.to_validator());
        }
    }

    pub(crate) fn extract_query_at_line(&self, uri: &str, line: usize) -> Option<EmbeddedQuery> {
        let docs = self.documents.read().ok()?;
        let content = docs.get(uri)?;

        if uri.ends_with(".rs") {
            return extract_rust_query_at_line(content, line);
        }

        let target_line = content.lines().nth(line)?;
        let (start_column, query) = extract_qail_candidate_from_line(target_line)?;
        Some(EmbeddedQuery {
            kind: EmbeddedQueryKind::Qail,
            text: query.clone(),
            start_line: line,
            start_column,
            end_line: line,
            end_column: start_column + query.len(),
        })
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

            for diag in nplus1_diags {
                let severity = match diag.severity {
                    qail_core::analyzer::NPlusOneSeverity::Error => DiagnosticSeverity::ERROR,
                    qail_core::analyzer::NPlusOneSeverity::Warning => DiagnosticSeverity::WARNING,
                };

                let mut message = diag.message.clone();
                if let Some(ref hint) = diag.hint {
                    message.push_str(&format!("\nHint: {}", hint));
                }

                diagnostics.push(Diagnostic {
                    range: Range {
                        start: Position {
                            line: (diag.line.saturating_sub(1)) as u32,
                            character: (diag.column.saturating_sub(1)) as u32,
                        },
                        end: Position {
                            line: (diag.line.saturating_sub(1)) as u32,
                            character: (diag.end_column.saturating_sub(1)) as u32,
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

        diagnostics
    }
}

const QAIL_ACTION_PREFIXES: [&str; 7] = ["get", "set", "add", "del", "with", "make", "mod"];

fn collect_text_qail_diagnostics(text: &str) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    for (line_num, line) in text.lines().enumerate() {
        if let Some((col, query_text)) = extract_qail_candidate_from_line(line)
            && let Err(e) = parse(&query_text)
        {
            diagnostics.push(diagnostic_from_parse_error(
                line_num,
                col,
                line_num,
                col + query_text.len(),
                e.to_string(),
            ));
        }
    }

    diagnostics
}

fn collect_rust_qail_diagnostics(text: &str) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    for query in detect_query_calls(text) {
        if !looks_like_qail_query(&query.sql) {
            continue;
        }

        if let Err(e) = parse(&query.sql) {
            diagnostics.push(diagnostic_from_parse_error(
                query.start_line.saturating_sub(1),
                query.start_column,
                query.end_line.saturating_sub(1),
                query.end_column,
                e.to_string(),
            ));
        }
    }

    diagnostics
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
        .map(query_call_to_embedded)
}

fn query_call_to_embedded(query: QueryCall) -> EmbeddedQuery {
    let kind = if looks_like_qail_query(&query.sql) {
        EmbeddedQueryKind::Qail
    } else {
        EmbeddedQueryKind::Sql
    };

    EmbeddedQuery {
        kind,
        text: query.sql,
        start_line: query.start_line.saturating_sub(1),
        start_column: query.start_column,
        end_line: query.end_line.saturating_sub(1),
        end_column: query.end_column,
    }
}

fn is_line_in_query_call(query: &QueryCall, line: usize) -> bool {
    let start = query.start_line.saturating_sub(1);
    let end = query.end_line.saturating_sub(1);
    (start..=end).contains(&line)
}

fn looks_like_qail_query(text: &str) -> bool {
    let head = text
        .trim_start()
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();

    QAIL_ACTION_PREFIXES.contains(&head.as_str())
}

fn extract_qail_candidate_from_line(line: &str) -> Option<(usize, String)> {
    let start = find_qail_start(line)?;
    let rest = &line[start..];

    let before = line[..start].trim_end();
    let is_quoted = before.ends_with("(\"")
        || before.ends_with("(r\"")
        || before.ends_with("(r#\"")
        || before.ends_with("= \"")
        || before.ends_with("= r\"")
        || before.ends_with("= r#\"");

    if is_quoted && let Some(end) = rest.find('"') {
        return Some((start, rest[..end].trim().to_string()));
    }

    Some((start, rest.trim().trim_end_matches(';').to_string()))
}

fn find_qail_start(line: &str) -> Option<usize> {
    for (idx, _) in line.char_indices() {
        let before_ok = if idx == 0 {
            true
        } else {
            let ch = line[..idx].chars().next_back().unwrap_or(' ');
            !is_ident_char(ch)
        };
        if !before_ok {
            continue;
        }

        for action in QAIL_ACTION_PREFIXES {
            let Some(tail) = line.get(idx..) else {
                continue;
            };
            if !tail.starts_with(action) {
                continue;
            }
            let after = idx + action.len();
            let Some(next) = line[after..].chars().next() else {
                continue;
            };
            if next.is_whitespace() {
                return Some(idx);
            }
        }
    }
    None
}

fn is_ident_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rust_query_span_detection_covers_chain_lines() {
        let src = r#"async fn run(pool: &Pool) {
    let rows = sqlx::query("SELECT * FROM users")
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
    let rows = sqlx::query("get users fields id")
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
}
