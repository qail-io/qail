//! QAIL Language Server Core

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
            std::path::Path::new(p).parent().map(|p| p.to_string_lossy().to_string())
        }) {
            self.load_schema(&workspace_root);
            if let Ok(mut loaded) = self.schema_loaded.write() {
                *loaded = true;
            }
        }
    }

    fn load_schema(&self, workspace_root: &str) {
        let qail_path = format!("{}/schema.qail", workspace_root);
        if let Ok(content) = std::fs::read_to_string(&qail_path)
            && let Ok(schema) = Schema::from_qail_schema(&content)
            && let Ok(mut s) = self.schema.write()
        {
            *s = Some(schema.to_validator());
            return;
        }

        let json_path = format!("{}/qail.schema.json", workspace_root);
        if let Ok(content) = std::fs::read_to_string(&json_path)
            && let Ok(schema) = Schema::from_json(&content)
            && let Ok(mut s) = self.schema.write()
        {
            *s = Some(schema.to_validator());
        }
    }

    pub fn extract_qail_at_line(&self, uri: &str, line: usize) -> Option<String> {
        let docs = self.documents.read().ok()?;
        let content = docs.get(uri)?;
        let target_line = content.lines().nth(line)?;

        let patterns = ["get::", "set::", "del::", "add::", "make::", "mod::"];

        for pattern in patterns {
            if let Some(start) = target_line.find(pattern) {
                let rest = &target_line[start..];
                if start > 1 {
                    let before = &target_line[..start];
                    if (before.ends_with("(\"") || before.ends_with("(r\"") || before.ends_with("= \""))
                        && let Some(end) = rest.find("\"")
                    {
                        return Some(rest[..end].to_string());
                    }
                }
                return Some(rest.to_string());
            }
        }
        None
    }

    pub fn get_diagnostics(&self, text: &str) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();
        let patterns = ["get::", "set::", "del::", "add::", "make::", "mod::"];

        for (line_num, line) in text.lines().enumerate() {
            for pattern in patterns {
                if let Some(col) = line.find(pattern) {
                    let query_start = col;
                    let query_line = &line[query_start..];

                    // Try to find end quote and validate
                    if let Some(query_end) = query_line.rfind("\"")
                        && let Err(e) = parse(&query_line[..query_end])
                    {
                        diagnostics.push(Diagnostic {
                            range: Range {
                                start: Position { line: line_num as u32, character: col as u32 },
                                end: Position { line: line_num as u32, character: (col + query_line.len()) as u32 },
                            },
                            severity: Some(DiagnosticSeverity::ERROR),
                            source: Some("qail".to_string()),
                            message: e.to_string(),
                            ..Default::default()
                        });
                    }
                }
            }
        }
        diagnostics
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
