//! QAIL Language Server Protocol Implementation
//!
//! Provides IDE features for QAIL queries:
//! - Syntax error diagnostics
//! - Hover information (SQL preview)
//! - Completion suggestions

use qail_core::parse;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};

#[derive(Debug)]
struct QailLanguageServer {
    client: Client,
}

impl QailLanguageServer {
    fn new(client: Client) -> Self {
        Self { client }
    }

    /// Parse QAIL and return diagnostics
    fn get_diagnostics(&self, text: &str) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        // Find QAIL patterns in the text
        for (line_num, line) in text.lines().enumerate() {
            // Look for QAIL patterns (get::, set::, del::, add::, make::, mod::)
            let patterns = ["get::", "set::", "del::", "add::", "make::", "mod::", "gen::"];
            
            for pattern in patterns {
                if let Some(start) = line.find(pattern) {
                    // Extract the QAIL query (until end of line or closing quote)
                    let query_start = start;
                    let rest = &line[query_start..];
                    
                    // Find end of query (quote or end of line)
                    let query_end = rest.find('"')
                        .or_else(|| rest.find('\''))
                        .unwrap_or(rest.len());
                    
                    let query = &rest[..query_end];
                    
                    // Try to parse
                    if let Err(e) = parse(query) {
                        diagnostics.push(Diagnostic {
                            range: Range {
                                start: Position {
                                    line: line_num as u32,
                                    character: start as u32,
                                },
                                end: Position {
                                    line: line_num as u32,
                                    character: (start + query_end) as u32,
                                },
                            },
                            severity: Some(DiagnosticSeverity::ERROR),
                            code: Some(NumberOrString::String("qail-parse".to_string())),
                            source: Some("qail-lsp".to_string()),
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
                        "@".to_string(),
                        "[".to_string(),
                        "#".to_string(),
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(MessageType::INFO, "QAIL Language Server initialized")
            .await;
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        let diagnostics = self.get_diagnostics(&params.text_document.text);
        self.client
            .publish_diagnostics(params.text_document.uri, diagnostics, None)
            .await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        if let Some(change) = params.content_changes.first() {
            let diagnostics = self.get_diagnostics(&change.text);
            self.client
                .publish_diagnostics(params.text_document.uri, diagnostics, None)
                .await;
        }
    }

    async fn hover(&self, _params: HoverParams) -> Result<Option<Hover>> {
        // For now, return basic hover info
        // In a full implementation, we'd extract the QAIL query at the position
        Ok(Some(Hover {
            contents: HoverContents::Markup(MarkupContent {
                kind: MarkupKind::Markdown,
                value: "**QAIL Query**\n\nHover over a QAIL pattern to see the generated SQL."
                    .to_string(),
            }),
            range: None,
        }))
    }

    async fn completion(&self, _params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let completions = vec![
            // Actions
            CompletionItem {
                label: "get::".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("SELECT query".to_string()),
                insert_text: Some("get::${1:table}•@${2:*}".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "set::".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("UPDATE query".to_string()),
                insert_text: Some("set::${1:table}•[${2:column}=${3:value}][${4:where}]".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "del::".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("DELETE query".to_string()),
                insert_text: Some("del::${1:table}•[${2:where}]".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "add::".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("INSERT query".to_string()),
                insert_text: Some("add::${1:table}•@${2:columns}[${3:values}]".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            // Modifiers
            CompletionItem {
                label: "get!::".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("SELECT DISTINCT".to_string()),
                insert_text: Some("get!::${1:table}•@${2:column}".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            // Aggregates
            CompletionItem {
                label: "#count".to_string(),
                kind: Some(CompletionItemKind::FUNCTION),
                detail: Some("COUNT aggregate".to_string()),
                ..Default::default()
            },
            CompletionItem {
                label: "#sum".to_string(),
                kind: Some(CompletionItemKind::FUNCTION),
                detail: Some("SUM aggregate".to_string()),
                ..Default::default()
            },
            CompletionItem {
                label: "#avg".to_string(),
                kind: Some(CompletionItemKind::FUNCTION),
                detail: Some("AVG aggregate".to_string()),
                ..Default::default()
            },
            // Cages
            CompletionItem {
                label: "[lim=".to_string(),
                kind: Some(CompletionItemKind::SNIPPET),
                detail: Some("LIMIT clause".to_string()),
                insert_text: Some("[lim=${1:10}]".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "[off=".to_string(),
                kind: Some(CompletionItemKind::SNIPPET),
                detail: Some("OFFSET clause".to_string()),
                insert_text: Some("[off=${1:0}]".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "[^".to_string(),
                kind: Some(CompletionItemKind::SNIPPET),
                detail: Some("ORDER BY ASC".to_string()),
                insert_text: Some("[^${1:column}]".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "[^!".to_string(),
                kind: Some(CompletionItemKind::SNIPPET),
                detail: Some("ORDER BY DESC".to_string()),
                insert_text: Some("[^!${1:column}]".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
        ];

        Ok(Some(CompletionResponse::Array(completions)))
    }
}

#[tokio::main]
async fn main() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(|client| QailLanguageServer::new(client));
    Server::new(stdin, stdout, socket).serve(service).await;
}
