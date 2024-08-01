//! QAIL Language Server Protocol Implementation
//!
//! Provides IDE features for QAIL queries:
//! - Syntax error diagnostics
//! - Hover information (SQL preview)
//! - Completion suggestions
//! - Schema validation

use std::collections::HashMap;
use std::sync::RwLock;
use qail_core::parse;
use qail_core::transpiler::ToSql;
use qail_core::schema::Schema;
use qail_core::validator::Validator;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};

#[derive(Debug)]
struct QailLanguageServer {
    client: Client,
    /// Cache of document contents for hover support
    documents: RwLock<HashMap<String, String>>,
    /// Optional schema for validation
    schema: RwLock<Option<Validator>>,
}

impl QailLanguageServer {
    fn new(client: Client) -> Self {
        Self { 
            client,
            documents: RwLock::new(HashMap::new()),
            schema: RwLock::new(None),
        }
    }

    /// Load schema from qail.schema.json in workspace root
    fn load_schema(&self, workspace_root: &str) {
        let schema_path = format!("{}/qail.schema.json", workspace_root);
        if let Ok(content) = std::fs::read_to_string(&schema_path) {
            if let Ok(schema) = Schema::from_json(&content) {
                let validator = schema.to_validator();
                if let Ok(mut s) = self.schema.write() {
                    *s = Some(validator);
                }
            }
        }
    }

    /// Extract QAIL query at a given line from cached document
    fn extract_qail_at_line(&self, uri: &str, line: usize) -> Option<String> {
        let docs = self.documents.read().ok()?;
        let content = docs.get(uri)?;
        let target_line = content.lines().nth(line)?;
        
        // Find QAIL pattern in line
        let patterns = ["get::", "set::", "del::", "add::", "make::", "mod::", "gen::"];
        for pattern in patterns {
            if let Some(start) = target_line.find(pattern) {
                let rest = &target_line[start..];
                // Find end of query
                let end = rest.find('"')
                    .or_else(|| rest.find('\''))
                    .unwrap_or(rest.len());
                return Some(rest[..end].to_string());
            }
        }
        None
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
                    match parse(query) {
                        Err(e) => {
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
                        Ok(cmd) => {
                            // Validate against schema if available
                            if let Ok(schema_guard) = self.schema.read() {
                                if let Some(validator) = schema_guard.as_ref() {
                                    if let Err(errors) = validator.validate_command(&cmd) {
                                        for error in errors {
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
                                                severity: Some(DiagnosticSeverity::WARNING),
                                                code: Some(NumberOrString::String("qail-schema".to_string())),
                                                source: Some("qail-lsp".to_string()),
                                                message: error,
                                                ..Default::default()
                                            });
                                        }
                                    }
                                }
                            }
                        }
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
        // Try to load schema from current directory
        self.load_schema(".");
        
        self.client
            .log_message(MessageType::INFO, "QAIL Language Server initialized")
            .await;
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        // Cache document content for hover
        if let Ok(mut docs) = self.documents.write() {
            docs.insert(
                params.text_document.uri.to_string(),
                params.text_document.text.clone(),
            );
        }
        
        let diagnostics = self.get_diagnostics(&params.text_document.text);
        self.client
            .publish_diagnostics(params.text_document.uri, diagnostics, None)
            .await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        if let Some(change) = params.content_changes.first() {
            // Update cached document
            if let Ok(mut docs) = self.documents.write() {
                docs.insert(
                    params.text_document.uri.to_string(),
                    change.text.clone(),
                );
            }
            
            let diagnostics = self.get_diagnostics(&change.text);
            self.client
                .publish_diagnostics(params.text_document.uri, diagnostics, None)
                .await;
        }
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = params.text_document_position_params.text_document.uri.to_string();
        let line = params.text_document_position_params.position.line as usize;
        
        // Try to extract and transpile QAIL query at this line
        if let Some(qail) = self.extract_qail_at_line(&uri, line) {
            match parse(&qail) {
                Ok(cmd) => {
                    let sql = cmd.to_sql();
                    return Ok(Some(Hover {
                        contents: HoverContents::Markup(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!("**QAIL → SQL**\n\n```sql\n{}\n```", sql),
                        }),
                        range: None,
                    }));
                }
                Err(e) => {
                    return Ok(Some(Hover {
                        contents: HoverContents::Markup(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!("**Parse Error**\n\n{}", e),
                        }),
                        range: None,
                    }));
                }
            }
        }
        
        // No QAIL found at this line
        Ok(None)
    }

    async fn completion(&self, _params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let mut completions = vec![
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

        // Add schema-aware table completions
        if let Ok(schema_guard) = self.schema.read() {
            if let Some(validator) = schema_guard.as_ref() {
                for table in validator.table_names() {
                    completions.push(CompletionItem {
                        label: format!("get::{}'_", table),
                        kind: Some(CompletionItemKind::CLASS),
                        detail: Some(format!("SELECT * FROM {}", table)),
                        ..Default::default()
                    });
                    completions.push(CompletionItem {
                        label: format!("set::{}", table),
                        kind: Some(CompletionItemKind::CLASS),
                        detail: Some(format!("UPDATE {}", table)),
                        ..Default::default()
                    });
                    completions.push(CompletionItem {
                        label: format!("add::{}", table),
                        kind: Some(CompletionItemKind::CLASS),
                        detail: Some(format!("INSERT INTO {}", table)),
                        ..Default::default()
                    });
                    completions.push(CompletionItem {
                        label: format!("del::{}", table),
                        kind: Some(CompletionItemKind::CLASS),
                        detail: Some(format!("DELETE FROM {}", table)),
                        ..Default::default()
                    });
                }
            }
        }

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
