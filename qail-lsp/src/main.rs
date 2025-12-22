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
        
        // Find QAIL pattern in line (v1 or v2 syntax)
        let v1_patterns = ["get::", "set::", "del::", "add::", "make::", "mod::", "gen::"];
        let v2_patterns = ["get ", "set ", "del ", "add ", "with "];
        
        // Try v1 patterns first
        for pattern in v1_patterns {
            if let Some(start) = target_line.find(pattern) {
                let rest = &target_line[start..];
                // Find end of query
                let end = rest.find('"')
                    .or_else(|| rest.find('\''))
                    .unwrap_or(rest.len());
                return Some(rest[..end].to_string());
            }
        }
        
        // Try v2 patterns
        for pattern in v2_patterns {
            if let Some(start) = target_line.find(pattern) {
                // For v2 syntax, query extends to end of line or newline
                let rest = &target_line[start..];
                return Some(rest.to_string());
            }
        }
        None
    }

    /// Try to determine the active table context at cursors position
    fn get_context_table(&self, uri: &str, line: usize, col: usize) -> Option<String> {
        let docs = self.documents.read().ok()?;
        let content = docs.get(uri)?;
        let target_line = content.lines().nth(line)?;

        if col > target_line.len() {
            return None;
        }

        let prefix = &target_line[..col];
        
        // Regex to find 'action::table' pattern
        // Simple heuristic: find last occurrence of '::'
        if let Some(idx) = prefix.rfind("::") {
            let potential_match = &prefix[idx+2..];
            // Table name is usually the next word
            // It might be followed by ' or [ or whitespace
            let table_end = potential_match.find(|c: char| !c.is_alphanumeric() && c != '_')
                .unwrap_or(potential_match.len());
            
            let table = &potential_match[..table_end];
            if !table.is_empty() {
                return Some(table.to_string());
            }
        }
        
        None
    }

    /// Parse QAIL and return diagnostics
    fn get_diagnostics(&self, text: &str) -> Vec<Diagnostic> {
        let mut diagnostics = Vec::new();

        // Track whether we're inside a multi-line CTE block (with::)
        // CTE blocks contain nested get::/set::/etc that shouldn't be parsed individually
        let mut in_cte_block = false;
        let mut cte_brace_depth = 0;

        // Find QAIL patterns in the text
        for (line_num, line) in text.lines().enumerate() {
            // Update CTE tracking state
            // Look for with:: pattern that starts a CTE block
            if line.contains("with::") {
                in_cte_block = true;
                // Count opening braces to track nesting
                cte_brace_depth += line.matches('{').count();
                cte_brace_depth = cte_brace_depth.saturating_sub(line.matches('}').count());
                // The with:: line itself should be parsed, but we skip for now since
                // the whole CTE needs to be assembled. For CTEs, we only validate the
                // final assembled query. Skip this line.
                continue;
            }
            
            // If we're inside a CTE block, track brace depth
            if in_cte_block {
                cte_brace_depth += line.matches('{').count();
                cte_brace_depth = cte_brace_depth.saturating_sub(line.matches('}').count());
                
                // Check if we've exited all CTE braces
                if cte_brace_depth == 0 {
                    // Check if this line has the closing of CTE (e.g. ends with } or has -> which exits CTE context)
                    // or if the line after with:: block ends with terminal patterns
                    if !line.contains("with::") {
                        in_cte_block = false;
                    }
                }
                
                // Skip parsing patterns inside CTE blocks - they're fragments, not standalone queries
                // This prevents false positives like "get::table !on(col)" being parsed without CTE context
                continue;
            }

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

    /// Extract word at cursor position
    fn get_word_at_position(&self, uri: &str, line: usize, col: usize) -> Option<String> {
        let docs = self.documents.read().ok()?;
        let content = docs.get(uri)?;
        let target_line = content.lines().nth(line)?;
        
        let chars: Vec<char> = target_line.chars().collect();
        if col >= chars.len() { return None; }
        
        // Find start
        let mut start = col;
        while start > 0 && (chars[start-1].is_alphanumeric() || chars[start-1] == '_') {
            start -= 1;
        }
        
        // Find end
        let mut end = col;
        while end < chars.len() && (chars[end].is_alphanumeric() || chars[end] == '_') {
            end += 1;
        }
        
        if start < end {
            Some(target_line[start..end].to_string())
        } else {
            None
        }
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
                definition_provider: Some(OneOf::Left(true)),
                rename_provider: Some(OneOf::Left(true)),
                code_action_provider: Some(CodeActionProviderCapability::Simple(true)),
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

    async fn goto_definition(&self, params: GotoDefinitionParams) -> Result<Option<GotoDefinitionResponse>> {
        let uri = params.text_document_position_params.text_document.uri.to_string();
        let line = params.text_document_position_params.position.line as usize;
        let col = params.text_document_position_params.position.character as usize;
        
        if let Some(word) = self.get_word_at_position(&uri, line, col) {
            // Check if it's a known table
            let is_table = if let Ok(schema) = self.schema.read() {
                if let Some(validator) = schema.as_ref() {
                    validator.table_names().contains(&word)
                } else {
                    false
                }
            } else {
                false
            };

            if is_table {
                // Return location in qail.schema.json
                // We assume it's in the root
                let root_uri = Option::<Url>::from(params.text_document_position_params.text_document.uri)
                    .and_then(|u| u.join("./qail.schema.json").ok());
                
                if let Some(schema_uri) = root_uri {
                    // Try to find line number
                    let mut line_num = 0;
                    if let Ok(path) = schema_uri.to_file_path() {
                        if let Ok(content) = std::fs::read_to_string(path) {
                            for (i, l) in content.lines().enumerate() {
                                if l.contains(&format!("\"name\": \"{}\"", word)) {
                                    line_num = i;
                                    break;
                                }
                            }
                        }
                    }

                    return Ok(Some(GotoDefinitionResponse::Scalar(Location {
                        uri: schema_uri,
                        range: Range {
                            start: Position { line: line_num as u32, character: 0 },
                            end: Position { line: line_num as u32, character: 100 },
                        }
                    })));
                }
            }
        }
        
        Ok(None)
    }

    async fn rename(&self, params: RenameParams) -> Result<Option<WorkspaceEdit>> {
        let uri = params.text_document_position.text_document.uri.to_string();
        let line = params.text_document_position.position.line as usize;
        let col = params.text_document_position.position.character as usize;
        let new_name = params.new_name;

        // Get word at cursor
        if let Some(word) = self.get_word_at_position(&uri, line, col) {
            // Read document content
            if let Ok(docs) = self.documents.read() {
                if let Some(content) = docs.get(&uri) {
                    let mut edits = Vec::new();
                    
                    // Simple textual find/replace in current doc (MVP)
                    for (i, line_str) in content.lines().enumerate() {
                        let mut start_idx = 0;
                        while let Some(idx) = line_str[start_idx..].find(&word) {
                            let abs_idx = start_idx + idx;
                            
                            // Check boundaries to ensure whole word match
                            let prev_char = if abs_idx > 0 { line_str.chars().nth(abs_idx - 1) } else { None };
                            let next_char = line_str.chars().nth(abs_idx + word.len());
                            
                            let is_start_boundary = prev_char.map_or(true, |c| !c.is_alphanumeric() && c != '_');
                            let is_end_boundary = next_char.map_or(true, |c| !c.is_alphanumeric() && c != '_');

                            if is_start_boundary && is_end_boundary {
                                edits.push(TextEdit {
                                    range: Range {
                                        start: Position { line: i as u32, character: abs_idx as u32 },
                                        end: Position { line: i as u32, character: (abs_idx + word.len()) as u32 },
                                    },
                                    new_text: new_name.clone(),
                                });
                            }
                            start_idx = abs_idx + word.len();
                        }
                    }

                    if !edits.is_empty() {
                         let mut changes = HashMap::new();
                         changes.insert(params.text_document_position.text_document.uri, edits);
                         return Ok(Some(WorkspaceEdit {
                             changes: Some(changes),
                             ..Default::default()
                         }));
                    }
                }
            }
        }
        Ok(None)
    }

    async fn code_action(&self, params: CodeActionParams) -> Result<Option<Vec<CodeActionOrCommand>>> {
        let mut actions = Vec::new();

        for diagnostic in params.context.diagnostics {
            if let Some(code) = &diagnostic.code {
                if let NumberOrString::String(s) = code {
                    if s == "qail-schema" {
                        // Check for "Did you mean '...'" in message
                        if let Some(start_idx) = diagnostic.message.find("Did you mean '") {
                            let rest = &diagnostic.message[start_idx + 14..];
                            if let Some(end_idx) = rest.find('\'') {
                                let suggestion = &rest[..end_idx];
                                
                                // Create fix
                                let mut changes = HashMap::new();
                                changes.insert(params.text_document.uri.clone(), vec![TextEdit {
                                    range: diagnostic.range,
                                    new_text: suggestion.to_string(),
                                }]);

                                actions.push(CodeActionOrCommand::CodeAction(CodeAction {
                                    title: format!("Change to '{}'", suggestion),
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
                        }
                    }
                }
            }
        }

        Ok(Some(actions))
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
                insert_text: Some("get::${1:table} : ${2:'_}".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "set::".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("UPDATE query".to_string()),
                insert_text: Some("set::${1:table} [ ${2:column}=${3:value} ][ ${4:where} ]".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "del::".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("DELETE query".to_string()),
                insert_text: Some("del::${1:table} [ ${2:where} ]".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "add::".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("INSERT query".to_string()),
                insert_text: Some("add::${1:table} : ${2:columns} [ ${3:values} ]".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            // Modifiers
            CompletionItem {
                label: "get!::".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("SELECT DISTINCT".to_string()),
                insert_text: Some("get!::${1:table} : ${2:column}".to_string()),
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
            // === V2 Canonical Syntax Completions ===
            CompletionItem {
                label: "get (v2)".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("SELECT query (v2 canonical)".to_string()),
                insert_text: Some("get ${1:table}\nfields\n  ${2:*}".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "with (v2)".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("CTE definition (v2 canonical)".to_string()),
                insert_text: Some("with ${1:cte_name} =\n  get ${2:table}\n  fields\n    ${3:*}".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "join (v2)".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("JOIN clause (v2 canonical)".to_string()),
                insert_text: Some("join ${1:table}\n  on ${2:condition}".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "where (v2)".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("WHERE clause (v2 canonical)".to_string()),
                insert_text: Some("where ${1:column} = ${2:value}".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "order by (v2)".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("ORDER BY clause (v2 canonical)".to_string()),
                insert_text: Some("order by\n  ${1:column} ${2|asc,desc|}".to_string()),
                insert_text_format: Some(InsertTextFormat::SNIPPET),
                ..Default::default()
            },
            CompletionItem {
                label: "fields (v2)".to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some("Column projection (v2 canonical)".to_string()),
                insert_text: Some("fields\n  ${1:column1},\n  ${2:column2}".to_string()),
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

                // Context-aware column completions
                let uri = _params.text_document_position.text_document.uri.to_string();
                let line = _params.text_document_position.position.line as usize;
                let col = _params.text_document_position.position.character as usize;

                if let Some(table) = self.get_context_table(&uri, line, col) {
                   if let Some(columns) = validator.column_names(&table) {
                       for col_name in columns {
                           completions.push(CompletionItem {
                               label: col_name.clone(),
                               kind: Some(CompletionItemKind::FIELD),
                               detail: Some(format!("Column of {}", table)),
                               ..Default::default()
                           });
                       }
                   }
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
