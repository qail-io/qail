//! Completion Handler - QAIL syntax suggestions

use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use crate::server::QailLanguageServer;

/// QAIL keyword completions
const QAIL_KEYWORDS: &[(&str, &str)] = &[
    ("get", "SELECT query - get::table [cols] ?filter"),
    ("set", "UPDATE query - set::table {col=val} ?filter"),
    ("add", "INSERT query - add::table {col=val}"),
    ("del", "DELETE query - del::table ?filter"),
    ("with", "CTE query - with::{name{...}}"),
];

/// QAIL operator completions
const QAIL_OPERATORS: &[(&str, &str)] = &[
    ("?", "WHERE clause filter"),
    ("!", "JOIN clause"),
    ("@", "ORDER BY clause"),
    ("#", "LIMIT clause"),
    ("^", "GROUP BY clause"),
];

impl QailLanguageServer {
    /// Handle completion request
    pub async fn handle_completion(
        &self,
        _params: CompletionParams,
    ) -> Result<Option<CompletionResponse>> {
        let mut items = Vec::new();

        for (keyword, doc) in QAIL_KEYWORDS {
            items.push(CompletionItem {
                label: format!("{}::", keyword),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some(doc.to_string()),
                insert_text: Some(format!("{}::", keyword)),
                ..Default::default()
            });
        }

        for (op, doc) in QAIL_OPERATORS {
            items.push(CompletionItem {
                label: op.to_string(),
                kind: Some(CompletionItemKind::OPERATOR),
                detail: Some(doc.to_string()),
                ..Default::default()
            });
        }

        let builder_methods = [
            ("QailCmd::get", "Start a SELECT query"),
            ("QailCmd::set", "Start an UPDATE query"),
            ("QailCmd::add", "Start an INSERT query"),
            ("QailCmd::del", "Start a DELETE query"),
            (".columns", "Specify columns to select"),
            (".filter", "Add WHERE condition"),
            (".order_by", "Add ORDER BY clause"),
            (".limit", "Add LIMIT clause"),
            (".set_value", "Set column value for UPDATE/INSERT"),
        ];

        for (method, doc) in builder_methods {
            items.push(CompletionItem {
                label: method.to_string(),
                kind: Some(CompletionItemKind::METHOD),
                detail: Some(doc.to_string()),
                ..Default::default()
            });
        }

        // TODO: Add schema-aware completions when Validator exposes table names

        Ok(Some(CompletionResponse::Array(items)))
    }
}
