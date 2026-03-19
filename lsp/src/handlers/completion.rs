//! Completion Handler - QAIL syntax suggestions

use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use crate::server::QailLanguageServer;

/// QAIL keyword completions
const QAIL_KEYWORDS: &[(&str, &str)] = &[
    (
        "get",
        "SELECT query - get users fields id, email where id = :id",
    ),
    (
        "set",
        "UPDATE query - set users values name = :name where id = :id",
    ),
    (
        "add",
        "INSERT query - add users fields name, email values :name, :email",
    ),
    ("del", "DELETE query - del users where id = :id"),
    (
        "with",
        "CTE query - with recent as (get users fields id) get recent fields id",
    ),
];

/// QAIL operator completions
const QAIL_OPERATORS: &[(&str, &str)] = &[
    ("fields", "Select output columns"),
    ("where", "Filter rows"),
    ("order by", "Sort rows"),
    ("limit", "Limit row count"),
    ("group by", "Group rows"),
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
                label: keyword.to_string(),
                kind: Some(CompletionItemKind::KEYWORD),
                detail: Some(doc.to_string()),
                insert_text: Some(format!("{} ", keyword)),
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

        if let Ok(schema) = self.schema.read()
            && let Some(validator) = schema.as_ref()
        {
            for table in validator.table_names() {
                items.push(CompletionItem {
                    label: format!("get {}", table),
                    kind: Some(CompletionItemKind::CLASS),
                    detail: Some(format!("SELECT * FROM {}", table)),
                    insert_text: Some(format!("get {} fields ", table)),
                    ..Default::default()
                });
            }

            for table in validator.table_names() {
                if let Some(cols) = validator.column_names(table) {
                    for col in cols {
                        items.push(CompletionItem {
                            label: format!("{}.{}", table, col),
                            kind: Some(CompletionItemKind::FIELD),
                            detail: Some(format!("Column in {}", table)),
                            ..Default::default()
                        });
                    }
                }
            }
        }

        Ok(Some(CompletionResponse::Array(items)))
    }
}
