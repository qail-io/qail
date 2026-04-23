//! Completion Handler - QAIL syntax suggestions

use std::collections::HashSet;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use crate::server::{EmbeddedQueryKind, QailLanguageServer};
use crate::utf16::Utf16Index;

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
    ("count", "COUNT query - count users fields id"),
    ("export", "COPY TO query - export users fields id"),
    ("make", "DDL query - make users fields id:int"),
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
        params: CompletionParams,
    ) -> Result<Option<CompletionResponse>> {
        let uri = params.text_document_position.text_document.uri.to_string();
        let position = params.text_document_position.position;
        let query = self.extract_query_at_position(&uri, position);
        let content = self.get_document(&uri);
        let mut items = Vec::new();

        if uri.ends_with(".rs") {
            let in_builder_context = content
                .as_deref()
                .is_some_and(|text| rust_builder_context(text, position));

            if !in_builder_context && query.is_none() {
                return Ok(None);
            }

            if in_builder_context {
                push_builder_method_items(&mut items);
            }

            if let Some(embedded) = query
                && embedded.kind == EmbeddedQueryKind::Qail
            {
                push_qail_keyword_items(&mut items);
                push_qail_operator_items(&mut items);
                if let Some(validator) = self.schema_validator_for_uri(&uri) {
                    push_schema_items(&mut items, &validator);
                }
            }
        } else {
            push_qail_keyword_items(&mut items);
            push_qail_operator_items(&mut items);

            if let Some(validator) = self.schema_validator_for_uri(&uri) {
                push_schema_items(&mut items, &validator);
            }
        }

        if items.is_empty() {
            return Ok(None);
        }

        let mut seen = HashSet::new();
        items.retain(|item| seen.insert(item.label.clone()));

        Ok(Some(CompletionResponse::Array(items)))
    }
}

fn rust_builder_context(content: &str, position: Position) -> bool {
    let index = Utf16Index::new(content);
    let Some(offset) = index.position_to_offset(position) else {
        return false;
    };

    let bytes = content.as_bytes();
    let mut stmt_start = offset;
    while stmt_start > 0 {
        let b = bytes[stmt_start - 1];
        if matches!(b, b';' | b'{' | b'}') {
            break;
        }
        stmt_start -= 1;
    }

    content
        .get(stmt_start..offset)
        .is_some_and(|context| context.contains("Qail::"))
}

fn push_qail_keyword_items(items: &mut Vec<CompletionItem>) {
    for (keyword, doc) in QAIL_KEYWORDS {
        items.push(CompletionItem {
            label: keyword.to_string(),
            kind: Some(CompletionItemKind::KEYWORD),
            detail: Some(doc.to_string()),
            insert_text: Some(format!("{} ", keyword)),
            ..Default::default()
        });
    }
}

fn push_qail_operator_items(items: &mut Vec<CompletionItem>) {
    for (op, doc) in QAIL_OPERATORS {
        items.push(CompletionItem {
            label: op.to_string(),
            kind: Some(CompletionItemKind::OPERATOR),
            detail: Some(doc.to_string()),
            ..Default::default()
        });
    }
}

fn push_builder_method_items(items: &mut Vec<CompletionItem>) {
    let builder_methods = [
        ("Qail::get", "Start a SELECT query"),
        ("Qail::set", "Start an UPDATE query"),
        ("Qail::add", "Start an INSERT query"),
        ("Qail::del", "Start a DELETE query"),
        ("Qail::put", "Start an UPSERT query"),
        ("Qail::export", "Start a COPY TO query"),
        (".columns", "Specify columns to select"),
        (".filter", "Add WHERE condition"),
        (".order_by", "Add ORDER BY clause"),
        (".limit", "Add LIMIT clause"),
        (".offset", "Add OFFSET clause"),
        (".set_value", "Set column value for UPDATE/INSERT"),
        (".returning", "Add RETURNING clause"),
        (".with_rls", "Inject RLS context"),
    ];

    for (method, doc) in builder_methods {
        items.push(CompletionItem {
            label: method.to_string(),
            kind: Some(CompletionItemKind::METHOD),
            detail: Some(doc.to_string()),
            ..Default::default()
        });
    }
}

fn push_schema_items(items: &mut Vec<CompletionItem>, validator: &qail_core::validator::Validator) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rust_builder_context_detects_active_builder_statement() {
        let src = r#"let _q = Qail::get("users")
    .columns(["id"])
    ."#;

        assert!(rust_builder_context(
            src,
            Position {
                line: 2,
                character: 5,
            }
        ));
    }

    #[test]
    fn rust_builder_context_ignores_previous_statement() {
        let src = r#"let _q = Qail::get("users");
foo."#;

        assert!(!rust_builder_context(
            src,
            Position {
                line: 1,
                character: 4,
            }
        ));
    }
}
