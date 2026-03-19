//! Hover Handler - Show SQL preview for QAIL code

use qail_core::parse;
use qail_core::transpiler::ToSql;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use crate::server::{EmbeddedQueryKind, QailLanguageServer};

impl QailLanguageServer {
    /// Handle hover request - show SQL preview for QAIL queries
    pub async fn handle_hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = params
            .text_document_position_params
            .text_document
            .uri
            .to_string();
        let line = params.text_document_position_params.position.line as usize;

        if let Some(query) = self.extract_query_at_line(&uri, line) {
            let range = Some(Range {
                start: Position {
                    line: query.start_line as u32,
                    character: query.start_column as u32,
                },
                end: Position {
                    line: query.end_line as u32,
                    character: query.end_column as u32,
                },
            });

            match query.kind {
                EmbeddedQueryKind::Qail => match parse(&query.text) {
                    Ok(cmd) => {
                        let sql = cmd.to_sql();
                        return Ok(Some(Hover {
                            contents: HoverContents::Markup(MarkupContent {
                                kind: MarkupKind::Markdown,
                                value: format!("**Generated SQL:**\n```sql\n{}\n```", sql),
                            }),
                            range,
                        }));
                    }
                    Err(e) => {
                        return Ok(Some(Hover {
                            contents: HoverContents::Markup(MarkupContent {
                                kind: MarkupKind::Markdown,
                                value: format!("**Parse Error:** {}", e),
                            }),
                            range,
                        }));
                    }
                },
                EmbeddedQueryKind::Sql => {
                    return Ok(Some(Hover {
                        contents: HoverContents::Markup(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!("**Detected SQL:**\n```sql\n{}\n```", query.text),
                        }),
                        range,
                    }));
                }
            }
        }

        Ok(None)
    }
}
