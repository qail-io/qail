//! Hover Handler - Show SQL preview for QAIL code

use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use qail_core::parse;
use qail_core::transpiler::ToSql;

use crate::server::QailLanguageServer;

impl QailLanguageServer {
    /// Handle hover request - show SQL preview for QAIL queries
    pub async fn handle_hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = params.text_document_position_params.text_document.uri.to_string();
        let line = params.text_document_position_params.position.line as usize;

        // Try to extract QAIL at this line
        if let Some(qail_text) = self.extract_qail_at_line(&uri, line) {
            match parse(&qail_text) {
                Ok(cmd) => {
                    let sql = cmd.to_sql();
                    return Ok(Some(Hover {
                        contents: HoverContents::Markup(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!("**Generated SQL:**\n```sql\n{}\n```", sql),
                        }),
                        range: None,
                    }));
                }
                Err(e) => {
                    return Ok(Some(Hover {
                        contents: HoverContents::Markup(MarkupContent {
                            kind: MarkupKind::Markdown,
                            value: format!("**Parse Error:** {}", e),
                        }),
                        range: None,
                    }));
                }
            }
        }

        Ok(None)
    }
}
