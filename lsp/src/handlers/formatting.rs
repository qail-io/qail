//! Document formatting handler for raw QAIL documents.

use qail_core::fmt::Formatter;
use qail_core::parse;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;

use crate::server::{EmbeddedQueryKind, QailLanguageServer, full_document_query_span};
use crate::utf16::Utf16Index;

impl QailLanguageServer {
    pub async fn handle_formatting(
        &self,
        params: DocumentFormattingParams,
    ) -> Result<Option<Vec<TextEdit>>> {
        let uri = params.text_document.uri.to_string();
        let docs = self
            .documents
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(content) = docs.get(&uri) else {
            return Ok(None);
        };

        let Some((kind, query_text, _, _, _, _)) = full_document_query_span(content) else {
            return Ok(None);
        };
        if kind != EmbeddedQueryKind::Qail {
            return Ok(None);
        }

        let Ok(cmd) = parse(query_text) else {
            return Ok(None);
        };
        let Ok(formatted) = Formatter::new().format(&cmd) else {
            return Ok(None);
        };

        let new_text = normalize_formatted_output(content, &formatted);
        if new_text == *content {
            return Ok(None);
        }

        let range = Range {
            start: Position {
                line: 0,
                character: 0,
            },
            end: text_end_position(content),
        };

        Ok(Some(vec![TextEdit { range, new_text }]))
    }
}

fn text_end_position(text: &str) -> Position {
    Utf16Index::new(text).offset_to_position(text.len())
}

fn normalize_formatted_output(original: &str, formatted: &str) -> String {
    let mut new_text = formatted.trim_end_matches('\n').to_string();
    if original.ends_with('\n') {
        new_text.push('\n');
    }
    new_text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_end_position_tracks_lines_and_columns() {
        let pos = text_end_position("a\nbc");
        assert_eq!(pos.line, 1);
        assert_eq!(pos.character, 2);
    }

    #[test]
    fn normalize_output_respects_original_trailing_newline() {
        let with_nl = normalize_formatted_output("get users\n", "get users\n\n");
        assert_eq!(with_nl, "get users\n");

        let without_nl = normalize_formatted_output("get users", "get users\n");
        assert_eq!(without_nl, "get users");
    }
}
