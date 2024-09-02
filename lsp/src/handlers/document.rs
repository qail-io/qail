//! Document lifecycle handlers

use tower_lsp::lsp_types::*;
use crate::server::QailLanguageServer;

impl QailLanguageServer {
    pub async fn handle_did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri.to_string();
        let text = params.text_document.text.clone();

        self.try_load_schema_from_uri(&uri);

        if let Ok(mut docs) = self.documents.write() {
            docs.insert(uri.clone(), text.clone());
        }

        let diagnostics = self.get_diagnostics(&text);
        self.client
            .publish_diagnostics(params.text_document.uri, diagnostics, None)
            .await;
    }

    pub async fn handle_did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri.to_string();

        if let Some(change) = params.content_changes.first() {
            let text = change.text.clone();

            if let Ok(mut docs) = self.documents.write() {
                docs.insert(uri.clone(), text.clone());
            }

            let diagnostics = self.get_diagnostics(&text);
            self.client
                .publish_diagnostics(params.text_document.uri, diagnostics, None)
                .await;
        }
    }
}
