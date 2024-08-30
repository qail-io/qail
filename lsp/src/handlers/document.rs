//! Document lifecycle handlers

use tower_lsp::lsp_types::*;
use crate::server::QailLanguageServer;

impl QailLanguageServer {
    /// Handle document open - cache content and publish diagnostics
    pub async fn handle_did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri.to_string();
        let text = params.text_document.text.clone();

        // Cache document content
        if let Ok(mut docs) = self.documents.write() {
            docs.insert(uri.clone(), text.clone());
        }

        // Publish diagnostics
        let diagnostics = self.get_diagnostics(&text);
        self.client
            .publish_diagnostics(params.text_document.uri, diagnostics, None)
            .await;
    }

    /// Handle document change - update cache and republish diagnostics
    pub async fn handle_did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri.to_string();

        if let Some(change) = params.content_changes.first() {
            let text = change.text.clone();

            // Update cache
            if let Ok(mut docs) = self.documents.write() {
                docs.insert(uri.clone(), text.clone());
            }

            // Republish diagnostics
            let diagnostics = self.get_diagnostics(&text);
            self.client
                .publish_diagnostics(params.text_document.uri, diagnostics, None)
                .await;
        }
    }
}
