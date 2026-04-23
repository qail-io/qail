//! Document lifecycle handlers

use crate::server::{OpenDocument, QailLanguageServer};
use tower_lsp::lsp_types::*;

impl QailLanguageServer {
    pub async fn handle_did_open(&self, params: DidOpenTextDocumentParams) {
        let uri = params.text_document.uri.to_string();
        let text = params.text_document.text.clone();
        let version = params.text_document.version;

        self.try_load_schema_from_uri(&uri);

        if let Ok(mut docs) = self.documents.write() {
            docs.insert(
                uri.clone(),
                OpenDocument {
                    text: text.clone(),
                    version,
                },
            );
        }

        let diagnostics = self.get_diagnostics(&text, &uri);
        self.client
            .publish_diagnostics(params.text_document.uri, diagnostics, Some(version))
            .await;
    }

    pub async fn handle_did_change(&self, params: DidChangeTextDocumentParams) {
        let uri = params.text_document.uri.to_string();
        let version = params.text_document.version;

        if let Some(change) = params.content_changes.last() {
            let text = change.text.clone();

            self.try_load_schema_from_uri(&uri);

            let should_apply_change = if let Ok(mut docs) = self.documents.write() {
                let current_version = docs.get(&uri).map(|doc| doc.version);
                if !should_apply_version(current_version, version) {
                    false
                } else {
                    docs.insert(
                        uri.clone(),
                        OpenDocument {
                            text: text.clone(),
                            version,
                        },
                    );
                    true
                }
            } else {
                false
            };

            if !should_apply_change {
                return;
            }

            let diagnostics = self.get_diagnostics(&text, &uri);
            self.client
                .publish_diagnostics(params.text_document.uri, diagnostics, Some(version))
                .await;

            if is_schema_uri(&uri) {
                let docs_snapshot = self
                    .documents
                    .read()
                    .map(|docs| {
                        docs.iter()
                            .filter(|(doc_uri, _)| *doc_uri != &uri)
                            .map(|(doc_uri, doc)| (doc_uri.clone(), doc.text.clone(), doc.version))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                for (doc_uri, doc_text, doc_version) in docs_snapshot {
                    let diagnostics = self.get_diagnostics(&doc_text, &doc_uri);
                    if let Ok(parsed_uri) = Url::parse(&doc_uri) {
                        self.client
                            .publish_diagnostics(parsed_uri, diagnostics, Some(doc_version))
                            .await;
                    }
                }
            }
        }
    }

    pub async fn handle_did_close(&self, params: DidCloseTextDocumentParams) {
        let uri = params.text_document.uri.to_string();
        if let Ok(mut docs) = self.documents.write() {
            docs.remove(&uri);
        }

        self.client
            .publish_diagnostics(params.text_document.uri, Vec::new(), None)
            .await;
    }

    pub async fn handle_did_change_watched_files(&self, params: DidChangeWatchedFilesParams) {
        let mut schema_touched = false;
        for change in &params.changes {
            if is_schema_uri(change.uri.as_str()) {
                schema_touched = true;
                self.try_load_schema_from_uri(change.uri.as_str());
            }
        }

        if !schema_touched {
            return;
        }

        let docs_snapshot = self
            .documents
            .read()
            .map(|docs| {
                docs.iter()
                    .map(|(uri, doc)| (uri.clone(), doc.text.clone(), doc.version))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        for (doc_uri, doc_text, doc_version) in docs_snapshot {
            if is_schema_uri(&doc_uri) {
                continue;
            }
            let diagnostics = self.get_diagnostics(&doc_text, &doc_uri);
            if let Ok(parsed_uri) = Url::parse(&doc_uri) {
                self.client
                    .publish_diagnostics(parsed_uri, diagnostics, Some(doc_version))
                    .await;
            }
        }
    }
}

fn should_apply_version(current: Option<i32>, incoming: i32) -> bool {
    current.is_none_or(|version| incoming > version)
}

fn is_schema_uri(uri: &str) -> bool {
    Url::parse(uri)
        .ok()
        .and_then(|url| url.to_file_path().ok())
        .and_then(|path| path.file_name().map(|name| name == "schema.qail"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::should_apply_version;
    use crate::server::QailLanguageServer;
    use tower_lsp::LspService;
    use tower_lsp::lsp_types::{
        DidChangeTextDocumentParams, DidChangeWatchedFilesParams, DidOpenTextDocumentParams,
        FileChangeType, FileEvent, TextDocumentContentChangeEvent, TextDocumentItem, Url,
        VersionedTextDocumentIdentifier,
    };

    fn create_temp_dir(prefix: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        dir.push(format!(
            "qail_lsp_{prefix}_{}_{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn version_updates_require_newer_sequence() {
        assert!(should_apply_version(None, 1));
        assert!(should_apply_version(Some(3), 4));
        assert!(!should_apply_version(Some(3), 3));
        assert!(!should_apply_version(Some(3), 2));
    }

    #[tokio::test]
    async fn did_change_ignores_out_of_order_versions_end_to_end() {
        let (service, _socket) = LspService::new(QailLanguageServer::new);
        let server = service.inner();
        let uri = Url::parse("file:///tmp/qail_lsp_versions.rs").expect("uri");

        server
            .handle_did_open(DidOpenTextDocumentParams {
                text_document: TextDocumentItem {
                    uri: uri.clone(),
                    language_id: "rust".to_string(),
                    version: 1,
                    text: "fn demo() {}".to_string(),
                },
            })
            .await;

        let fresh_text = "fn demo_v2() {}".to_string();
        server
            .handle_did_change(DidChangeTextDocumentParams {
                text_document: VersionedTextDocumentIdentifier {
                    uri: uri.clone(),
                    version: 2,
                },
                content_changes: vec![TextDocumentContentChangeEvent {
                    range: None,
                    range_length: None,
                    text: fresh_text.clone(),
                }],
            })
            .await;

        server
            .handle_did_change(DidChangeTextDocumentParams {
                text_document: VersionedTextDocumentIdentifier {
                    uri: uri.clone(),
                    version: 1,
                },
                content_changes: vec![TextDocumentContentChangeEvent {
                    range: None,
                    range_length: None,
                    text: "fn stale() {}".to_string(),
                }],
            })
            .await;

        let docs = server.documents.read().expect("documents lock");
        let doc = docs.get(uri.as_str()).expect("open document");
        assert_eq!(doc.version, 2);
        assert_eq!(doc.text, fresh_text);
    }

    #[tokio::test]
    async fn watched_schema_change_reloads_schema_and_preserves_document_state() {
        let root = create_temp_dir("watch");
        let src_dir = root.join("src");
        fs::create_dir_all(&src_dir).expect("src dir");
        let schema_path = root.join("schema.qail");
        fs::write(
            &schema_path,
            r#"
table users {
  id UUID
}
"#,
        )
        .expect("initial schema");

        let file_path = src_dir.join("main.rs");
        let file_uri = Url::from_file_path(&file_path).expect("file uri");
        let schema_uri = Url::from_file_path(&schema_path).expect("schema uri");

        let (service, _socket) = LspService::new(QailLanguageServer::new);
        let server = service.inner();
        let doc_version = 7;
        server
            .handle_did_open(DidOpenTextDocumentParams {
                text_document: TextDocumentItem {
                    uri: file_uri.clone(),
                    language_id: "rust".to_string(),
                    version: doc_version,
                    text: "fn demo() { let _ = query(\"get users fields id\"); }".to_string(),
                },
            })
            .await;

        let initial_schema_mtime = {
            let schemas = server.schemas.read().expect("schema cache");
            let root_cache = schemas.get(&root).expect("root schema cache");
            root_cache.schema_mtime
        };

        std::thread::sleep(Duration::from_secs(1));
        fs::write(
            &schema_path,
            r#"
table users {
  id UUID
  tenant_id UUID
}
"#,
        )
        .expect("updated schema");

        server
            .handle_did_change_watched_files(DidChangeWatchedFilesParams {
                changes: vec![FileEvent {
                    uri: schema_uri,
                    typ: FileChangeType::CHANGED,
                }],
            })
            .await;

        let (updated_schema_mtime, doc_text, doc_version_after) = {
            let schemas = server.schemas.read().expect("schema cache");
            let root_cache = schemas.get(&root).expect("root schema cache");
            let docs = server.documents.read().expect("documents lock");
            let doc = docs.get(file_uri.as_str()).expect("open document");
            (root_cache.schema_mtime, doc.text.clone(), doc.version)
        };

        assert_ne!(
            initial_schema_mtime, updated_schema_mtime,
            "schema cache should refresh after watched change"
        );
        assert_eq!(doc_version_after, doc_version);
        assert_eq!(
            doc_text,
            "fn demo() { let _ = query(\"get users fields id\"); }"
        );

        let _ = fs::remove_dir_all(root);
    }
}
