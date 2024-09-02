//! QAIL Language Server - Entry Point
//!
//! Clean modular architecture for the QAIL LSP

mod handlers;
mod server;

use server::QailLanguageServer;
use tower_lsp::{LspService, Server};

#[tokio::main]
async fn main() {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(QailLanguageServer::new);
    Server::new(stdin, stdout, socket).serve(service).await;
}
