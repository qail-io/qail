//! WebAssembly build of the QAIL MCP server.
//!
//! This crate is a transport shim and nothing more. All dispatch, tool logic,
//! resources, and prompts live in [`qail_mcp`], so the stdio server
//! (`qail-mcp`) and the remote HTTP server (`mcp-worker`) execute the exact
//! same code and cannot drift apart.
//!
//! The host is responsible for HTTP framing: reading the request body, calling
//! [`handle_rpc`], and mapping `None` to `202 Accepted`.

use wasm_bindgen::prelude::wasm_bindgen;

/// Handle one JSON-RPC message, returning the serialized response.
///
/// Returns `undefined` for notifications (messages with no `id`), which have no
/// response by JSON-RPC definition — the caller should answer `202 Accepted`.
/// Malformed JSON yields a JSON-RPC `-32700` envelope rather than throwing, so
/// the host never has to distinguish an exception from a protocol error.
#[wasm_bindgen]
pub fn handle_rpc(line: &str) -> Option<String> {
    qail_mcp::handle_rpc(line)
}

/// The `qail-core` version this build was compiled against.
///
/// Exposed so the Worker can serve it from `/mcp/health` and the docs site can
/// assert the deployed server matches its published version.
#[wasm_bindgen]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
