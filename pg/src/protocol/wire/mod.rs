//! PostgreSQL Wire Protocol Messages
//!
//! Implementation of the PostgreSQL Frontend/Backend Protocol.
//! Reference: <https://www.postgresql.org/docs/current/protocol-message-formats.html>
//!
//! Split into sub-modules:
//! - `types` — message enums, structs, and error types
//! - `frontend` — `FrontendMessage` encoder (client → server)
//! - `backend` — `BackendMessage` decoder (server → client)

mod backend;
mod frontend;
mod types;
#[cfg(test)]
mod tests;

pub use types::*;
