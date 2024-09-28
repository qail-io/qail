//! PostgreSQL Wire Protocol (Layer 2: Pure, Sync)
//!
//! No async, no I/O, no tokio - just AST → bytes computation.

pub mod ast_encoder;
pub mod auth;
pub mod copy_encoder;
pub mod encoder;
pub mod types;
pub mod error;
pub mod wire;

pub use error::EncodeError;

pub use ast_encoder::AstEncoder;
pub use auth::ScramClient;
pub use copy_encoder::{encode_copy_batch, encode_copy_value};
pub use encoder::PgEncoder;
pub use types::{is_array_oid, oid, oid_to_name};
pub use wire::*;
