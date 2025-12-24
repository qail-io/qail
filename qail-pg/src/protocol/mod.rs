//! PostgreSQL Wire Protocol (Layer 2: Pure, Sync)
//!
//! This module contains the pure, synchronous protocol encoder.
//! No async, no I/O, no tokio - just AST → bytes computation.

pub mod wire;
pub mod encoder;

pub use wire::*;
pub use encoder::PgEncoder;
