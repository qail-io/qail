//! Schema module for QAIL Gateway
//!
//! Loads table schemas from `.qail` files and schema module directories.
//! Provides schema metadata to the router for auto-REST route generation.

mod convert;
mod registry;
mod types;
mod validate;

pub use registry::SchemaRegistry;
pub use types::{GatewayColumn, GatewayForeignKey, GatewayTable};

#[cfg(test)]
mod tests;
