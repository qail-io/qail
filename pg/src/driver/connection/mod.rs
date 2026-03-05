//! PostgreSQL Connection
//!
//! Low-level TCP connection with wire protocol handling.
//! This is Layer 3 (async I/O).
//!
//! Sub-modules:
//! - `types` — structs, enums, constants
//! - `connect` — connection establishment (plain, TLS, mTLS, Unix, GSSENC)
//! - `startup` — startup handshake, auth, prepared statement management
//! - `helpers` — free functions: metrics, GSS token, MD5, SCRAM, Drop impl

mod connect;
mod helpers;
mod startup;
pub(crate) mod types;
#[cfg(test)]
mod tests;

pub use types::{PgConnection, TlsConfig};
pub(crate) use helpers::parse_affected_rows;
pub(crate) use types::{CANCEL_REQUEST_CODE, StatementCache};
