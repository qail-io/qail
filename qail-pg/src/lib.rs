//! QAIL PostgreSQL Driver
//!
//! Native PostgreSQL wire protocol driver for QAIL.
//!
//! # Architecture
//!
//! This crate implements two layers:
//!
//! ## Layer 2: Protocol (Pure, Sync)
//! - `protocol::PgEncoder` - Compiles QailCmd to wire protocol bytes
//! - No async, no I/O, no tokio
//! - Input: AST → Output: BytesMut
//!
//! ## Layer 3: Driver (Async I/O)
//! - `driver::PgConnection` - TCP networking
//! - Uses tokio for async I/O
//! - Sends bytes from Layer 2, receives responses
//!
//! # Example
//!
//! ```ignore
//! use qail_core::ast::QailCmd;
//! use qail_pg::protocol::PgEncoder;
//!
//! // Layer 2: Pure computation (no async needed)
//! let cmd = QailCmd::get("users");
//! let bytes = PgEncoder::encode_simple_query(&cmd);
//!
//! // Layer 3: Async I/O (uses tokio)
//! let mut driver = PgDriver::connect("localhost", 5432, "user", "db").await?;
//! let rows = driver.fetch_all(&cmd).await?;
//! ```

pub mod protocol;
pub mod driver;

pub use protocol::PgEncoder;
pub use driver::{PgDriver, PgConnection, PgRow, PgError, PgResult};
