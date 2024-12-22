//! # QAIL Gateway
//!
//! The native data layer that can replace REST/GraphQL with binary AST protocol.
//!
//! ## Architecture
//!
//! ```text
//! Client → QAIL AST (binary) → Gateway → Postgres/Qdrant/Redis
//! ```
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use qail_gateway::Gateway;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let gateway = Gateway::builder()
//!         .database("postgres://localhost/mydb")
//!         .bind("0.0.0.0:8080")
//!         .build_and_init()
//!         .await?;
//!     
//!     gateway.serve().await?;
//!     Ok(())
//! }
//! ```

#![deny(warnings)]
#![deny(clippy::all)]
#![deny(unused_imports)]
#![deny(dead_code)]

pub mod auth;
pub mod config;
pub mod error;
pub mod handler;
pub mod metrics;
pub mod middleware;
pub mod policy;
pub mod router;
pub mod schema;
pub mod server;
pub mod ws;

pub use config::GatewayConfig;
pub use error::GatewayError;
pub use server::{Gateway, GatewayState};
