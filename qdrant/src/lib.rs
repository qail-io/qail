//! QAIL driver for Qdrant vector database.
//!
//! ⚠️ **BETA** - This crate is under active development. API may change.
//!
//! Native Rust driver with AST-based query building for vector similarity search.
//!
//! # Example
//! ```ignore
//! use qail_core::prelude::*;
//! use qail_qdrant::QdrantDriver;
//!
//! let driver = QdrantDriver::connect("localhost", 6333).await?;
//!
//! // Vector similarity search
//! let results = driver.search(
//!     Qail::search("products")
//!         .vector(embedding)
//!         .filter("category", Operator::Eq, "electronics")
//!         .limit(10)
//! ).await?;
//! ```

pub mod driver;
pub mod error;
pub mod point;
pub mod pool;
pub mod proto_encoder;
pub mod protocol;

pub use driver::{QdrantDriver, Distance};
pub use error::{QdrantError, QdrantResult};
pub use point::{Point, PointId, Payload, SparseVector, VectorData, MultiVectorPoint};
pub use pool::{QdrantPool, PoolConfig, PooledConnection};

/// Re-export qail-core prelude for convenience.
pub mod prelude {
    pub use qail_core::prelude::*;
    pub use crate::{QdrantDriver, QdrantError, QdrantResult, Point, PointId, Payload};
    pub use crate::{SparseVector, VectorData, MultiVectorPoint};
    pub use crate::{QdrantPool, PoolConfig};
}
