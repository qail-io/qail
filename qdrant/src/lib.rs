//! QAIL driver for Qdrant vector database.
//!
//! ⚠️ **BETA** - This crate is under active development. API may change.
//!
//! Native Rust driver with zero-copy gRPC and AST-based query building.
//!
//! # Example
//! ```ignore
//! use qail_core::prelude::*;
//! use qail_qdrant::QdrantDriver;
//!
//! let driver = QdrantDriver::connect("localhost", 6334).await?;
//!
//! // Vector similarity search
//! let results = driver.search("products", &embedding, 10, None).await?;
//! ```

pub mod driver;
pub mod error;
pub mod grpc_transport;
pub mod point;
pub mod proto_decoder;
pub mod proto_encoder;
pub mod protocol;

pub use driver::QdrantDriver;
pub use error::{QdrantError, QdrantResult};
pub use point::{Point, PointId, Payload, SparseVector, VectorData, MultiVectorPoint};

/// Re-export qail-core prelude for convenience.
pub mod prelude {
    pub use qail_core::prelude::*;
    pub use crate::{QdrantDriver, QdrantError, QdrantResult, Point, PointId, Payload};
    pub use crate::{SparseVector, VectorData, MultiVectorPoint};
}

/// Distance metrics for vector similarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distance {
    Cosine,
    Euclidean,
    Dot,
}
