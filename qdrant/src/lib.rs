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
pub mod transport;
pub mod point;
pub mod decoder;
pub mod encoder;
pub mod protocol;
pub mod pool;

pub use driver::QdrantDriver;
pub use error::{QdrantError, QdrantResult};
pub use point::{Point, PointId, Payload, PayloadValue, SparseVector, VectorData, MultiVectorPoint, ScoredPoint};
pub use pool::{QdrantPool, PoolConfig, PooledConnection};
pub use decoder::ScrollResult;
pub use encoder::FieldType;

/// Re-export qail-core prelude for convenience.
pub mod prelude {
    pub use qail_core::prelude::*;
    pub use crate::{QdrantDriver, QdrantError, QdrantResult, Point, PointId, Payload, PayloadValue, ScoredPoint};
    pub use crate::{SparseVector, VectorData, MultiVectorPoint};
    pub use crate::{QdrantPool, PoolConfig, ScrollResult, FieldType};
}

/// Distance metrics for vector similarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distance {
    Cosine,
    Euclidean,
    Dot,
}
