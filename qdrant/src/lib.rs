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

pub mod decoder;
pub mod driver;
pub mod encoder;
pub mod error;
pub mod point;
pub mod pool;
pub mod protocol;
pub mod transport;

pub use decoder::ScrollResult;
pub use driver::QdrantDriver;
pub use encoder::FieldType;
pub use error::{QdrantError, QdrantResult};
pub use point::{
    MultiVectorPoint, Payload, PayloadValue, Point, PointId, ScoredPoint, SparseVector, VectorData,
};
pub use pool::{PoolConfig, PooledConnection, QdrantPool};

/// Re-export qail-core prelude for convenience.
pub mod prelude {
    pub use crate::{FieldType, PoolConfig, QdrantPool, ScrollResult};
    pub use crate::{MultiVectorPoint, SparseVector, VectorData};
    pub use crate::{
        Payload, PayloadValue, Point, PointId, QdrantDriver, QdrantError, QdrantResult, ScoredPoint,
    };
    pub use qail_core::prelude::*;
}

/// Distance metrics for vector similarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Distance {
    /// Cosine similarity (normalised dot product).
    Cosine,
    /// Euclidean (L2) distance.
    Euclidean,
    /// Dot product similarity.
    Dot,
}
