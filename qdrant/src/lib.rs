//! QAIL Vector Bridge for Qdrant.
//!
//! `qail-qdrant` provides Qdrant vector search, collection, upsert, delete, and
//! payload filter operations with QAIL-compatible filter semantics.
//!
//! This crate does not use SQL. The SQL string/SQL bytes distinction applies to
//! the PostgreSQL crates (`qail-core` and `qail-pg`), while this crate maps
//! filter intent to Qdrant request models.
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
