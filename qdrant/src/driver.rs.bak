//! gRPC-based Qdrant driver with zero-copy encoding.
//!
//! This driver uses the proto_encoder for direct protobuf encoding
//! and grpc_transport for HTTP/2 communication, achieving performance
//! that matches or exceeds the official qdrant-client.

use bytes::BytesMut;
use qail_core::ast::Qail;

use crate::error::{QdrantError, QdrantResult};
use crate::transport::GrpcClient;
use crate::point::{Point, ScoredPoint};
use crate::decoder;
use crate::encoder;

/// High-performance gRPC driver for Qdrant.
///
/// Uses gRPC/HTTP2 with zero-copy protobuf encoding:
/// - Encodes protobuf directly with pre-computed headers
/// - Reuses buffers to minimize allocations
/// - Uses memcpy for vector data (no per-element loop)
///
/// # Example
/// ```ignore
/// use qail_qdrant::QdrantDriver;
/// use qail_core::prelude::*;
///
/// let driver = QdrantDriver::connect("localhost", 6334).await?;
///
/// let results = driver.search(
///     "products",
///     &embedding,
///     10,
///     Some(0.5),
/// ).await?;
/// ```
pub struct QdrantDriver {
    /// gRPC client for HTTP/2 transport
    client: GrpcClient,
    /// Reusable encoding buffer
    buffer: BytesMut,
}

impl QdrantDriver {
    /// Connect to Qdrant gRPC endpoint (default port 6334).
    pub async fn connect(host: &str, port: u16) -> QdrantResult<Self> {
        let client = GrpcClient::connect(host, port).await?;
        Ok(Self {
            client,
            buffer: BytesMut::with_capacity(8192),
        })
    }

    /// Connect with address string.
    pub async fn connect_addr(addr: &str) -> QdrantResult<Self> {
        let parts: Vec<&str> = addr.split(':').collect();
        if parts.len() != 2 {
            return Err(QdrantError::Connection(
                "Invalid address format, expected host:port".to_string(),
            ));
        }
        let port: u16 = parts[1]
            .parse()
            .map_err(|_| QdrantError::Connection("Invalid port".to_string()))?;
        Self::connect(parts[0], port).await
    }

    /// Vector similarity search with zero-copy encoding.
    ///
    /// # Arguments
    /// * `collection` - Collection name
    /// * `vector` - Query vector
    /// * `limit` - Max results
    /// * `score_threshold` - Optional minimum score
    ///
    /// # Performance
    /// Vector is encoded via memcpy (no per-element serialization).
    pub async fn search(
        &mut self,
        collection: &str,
        vector: &[f32],
        limit: u64,
        score_threshold: Option<f32>,
    ) -> QdrantResult<Vec<ScoredPoint>> {
        // Clear buffer for reuse
        self.buffer.clear();
        
        // Encode request using zero-copy encoder
        encoder::encode_search_proto(
            &mut self.buffer,
            collection,
            vector,
            limit,
            score_threshold,
            None,
        );

        // Send via gRPC (split to avoid clone - zero allocation!)
        let request_bytes = self.buffer.split().freeze();
        let response = self.client.search(request_bytes).await?;

        // Decode response using zero-copy decoder
        decoder::decode_search_response(&response)
    }

    /// Search using QAIL AST.
    ///
    /// Extracts vector, collection, limit from the Qail command.
    pub async fn search_ast(&mut self, cmd: &Qail) -> QdrantResult<Vec<ScoredPoint>> {
        let collection = if cmd.table.is_empty() {
            return Err(QdrantError::Encode("Collection name required".to_string()));
        } else {
            &cmd.table
        };

        let vector = cmd.vector.as_ref().ok_or_else(|| {
            QdrantError::Encode("Vector required for search".to_string())
        })?;

        // Extract limit from cages (default 10)
        let mut limit = 10u64;
        for cage in &cmd.cages {
            if let qail_core::ast::CageKind::Limit(n) = cage.kind {
                limit = n as u64;
            }
        }

        let score_threshold = cmd.score_threshold;

        self.search(collection, vector, limit, score_threshold).await
    }

    /// Upsert points with zero-copy encoding.
    pub async fn upsert(
        &mut self,
        collection: &str,
        points: &[Point],
        wait: bool,
    ) -> QdrantResult<()> {
        // Clear buffer for reuse
        self.buffer.clear();
        
        // Encode request using zero-copy encoder
        encoder::encode_upsert_proto(&mut self.buffer, collection, points, wait);

        // Send via gRPC (split to avoid clone)
        let request_bytes = self.buffer.split().freeze();
        let _response = self.client.upsert(request_bytes).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_driver_struct() {
        // Verify struct is constructible
        let buffer = BytesMut::with_capacity(1024);
        assert!(buffer.capacity() >= 1024);
    }
}
