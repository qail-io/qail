//! gRPC transport for Qdrant using HTTP/2.
//!
//! This module provides a low-level gRPC client that:
//! - Uses h2 for HTTP/2 framing
//! - Sends pre-encoded protobuf messages
//! - Handles gRPC response decoding
//!
//! Unlike tonic, we control the entire encoding path for zero-copy performance.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use h2::client::{self, SendRequest};
use http::{Request, Uri};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::error::{QdrantError, QdrantResult};

/// gRPC content type
const GRPC_CONTENT_TYPE: &str = "application/grpc";

/// Qdrant gRPC service paths
#[allow(dead_code)]
const POINTS_SERVICE: &str = "qdrant.Points";

// gRPC method paths
const METHOD_SEARCH: &str = "/qdrant.Points/Search";
const METHOD_UPSERT: &str = "/qdrant.Points/Upsert";
const METHOD_DELETE: &str = "/qdrant.Points/Delete";
const METHOD_GET: &str = "/qdrant.Points/Get";
const METHOD_SCROLL: &str = "/qdrant.Points/Scroll";
const METHOD_RECOMMEND: &str = "/qdrant.Points/Recommend";
const METHOD_CREATE_COLLECTION: &str = "/qdrant.Collections/Create";
const METHOD_DELETE_COLLECTION: &str = "/qdrant.Collections/Delete";

/// gRPC client for Qdrant.
///
/// Uses HTTP/2 with persistent connection for efficient request pipelining.
pub struct GrpcClient {
    /// HTTP/2 send request handle
    sender: Arc<Mutex<SendRequest<Bytes>>>,
    /// Server URI
    #[allow(dead_code)]
    uri: Uri,
    /// Reusable buffer for encoding
    #[allow(dead_code)]
    buffer: Mutex<BytesMut>,
}

impl GrpcClient {
    /// Connect to Qdrant gRPC endpoint.
    pub async fn connect(host: &str, port: u16) -> QdrantResult<Self> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| QdrantError::Connection(format!("TCP connect failed: {}", e)))?;

        // Perform HTTP/2 handshake
        let (sender, connection) = client::handshake(stream)
            .await
            .map_err(|e| QdrantError::Connection(format!("H2 handshake failed: {}", e)))?;

        // Spawn connection driver
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("gRPC connection error: {}", e);
            }
        });

        let uri: Uri = format!("http://{}:{}", host, port)
            .parse()
            .map_err(|e| QdrantError::Connection(format!("Invalid URI: {}", e)))?;

        Ok(Self {
            sender: Arc::new(Mutex::new(sender)),
            uri,
            buffer: Mutex::new(BytesMut::with_capacity(8192)),
        })
    }

    /// Send a gRPC request and receive response.
    ///
    /// # Arguments
    /// * `method` - gRPC method path (e.g., "/qdrant.Points/Search")
    /// * `body` - Pre-encoded protobuf message
    ///
    /// # Returns
    /// The raw protobuf response body (without gRPC framing)
    pub async fn call(&self, method: &str, body: Bytes) -> QdrantResult<Bytes> {
        // Frame the message with gRPC length prefix
        let framed = grpc_frame(body);

        // Build HTTP/2 request
        let request = Request::builder()
            .method("POST")
            .uri(method)
            .header("content-type", GRPC_CONTENT_TYPE)
            .header("te", "trailers")
            .body(())
            .map_err(|e| QdrantError::Encode(format!("Request build failed: {}", e)))?;

        // Send request
        let sender = self.sender.lock().await;
        let mut ready_sender = sender.clone().ready().await
            .map_err(|e| QdrantError::Grpc(format!("Connection not ready: {}", e)))?;

        let (response, mut send_body) = ready_sender
            .send_request(request, false)
            .map_err(|e| QdrantError::Grpc(format!("Send request failed: {}", e)))?;
        
        // Release lock early
        drop(sender);

        // Send body
        send_body
            .send_data(framed, true)
            .map_err(|e| QdrantError::Grpc(format!("Send body failed: {}", e)))?;

        // Receive response
        let (head, mut body) = response
            .await
            .map_err(|e| QdrantError::Grpc(format!("Response failed: {}", e)))?
            .into_parts();

        // Check status
        if head.status != http::StatusCode::OK {
            return Err(QdrantError::Grpc(format!(
                "gRPC error: HTTP {}",
                head.status
            )));
        }

        // Read body
        let mut response_buf = BytesMut::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk.map_err(|e| QdrantError::Decode(format!("Body read failed: {}", e)))?;
            response_buf.extend_from_slice(&chunk);
            let _ = body.flow_control().release_capacity(chunk.len());
        }

        // Check gRPC status in trailers
        let trailers = body.trailers().await
            .map_err(|e| QdrantError::Grpc(format!("Trailers failed: {}", e)))?;
        
        if let Some(trailers) = trailers
            && let Some(status) = trailers.get("grpc-status")
            && status != "0"
        {
            let message = trailers
                .get("grpc-message")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("Unknown error");
            return Err(QdrantError::Grpc(format!(
                "gRPC status {}: {}",
                status.to_str().unwrap_or("?"),
                message
            )));
        }

        // Remove gRPC frame header (5 bytes: 1 compress flag + 4 length)
        let response_bytes = grpc_unframe(response_buf.freeze())?;

        Ok(response_bytes)
    }

    /// Search using pre-encoded protobuf.
    pub async fn search(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_SEARCH, encoded_request).await
    }

    /// Upsert using pre-encoded protobuf.
    pub async fn upsert(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_UPSERT, encoded_request).await
    }

    /// Delete points using pre-encoded protobuf.
    pub async fn delete(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_DELETE, encoded_request).await
    }

    /// Get points by ID using pre-encoded protobuf.
    pub async fn get(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_GET, encoded_request).await
    }

    /// Scroll through points using pre-encoded protobuf.
    pub async fn scroll(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_SCROLL, encoded_request).await
    }

    /// Recommend similar points using pre-encoded protobuf.
    pub async fn recommend(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_RECOMMEND, encoded_request).await
    }

    /// Create collection using pre-encoded protobuf.
    pub async fn create_collection(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_CREATE_COLLECTION, encoded_request).await
    }

    /// Delete collection using pre-encoded protobuf.
    pub async fn delete_collection(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_DELETE_COLLECTION, encoded_request).await
    }
}

/// Frame a protobuf message for gRPC transport.
///
/// gRPC uses a 5-byte header:
/// - 1 byte: compression flag (0 = uncompressed)
/// - 4 bytes: message length (big-endian)
fn grpc_frame(message: Bytes) -> Bytes {
    let len = message.len();
    let mut frame = BytesMut::with_capacity(5 + len);
    
    // Compression flag: 0 = not compressed
    frame.put_u8(0);
    // Message length (big-endian u32)
    frame.put_u32(len as u32);
    // Message body
    frame.extend_from_slice(&message);
    
    frame.freeze()
}

/// Remove gRPC framing from response.
/// Returns empty Bytes if response has no body (common for write operations).
fn grpc_unframe(mut data: Bytes) -> QdrantResult<Bytes> {
    // Empty response is valid for write operations like upsert
    if data.is_empty() {
        return Ok(Bytes::new());
    }
    
    if data.len() < 5 {
        return Err(QdrantError::Decode("Response too short for gRPC frame".to_string()));
    }
    
    let _compress = data.get_u8();
    let len = data.get_u32() as usize;
    
    // Empty message is valid (e.g., PointsOperationResponse with no errors)
    if len == 0 {
        return Ok(Bytes::new());
    }
    
    if data.len() < len {
        return Err(QdrantError::Decode(format!(
            "Response truncated: expected {} bytes, got {}",
            len,
            data.len()
        )));
    }
    
    Ok(data.slice(0..len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_frame() {
        let message = Bytes::from_static(b"hello");
        let framed = grpc_frame(message);
        
        // Should be 5 header bytes + 5 message bytes
        assert_eq!(framed.len(), 10);
        assert_eq!(framed[0], 0); // no compression
        assert_eq!(&framed[1..5], &[0, 0, 0, 5]); // length = 5
        assert_eq!(&framed[5..], b"hello");
    }

    #[test]
    fn test_grpc_unframe() {
        let mut data = BytesMut::new();
        data.put_u8(0); // compress flag
        data.put_u32(5); // length
        data.extend_from_slice(b"hello");
        
        let result = grpc_unframe(data.freeze()).unwrap();
        assert_eq!(&result[..], b"hello");
    }
}
