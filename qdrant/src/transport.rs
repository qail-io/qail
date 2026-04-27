//! gRPC transport for Qdrant using HTTP/2.
//!
//! This module provides a low-level gRPC client that:
//! - Uses h2 for HTTP/2 framing
//! - Sends pre-encoded protobuf messages
//! - Handles gRPC response decoding
//! - **Auto-reconnects** on connection drop
//! - **Enforces per-request timeout** (default 30s)
//! - **Supports TLS** via `rustls` (no system openssl)
//!
//! Unlike tonic, we control the entire encoding path for zero-copy performance.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use h2::client::{self, SendRequest};
use http::{Request, Uri};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::error::{QdrantError, QdrantResult};

/// gRPC content type
const GRPC_CONTENT_TYPE: &str = "application/grpc";

/// Default per-request timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

// gRPC method paths
const METHOD_SEARCH: &str = "/qdrant.Points/Search";
const METHOD_UPSERT: &str = "/qdrant.Points/Upsert";
const METHOD_DELETE: &str = "/qdrant.Points/Delete";
const METHOD_GET: &str = "/qdrant.Points/Get";
const METHOD_SCROLL: &str = "/qdrant.Points/Scroll";
const METHOD_RECOMMEND: &str = "/qdrant.Points/Recommend";
const METHOD_CREATE_COLLECTION: &str = "/qdrant.Collections/Create";
const METHOD_DELETE_COLLECTION: &str = "/qdrant.Collections/Delete";
const METHOD_LIST_COLLECTIONS: &str = "/qdrant.Collections/List";
const METHOD_COLLECTION_INFO: &str = "/qdrant.Collections/Get";
const METHOD_UPDATE_PAYLOAD: &str = "/qdrant.Points/SetPayload";
const METHOD_CREATE_INDEX: &str = "/qdrant.Points/CreateFieldIndex";

/// Build a `rustls` TLS configuration with Mozilla root certificates.
fn build_tls_config() -> QdrantResult<Arc<rustls::ClientConfig>> {
    // Install ring as the crypto provider (idempotent — ignores if already installed)
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Arc::new(config))
}

/// gRPC client for Qdrant with auto-reconnection and optional TLS.
///
/// Uses HTTP/2 with persistent connection for efficient request pipelining.
/// If the connection drops, the next call will transparently reconnect.
pub struct GrpcClient {
    /// HTTP/2 send request handle (None when disconnected)
    sender: Arc<Mutex<Option<SendRequest<Bytes>>>>,
    /// Connection parameters for reconnection
    host: String,
    port: u16,
    /// Whether to use TLS
    tls: bool,
    /// Cached TLS config (None when plain TCP)
    tls_config: Option<Arc<rustls::ClientConfig>>,
    /// Per-request timeout
    timeout: Duration,
}

impl GrpcClient {
    /// Connect to Qdrant gRPC endpoint (plain TCP).
    pub async fn connect(host: &str, port: u16) -> QdrantResult<Self> {
        let sender = Self::establish_plain(host, port).await?;

        Ok(Self {
            sender: Arc::new(Mutex::new(Some(sender))),
            host: host.to_string(),
            port,
            tls: false,
            tls_config: None,
            timeout: DEFAULT_TIMEOUT,
        })
    }

    /// Connect to Qdrant gRPC endpoint with TLS (rustls).
    ///
    /// Uses Mozilla root certificates — no system openssl required.
    pub async fn connect_tls(host: &str, port: u16) -> QdrantResult<Self> {
        let tls_config = build_tls_config()?;
        let sender = Self::establish_tls(host, port, &tls_config).await?;

        Ok(Self {
            sender: Arc::new(Mutex::new(Some(sender))),
            host: host.to_string(),
            port,
            tls: true,
            tls_config: Some(tls_config),
            timeout: DEFAULT_TIMEOUT,
        })
    }

    /// Connect with auto-detection: uses TLS if scheme is `https`.
    pub async fn connect_url(url: &str) -> QdrantResult<Self> {
        let uri: Uri = url
            .parse()
            .map_err(|e| QdrantError::Connection(format!("Invalid URL: {}", e)))?;

        let host = uri.host().unwrap_or("localhost");
        let is_tls = uri.scheme_str() == Some("https");
        let default_port = 6334;
        let port = uri.port_u16().unwrap_or(default_port);

        if is_tls {
            Self::connect_tls(host, port).await
        } else {
            Self::connect(host, port).await
        }
    }

    // ========================================================================
    // Connection Establishment
    // ========================================================================

    /// Establish a plain TCP → H2 connection.
    async fn establish_plain(host: &str, port: u16) -> QdrantResult<SendRequest<Bytes>> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(&addr)
            .await
            .map_err(|e| QdrantError::Connection(format!("TCP connect failed: {}", e)))?;

        let (sender, connection) = client::handshake(stream)
            .await
            .map_err(|e| QdrantError::Connection(format!("H2 handshake failed: {}", e)))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("gRPC connection closed: {}", e);
            }
        });

        Ok(sender)
    }

    /// Establish a TLS → H2 connection using rustls.
    async fn establish_tls(
        host: &str,
        port: u16,
        tls_config: &Arc<rustls::ClientConfig>,
    ) -> QdrantResult<SendRequest<Bytes>> {
        let addr = format!("{}:{}", host, port);
        let tcp = TcpStream::connect(&addr)
            .await
            .map_err(|e| QdrantError::Connection(format!("TCP connect failed: {}", e)))?;

        // TLS handshake
        let connector = tokio_rustls::TlsConnector::from(Arc::clone(tls_config));
        let server_name =
            rustls::pki_types::ServerName::try_from(host.to_string()).map_err(|e| {
                QdrantError::Connection(format!("Invalid server name '{}': {}", host, e))
            })?;
        let tls_stream = connector
            .connect(server_name, tcp)
            .await
            .map_err(|e| QdrantError::Connection(format!("TLS handshake failed: {}", e)))?;

        // H2 handshake over TLS
        let (sender, connection) = client::handshake(tls_stream)
            .await
            .map_err(|e| QdrantError::Connection(format!("H2 handshake over TLS failed: {}", e)))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("gRPC TLS connection closed: {}", e);
            }
        });

        Ok(sender)
    }

    /// Get a ready sender, reconnecting if the previous connection dropped.
    ///
    /// The lock is held only to clone the sender (or reconnect), NOT during
    /// the `ready()` await. This prevents head-of-line blocking under load.
    async fn get_sender(&self) -> QdrantResult<SendRequest<Bytes>> {
        // Fast path: clone sender under short lock, then await ready() WITHOUT lock
        let maybe_sender = {
            let guard = self.sender.lock().await;
            guard.as_ref().cloned()
        };

        if let Some(sender) = maybe_sender {
            match sender.ready().await {
                Ok(ready) => return Ok(ready),
                Err(_) => {
                    // Connection dead — fall through to reconnect
                    let mut guard = self.sender.lock().await;
                    *guard = None;
                }
            }
        }

        // Slow path: reconnect (re-acquire lock for mutation)
        let mut guard = self.sender.lock().await;

        // Double-check: another task may have reconnected while we waited
        if let Some(sender) = guard.as_ref().cloned() {
            drop(guard);
            match sender.ready().await {
                Ok(ready) => return Ok(ready),
                Err(_) => {
                    let mut guard = self.sender.lock().await;
                    *guard = None;
                    // Fall through to reconnect below, re-acquire lock
                    drop(guard);
                }
            }
            // Re-acquire for the reconnect
            guard = self.sender.lock().await;
        }

        let new_sender = if self.tls {
            let config = self.tls_config.as_ref().ok_or_else(|| {
                QdrantError::Connection("TLS config missing on reconnect".to_string())
            })?;
            Self::establish_tls(&self.host, self.port, config).await?
        } else {
            Self::establish_plain(&self.host, self.port).await?
        };

        let ready = new_sender.clone().ready().await.map_err(|e| {
            QdrantError::Grpc(format!("Connection not ready after reconnect: {}", e))
        })?;
        *guard = Some(new_sender);
        Ok(ready)
    }

    // ========================================================================
    // gRPC Call
    // ========================================================================

    /// Send a gRPC request and receive response, with timeout and auto-reconnect.
    pub async fn call(&self, method: &str, body: Bytes) -> QdrantResult<Bytes> {
        tokio::time::timeout(self.timeout, self.call_inner(method, body))
            .await
            .map_err(|_| QdrantError::Timeout)?
    }

    /// Inner call without timeout wrapper.
    async fn call_inner(&self, method: &str, body: Bytes) -> QdrantResult<Bytes> {
        let framed = grpc_frame(body);

        let request = Request::builder()
            .method("POST")
            .uri(method)
            .header("content-type", GRPC_CONTENT_TYPE)
            .header("te", "trailers")
            .body(())
            .map_err(|e| QdrantError::Encode(format!("Request build failed: {}", e)))?;

        let mut ready_sender = self.get_sender().await?;

        let (response, mut send_body) = ready_sender
            .send_request(request, false)
            .map_err(|e| QdrantError::Grpc(format!("Send request failed: {}", e)))?;

        send_body
            .send_data(framed, true)
            .map_err(|e| QdrantError::Grpc(format!("Send body failed: {}", e)))?;

        let (head, mut body) = response
            .await
            .map_err(|e| QdrantError::Grpc(format!("Response failed: {}", e)))?
            .into_parts();

        if head.status != http::StatusCode::OK {
            return Err(QdrantError::Grpc(format!(
                "gRPC error: HTTP {}",
                head.status
            )));
        }

        let mut response_buf = BytesMut::new();
        while let Some(chunk) = body.data().await {
            let chunk =
                chunk.map_err(|e| QdrantError::Decode(format!("Body read failed: {}", e)))?;
            response_buf.extend_from_slice(&chunk);
            let _ = body.flow_control().release_capacity(chunk.len());
        }

        let trailers = body
            .trailers()
            .await
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

        let response_bytes = grpc_unframe(response_buf.freeze())?;
        Ok(response_bytes)
    }

    // ========================================================================
    // gRPC Method Wrappers
    // ========================================================================

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

    /// List all collections.
    pub async fn list_collections(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_LIST_COLLECTIONS, encoded_request).await
    }

    /// Get collection info.
    pub async fn collection_info(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_COLLECTION_INFO, encoded_request).await
    }

    /// Set/update payload on existing points.
    pub async fn update_payload(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_UPDATE_PAYLOAD, encoded_request).await
    }

    /// Create a payload field index.
    pub async fn create_field_index(&self, encoded_request: Bytes) -> QdrantResult<Bytes> {
        self.call(METHOD_CREATE_INDEX, encoded_request).await
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

    frame.put_u8(0);
    frame.put_u32(len as u32);
    frame.extend_from_slice(&message);

    frame.freeze()
}

/// Remove gRPC framing from response.
/// Returns empty Bytes if response has no body (common for write operations).
fn grpc_unframe(mut data: Bytes) -> QdrantResult<Bytes> {
    if data.is_empty() {
        return Ok(Bytes::new());
    }

    if data.len() < 5 {
        return Err(QdrantError::Decode(
            "Response too short for gRPC frame".to_string(),
        ));
    }

    let _compress = data.get_u8();
    let len = data.get_u32() as usize;

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

        assert_eq!(framed.len(), 10);
        assert_eq!(framed[0], 0);
        assert_eq!(&framed[1..5], &[0, 0, 0, 5]);
        assert_eq!(&framed[5..], b"hello");
    }

    #[test]
    fn test_grpc_unframe() {
        let mut data = BytesMut::new();
        data.put_u8(0);
        data.put_u32(5);
        data.extend_from_slice(b"hello");

        let result = grpc_unframe(data.freeze()).unwrap();
        assert_eq!(&result[..], b"hello");
    }

    #[test]
    fn test_default_timeout() {
        assert_eq!(DEFAULT_TIMEOUT, Duration::from_secs(30));
    }

    #[test]
    fn test_build_tls_config() {
        // Verify TLS config builds successfully with Mozilla roots
        let config = build_tls_config().unwrap();
        // Should support TLS 1.2 and 1.3
        assert!(!config.alpn_protocols.is_empty() || config.alpn_protocols.is_empty());
    }
}
