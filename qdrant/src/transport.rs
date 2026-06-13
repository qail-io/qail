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
/// Maximum buffered unary gRPC response frame size.
const MAX_GRPC_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

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

    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec()];

    Ok(Arc::new(config))
}

struct ConnectionState {
    sender: Option<SendRequest<Bytes>>,
    generation: u64,
}

/// gRPC client for Qdrant with auto-reconnection and optional TLS.
///
/// Uses HTTP/2 with persistent connection for efficient request pipelining.
/// If the connection drops, the next call will transparently reconnect.
pub struct GrpcClient {
    /// HTTP/2 send request handle and connection generation
    state: Arc<Mutex<ConnectionState>>,
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
            state: Arc::new(Mutex::new(ConnectionState {
                sender: Some(sender),
                generation: 0,
            })),
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
            state: Arc::new(Mutex::new(ConnectionState {
                sender: Some(sender),
                generation: 0,
            })),
            host: host.to_string(),
            port,
            tls: true,
            tls_config: Some(tls_config),
            timeout: DEFAULT_TIMEOUT,
        })
    }

    /// Connect with auto-detection: uses TLS if scheme is `https`.
    pub async fn connect_url(url: &str) -> QdrantResult<Self> {
        let endpoint = parse_connect_url(url)?;

        if endpoint.tls {
            Self::connect_tls(&endpoint.host, endpoint.port).await
        } else {
            Self::connect(&endpoint.host, endpoint.port).await
        }
    }

    // ========================================================================
    // Connection Establishment
    // ========================================================================

    /// Establish a plain TCP → H2 connection.
    async fn establish_plain(host: &str, port: u16) -> QdrantResult<SendRequest<Bytes>> {
        let addr = socket_addr(host, port);
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
        let addr = socket_addr(host, port);
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
    /// The fast path holds the lock only to clone the sender, then awaits
    /// `ready()` without the lock. Reconnects are serialized to avoid storms.
    async fn get_sender(&self) -> QdrantResult<SendRequest<Bytes>> {
        // Fast path: clone sender under short lock, then await ready() WITHOUT lock
        let (maybe_sender, initial_gen) = {
            let guard = self.state.lock().await;
            (guard.sender.clone(), guard.generation)
        };

        #[allow(clippy::collapsible_if)]
        if let Some(sender) = maybe_sender {
            if let Ok(ready) = sender.ready().await {
                return Ok(ready);
            }
        }

        // Slow path: reconnect (re-acquire lock for mutation)
        let mut guard = self.state.lock().await;

        let mut current_gen = initial_gen;
        // Double-check: another thread might have reconnected while we were waiting/testing
        while guard.generation != current_gen {
            if let Some(sender) = guard.sender.clone() {
                let new_gen = guard.generation;
                drop(guard);
                match sender.ready().await {
                    Ok(ready) => return Ok(ready),
                    Err(_) => {
                        // The new one is also dead! Re-acquire lock.
                        guard = self.state.lock().await;
                        current_gen = new_gen;
                    }
                }
            } else {
                break;
            }
        }

        // If the generation is still current_gen, it means the dead connection we checked
        // is still the one stored in `guard.sender`. We should clear it before establishing
        // a new one.
        if guard.generation == current_gen {
            guard.sender = None;
        }

        if guard.sender.is_none() {
            let new_sender = if self.tls {
                let config = self.tls_config.as_ref().ok_or_else(|| {
                    QdrantError::Connection("TLS config missing on reconnect".to_string())
                })?;
                Self::establish_tls(&self.host, self.port, config).await?
            } else {
                Self::establish_plain(&self.host, self.port).await?
            };

            guard.sender = Some(new_sender);
            guard.generation = guard.generation.wrapping_add(1);
        }

        let sender = match guard.sender.clone() {
            Some(sender) => sender,
            None => {
                return Err(QdrantError::Connection(
                    "missing gRPC sender after reconnect".to_string(),
                ));
            }
        };
        drop(guard);

        let ready = sender.ready().await.map_err(|e| {
            QdrantError::Grpc(format!("Connection not ready after reconnect: {}", e))
        })?;

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
        let framed = grpc_frame(body)?;

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
        reject_nonzero_grpc_status(&head.headers)?;

        let mut response_buf = BytesMut::new();
        while let Some(chunk) = body.data().await {
            let chunk =
                chunk.map_err(|e| QdrantError::Decode(format!("Body read failed: {}", e)))?;
            let chunk_len = chunk.len();
            let next_len = response_buf
                .len()
                .checked_add(chunk_len)
                .ok_or_else(|| QdrantError::Decode("gRPC response size overflow".to_string()))?;
            if next_len > MAX_GRPC_RESPONSE_BYTES {
                return Err(QdrantError::Decode(format!(
                    "gRPC response too large: {} bytes (max {})",
                    next_len, MAX_GRPC_RESPONSE_BYTES
                )));
            }
            response_buf.extend_from_slice(&chunk);

            if response_buf.len() >= 5 {
                let declared_len = u32::from_be_bytes([
                    response_buf[1],
                    response_buf[2],
                    response_buf[3],
                    response_buf[4],
                ]) as usize;
                if declared_len > MAX_GRPC_RESPONSE_BYTES.saturating_sub(5) {
                    return Err(QdrantError::Decode(format!(
                        "gRPC response frame too large: {} bytes (max {})",
                        declared_len,
                        MAX_GRPC_RESPONSE_BYTES.saturating_sub(5)
                    )));
                }
            }

            let _ = body.flow_control().release_capacity(chunk_len);
        }

        let trailers = body
            .trailers()
            .await
            .map_err(|e| QdrantError::Grpc(format!("Trailers failed: {}", e)))?;

        if let Some(trailers) = trailers
            && let Err(err) = reject_nonzero_grpc_status(&trailers)
        {
            return Err(err);
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

#[derive(Debug, PartialEq, Eq)]
struct ConnectEndpoint {
    host: String,
    port: u16,
    tls: bool,
}

fn parse_connect_url(url: &str) -> QdrantResult<ConnectEndpoint> {
    if url.contains('#') {
        return Err(QdrantError::Connection(
            "Qdrant URL must not include a path, query, or fragment".to_string(),
        ));
    }
    let uri: Uri = url
        .parse()
        .map_err(|e| QdrantError::Connection(format!("Invalid URL: {}", e)))?;

    let scheme = uri
        .scheme_str()
        .ok_or_else(|| QdrantError::Connection("URL scheme is required".to_string()))?;
    let tls = match scheme {
        "http" => false,
        "https" => true,
        other => {
            return Err(QdrantError::Connection(format!(
                "Unsupported Qdrant URL scheme: {}",
                other
            )));
        }
    };
    let host = uri
        .host()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| QdrantError::Connection("URL host is required".to_string()))?
        .to_string();
    if uri.path() != "/" || uri.query().is_some() {
        return Err(QdrantError::Connection(
            "Qdrant URL must not include a path, query, or fragment".to_string(),
        ));
    }
    let port = uri.port_u16().unwrap_or(6334);

    Ok(ConnectEndpoint { host, port, tls })
}

fn socket_addr(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{}]:{}", host, port)
    } else {
        format!("{}:{}", host, port)
    }
}

/// Frame a protobuf message for gRPC transport.
///
/// gRPC uses a 5-byte header:
/// - 1 byte: compression flag (0 = uncompressed)
/// - 4 bytes: message length (big-endian)
fn grpc_frame(message: Bytes) -> QdrantResult<Bytes> {
    let len = grpc_frame_len(message.len())?;
    let capacity = message
        .len()
        .checked_add(5)
        .ok_or_else(|| QdrantError::Encode("gRPC request frame size overflow".to_string()))?;
    let mut frame = BytesMut::with_capacity(capacity);

    frame.put_u8(0);
    frame.put_u32(len);
    frame.extend_from_slice(&message);

    Ok(frame.freeze())
}

fn grpc_frame_len(len: usize) -> QdrantResult<u32> {
    u32::try_from(len).map_err(|_| {
        QdrantError::Encode(format!(
            "gRPC request frame too large: {} bytes (max {})",
            len,
            u32::MAX
        ))
    })
}

fn reject_nonzero_grpc_status(headers: &http::HeaderMap) -> QdrantResult<()> {
    let Some(status) = headers.get("grpc-status") else {
        return Ok(());
    };
    if status == "0" {
        return Ok(());
    }

    let message = headers
        .get("grpc-message")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("Unknown error");
    Err(QdrantError::Grpc(format!(
        "gRPC status {}: {}",
        status.to_str().unwrap_or("?"),
        message
    )))
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

    let compress = data.get_u8();
    if compress != 0 {
        return Err(QdrantError::Decode(format!(
            "Unsupported compressed gRPC response frame: {}",
            compress
        )));
    }
    let len = data.get_u32() as usize;

    if len > MAX_GRPC_RESPONSE_BYTES.saturating_sub(5) {
        return Err(QdrantError::Decode(format!(
            "gRPC response frame too large: {} bytes (max {})",
            len,
            MAX_GRPC_RESPONSE_BYTES.saturating_sub(5)
        )));
    }

    if data.len() < len {
        return Err(QdrantError::Decode(format!(
            "Response truncated: expected {} bytes, got {}",
            len,
            data.len()
        )));
    }
    if data.len() != len {
        return Err(QdrantError::Decode(format!(
            "Trailing bytes after gRPC response frame: expected {} bytes, got {}",
            len,
            data.len()
        )));
    }
    if len == 0 {
        return Ok(Bytes::new());
    }

    Ok(data.slice(0..len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_frame() {
        let message = Bytes::from_static(b"hello");
        let framed = grpc_frame(message).unwrap();

        assert_eq!(framed.len(), 10);
        assert_eq!(framed[0], 0);
        assert_eq!(&framed[1..5], &[0, 0, 0, 5]);
        assert_eq!(&framed[5..], b"hello");
    }

    #[test]
    fn test_grpc_frame_rejects_oversized_message_len() {
        let len = u32::MAX as usize + 1;
        let err = grpc_frame_len(len).unwrap_err();
        assert!(matches!(err, QdrantError::Encode(msg) if msg.contains("too large")));
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
    fn test_grpc_unframe_rejects_compressed_frame() {
        let mut data = BytesMut::new();
        data.put_u8(1);
        data.put_u32(5);
        data.extend_from_slice(b"hello");

        let err = grpc_unframe(data.freeze()).unwrap_err();
        assert!(matches!(err, QdrantError::Decode(msg) if msg.contains("compressed")));
    }

    #[test]
    fn test_grpc_unframe_rejects_oversized_declared_frame() {
        let mut data = BytesMut::new();
        data.put_u8(0);
        data.put_u32((MAX_GRPC_RESPONSE_BYTES - 4) as u32);

        let err = grpc_unframe(data.freeze()).unwrap_err();
        assert!(matches!(err, QdrantError::Decode(msg) if msg.contains("too large")));
    }

    #[test]
    fn test_grpc_unframe_rejects_trailing_bytes() {
        let mut data = BytesMut::new();
        data.put_u8(0);
        data.put_u32(5);
        data.extend_from_slice(b"hello");
        data.extend_from_slice(b"extra");

        let err = grpc_unframe(data.freeze()).unwrap_err();
        assert!(matches!(err, QdrantError::Decode(msg) if msg.contains("Trailing bytes")));
    }

    #[test]
    fn test_grpc_unframe_rejects_zero_length_frame_with_trailing_bytes() {
        let mut data = BytesMut::new();
        data.put_u8(0);
        data.put_u32(0);
        data.put_u8(0);

        let err = grpc_unframe(data.freeze()).unwrap_err();
        assert!(matches!(err, QdrantError::Decode(msg) if msg.contains("Trailing bytes")));
    }

    #[test]
    fn test_grpc_status_headers_accept_zero_and_missing() {
        let missing = http::HeaderMap::new();
        reject_nonzero_grpc_status(&missing).unwrap();

        let mut ok = http::HeaderMap::new();
        ok.insert("grpc-status", http::HeaderValue::from_static("0"));
        reject_nonzero_grpc_status(&ok).unwrap();
    }

    #[test]
    fn test_grpc_status_headers_reject_nonzero() {
        let mut headers = http::HeaderMap::new();
        headers.insert("grpc-status", http::HeaderValue::from_static("7"));
        headers.insert(
            "grpc-message",
            http::HeaderValue::from_static("permission denied"),
        );

        let err = reject_nonzero_grpc_status(&headers).unwrap_err();
        assert!(
            matches!(err, QdrantError::Grpc(msg) if msg.contains("gRPC status 7") && msg.contains("permission denied"))
        );
    }

    #[test]
    fn test_default_timeout() {
        assert_eq!(DEFAULT_TIMEOUT, Duration::from_secs(30));
    }

    #[test]
    fn test_parse_connect_url_requires_http_scheme_and_host() {
        assert_eq!(
            parse_connect_url("http://localhost:6334").unwrap(),
            ConnectEndpoint {
                host: "localhost".to_string(),
                port: 6334,
                tls: false,
            }
        );
        assert_eq!(
            parse_connect_url("https://cloud.qdrant.io").unwrap(),
            ConnectEndpoint {
                host: "cloud.qdrant.io".to_string(),
                port: 6334,
                tls: true,
            }
        );

        assert!(parse_connect_url("ftp://localhost:6334").is_err());
        assert!(parse_connect_url("localhost:6334").is_err());
        assert!(parse_connect_url("https:///collections").is_err());
        assert!(parse_connect_url("https://cloud.qdrant.io/collections").is_err());
        assert!(parse_connect_url("https://cloud.qdrant.io?api-key=x").is_err());
        assert!(parse_connect_url("https://cloud.qdrant.io#frag").is_err());
    }

    #[test]
    fn test_socket_addr_brackets_ipv6_hosts() {
        assert_eq!(socket_addr("127.0.0.1", 6334), "127.0.0.1:6334");
        assert_eq!(socket_addr("::1", 6334), "[::1]:6334");
        assert_eq!(socket_addr("[::1]", 6334), "[::1]:6334");
    }

    #[test]
    fn test_build_tls_config() {
        // Verify TLS config builds successfully with Mozilla roots
        let config = build_tls_config().unwrap();
        assert!(
            config
                .alpn_protocols
                .iter()
                .any(|protocol| protocol == b"h2"),
            "gRPC over TLS must advertise h2 via ALPN"
        );
    }

    #[tokio::test]
    async fn test_concurrent_reconnection_under_storm() {
        // Start a mock H2 server
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let port = addr.port();

        let connection_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let conn_count = Arc::clone(&connection_count);

        let _server_handle = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                conn_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(async move {
                    if let Ok(mut handshake) = h2::server::handshake(stream).await {
                        while let Some(Ok((_req, mut respond))) = handshake.accept().await {
                            let response = http::Response::builder().status(200).body(()).unwrap();
                            let _: Result<h2::SendStream<bytes::Bytes>, _> =
                                respond.send_response(response, true);
                        }
                    }
                });
            }
        });

        // 1. Establish initial connection
        let client = GrpcClient::connect("127.0.0.1", port).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(
            connection_count.load(std::sync::atomic::Ordering::SeqCst),
            1
        );

        // 2. Corrupt the sender to trigger a reconnection slow path in all tasks.
        // We'll set the sender to None, simulating a disconnected state.
        {
            let mut guard = client.state.lock().await;
            guard.sender = None;
        }

        // 3. Fire 30 concurrent tasks calling get_sender()
        let client = Arc::new(client);
        let mut handles = Vec::new();
        for _ in 0..30 {
            let client_clone = Arc::clone(&client);
            handles.push(tokio::spawn(async move {
                let sender = client_clone.get_sender().await;
                assert!(sender.is_ok());
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // 4. Verify that exactly ONE reconnection occurred!
        // (Initial connection = 1, reconnection = 1, total = 2)
        let total_connections = connection_count.load(std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            total_connections, 2,
            "Expected exactly 2 connections (1 initial + 1 reconnect), got {}",
            total_connections
        );
    }
}
