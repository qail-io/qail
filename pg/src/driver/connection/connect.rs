//! Connection establishment — connect_*, TLS, mTLS, Unix socket.

#[cfg(all(target_os = "linux", feature = "io_uring"))]
use super::helpers::should_try_uring_plain;
use super::helpers::{
    connect_backend_for_stream, connect_error_kind, plain_connect_attempt_backend,
    record_connect_attempt, record_connect_result,
};
use super::types::{
    BUFFER_CAPACITY, CONNECT_BACKEND_TOKIO, CONNECT_TRANSPORT_GSSENC, CONNECT_TRANSPORT_MTLS,
    CONNECT_TRANSPORT_PLAIN, CONNECT_TRANSPORT_TLS, ConnectParams, DEFAULT_CONNECT_TIMEOUT,
    GSSENC_REQUEST, GssEncNegotiationResult, PgConnection, SSL_REQUEST, STMT_CACHE_CAPACITY,
    StatementCache, TlsConfig, has_logical_replication_startup_mode,
};
use crate::driver::stream::PgStream;
use crate::driver::{AuthSettings, ConnectOptions, GssEncMode, PgError, PgResult, TlsMode};
use crate::protocol::PROTOCOL_VERSION_3_0;
use crate::protocol::wire::FrontendMessage;
use bytes::BytesMut;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

#[inline]
fn protocol_version_from_minor(minor: u16) -> i32 {
    ((3i32) << 16) | i32::from(minor)
}

fn is_explicit_protocol_version_rejection(err: &PgError) -> bool {
    let msg = match err {
        PgError::Connection(msg) | PgError::Protocol(msg) | PgError::Auth(msg) => msg,
        PgError::Query(msg) => msg,
        PgError::QueryServer(server) => &server.message,
        _ => return false,
    };

    let lower = msg.to_ascii_lowercase();
    lower.contains("unsupported frontend protocol")
        || lower.contains("frontend protocol") && lower.contains("unsupported")
        || lower.contains("protocol version") && lower.contains("not support")
}

impl PgConnection {
    /// Connect to PostgreSQL server without authentication (trust mode).
    ///
    /// # Arguments
    ///
    /// * `host` — PostgreSQL server hostname or IP.
    /// * `port` — TCP port (typically 5432).
    /// * `user` — PostgreSQL role name.
    /// * `database` — Target database name.
    pub async fn connect(host: &str, port: u16, user: &str, database: &str) -> PgResult<Self> {
        Self::connect_with_password(host, port, user, database, None).await
    }

    /// Connect to PostgreSQL server with optional password authentication.
    /// Includes a default 10-second timeout covering TCP connect + handshake.
    ///
    /// Startup requests protocol 3.2 by default and performs a one-shot retry
    /// with protocol 3.0 only when startup fails due to explicit
    /// protocol-version rejection from the server.
    pub async fn connect_with_password(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> PgResult<Self> {
        Self::connect_with_password_and_auth(
            host,
            port,
            user,
            database,
            password,
            AuthSettings::default(),
        )
        .await
    }

    /// Connect to PostgreSQL with explicit enterprise options.
    ///
    /// Negotiation preface order follows libpq:
    ///   1. If gss_enc_mode != Disable → try GSSENCRequest on fresh TCP
    ///   2. If GSSENC rejected/unavailable and tls_mode != Disable → try SSLRequest
    ///   3. If both rejected/unavailable → plain StartupMessage
    ///
    /// The StartupMessage protocol version behavior is the same as
    /// `connect_with_password`: request protocol 3.2 first, then retry once
    /// with 3.0 only on explicit protocol-version rejection.
    pub async fn connect_with_options(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        options: ConnectOptions,
    ) -> PgResult<Self> {
        let ConnectOptions {
            tls_mode,
            gss_enc_mode,
            tls_ca_cert_pem,
            mtls,
            gss_token_provider,
            gss_token_provider_ex,
            auth,
            startup_params,
        } = options;

        if mtls.is_some() && matches!(tls_mode, TlsMode::Disable) {
            return Err(PgError::Connection(
                "Invalid connect options: mTLS requires tls_mode=Prefer or Require".to_string(),
            ));
        }

        // Enforce gss_enc_mode policy before mTLS early-return.
        // GSSENC and mTLS are both transport-level encryption; using
        // both simultaneously is not supported by the PostgreSQL protocol.
        if gss_enc_mode == GssEncMode::Require && mtls.is_some() {
            return Err(PgError::Connection(
                "gssencmode=require is incompatible with mTLS — both provide \
                 transport encryption; use one or the other"
                    .to_string(),
            ));
        }

        if let Some(mtls_config) = mtls {
            // gss_enc_mode is Disable or Prefer here (Require rejected above).
            // mTLS already provides transport encryption; skip GSSENC.
            return Self::connect_mtls_with_password_and_auth_and_gss(
                ConnectParams {
                    host,
                    port,
                    user,
                    database,
                    password,
                    auth_settings: auth,
                    gss_token_provider,
                    gss_token_provider_ex,
                    protocol_minor: Self::default_protocol_minor(),
                    startup_params: startup_params.clone(),
                },
                mtls_config,
            )
            .await;
        }

        // ── Phase 1: Try GSSENC if requested ──────────────────────────
        if gss_enc_mode != GssEncMode::Disable {
            match Self::try_gssenc_request(host, port).await {
                Ok(GssEncNegotiationResult::Accepted(tcp_stream)) => {
                    let connect_started = Instant::now();
                    record_connect_attempt(CONNECT_TRANSPORT_GSSENC, CONNECT_BACKEND_TOKIO);
                    #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
                    {
                        let default_minor = Self::default_protocol_minor();
                        let mut result = Self::connect_gssenc_accepted_with_timeout(
                            tcp_stream,
                            host,
                            user,
                            database,
                            password,
                            auth,
                            gss_token_provider,
                            gss_token_provider_ex.clone(),
                            startup_params.clone(),
                            default_minor,
                        )
                        .await;
                        if let Err(err) = &result {
                            if default_minor > 0 && is_explicit_protocol_version_rejection(err) {
                                let downgrade_minor = (PROTOCOL_VERSION_3_0 & 0xFFFF) as u16;
                                let retry_stream = match Self::try_gssenc_request(host, port).await
                                {
                                    Ok(GssEncNegotiationResult::Accepted(stream)) => stream,
                                    Ok(GssEncNegotiationResult::Rejected) => {
                                        return Err(PgError::Connection(
                                            "Protocol downgrade retry failed: server rejected GSSENCRequest"
                                                .to_string(),
                                        ));
                                    }
                                    Ok(GssEncNegotiationResult::ServerError) => {
                                        return Err(PgError::Connection(
                                            "Protocol downgrade retry failed: server returned error to GSSENCRequest"
                                                .to_string(),
                                        ));
                                    }
                                    Err(e) => {
                                        return Err(e);
                                    }
                                };
                                result = Self::connect_gssenc_accepted_with_timeout(
                                    retry_stream,
                                    host,
                                    user,
                                    database,
                                    password,
                                    auth,
                                    gss_token_provider,
                                    gss_token_provider_ex,
                                    startup_params.clone(),
                                    downgrade_minor,
                                )
                                .await;
                            }
                        }
                        record_connect_result(
                            CONNECT_TRANSPORT_GSSENC,
                            CONNECT_BACKEND_TOKIO,
                            &result,
                            connect_started.elapsed(),
                        );
                        return result;
                    }
                    #[cfg(not(all(feature = "enterprise-gssapi", target_os = "linux")))]
                    {
                        let _ = tcp_stream;
                        let err = PgError::Connection(
                            "Server accepted GSSENCRequest but GSSAPI encryption requires \
                             feature enterprise-gssapi on Linux"
                                .to_string(),
                        );
                        metrics::histogram!(
                            "qail_pg_connect_duration_seconds",
                            "transport" => CONNECT_TRANSPORT_GSSENC,
                            "backend" => CONNECT_BACKEND_TOKIO,
                            "outcome" => "error"
                        )
                        .record(connect_started.elapsed().as_secs_f64());
                        metrics::counter!(
                            "qail_pg_connect_failure_total",
                            "transport" => CONNECT_TRANSPORT_GSSENC,
                            "backend" => CONNECT_BACKEND_TOKIO,
                            "error_kind" => connect_error_kind(&err)
                        )
                        .increment(1);
                        return Err(err);
                    }
                }
                Ok(GssEncNegotiationResult::Rejected)
                | Ok(GssEncNegotiationResult::ServerError) => {
                    if gss_enc_mode == GssEncMode::Require {
                        return Err(PgError::Connection(
                            "gssencmode=require but server rejected GSSENCRequest".to_string(),
                        ));
                    }
                    // gss_enc_mode == Prefer — fall through to TLS / plain
                }
                Err(e) => {
                    if gss_enc_mode == GssEncMode::Require {
                        return Err(e);
                    }
                    // gss_enc_mode == Prefer — connection error, fall through
                    tracing::debug!(
                        host = %host,
                        port = %port,
                        error = %e,
                        "gssenc_prefer_fallthrough"
                    );
                }
            }
        }

        // ── Phase 2: TLS / plain per sslmode ──────────────────────────
        match tls_mode {
            TlsMode::Disable => {
                Self::connect_with_password_and_auth_and_gss(ConnectParams {
                    host,
                    port,
                    user,
                    database,
                    password,
                    auth_settings: auth,
                    gss_token_provider,
                    gss_token_provider_ex,
                    protocol_minor: Self::default_protocol_minor(),
                    startup_params: startup_params.clone(),
                })
                .await
            }
            TlsMode::Require => {
                Self::connect_tls_with_auth_and_gss(
                    ConnectParams {
                        host,
                        port,
                        user,
                        database,
                        password,
                        auth_settings: auth,
                        gss_token_provider,
                        gss_token_provider_ex,
                        protocol_minor: Self::default_protocol_minor(),
                        startup_params: startup_params.clone(),
                    },
                    tls_ca_cert_pem.as_deref(),
                )
                .await
            }
            TlsMode::Prefer => {
                match Self::connect_tls_with_auth_and_gss(
                    ConnectParams {
                        host,
                        port,
                        user,
                        database,
                        password,
                        auth_settings: auth,
                        gss_token_provider,
                        gss_token_provider_ex: gss_token_provider_ex.clone(),
                        protocol_minor: Self::default_protocol_minor(),
                        startup_params: startup_params.clone(),
                    },
                    tls_ca_cert_pem.as_deref(),
                )
                .await
                {
                    Ok(conn) => Ok(conn),
                    Err(PgError::Connection(msg))
                        if msg.contains("Server does not support TLS") =>
                    {
                        Self::connect_with_password_and_auth_and_gss(ConnectParams {
                            host,
                            port,
                            user,
                            database,
                            password,
                            auth_settings: auth,
                            gss_token_provider,
                            gss_token_provider_ex,
                            protocol_minor: Self::default_protocol_minor(),
                            startup_params: startup_params.clone(),
                        })
                        .await
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Attempt GSSAPI session encryption negotiation.
    ///
    /// Opens a fresh TCP connection, sends GSSENCRequest (80877104),
    /// reads exactly one byte (CVE-2021-23222 safe), and returns
    /// the result.  The entire operation is bounded by
    /// `DEFAULT_CONNECT_TIMEOUT`.
    async fn try_gssenc_request(host: &str, port: u16) -> PgResult<GssEncNegotiationResult> {
        tokio::time::timeout(
            DEFAULT_CONNECT_TIMEOUT,
            Self::try_gssenc_request_inner(host, port),
        )
        .await
        .map_err(|_| {
            PgError::Connection(format!(
                "GSSENCRequest timeout after {:?}",
                DEFAULT_CONNECT_TIMEOUT
            ))
        })?
    }

    /// Inner GSSENCRequest logic without timeout wrapper.
    async fn try_gssenc_request_inner(host: &str, port: u16) -> PgResult<GssEncNegotiationResult> {
        use tokio::io::AsyncReadExt;

        let addr = format!("{}:{}", host, port);
        let mut tcp_stream = TcpStream::connect(&addr).await?;
        tcp_stream.set_nodelay(true)?;

        // Send the 8-byte GSSENCRequest.
        tcp_stream.write_all(&GSSENC_REQUEST).await?;
        tcp_stream.flush().await?;

        // CVE-2021-23222: Read exactly one byte.  The server must
        // respond with a single 'G' or 'N'.  Any additional bytes
        // in the buffer indicate a buffer-stuffing attack.
        let mut response = [0u8; 1];
        tcp_stream.read_exact(&mut response).await?;

        match response[0] {
            b'G' => {
                // CVE-2021-23222 check: verify no extra bytes are buffered.
                // Use a non-blocking peek to detect leftover data.
                let mut peek_buf = [0u8; 1];
                match tcp_stream.try_read(&mut peek_buf) {
                    Ok(0) => {} // EOF — fine (shouldn't happen yet but harmless)
                    Ok(_n) => {
                        // Extra bytes after 'G' — possible buffer-stuffing.
                        return Err(PgError::Connection(
                            "Protocol violation: extra bytes after GSSENCRequest 'G' response \
                             (possible CVE-2021-23222 buffer-stuffing attack)"
                                .to_string(),
                        ));
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No extra data — this is the expected path.
                    }
                    Err(e) => {
                        return Err(PgError::Io(e));
                    }
                }
                Ok(GssEncNegotiationResult::Accepted(tcp_stream))
            }
            b'N' => Ok(GssEncNegotiationResult::Rejected),
            b'E' => {
                // Server sent an ErrorMessage.  Per CVE-2024-10977 we
                // must NOT display this to users since the server has
                // not been authenticated.  Log at trace only.
                tracing::trace!(
                    host = %host,
                    port = %port,
                    "gssenc_request_server_error (suppressed per CVE-2024-10977)"
                );
                Ok(GssEncNegotiationResult::ServerError)
            }
            other => Err(PgError::Connection(format!(
                "Unexpected response to GSSENCRequest: 0x{:02X} \
                     (expected 'G'=0x47 or 'N'=0x4E)",
                other
            ))),
        }
    }

    #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
    async fn connect_gssenc_accepted_with_timeout(
        tcp_stream: TcpStream,
        host: &str,
        user: &str,
        database: &str,
        password: Option<&str>,
        auth_settings: AuthSettings,
        gss_token_provider: Option<super::super::GssTokenProvider>,
        gss_token_provider_ex: Option<super::super::GssTokenProviderEx>,
        startup_params: Vec<(String, String)>,
        protocol_minor: u16,
    ) -> PgResult<Self> {
        let gssenc_fut = async {
            let gss_stream = super::super::gss::gssenc_handshake(tcp_stream, host)
                .await
                .map_err(PgError::Auth)?;
            let mut conn = Self {
                stream: PgStream::GssEnc(gss_stream),
                buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
                write_buf: BytesMut::with_capacity(BUFFER_CAPACITY),
                sql_buf: BytesMut::with_capacity(512),
                params_buf: Vec::with_capacity(16),
                prepared_statements: HashMap::new(),
                stmt_cache: StatementCache::new(STMT_CACHE_CAPACITY),
                column_info_cache: HashMap::new(),
                process_id: 0,
                secret_key: 0,
                cancel_key_bytes: Vec::new(),
                requested_protocol_minor: protocol_minor,
                negotiated_protocol_minor: protocol_minor,
                notifications: VecDeque::new(),
                replication_stream_active: false,
                replication_mode_enabled: has_logical_replication_startup_mode(&startup_params),
                last_replication_wal_end: None,
                io_desynced: false,
                pending_statement_closes: Vec::new(),
                draining_statement_closes: false,
            };
            conn.send(FrontendMessage::Startup {
                user: user.to_string(),
                database: database.to_string(),
                protocol_version: protocol_version_from_minor(protocol_minor),
                startup_params: startup_params.clone(),
            })
            .await?;
            conn.handle_startup(
                user,
                password,
                auth_settings,
                gss_token_provider,
                gss_token_provider_ex,
            )
            .await?;
            Ok(conn)
        };
        tokio::time::timeout(DEFAULT_CONNECT_TIMEOUT, gssenc_fut)
            .await
            .map_err(|_| {
                PgError::Connection(format!(
                    "GSSENC connection timeout after {:?} (handshake + auth)",
                    DEFAULT_CONNECT_TIMEOUT
                ))
            })?
    }

    /// Connect to PostgreSQL server with optional password authentication and auth policy.
    pub async fn connect_with_password_and_auth(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        auth_settings: AuthSettings,
    ) -> PgResult<Self> {
        Self::connect_with_password_and_auth_and_gss(ConnectParams {
            host,
            port,
            user,
            database,
            password,
            auth_settings,
            gss_token_provider: None,
            gss_token_provider_ex: None,
            protocol_minor: Self::default_protocol_minor(),
            startup_params: Vec::new(),
        })
        .await
    }

    async fn connect_with_password_and_auth_and_gss(params: ConnectParams<'_>) -> PgResult<Self> {
        let first = Self::connect_with_password_and_auth_and_gss_once(params.clone()).await;
        if let Err(err) = &first
            && params.protocol_minor > 0
            && is_explicit_protocol_version_rejection(err)
        {
            let mut downgraded = params;
            downgraded.protocol_minor = (PROTOCOL_VERSION_3_0 & 0xFFFF) as u16;
            return Self::connect_with_password_and_auth_and_gss_once(downgraded).await;
        }
        first
    }

    async fn connect_with_password_and_auth_and_gss_once(
        params: ConnectParams<'_>,
    ) -> PgResult<Self> {
        let connect_started = Instant::now();
        let attempt_backend = plain_connect_attempt_backend();
        record_connect_attempt(CONNECT_TRANSPORT_PLAIN, attempt_backend);
        let result = tokio::time::timeout(
            DEFAULT_CONNECT_TIMEOUT,
            Self::connect_with_password_inner(params),
        )
        .await
        .map_err(|_| {
            PgError::Connection(format!(
                "Connection timeout after {:?} (TCP connect + handshake)",
                DEFAULT_CONNECT_TIMEOUT
            ))
        })?;
        let backend = result
            .as_ref()
            .map(|conn| connect_backend_for_stream(&conn.stream))
            .unwrap_or(attempt_backend);
        record_connect_result(
            CONNECT_TRANSPORT_PLAIN,
            backend,
            &result,
            connect_started.elapsed(),
        );
        result
    }

    /// Inner connection logic without timeout wrapper.
    async fn connect_with_password_inner(params: ConnectParams<'_>) -> PgResult<Self> {
        let ConnectParams {
            host,
            port,
            user,
            database,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
            protocol_minor,
            startup_params,
        } = params;
        let replication_mode_enabled = has_logical_replication_startup_mode(&startup_params);
        let addr = format!("{}:{}", host, port);
        let stream = Self::connect_plain_stream(&addr).await?;

        let mut conn = Self {
            stream,
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            write_buf: BytesMut::with_capacity(BUFFER_CAPACITY), // 64KB write buffer
            sql_buf: BytesMut::with_capacity(512),
            params_buf: Vec::with_capacity(16), // SQL encoding buffer
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(STMT_CACHE_CAPACITY),
            column_info_cache: HashMap::new(),
            process_id: 0,
            secret_key: 0,
            cancel_key_bytes: Vec::new(),
            requested_protocol_minor: protocol_minor,
            negotiated_protocol_minor: protocol_minor,
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        };

        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
            protocol_version: protocol_version_from_minor(protocol_minor),
            startup_params,
        })
        .await?;

        conn.handle_startup(
            user,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
        )
        .await?;

        Ok(conn)
    }

    async fn connect_plain_stream(addr: &str) -> PgResult<PgStream> {
        let tcp_stream = TcpStream::connect(addr).await?;
        tcp_stream.set_nodelay(true)?;

        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            if should_try_uring_plain() {
                let std_stream = tcp_stream.into_std()?;
                let fallback_std = std_stream.try_clone()?;
                match super::super::uring::UringTcpStream::from_std(std_stream) {
                    Ok(uring_stream) => {
                        tracing::info!(
                            addr = %addr,
                            "qail-pg: using io_uring plain TCP transport"
                        );
                        return Ok(PgStream::Uring(uring_stream));
                    }
                    Err(e) => {
                        tracing::warn!(
                            addr = %addr,
                            error = %e,
                            "qail-pg: io_uring stream conversion failed; falling back to tokio TCP"
                        );
                        fallback_std.set_nonblocking(true)?;
                        let fallback = TcpStream::from_std(fallback_std)?;
                        return Ok(PgStream::Tcp(fallback));
                    }
                }
            }
        }

        Ok(PgStream::Tcp(tcp_stream))
    }

    /// Connect to PostgreSQL server with TLS encryption.
    /// Includes a default 10-second timeout covering TCP connect + TLS + handshake.
    pub async fn connect_tls(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> PgResult<Self> {
        Self::connect_tls_with_auth(
            host,
            port,
            user,
            database,
            password,
            AuthSettings::default(),
            None,
        )
        .await
    }

    /// Connect to PostgreSQL over TLS with explicit auth policy and optional custom CA bundle.
    pub async fn connect_tls_with_auth(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        auth_settings: AuthSettings,
        ca_cert_pem: Option<&[u8]>,
    ) -> PgResult<Self> {
        Self::connect_tls_with_auth_and_gss(
            ConnectParams {
                host,
                port,
                user,
                database,
                password,
                auth_settings,
                gss_token_provider: None,
                gss_token_provider_ex: None,
                protocol_minor: Self::default_protocol_minor(),
                startup_params: Vec::new(),
            },
            ca_cert_pem,
        )
        .await
    }

    async fn connect_tls_with_auth_and_gss(
        params: ConnectParams<'_>,
        ca_cert_pem: Option<&[u8]>,
    ) -> PgResult<Self> {
        let first = Self::connect_tls_with_auth_and_gss_once(params.clone(), ca_cert_pem).await;
        if let Err(err) = &first
            && params.protocol_minor > 0
            && is_explicit_protocol_version_rejection(err)
        {
            let mut downgraded = params;
            downgraded.protocol_minor = (PROTOCOL_VERSION_3_0 & 0xFFFF) as u16;
            return Self::connect_tls_with_auth_and_gss_once(downgraded, ca_cert_pem).await;
        }
        first
    }

    async fn connect_tls_with_auth_and_gss_once(
        params: ConnectParams<'_>,
        ca_cert_pem: Option<&[u8]>,
    ) -> PgResult<Self> {
        let connect_started = Instant::now();
        record_connect_attempt(CONNECT_TRANSPORT_TLS, CONNECT_BACKEND_TOKIO);
        let result = tokio::time::timeout(
            DEFAULT_CONNECT_TIMEOUT,
            Self::connect_tls_inner(params, ca_cert_pem),
        )
        .await
        .map_err(|_| {
            PgError::Connection(format!(
                "TLS connection timeout after {:?}",
                DEFAULT_CONNECT_TIMEOUT
            ))
        })?;
        record_connect_result(
            CONNECT_TRANSPORT_TLS,
            CONNECT_BACKEND_TOKIO,
            &result,
            connect_started.elapsed(),
        );
        result
    }

    /// Inner TLS connection logic without timeout wrapper.
    async fn connect_tls_inner(
        params: ConnectParams<'_>,
        ca_cert_pem: Option<&[u8]>,
    ) -> PgResult<Self> {
        let ConnectParams {
            host,
            port,
            user,
            database,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
            protocol_minor,
            startup_params,
        } = params;
        let replication_mode_enabled = has_logical_replication_startup_mode(&startup_params);
        use tokio::io::AsyncReadExt;
        use tokio_rustls::TlsConnector;
        use tokio_rustls::rustls::ClientConfig;
        use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, pem::PemObject};

        let addr = format!("{}:{}", host, port);
        let mut tcp_stream = TcpStream::connect(&addr).await?;

        // Send SSLRequest
        tcp_stream.write_all(&SSL_REQUEST).await?;

        // Read response
        let mut response = [0u8; 1];
        tcp_stream.read_exact(&mut response).await?;

        if response[0] != b'S' {
            return Err(PgError::Connection(
                "Server does not support TLS".to_string(),
            ));
        }

        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();

        if let Some(ca_pem) = ca_cert_pem {
            let certs = CertificateDer::pem_slice_iter(ca_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| PgError::Connection(format!("Invalid CA certificate PEM: {}", e)))?;
            if certs.is_empty() {
                return Err(PgError::Connection(
                    "No CA certificates found in provided PEM".to_string(),
                ));
            }
            for cert in certs {
                let _ = root_cert_store.add(cert);
            }
        } else {
            let certs = rustls_native_certs::load_native_certs();
            for cert in certs.certs {
                let _ = root_cert_store.add(cert);
            }
        }

        let config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();

        let connector = TlsConnector::from(Arc::new(config));
        let server_name = ServerName::try_from(host.to_string())
            .map_err(|_| PgError::Connection("Invalid hostname for TLS".to_string()))?;

        let tls_stream = connector
            .connect(server_name, tcp_stream)
            .await
            .map_err(|e| PgError::Connection(format!("TLS handshake failed: {}", e)))?;

        let mut conn = Self {
            stream: PgStream::Tls(Box::new(tls_stream)),
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            write_buf: BytesMut::with_capacity(BUFFER_CAPACITY),
            sql_buf: BytesMut::with_capacity(512),
            params_buf: Vec::with_capacity(16),
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(STMT_CACHE_CAPACITY),
            column_info_cache: HashMap::new(),
            process_id: 0,
            secret_key: 0,
            cancel_key_bytes: Vec::new(),
            requested_protocol_minor: protocol_minor,
            negotiated_protocol_minor: protocol_minor,
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        };

        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
            protocol_version: protocol_version_from_minor(protocol_minor),
            startup_params,
        })
        .await?;

        conn.handle_startup(
            user,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
        )
        .await?;

        Ok(conn)
    }

    /// Connect with mutual TLS (client certificate authentication).
    /// # Arguments
    /// * `host` - PostgreSQL server hostname
    /// * `port` - PostgreSQL server port
    /// * `user` - Database user
    /// * `database` - Database name
    /// * `config` - TLS configuration with client cert/key
    /// # Example
    /// ```ignore
    /// let config = TlsConfig {
    ///     client_cert_pem: include_bytes!("client.crt").to_vec(),
    ///     client_key_pem: include_bytes!("client.key").to_vec(),
    ///     ca_cert_pem: Some(include_bytes!("ca.crt").to_vec()),
    /// };
    /// let conn = PgConnection::connect_mtls("localhost", 5432, "user", "db", config).await?;
    /// ```
    pub async fn connect_mtls(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        config: TlsConfig,
    ) -> PgResult<Self> {
        Self::connect_mtls_with_password_and_auth(
            host,
            port,
            user,
            database,
            None,
            config,
            AuthSettings::default(),
        )
        .await
    }

    /// Connect with mutual TLS and optional password fallback.
    pub async fn connect_mtls_with_password_and_auth(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        config: TlsConfig,
        auth_settings: AuthSettings,
    ) -> PgResult<Self> {
        Self::connect_mtls_with_password_and_auth_and_gss(
            ConnectParams {
                host,
                port,
                user,
                database,
                password,
                auth_settings,
                gss_token_provider: None,
                gss_token_provider_ex: None,
                protocol_minor: Self::default_protocol_minor(),
                startup_params: Vec::new(),
            },
            config,
        )
        .await
    }

    async fn connect_mtls_with_password_and_auth_and_gss(
        params: ConnectParams<'_>,
        config: TlsConfig,
    ) -> PgResult<Self> {
        let first =
            Self::connect_mtls_with_password_and_auth_and_gss_once(params.clone(), config.clone())
                .await;
        if let Err(err) = &first
            && params.protocol_minor > 0
            && is_explicit_protocol_version_rejection(err)
        {
            let mut downgraded = params;
            downgraded.protocol_minor = (PROTOCOL_VERSION_3_0 & 0xFFFF) as u16;
            return Self::connect_mtls_with_password_and_auth_and_gss_once(downgraded, config)
                .await;
        }
        first
    }

    async fn connect_mtls_with_password_and_auth_and_gss_once(
        params: ConnectParams<'_>,
        config: TlsConfig,
    ) -> PgResult<Self> {
        let connect_started = Instant::now();
        record_connect_attempt(CONNECT_TRANSPORT_MTLS, CONNECT_BACKEND_TOKIO);
        let result = tokio::time::timeout(
            DEFAULT_CONNECT_TIMEOUT,
            Self::connect_mtls_inner(params, config),
        )
        .await
        .map_err(|_| {
            PgError::Connection(format!(
                "mTLS connection timeout after {:?}",
                DEFAULT_CONNECT_TIMEOUT
            ))
        })?;
        record_connect_result(
            CONNECT_TRANSPORT_MTLS,
            CONNECT_BACKEND_TOKIO,
            &result,
            connect_started.elapsed(),
        );
        result
    }

    /// Inner mTLS connection logic without timeout wrapper.
    async fn connect_mtls_inner(params: ConnectParams<'_>, config: TlsConfig) -> PgResult<Self> {
        let ConnectParams {
            host,
            port,
            user,
            database,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
            protocol_minor,
            startup_params,
        } = params;
        let replication_mode_enabled = has_logical_replication_startup_mode(&startup_params);
        use tokio::io::AsyncReadExt;
        use tokio_rustls::TlsConnector;
        use tokio_rustls::rustls::{
            ClientConfig,
            pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject},
        };

        let addr = format!("{}:{}", host, port);
        let mut tcp_stream = TcpStream::connect(&addr).await?;

        // Send SSLRequest
        tcp_stream.write_all(&SSL_REQUEST).await?;

        // Read response
        let mut response = [0u8; 1];
        tcp_stream.read_exact(&mut response).await?;

        if response[0] != b'S' {
            return Err(PgError::Connection(
                "Server does not support TLS".to_string(),
            ));
        }

        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();

        if let Some(ca_pem) = &config.ca_cert_pem {
            let certs = CertificateDer::pem_slice_iter(ca_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| PgError::Connection(format!("Invalid CA certificate PEM: {}", e)))?;
            if certs.is_empty() {
                return Err(PgError::Connection(
                    "No CA certificates found in provided PEM".to_string(),
                ));
            }
            for cert in certs {
                let _ = root_cert_store.add(cert);
            }
        } else {
            // Use system certs
            let certs = rustls_native_certs::load_native_certs();
            for cert in certs.certs {
                let _ = root_cert_store.add(cert);
            }
        }

        let client_certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(&config.client_cert_pem)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| PgError::Connection(format!("Invalid client cert PEM: {}", e)))?;
        if client_certs.is_empty() {
            return Err(PgError::Connection(
                "No client certificates found in PEM".to_string(),
            ));
        }

        let client_key = PrivateKeyDer::from_pem_slice(&config.client_key_pem)
            .map_err(|e| PgError::Connection(format!("Invalid client key PEM: {}", e)))?;

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_client_auth_cert(client_certs, client_key)
            .map_err(|e| PgError::Connection(format!("Invalid client cert/key: {}", e)))?;

        let connector = TlsConnector::from(Arc::new(tls_config));
        let server_name = ServerName::try_from(host.to_string())
            .map_err(|_| PgError::Connection("Invalid hostname for TLS".to_string()))?;

        let tls_stream = connector
            .connect(server_name, tcp_stream)
            .await
            .map_err(|e| PgError::Connection(format!("mTLS handshake failed: {}", e)))?;

        let mut conn = Self {
            stream: PgStream::Tls(Box::new(tls_stream)),
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            write_buf: BytesMut::with_capacity(BUFFER_CAPACITY),
            sql_buf: BytesMut::with_capacity(512),
            params_buf: Vec::with_capacity(16),
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(STMT_CACHE_CAPACITY),
            column_info_cache: HashMap::new(),
            process_id: 0,
            secret_key: 0,
            cancel_key_bytes: Vec::new(),
            requested_protocol_minor: protocol_minor,
            negotiated_protocol_minor: protocol_minor,
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        };

        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
            protocol_version: protocol_version_from_minor(protocol_minor),
            startup_params,
        })
        .await?;

        conn.handle_startup(
            user,
            password,
            auth_settings,
            gss_token_provider,
            gss_token_provider_ex,
        )
        .await?;

        Ok(conn)
    }

    /// Connect to PostgreSQL server via Unix domain socket.
    #[cfg(unix)]
    pub async fn connect_unix(
        socket_path: &str,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> PgResult<Self> {
        let default_minor = Self::default_protocol_minor();
        let first =
            Self::connect_unix_with_protocol(socket_path, user, database, password, default_minor)
                .await;
        if let Err(err) = &first
            && default_minor > 0
            && is_explicit_protocol_version_rejection(err)
        {
            let downgrade_minor = (PROTOCOL_VERSION_3_0 & 0xFFFF) as u16;
            return Self::connect_unix_with_protocol(
                socket_path,
                user,
                database,
                password,
                downgrade_minor,
            )
            .await;
        }
        first
    }

    #[cfg(unix)]
    async fn connect_unix_with_protocol(
        socket_path: &str,
        user: &str,
        database: &str,
        password: Option<&str>,
        protocol_minor: u16,
    ) -> PgResult<Self> {
        use tokio::net::UnixStream;

        let unix_stream = UnixStream::connect(socket_path).await?;

        let mut conn = Self {
            stream: PgStream::Unix(unix_stream),
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY),
            write_buf: BytesMut::with_capacity(BUFFER_CAPACITY),
            sql_buf: BytesMut::with_capacity(512),
            params_buf: Vec::with_capacity(16),
            prepared_statements: HashMap::new(),
            stmt_cache: StatementCache::new(STMT_CACHE_CAPACITY),
            column_info_cache: HashMap::new(),
            process_id: 0,
            secret_key: 0,
            cancel_key_bytes: Vec::new(),
            requested_protocol_minor: protocol_minor,
            negotiated_protocol_minor: protocol_minor,
            notifications: VecDeque::new(),
            replication_stream_active: false,
            replication_mode_enabled: false,
            last_replication_wal_end: None,
            io_desynced: false,
            pending_statement_closes: Vec::new(),
            draining_statement_closes: false,
        };

        conn.send(FrontendMessage::Startup {
            user: user.to_string(),
            database: database.to_string(),
            protocol_version: protocol_version_from_minor(protocol_minor),
            startup_params: Vec::new(),
        })
        .await?;

        conn.handle_startup(user, password, AuthSettings::default(), None, None)
            .await?;

        Ok(conn)
    }
}

#[cfg(test)]
mod tests {
    use super::{is_explicit_protocol_version_rejection, protocol_version_from_minor};
    use crate::driver::PgError;

    #[test]
    fn protocol_version_from_minor_encodes_major_3() {
        assert_eq!(protocol_version_from_minor(2), 196610);
        assert_eq!(protocol_version_from_minor(0), 196608);
    }

    #[test]
    fn explicit_protocol_rejection_detection_is_case_insensitive() {
        let err = PgError::Connection("Unsupported frontend protocol 3.2".to_string());
        assert!(is_explicit_protocol_version_rejection(&err));

        let err = PgError::Protocol("server: Protocol VERSION not supported".to_string());
        assert!(is_explicit_protocol_version_rejection(&err));
    }

    #[test]
    fn explicit_protocol_rejection_does_not_match_unrelated_errors() {
        let err = PgError::Connection("connection reset by peer".to_string());
        assert!(!is_explicit_protocol_version_rejection(&err));
    }
}
