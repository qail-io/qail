//! Free helper functions — GSS token gen, metrics, MD5 password, SCRAM selection, Drop.

#[cfg(all(target_os = "linux", feature = "io_uring"))]
use super::types::CONNECT_BACKEND_IO_URING;
use super::types::{CONNECT_BACKEND_TOKIO, PgConnection};
use crate::driver::stream::PgStream;
use crate::driver::{
    EnterpriseAuthMechanism, GssTokenProvider, GssTokenProviderEx, GssTokenRequest, PgError,
    PgResult, ScramChannelBindingMode,
};

pub(super) fn generate_gss_token(
    session_id: u64,
    mechanism: EnterpriseAuthMechanism,
    server_token: Option<&[u8]>,
    legacy_provider: Option<GssTokenProvider>,
    stateful_provider: Option<&GssTokenProviderEx>,
) -> Result<Vec<u8>, String> {
    if let Some(provider) = stateful_provider {
        return provider(GssTokenRequest {
            session_id,
            mechanism,
            server_token,
        });
    }

    if let Some(provider) = legacy_provider {
        return provider(mechanism, server_token);
    }

    Err("No GSS token provider configured".to_string())
}

pub(super) fn plain_connect_attempt_backend() -> &'static str {
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    {
        if should_try_uring_plain() {
            return CONNECT_BACKEND_IO_URING;
        }
    }
    CONNECT_BACKEND_TOKIO
}

pub(super) fn connect_backend_for_stream(stream: &PgStream) -> &'static str {
    match stream {
        PgStream::Tcp(_) => CONNECT_BACKEND_TOKIO,
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        PgStream::Uring(_) => CONNECT_BACKEND_IO_URING,
        PgStream::Tls(_) => CONNECT_BACKEND_TOKIO,
        #[cfg(unix)]
        PgStream::Unix(_) => CONNECT_BACKEND_TOKIO,
        #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
        PgStream::GssEnc(_) => CONNECT_BACKEND_TOKIO,
    }
}

pub(super) fn connect_error_kind(error: &PgError) -> &'static str {
    match error {
        PgError::Connection(_) => "connection",
        PgError::Protocol(_) => "protocol",
        PgError::Auth(_) => "auth",
        PgError::Query(_) | PgError::QueryServer(_) => "query",
        PgError::NoRows => "no_rows",
        PgError::Io(_) => "io",
        PgError::Encode(_) => "encode",
        PgError::Timeout(_) => "timeout",
        PgError::PoolExhausted { .. } => "pool_exhausted",
        PgError::PoolClosed => "pool_closed",
    }
}

pub(super) fn record_connect_attempt(transport: &'static str, backend: &'static str) {
    metrics::counter!(
        "qail_pg_connect_attempt_total",
        "transport" => transport,
        "backend" => backend
    )
    .increment(1);
}

pub(super) fn record_connect_result(
    transport: &'static str,
    backend: &'static str,
    result: &PgResult<PgConnection>,
    elapsed: std::time::Duration,
) {
    let outcome = if result.is_ok() { "success" } else { "error" };
    metrics::histogram!(
        "qail_pg_connect_duration_seconds",
        "transport" => transport,
        "backend" => backend,
        "outcome" => outcome
    )
    .record(elapsed.as_secs_f64());

    if let Err(error) = result {
        metrics::counter!(
            "qail_pg_connect_failure_total",
            "transport" => transport,
            "backend" => backend,
            "error_kind" => connect_error_kind(error)
        )
        .increment(1);
    } else {
        metrics::counter!(
            "qail_pg_connect_success_total",
            "transport" => transport,
            "backend" => backend
        )
        .increment(1);
    }
}

pub(super) fn select_scram_mechanism(
    mechanisms: &[String],
    tls_server_end_point_binding: Option<Vec<u8>>,
    channel_binding_mode: ScramChannelBindingMode,
) -> Result<(String, Option<Vec<u8>>), String> {
    let has_scram = mechanisms.iter().any(|m| m == "SCRAM-SHA-256");
    let has_scram_plus = mechanisms.iter().any(|m| m == "SCRAM-SHA-256-PLUS");

    match channel_binding_mode {
        ScramChannelBindingMode::Disable => {
            if has_scram {
                return Ok(("SCRAM-SHA-256".to_string(), None));
            }
            Err(format!(
                "channel_binding=disable, but server does not advertise SCRAM-SHA-256. Available: {:?}",
                mechanisms
            ))
        }
        ScramChannelBindingMode::Prefer => {
            if has_scram_plus {
                if let Some(binding) = tls_server_end_point_binding {
                    return Ok(("SCRAM-SHA-256-PLUS".to_string(), Some(binding)));
                }

                if has_scram {
                    return Ok(("SCRAM-SHA-256".to_string(), None));
                }

                return Err(
                    "Server requires SCRAM-SHA-256-PLUS but TLS channel binding is unavailable"
                        .to_string(),
                );
            }

            if has_scram {
                return Ok(("SCRAM-SHA-256".to_string(), None));
            }

            Err(format!(
                "Server doesn't support SCRAM-SHA-256. Available: {:?}",
                mechanisms
            ))
        }
        ScramChannelBindingMode::Require => {
            if !has_scram_plus {
                return Err(
                    "channel_binding=require, but server does not advertise SCRAM-SHA-256-PLUS"
                        .to_string(),
                );
            }
            let binding = tls_server_end_point_binding.ok_or_else(|| {
                "channel_binding=require, but TLS channel binding data is unavailable".to_string()
            })?;
            Ok(("SCRAM-SHA-256-PLUS".to_string(), Some(binding)))
        }
    }
}

/// PostgreSQL MD5 password response: `md5` + md5(hex(md5(password + user)) + 4-byte salt).
pub(super) fn md5_password_message(user: &str, password: &str, salt: [u8; 4]) -> String {
    use md5::{Digest, Md5};

    let mut inner = Md5::new();
    inner.update(password.as_bytes());
    inner.update(user.as_bytes());
    let inner_hex = format!("{:x}", inner.finalize());

    let mut outer = Md5::new();
    outer.update(inner_hex.as_bytes());
    outer.update(salt);
    format!("md5{:x}", outer.finalize())
}

/// Drop implementation sends Terminate packet if possible.
/// This ensures proper cleanup even without explicit close() call.
impl Drop for PgConnection {
    fn drop(&mut self) {
        // Try to send Terminate packet synchronously using try_write
        // This is best-effort - if it fails, TCP RST will handle cleanup
        let terminate: [u8; 5] = [b'X', 0, 0, 0, 4];

        match &mut self.stream {
            PgStream::Tcp(tcp) => {
                // try_write is non-blocking
                let _ = tcp.try_write(&terminate);
            }
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            PgStream::Uring(stream) => {
                // io_uring transport uses blocking worker operations;
                // terminate packet in Drop is not viable, but force socket
                // shutdown so timed-out worker ops unblock promptly.
                let _ = stream.abort_inflight();
            }
            PgStream::Tls(_) => {
                // TLS requires async write which we can't do in Drop.
                // The TCP connection close will still notify the server.
                // For graceful TLS shutdown, use connection.close() explicitly.
            }
            #[cfg(unix)]
            PgStream::Unix(unix) => {
                let _ = unix.try_write(&terminate);
            }
            #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
            PgStream::GssEnc(_) => {
                // GSSENC requires async wrap+write; skip in Drop.
            }
        }
    }
}

pub(crate) fn parse_affected_rows(tag: &str) -> u64 {
    tag.split_whitespace()
        .last()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub(super) fn should_try_uring_plain() -> bool {
    super::super::io_backend::should_use_uring_plain_transport()
}
