//! Linux Kerberos/GSS provider for PostgreSQL enterprise auth.
//!
//! This module is intentionally behind `enterprise-gssapi` + Linux cfg.

use super::{EnterpriseAuthMechanism, GssTokenProviderEx, GssTokenRequest};
use std::collections::HashMap;
use std::ffi::c_void;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Configuration for the built-in Linux Kerberos/GSS token provider.
#[derive(Debug, Clone)]
pub struct LinuxKrb5ProviderConfig {
    /// PostgreSQL service name (defaults to `postgres` in most deployments).
    pub service: String,
    /// PostgreSQL host used for host-based target naming.
    pub host: String,
    /// Optional full GSS host-based target override (e.g. `postgres@db.internal`).
    pub target_name: Option<String>,
}

impl LinuxKrb5ProviderConfig {
    fn target_name(&self) -> Result<String, String> {
        if let Some(target) = self.target_name.as_ref() {
            let target = target.trim();
            if target.is_empty() {
                return Err("LinuxKrb5ProviderConfig.target_name must not be empty".to_string());
            }
            return Ok(target.to_string());
        }

        let service = self.service.trim();
        if service.is_empty() {
            return Err("LinuxKrb5ProviderConfig.service must not be empty".to_string());
        }

        let host = self.host.trim();
        if host.is_empty() {
            return Err("LinuxKrb5ProviderConfig.host must not be empty".to_string());
        }

        Ok(format!("{}@{}", service, host))
    }
}

/// Result of Linux Kerberos environment preflight checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinuxKrb5PreflightReport {
    /// Resolved GSS host-based target name (`service@host`).
    pub target_name: String,
    /// Non-fatal diagnostics detected during validation.
    pub warnings: Vec<String>,
}

/// Validate Linux Kerberos runtime prerequisites and emit actionable diagnostics.
///
/// This check is intentionally conservative:
/// - hard-fails on invalid explicit paths (`KRB5_CONFIG`, `KRB5CCNAME=FILE:...`, keytabs)
/// - emits warnings when runtime source cannot be validated statically
pub fn linux_krb5_preflight(
    config: &LinuxKrb5ProviderConfig,
) -> Result<LinuxKrb5PreflightReport, String> {
    let target_name = config.target_name()?;
    let mut warnings = Vec::new();

    if let Ok(raw_cfg) = std::env::var("KRB5_CONFIG") {
        let mut found = false;
        for candidate in raw_cfg.split(':').filter(|v| !v.trim().is_empty()) {
            if Path::new(candidate).exists() {
                found = true;
                break;
            }
        }
        if !found {
            return Err(format!(
                "Kerberos preflight failed: KRB5_CONFIG is set but no listed file exists: {}",
                raw_cfg
            ));
        }
    } else if !Path::new("/etc/krb5.conf").exists() {
        warnings.push(
            "Kerberos preflight: /etc/krb5.conf not found and KRB5_CONFIG is unset; relying on system defaults"
                .to_string(),
        );
    }

    let mut explicit_cred_source = false;

    if let Ok(ccache) = std::env::var("KRB5CCNAME") {
        explicit_cred_source = true;
        validate_cache_env("KRB5CCNAME", &ccache, &mut warnings)?;
    }

    for env_name in ["KRB5_CLIENT_KTNAME", "KRB5_KTNAME"] {
        if let Ok(keytab) = std::env::var(env_name) {
            explicit_cred_source = true;
            validate_keytab_env(env_name, &keytab)?;
        }
    }

    if !explicit_cred_source {
        warnings.push(
            "Kerberos preflight: no explicit credential source set (KRB5CCNAME/KRB5_CLIENT_KTNAME/KRB5_KTNAME); relying on default cache discovery"
                .to_string(),
        );
    }

    Ok(LinuxKrb5PreflightReport {
        target_name,
        warnings,
    })
}

/// Wrapper that tracks when a GSS session was last active.
struct TrackedSession {
    session: LinuxKrb5Session,
    last_seen: Instant,
}

// SAFETY: LinuxKrb5Session contains *mut c_void (GSSAPI context handle),
// which is only accessed under a Mutex lock in `linux_krb5_token_provider`.
// The GSS context is never shared across threads without synchronisation.
unsafe impl Send for TrackedSession {}
unsafe impl Sync for TrackedSession {}

/// Sessions older than this are pruned on every callback entry.
const GSS_SESSION_TTL: Duration = Duration::from_secs(120);

/// Hard cap on concurrent in-flight GSS handshakes.
const GSS_MAX_SESSIONS: usize = 256;

/// Prune sessions older than `GSS_SESSION_TTL`.
fn prune_stale_sessions(sessions: &mut HashMap<u64, TrackedSession>) {
    let now = Instant::now();
    let cutoff = now.checked_sub(GSS_SESSION_TTL).unwrap_or(now);
    sessions.retain(|_, ts| ts.last_seen > cutoff);
}

/// Create a built-in Linux Kerberos provider for PostgreSQL GSS auth flow.
///
/// Requirements:
/// - process has valid Kerberos credentials (ticket cache / keytab)
/// - PostgreSQL server is configured for GSS/Kerberos auth
/// - crate built with `enterprise-gssapi` feature on Linux
pub fn linux_krb5_token_provider(
    config: LinuxKrb5ProviderConfig,
) -> Result<GssTokenProviderEx, String> {
    let target_name = linux_krb5_preflight(&config)?.target_name;
    let sessions: Arc<Mutex<HashMap<u64, TrackedSession>>> = Arc::new(Mutex::new(HashMap::new()));

    Ok(Arc::new(
        move |request: GssTokenRequest<'_>| -> Result<Vec<u8>, String> {
            match request.mechanism {
                EnterpriseAuthMechanism::KerberosV5 | EnterpriseAuthMechanism::GssApi => {}
                EnterpriseAuthMechanism::Sspi => {
                    return Err(
                    "SSPI is Windows-specific; linux_krb5_token_provider supports Kerberos/GSSAPI only"
                        .to_string(),
                );
                }
            }

            let mut sessions = sessions
                .lock()
                .map_err(|_| "linux_krb5_token_provider session lock poisoned".to_string())?;

            // Prune stale sessions on every entry to bound memory.
            prune_stale_sessions(&mut sessions);

            if request.server_token.is_none() {
                // Fresh handshake start. Drop stale session if it exists.
                sessions.remove(&request.session_id);

                // Enforce max sessions cap after pruning.
                if sessions.len() >= GSS_MAX_SESSIONS {
                    return Err(format!(
                        "GSS session limit reached ({} active); cannot start new handshake",
                        GSS_MAX_SESSIONS
                    ));
                }

                let mut inner = LinuxKrb5Session::new(&target_name, request.mechanism)?;
                let (token, complete) = inner.step(None)?;
                if !complete {
                    sessions.insert(
                        request.session_id,
                        TrackedSession {
                            session: inner,
                            last_seen: Instant::now(),
                        },
                    );
                }
                return Ok(token);
            }

            let mut tracked = sessions.remove(&request.session_id).ok_or_else(|| {
                format!(
                "No active Kerberos session for session_id={} (received GSSContinue before init)",
                request.session_id
            )
            })?;

            if tracked.session.mechanism != request.mechanism {
                return Err(format!(
                    "Kerberos mechanism mismatch for session_id={} (expected {:?}, got {:?})",
                    request.session_id, tracked.session.mechanism, request.mechanism
                ));
            }

            let (token, complete) = tracked.session.step(request.server_token)?;
            if !complete {
                tracked.last_seen = Instant::now();
                sessions.insert(request.session_id, tracked);
            }
            Ok(token)
        },
    ))
}

fn validate_cache_env(env_name: &str, raw: &str, warnings: &mut Vec<String>) -> Result<(), String> {
    if let Some(path) = raw.strip_prefix("FILE:") {
        if path.is_empty() {
            return Err(format!(
                "Kerberos preflight failed: {} uses FILE: but path is empty",
                env_name
            ));
        }
        if !Path::new(path).exists() {
            return Err(format!(
                "Kerberos preflight failed: {} points to missing credential cache file: {}",
                env_name, path
            ));
        }
        return Ok(());
    }

    if let Some(path) = raw.strip_prefix("DIR:") {
        if path.is_empty() {
            return Err(format!(
                "Kerberos preflight failed: {} uses DIR: but path is empty",
                env_name
            ));
        }
        if !Path::new(path).is_dir() {
            return Err(format!(
                "Kerberos preflight failed: {} points to missing credential cache directory: {}",
                env_name, path
            ));
        }
        return Ok(());
    }

    for scheme in ["KEYRING:", "KCM:", "MEMORY:", "API:"] {
        if raw.starts_with(scheme) {
            warnings.push(format!(
                "Kerberos preflight: {} uses {} cache; path validation skipped",
                env_name,
                scheme.trim_end_matches(':')
            ));
            return Ok(());
        }
    }

    if raw.contains(':') {
        warnings.push(format!(
            "Kerberos preflight: {} uses unsupported cache spec '{}'; validation skipped",
            env_name, raw
        ));
        return Ok(());
    }

    if !Path::new(raw).exists() {
        return Err(format!(
            "Kerberos preflight failed: {} points to missing credential cache file: {}",
            env_name, raw
        ));
    }

    Ok(())
}

fn validate_keytab_env(env_name: &str, raw: &str) -> Result<(), String> {
    if let Some(path) = raw.strip_prefix("FILE:") {
        if path.is_empty() {
            return Err(format!(
                "Kerberos preflight failed: {} uses FILE: but path is empty",
                env_name
            ));
        }
        if !Path::new(path).exists() {
            return Err(format!(
                "Kerberos preflight failed: {} points to missing keytab file: {}",
                env_name, path
            ));
        }
        return Ok(());
    }

    if raw.contains(':') {
        return Err(format!(
            "Kerberos preflight failed: {} has unsupported keytab spec '{}'",
            env_name, raw
        ));
    }

    if !Path::new(raw).exists() {
        return Err(format!(
            "Kerberos preflight failed: {} points to missing keytab file: {}",
            env_name, raw
        ));
    }

    Ok(())
}

type OmUint32 = u32;

#[repr(C)]
struct GssOidDesc {
    length: OmUint32,
    elements: *mut c_void,
}

#[repr(C)]
struct GssBufferDesc {
    length: usize,
    value: *mut c_void,
}

type GssOid = *mut GssOidDesc;
type GssName = *mut c_void;
type GssContext = *mut c_void;
type GssCredential = *mut c_void;
type GssChannelBindings = *mut c_void;

const GSS_S_COMPLETE: OmUint32 = 0;
const GSS_S_CONTINUE_NEEDED: OmUint32 = 1;
const GSS_C_GSS_CODE: i32 = 1;
const GSS_C_MECH_CODE: i32 = 2;
const GSS_C_MUTUAL_FLAG: OmUint32 = 0x0000_0002;
const GSS_C_SEQUENCE_FLAG: OmUint32 = 0x0000_0008;
const GSS_C_CONF_FLAG: OmUint32 = 0x0000_0010;

#[link(name = "gssapi_krb5")]
unsafe extern "C" {
    static GSS_C_NT_HOSTBASED_SERVICE: GssOid;

    fn gss_import_name(
        minor_status: *mut OmUint32,
        input_name_buffer: *const GssBufferDesc,
        input_name_type: GssOid,
        output_name: *mut GssName,
    ) -> OmUint32;

    fn gss_release_name(minor_status: *mut OmUint32, input_name: *mut GssName) -> OmUint32;

    fn gss_init_sec_context(
        minor_status: *mut OmUint32,
        initiator_cred_handle: GssCredential,
        context_handle: *mut GssContext,
        target_name: GssName,
        mech_type: GssOid,
        req_flags: OmUint32,
        time_req: OmUint32,
        input_chan_bindings: GssChannelBindings,
        input_token: *const GssBufferDesc,
        actual_mech_type: *mut GssOid,
        output_token: *mut GssBufferDesc,
        ret_flags: *mut OmUint32,
        time_rec: *mut OmUint32,
    ) -> OmUint32;

    fn gss_delete_sec_context(
        minor_status: *mut OmUint32,
        context_handle: *mut GssContext,
        output_token: *mut GssBufferDesc,
    ) -> OmUint32;

    fn gss_release_buffer(minor_status: *mut OmUint32, buffer: *mut GssBufferDesc) -> OmUint32;

    fn gss_display_status(
        minor_status: *mut OmUint32,
        status_value: OmUint32,
        status_type: i32,
        mech_type: GssOid,
        message_context: *mut OmUint32,
        status_string: *mut GssBufferDesc,
    ) -> OmUint32;

    fn gss_wrap(
        minor_status: *mut OmUint32,
        context_handle: GssContext,
        conf_req_flag: i32,
        qop_req: OmUint32,
        input_message_buffer: *const GssBufferDesc,
        conf_state: *mut i32,
        output_message_buffer: *mut GssBufferDesc,
    ) -> OmUint32;

    fn gss_unwrap(
        minor_status: *mut OmUint32,
        context_handle: GssContext,
        input_message_buffer: *const GssBufferDesc,
        output_message_buffer: *mut GssBufferDesc,
        conf_state: *mut i32,
        qop_state: *mut OmUint32,
    ) -> OmUint32;
}

struct LinuxKrb5Session {
    context: GssContext,
    target_name: GssName,
    mechanism: EnterpriseAuthMechanism,
}

impl LinuxKrb5Session {
    fn new(target: &str, mechanism: EnterpriseAuthMechanism) -> Result<Self, String> {
        let mut minor: OmUint32 = 0;
        let mut output_name: GssName = std::ptr::null_mut();
        let input = GssBufferDesc {
            length: target.len(),
            value: target.as_ptr() as *mut c_void,
        };

        let name_type = unsafe { GSS_C_NT_HOSTBASED_SERVICE };
        if name_type.is_null() {
            return Err("GSS_C_NT_HOSTBASED_SERVICE resolved to null pointer".to_string());
        }

        let major = unsafe {
            gss_import_name(
                &mut minor,
                &input as *const GssBufferDesc,
                name_type,
                &mut output_name,
            )
        };

        if is_gss_error(major) {
            return Err(format!(
                "gss_import_name failed for target '{}': {}",
                target,
                format_gss_error(major, minor)
            ));
        }

        Ok(Self {
            context: std::ptr::null_mut(),
            target_name: output_name,
            mechanism,
        })
    }

    fn step(&mut self, input_token: Option<&[u8]>) -> Result<(Vec<u8>, bool), String> {
        let mut minor: OmUint32 = 0;
        let mut output = GssBufferDesc {
            length: 0,
            value: std::ptr::null_mut(),
        };
        let mut input = GssBufferDesc {
            length: 0,
            value: std::ptr::null_mut(),
        };

        let input_ptr = if let Some(bytes) = input_token {
            input.length = bytes.len();
            input.value = bytes.as_ptr() as *mut c_void;
            &input as *const GssBufferDesc
        } else {
            std::ptr::null()
        };

        let mut context = self.context;
        let major = unsafe {
            gss_init_sec_context(
                &mut minor,
                std::ptr::null_mut(),
                &mut context,
                self.target_name,
                std::ptr::null_mut(),
                GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG,
                0,
                std::ptr::null_mut(),
                input_ptr,
                std::ptr::null_mut(),
                &mut output,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        self.context = context;

        let token = take_gss_buffer(&mut output);

        if is_gss_error(major) {
            return Err(format!(
                "gss_init_sec_context failed: {}",
                format_gss_error(major, minor)
            ));
        }

        let complete = major == GSS_S_COMPLETE;
        let continue_needed = (major & GSS_S_CONTINUE_NEEDED) != 0;

        if !complete && !continue_needed {
            return Err(format!(
                "gss_init_sec_context returned unexpected status {}",
                major
            ));
        }

        Ok((token, complete))
    }
}

impl Drop for LinuxKrb5Session {
    fn drop(&mut self) {
        let mut minor: OmUint32 = 0;

        if !self.context.is_null() {
            let _ = unsafe {
                gss_delete_sec_context(
                    &mut minor,
                    &mut self.context,
                    std::ptr::null_mut::<GssBufferDesc>(),
                )
            };
            self.context = std::ptr::null_mut();
        }

        if !self.target_name.is_null() {
            let _ = unsafe { gss_release_name(&mut minor, &mut self.target_name) };
            self.target_name = std::ptr::null_mut();
        }
    }
}

fn is_gss_error(major: OmUint32) -> bool {
    (major & 0xFF00_0000) != 0
}

fn format_gss_error(major: OmUint32, minor: OmUint32) -> String {
    format!(
        "major={} ({}) minor={} ({})",
        major,
        status_messages(major, GSS_C_GSS_CODE),
        minor,
        status_messages(minor, GSS_C_MECH_CODE),
    )
}

fn status_messages(status: OmUint32, status_type: i32) -> String {
    let mut messages = Vec::new();
    let mut message_context: OmUint32 = 0;

    loop {
        let mut minor: OmUint32 = 0;
        let mut msg_buf = GssBufferDesc {
            length: 0,
            value: std::ptr::null_mut(),
        };

        let major = unsafe {
            gss_display_status(
                &mut minor,
                status,
                status_type,
                std::ptr::null_mut(),
                &mut message_context,
                &mut msg_buf,
            )
        };

        let line = take_gss_buffer(&mut msg_buf);
        if !line.is_empty() {
            messages.push(String::from_utf8_lossy(&line).to_string());
        }

        if is_gss_error(major) || message_context == 0 {
            break;
        }
    }

    if messages.is_empty() {
        format!("code {}", status)
    } else {
        messages.join("; ")
    }
}

fn take_gss_buffer(buffer: &mut GssBufferDesc) -> Vec<u8> {
    let bytes = if buffer.length == 0 || buffer.value.is_null() {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(buffer.value as *const u8, buffer.length).to_vec() }
    };

    let mut minor: OmUint32 = 0;
    let _ = unsafe { gss_release_buffer(&mut minor, buffer) };

    bytes
}

// ══════════════════════════════════════════════════════════════════════
// GSSENC Transport: GssEncStream + handshake
// ══════════════════════════════════════════════════════════════════════

use std::io;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

/// Max GSSENC data packet size (matches PostgreSQL `PQ_GSS_MAX_PACKET_SIZE`).
/// This is the maximum length of a single wrapped data frame on the wire.
const PQ_GSS_MAX_PACKET: usize = 16384;

/// Max GSSENC auth-handshake buffer (matches PostgreSQL `PQ_GSS_AUTH_BUFFER_SIZE`).
/// GSSAPI library can produce larger tokens during authentication exchange.
const PQ_GSS_AUTH_BUFFER: usize = 65536;

/// Read-side state machine for length-prefixed GSSENC framing.
///
/// Tracks partial progress so that `poll_read` resuming from `Pending`
/// does not lose data or duplicate reads.
enum ReadState {
    /// No in-flight read; need a new packet.
    Idle,
    /// Partially read the 4-byte length header.
    ReadingHeader { buf: [u8; 4], filled: usize },
    /// Partially reading the wrapped payload.
    ReadingPayload { buf: Vec<u8>, filled: usize },
}

/// Pending outbound write state.
///
/// Stores the already-wrapped (length-prefixed) packet and how much
/// has been written to TCP, so `poll_write` can resume after `Pending`.
struct PendingWrite {
    /// The full wrapped packet (4-byte length + GSS token).
    data: Vec<u8>,
    /// Bytes of `data` already written to TCP.
    written: usize,
    /// Original plaintext byte count (reported to caller on completion).
    plaintext_len: usize,
}

/// GSSAPI-encrypted stream wrapping a raw TCP connection.
///
/// All traffic is framed as length-prefixed (4-byte big-endian) GSS-wrapped
/// packets, following PostgreSQL’s GSSAPI Session Encryption protocol
/// (§54.2.11).
///
/// The stream holds the established GSS security context and performs
/// `gss_wrap`/`gss_unwrap` on every write/read.  Read and write sides
/// maintain state machines so that partial progress across `Pending`
/// returns is preserved.
pub struct GssEncStream {
    /// Underlying TCP stream.
    tcp: TcpStream,
    /// Established GSS security context handle.
    context: GssContext,
    /// Target name (kept alive for context lifetime).
    _target_name: GssName,
    /// Buffer for decrypted plaintext not yet consumed by read.
    read_buf: Vec<u8>,
    /// Current read offset into `read_buf`.
    read_pos: usize,
    /// Read-side state machine for framing.
    read_state: ReadState,
    /// Pending outbound write (None when idle).
    pending_write: Option<PendingWrite>,
}

// SAFETY: The GSS context handle (*mut c_void) is only accessed
// synchronously within poll_read/poll_write. The stream is exclusively
// owned and never shared across threads without Mutex.
unsafe impl Send for GssEncStream {}
unsafe impl Sync for GssEncStream {}

impl GssEncStream {
    /// Create a new GSSENC stream with an established context.
    fn new(tcp: TcpStream, context: GssContext, target_name: GssName) -> Self {
        Self {
            tcp,
            context,
            _target_name: target_name,
            read_buf: Vec::new(),
            read_pos: 0,
            read_state: ReadState::Idle,
            pending_write: None,
        }
    }

    /// Wrap plaintext with `gss_wrap` and return the length-prefixed packet.
    fn wrap(&self, plaintext: &[u8]) -> io::Result<Vec<u8>> {
        let mut minor: OmUint32 = 0;
        let mut conf_state: i32 = 0;
        let input = GssBufferDesc {
            length: plaintext.len(),
            value: plaintext.as_ptr() as *mut c_void,
        };
        let mut output = GssBufferDesc {
            length: 0,
            value: std::ptr::null_mut(),
        };

        let major = unsafe {
            gss_wrap(
                &mut minor,
                self.context,
                1, // conf_req_flag: request confidentiality
                0, // default QOP
                &input,
                &mut conf_state,
                &mut output,
            )
        };

        if is_gss_error(major) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("gss_wrap failed: {}", format_gss_error(major, minor)),
            ));
        }

        if conf_state == 0 {
            // Server did not apply confidentiality — integrity-only.
            // For GSSENC this is a protocol violation.
            let _ = take_gss_buffer(&mut output);
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "gss_wrap did not provide confidentiality (conf_state=0)",
            ));
        }

        let wrapped = take_gss_buffer(&mut output);

        // Length-prefix: 4-byte big-endian length of the wrapped token.
        let mut packet = Vec::with_capacity(4 + wrapped.len());
        packet.extend_from_slice(&(wrapped.len() as u32).to_be_bytes());
        packet.extend_from_slice(&wrapped);
        Ok(packet)
    }

    /// Unwrap a GSS-wrapped token to plaintext.
    fn unwrap_token(&self, wrapped: &[u8]) -> io::Result<Vec<u8>> {
        let mut minor: OmUint32 = 0;
        let mut conf_state: i32 = 0;
        let mut qop_state: OmUint32 = 0;
        let input = GssBufferDesc {
            length: wrapped.len(),
            value: wrapped.as_ptr() as *mut c_void,
        };
        let mut output = GssBufferDesc {
            length: 0,
            value: std::ptr::null_mut(),
        };

        let major = unsafe {
            gss_unwrap(
                &mut minor,
                self.context,
                &input,
                &mut output,
                &mut conf_state,
                &mut qop_state,
            )
        };

        if is_gss_error(major) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("gss_unwrap failed: {}", format_gss_error(major, minor)),
            ));
        }

        if conf_state == 0 {
            // Inbound message was integrity-only, not encrypted.
            // This is a protocol violation for GSSENC.
            let _ = take_gss_buffer(&mut output);
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "gss_unwrap: inbound message lacks confidentiality (conf_state=0)",
            ));
        }

        Ok(take_gss_buffer(&mut output))
    }

    /// Drive the read-side state machine: read a length-prefixed GSSENC
    /// packet from TCP, unwrap it, and populate `read_buf`.
    ///
    /// Returns `Poll::Ready(Ok(()))` when `read_buf` has plaintext,
    /// `Poll::Pending` when waiting for TCP data (state is preserved),
    /// or `Poll::Ready(Err(_))` on failure.
    fn poll_fill_read_buf(&mut self, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        loop {
            match &mut self.read_state {
                ReadState::Idle => {
                    self.read_state = ReadState::ReadingHeader {
                        buf: [0u8; 4],
                        filled: 0,
                    };
                }
                ReadState::ReadingHeader { buf, filled } => {
                    while *filled < 4 {
                        let mut rb = ReadBuf::new(&mut buf[*filled..]);
                        match Pin::new(&mut self.tcp).poll_read(cx, &mut rb) {
                            Poll::Ready(Ok(())) => {
                                let n = rb.filled().len();
                                if n == 0 {
                                    self.read_state = ReadState::Idle;
                                    self.read_buf.clear();
                                    self.read_pos = 0;
                                    return Poll::Ready(Ok(()));
                                }
                                *filled += n;
                            }
                            Poll::Ready(Err(e)) => {
                                self.read_state = ReadState::Idle;
                                return Poll::Ready(Err(e));
                            }
                            Poll::Pending => return Poll::Pending,
                        }
                    }

                    let len = u32::from_be_bytes(*buf) as usize;
                    if len == 0 {
                        self.read_state = ReadState::Idle;
                        self.read_buf.clear();
                        self.read_pos = 0;
                        return Poll::Ready(Ok(()));
                    }
                    if len > PQ_GSS_MAX_PACKET * 4 {
                        self.read_state = ReadState::Idle;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("GSSENC packet too large: {} bytes", len),
                        )));
                    }

                    self.read_state = ReadState::ReadingPayload {
                        buf: vec![0u8; len],
                        filled: 0,
                    };
                }
                ReadState::ReadingPayload { buf, filled } => {
                    while *filled < buf.len() {
                        let mut rb = ReadBuf::new(&mut buf[*filled..]);
                        match Pin::new(&mut self.tcp).poll_read(cx, &mut rb) {
                            Poll::Ready(Ok(())) => {
                                let n = rb.filled().len();
                                if n == 0 {
                                    self.read_state = ReadState::Idle;
                                    return Poll::Ready(Err(io::Error::new(
                                        io::ErrorKind::UnexpectedEof,
                                        "GSSENC: EOF during wrapped payload read",
                                    )));
                                }
                                *filled += n;
                            }
                            Poll::Ready(Err(e)) => {
                                self.read_state = ReadState::Idle;
                                return Poll::Ready(Err(e));
                            }
                            Poll::Pending => return Poll::Pending,
                        }
                    }

                    // Full wrapped token received — unwrap.
                    let wrapped = std::mem::take(buf);
                    self.read_state = ReadState::Idle;
                    self.read_buf = self.unwrap_token(&wrapped)?;
                    self.read_pos = 0;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }

    /// Drive pending write to completion.
    fn poll_flush_pending_write(&mut self, cx: &mut TaskContext<'_>) -> Poll<io::Result<usize>> {
        let pw = self.pending_write.as_mut().expect("no pending write");
        while pw.written < pw.data.len() {
            match Pin::new(&mut self.tcp).poll_write(cx, &pw.data[pw.written..]) {
                Poll::Ready(Ok(n)) => {
                    if n == 0 {
                        self.pending_write = None;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "GSSENC: TCP write returned 0",
                        )));
                    }
                    pw.written += n;
                }
                Poll::Ready(Err(e)) => {
                    self.pending_write = None;
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        let plaintext_len = pw.plaintext_len;
        self.pending_write = None;
        Poll::Ready(Ok(plaintext_len))
    }
}

impl AsyncRead for GssEncStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // Serve from decryption buffer if we have unconsumed plaintext.
        if self.read_pos < self.read_buf.len() {
            let available = &self.read_buf[self.read_pos..];
            let to_copy = available.len().min(buf.remaining());
            buf.put_slice(&available[..to_copy]);
            self.read_pos += to_copy;
            return Poll::Ready(Ok(()));
        }

        // Drive the state machine to read & unwrap a new packet.
        let this = self.get_mut();
        match this.poll_fill_read_buf(cx) {
            Poll::Ready(Ok(())) => {
                if this.read_buf.is_empty() {
                    return Poll::Ready(Ok(())); // EOF
                }
                let available = &this.read_buf[this.read_pos..];
                let to_copy = available.len().min(buf.remaining());
                buf.put_slice(&available[..to_copy]);
                this.read_pos += to_copy;
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for GssEncStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        // Resume pending write from a previous Pending.
        if this.pending_write.is_some() {
            return this.poll_flush_pending_write(cx);
        }

        // Chunk to PQ_GSS_MAX_PACKET size and wrap.
        let to_wrap = &buf[..buf.len().min(PQ_GSS_MAX_PACKET)];
        let packet = this.wrap(to_wrap)?;
        let plaintext_len = to_wrap.len();

        this.pending_write = Some(PendingWrite {
            data: packet,
            written: 0,
            plaintext_len,
        });
        this.poll_flush_pending_write(cx)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().tcp).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().tcp).poll_shutdown(cx)
    }
}

impl Drop for GssEncStream {
    fn drop(&mut self) {
        if !self.context.is_null() {
            let mut minor: OmUint32 = 0;
            let _ = unsafe {
                gss_delete_sec_context(
                    &mut minor,
                    &mut self.context,
                    std::ptr::null_mut::<GssBufferDesc>(),
                )
            };
            self.context = std::ptr::null_mut();
        }
        if !self._target_name.is_null() {
            let mut minor: OmUint32 = 0;
            let _ = unsafe { gss_release_name(&mut minor, &mut self._target_name) };
            self._target_name = std::ptr::null_mut();
        }
    }
}

/// RAII guard for GSS resources during handshake.
///
/// Releases `target_name` and `context` on drop unless ownership is
/// transferred out via `into_stream()`.
struct GssHandshakeGuard {
    context: GssContext,
    target_name: GssName,
}

impl GssHandshakeGuard {
    fn new(target_name: GssName) -> Self {
        Self {
            context: std::ptr::null_mut(),
            target_name,
        }
    }

    /// Transfer ownership to a `GssEncStream` — suppresses cleanup.
    fn into_stream(mut self, tcp: TcpStream) -> GssEncStream {
        let stream = GssEncStream::new(tcp, self.context, self.target_name);
        // Prevent Drop from releasing resources now owned by the stream.
        self.context = std::ptr::null_mut();
        self.target_name = std::ptr::null_mut();
        stream
    }
}

impl Drop for GssHandshakeGuard {
    fn drop(&mut self) {
        let mut minor: OmUint32 = 0;
        if !self.context.is_null() {
            let _ = unsafe {
                gss_delete_sec_context(
                    &mut minor,
                    &mut self.context,
                    std::ptr::null_mut::<GssBufferDesc>(),
                )
            };
        }
        if !self.target_name.is_null() {
            let _ = unsafe { gss_release_name(&mut minor, &mut self.target_name) };
        }
    }
}

/// Perform the GSSAPI session encryption handshake on a TCP stream that
/// already received a `G` response to GSSENCRequest.
///
/// This runs `gss_init_sec_context` in a loop, exchanging length-prefixed
/// tokens with the server, until the context is established.
///
/// Returns a `GssEncStream` ready for encrypted I/O.
/// GSS resources are cleaned up on all error paths via `GssHandshakeGuard`.
pub(crate) async fn gssenc_handshake(
    mut tcp: TcpStream,
    host: &str,
) -> Result<GssEncStream, String> {
    // Import the server’s target name (host-based service principal).
    let target_str = format!("postgres@{}", host);
    let mut minor: OmUint32 = 0;
    let mut target_name: GssName = std::ptr::null_mut();
    let name_buf = GssBufferDesc {
        length: target_str.len(),
        value: target_str.as_ptr() as *mut c_void,
    };
    let major = unsafe {
        gss_import_name(
            &mut minor,
            &name_buf,
            GSS_C_NT_HOSTBASED_SERVICE,
            &mut target_name,
        )
    };
    if is_gss_error(major) {
        return Err(format!(
            "gss_import_name failed: {}",
            format_gss_error(major, minor)
        ));
    }

    // Guard cleans up target_name + context on any error path.
    let mut guard = GssHandshakeGuard::new(target_name);

    let mut input_token: Option<Vec<u8>> = None;
    let mut roundtrips = 0u32;
    const MAX_GSSENC_ROUNDTRIPS: u32 = 10;

    loop {
        roundtrips += 1;
        if roundtrips > MAX_GSSENC_ROUNDTRIPS {
            return Err(format!(
                "GSSENC handshake exceeded {} roundtrips",
                MAX_GSSENC_ROUNDTRIPS
            ));
        }

        let mut output = GssBufferDesc {
            length: 0,
            value: std::ptr::null_mut(),
        };
        let mut input = GssBufferDesc {
            length: 0,
            value: std::ptr::null_mut(),
        };
        let input_ptr = if let Some(ref bytes) = input_token {
            input.length = bytes.len();
            input.value = bytes.as_ptr() as *mut c_void;
            &input as *const GssBufferDesc
        } else {
            std::ptr::null()
        };

        let mut ret_flags: OmUint32 = 0;
        let major = unsafe {
            gss_init_sec_context(
                &mut minor,
                std::ptr::null_mut(), // use default credentials
                &mut guard.context,
                guard.target_name,
                std::ptr::null_mut(), // default mechanism
                GSS_C_MUTUAL_FLAG | GSS_C_SEQUENCE_FLAG | GSS_C_CONF_FLAG,
                0,
                std::ptr::null_mut(), // no channel bindings
                input_ptr,
                std::ptr::null_mut(), // actual_mech
                &mut output,
                &mut ret_flags,
                std::ptr::null_mut(), // time_rec
            )
        };

        let token = take_gss_buffer(&mut output);

        if is_gss_error(major) {
            return Err(format!(
                "GSSENC gss_init_sec_context failed: {}",
                format_gss_error(major, minor)
            ));
        }

        // Send the output token to the server (length-prefixed).
        if !token.is_empty() {
            let len_bytes = (token.len() as u32).to_be_bytes();
            tcp.write_all(&len_bytes)
                .await
                .map_err(|e| format!("GSSENC send token: {e}"))?;
            tcp.write_all(&token)
                .await
                .map_err(|e| format!("GSSENC send token payload: {e}"))?;
            tcp.flush()
                .await
                .map_err(|e| format!("GSSENC flush: {e}"))?;
        }

        if major == GSS_S_COMPLETE {
            // Verify confidentiality was negotiated.
            if (ret_flags & GSS_C_CONF_FLAG) == 0 {
                return Err("GSSENC context established without confidentiality — \
                     server does not support encryption"
                    .to_string());
            }
            // Transfer ownership — guard won't release resources.
            return Ok(guard.into_stream(tcp));
        }

        // GSS_S_CONTINUE_NEEDED: read server’s response token.
        let mut len_buf = [0u8; 4];
        tcp.read_exact(&mut len_buf)
            .await
            .map_err(|e| format!("GSSENC read token length: {e}"))?;
        let server_len = u32::from_be_bytes(len_buf) as usize;

        if server_len == 0 || server_len > PQ_GSS_AUTH_BUFFER {
            return Err(format!(
                "GSSENC server token invalid length: {}",
                server_len
            ));
        }

        let mut server_token = vec![0u8; server_len];
        tcp.read_exact(&mut server_token)
            .await
            .map_err(|e| format!("GSSENC read server token: {e}"))?;

        input_token = Some(server_token);
    }
}
