//! Authentication and security types: ScramChannelBindingMode, EnterpriseAuthMechanism,
//! GssTokenProvider, GssTokenRequest, AuthSettings, TlsMode, GssEncMode, ConnectOptions.

use super::connection::TlsConfig;
use std::sync::Arc;

/// SCRAM channel-binding policy during SASL negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScramChannelBindingMode {
    /// Do not use `SCRAM-SHA-256-PLUS` even when available.
    Disable,
    /// Prefer `SCRAM-SHA-256-PLUS`, fallback to plain SCRAM if needed.
    #[default]
    Prefer,
    /// Require `SCRAM-SHA-256-PLUS` and fail otherwise.
    Require,
}

impl ScramChannelBindingMode {
    /// Parse common config string values.
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "disable" | "off" | "false" | "no" => Some(Self::Disable),
            "prefer" | "on" | "true" | "yes" => Some(Self::Prefer),
            "require" | "required" => Some(Self::Require),
            _ => None,
        }
    }
}

/// Enterprise authentication mechanisms initiated by PostgreSQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnterpriseAuthMechanism {
    /// Kerberos V5 (`AuthenticationKerberosV5`, auth code `2`).
    KerberosV5,
    /// GSSAPI (`AuthenticationGSS`, auth code `7`).
    GssApi,
    /// SSPI (`AuthenticationSSPI`, auth code `9`, primarily Windows servers).
    Sspi,
}

/// Callback used to generate GSS/SSPI response tokens.
///
/// The callback receives:
/// - negotiated enterprise auth mechanism
/// - optional server challenge bytes (`None` for initial token)
///
/// It must return the client response token bytes to send in `GSSResponse`.
pub type GssTokenProvider = fn(EnterpriseAuthMechanism, Option<&[u8]>) -> Result<Vec<u8>, String>;

/// Structured token request for stateful Kerberos/GSS/SSPI providers.
#[derive(Debug, Clone, Copy)]
pub struct GssTokenRequest<'a> {
    /// Stable per-handshake identifier so providers can keep per-connection state.
    pub session_id: u64,
    /// Negotiated enterprise auth mechanism.
    pub mechanism: EnterpriseAuthMechanism,
    /// Server challenge token (`None` for initial token).
    pub server_token: Option<&'a [u8]>,
}

/// Stateful callback for Kerberos/GSS/SSPI response generation.
///
/// Use this when the underlying auth stack needs per-handshake context between
/// `AuthenticationGSS` and `AuthenticationGSSContinue` messages.
pub type GssTokenProviderEx =
    Arc<dyn for<'a> Fn(GssTokenRequest<'a>) -> Result<Vec<u8>, String> + Send + Sync>;

/// Password-auth mechanism policy.
///
/// Defaults allow all PostgreSQL password mechanisms for compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthSettings {
    /// Allow server-requested cleartext password auth.
    pub allow_cleartext_password: bool,
    /// Allow server-requested MD5 password auth.
    pub allow_md5_password: bool,
    /// Allow server-requested SCRAM auth.
    pub allow_scram_sha_256: bool,
    /// Allow server-requested Kerberos V5 auth flow.
    pub allow_kerberos_v5: bool,
    /// Allow server-requested GSSAPI auth flow.
    pub allow_gssapi: bool,
    /// Allow server-requested SSPI auth flow.
    pub allow_sspi: bool,
    /// SCRAM channel-binding requirement.
    pub channel_binding: ScramChannelBindingMode,
}

impl Default for AuthSettings {
    fn default() -> Self {
        Self {
            allow_cleartext_password: true,
            allow_md5_password: true,
            allow_scram_sha_256: true,
            allow_kerberos_v5: false,
            allow_gssapi: false,
            allow_sspi: false,
            channel_binding: ScramChannelBindingMode::Prefer,
        }
    }
}

impl AuthSettings {
    /// Restrictive mode: SCRAM-only password auth.
    pub fn scram_only() -> Self {
        Self {
            allow_cleartext_password: false,
            allow_md5_password: false,
            allow_scram_sha_256: true,
            allow_kerberos_v5: false,
            allow_gssapi: false,
            allow_sspi: false,
            channel_binding: ScramChannelBindingMode::Prefer,
        }
    }

    /// Restrictive mode: enterprise Kerberos/GSS only (no password auth).
    pub fn gssapi_only() -> Self {
        Self {
            allow_cleartext_password: false,
            allow_md5_password: false,
            allow_scram_sha_256: false,
            allow_kerberos_v5: true,
            allow_gssapi: true,
            allow_sspi: true,
            channel_binding: ScramChannelBindingMode::Prefer,
        }
    }

    pub(crate) fn has_any_password_method(self) -> bool {
        self.allow_cleartext_password || self.allow_md5_password || self.allow_scram_sha_256
    }
}

/// TLS policy for connection establishment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TlsMode {
    /// Do not attempt TLS.
    #[default]
    Disable,
    /// Try TLS first; fallback to plaintext only when server has no TLS support.
    Prefer,
    /// Require TLS and fail if unavailable.
    Require,
}

impl TlsMode {
    /// Parse libpq-style `sslmode` values.
    pub fn parse_sslmode(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "disable" => Some(Self::Disable),
            "allow" | "prefer" => Some(Self::Prefer),
            "require" | "verify-ca" | "verify-full" => Some(Self::Require),
            _ => None,
        }
    }
}

/// GSSAPI encryption mode for transport-level encryption via Kerberos.
///
/// Controls whether the driver attempts GSSAPI session encryption
/// (GSSENCRequest) before falling back to TLS or plaintext.
///
/// See: PostgreSQL protocol §54.2.11 — GSSAPI Session Encryption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GssEncMode {
    /// Never attempt GSSAPI encryption.
    #[default]
    Disable,
    /// Try GSSAPI encryption first; fall back to TLS or plaintext.
    Prefer,
    /// Require GSSAPI encryption — fail if the server rejects GSSENCRequest.
    Require,
}

impl GssEncMode {
    /// Parse libpq-style `gssencmode` values.
    pub fn parse_gssencmode(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "disable" => Some(Self::Disable),
            "prefer" => Some(Self::Prefer),
            "require" => Some(Self::Require),
            _ => None,
        }
    }
}

/// Advanced connection options for enterprise deployments.
///
/// Protocol-version controls are intentionally not exposed here in this
/// milestone. The driver requests protocol 3.2 by default and performs a
/// one-shot fallback to protocol 3.0 only on explicit version rejection.
#[derive(Clone, Default)]
pub struct ConnectOptions {
    /// TLS mode for the primary connection.
    pub tls_mode: TlsMode,
    /// GSSAPI session encryption mode.
    pub gss_enc_mode: GssEncMode,
    /// Optional custom CA bundle (PEM) for TLS server validation.
    pub tls_ca_cert_pem: Option<Vec<u8>>,
    /// Optional mTLS client certificate/key config.
    pub mtls: Option<TlsConfig>,
    /// Optional callback for Kerberos/GSS/SSPI token generation.
    pub gss_token_provider: Option<GssTokenProvider>,
    /// Optional stateful Kerberos/GSS/SSPI token provider.
    pub gss_token_provider_ex: Option<GssTokenProviderEx>,
    /// Password-auth policy.
    pub auth: AuthSettings,
    /// Additional startup parameters sent in StartupMessage.
    /// Example: `replication=database` for logical replication mode.
    pub startup_params: Vec<(String, String)>,
}

impl std::fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("tls_mode", &self.tls_mode)
            .field("gss_enc_mode", &self.gss_enc_mode)
            .field(
                "tls_ca_cert_pem",
                &self.tls_ca_cert_pem.as_ref().map(std::vec::Vec::len),
            )
            .field("mtls", &self.mtls.as_ref().map(|_| "<configured>"))
            .field(
                "gss_token_provider",
                &self.gss_token_provider.as_ref().map(|_| "<configured>"),
            )
            .field(
                "gss_token_provider_ex",
                &self.gss_token_provider_ex.as_ref().map(|_| "<configured>"),
            )
            .field("auth", &self.auth)
            .field("startup_params_count", &self.startup_params.len())
            .finish()
    }
}

impl ConnectOptions {
    /// Add a startup parameter.
    ///
    /// Example: `opts.with_startup_param("application_name", "qail-repl")`.
    pub fn with_startup_param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        self.startup_params
            .retain(|(existing, _)| !existing.eq_ignore_ascii_case(&key));
        self.startup_params.push((key, value));
        self
    }

    /// Enable logical replication startup mode (`replication=database`).
    pub fn with_logical_replication(mut self) -> Self {
        self.startup_params
            .retain(|(k, _)| !k.eq_ignore_ascii_case("replication"));
        self.startup_params
            .push(("replication".to_string(), "database".to_string()));
        self
    }
}
