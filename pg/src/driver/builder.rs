//! PgDriverBuilder — ergonomic builder pattern for PgDriver connections.

use super::auth_types::{
    AuthSettings, ConnectOptions, GssEncMode, GssTokenProvider, GssTokenProviderEx,
    ScramChannelBindingMode, TlsMode,
};
use super::core::PgDriver;
use super::types::{PgError, PgResult};
use crate::driver::connection::TlsConfig;

// ============================================================================
// Connection Builder
// ============================================================================

/// Builder for creating PgDriver connections with named parameters.
/// # Example
/// ```ignore
/// let driver = PgDriver::builder()
///     .host("localhost")
///     .port(5432)
///     .user("admin")
///     .database("mydb")
///     .password("secret")
///     .connect()
///     .await?;
/// ```
#[derive(Default)]
pub struct PgDriverBuilder {
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    database: Option<String>,
    password: Option<String>,
    timeout: Option<std::time::Duration>,
    pub(crate) connect_options: ConnectOptions,
}

impl PgDriverBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the host (default: "127.0.0.1").
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Set the port (default: 5432).
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Set the username (required).
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Set the database name (required).
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Set the password (optional, for cleartext/MD5/SCRAM-SHA-256 auth).
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set connection timeout (optional).
    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set TLS policy (`disable`, `prefer`, `require`).
    pub fn tls_mode(mut self, mode: TlsMode) -> Self {
        self.connect_options.tls_mode = mode;
        self
    }

    /// Set GSSAPI session encryption mode (`disable`, `prefer`, `require`).
    pub fn gss_enc_mode(mut self, mode: GssEncMode) -> Self {
        self.connect_options.gss_enc_mode = mode;
        self
    }

    /// Set custom CA bundle PEM for TLS validation.
    pub fn tls_ca_cert_pem(mut self, ca_pem: Vec<u8>) -> Self {
        self.connect_options.tls_ca_cert_pem = Some(ca_pem);
        self
    }

    /// Enable mTLS using client certificate/key config.
    pub fn mtls(mut self, config: TlsConfig) -> Self {
        self.connect_options.mtls = Some(config);
        self.connect_options.tls_mode = TlsMode::Require;
        self
    }

    /// Override password-auth policy.
    pub fn auth_settings(mut self, settings: AuthSettings) -> Self {
        self.connect_options.auth = settings;
        self
    }

    /// Set SCRAM channel-binding mode.
    pub fn channel_binding_mode(mut self, mode: ScramChannelBindingMode) -> Self {
        self.connect_options.auth.channel_binding = mode;
        self
    }

    /// Set Kerberos/GSS/SSPI token provider callback.
    pub fn gss_token_provider(mut self, provider: GssTokenProvider) -> Self {
        self.connect_options.gss_token_provider = Some(provider);
        self
    }

    /// Set a stateful Kerberos/GSS/SSPI token provider.
    pub fn gss_token_provider_ex(mut self, provider: GssTokenProviderEx) -> Self {
        self.connect_options.gss_token_provider_ex = Some(provider);
        self
    }

    /// Add a custom StartupMessage parameter.
    ///
    /// Example: `.startup_param("application_name", "qail-replica")`
    pub fn startup_param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        self.connect_options
            .startup_params
            .retain(|(existing, _)| !existing.eq_ignore_ascii_case(&key));
        self.connect_options.startup_params.push((key, value));
        self
    }

    /// Enable logical replication startup mode (`replication=database`).
    ///
    /// This is required before issuing commands like `IDENTIFY_SYSTEM` or
    /// `CREATE_REPLICATION_SLOT` on a replication connection.
    pub fn logical_replication(mut self) -> Self {
        self.connect_options
            .startup_params
            .retain(|(k, _)| !k.eq_ignore_ascii_case("replication"));
        self.connect_options
            .startup_params
            .push(("replication".to_string(), "database".to_string()));
        self
    }

    /// Connect to PostgreSQL using the configured parameters.
    pub async fn connect(self) -> PgResult<PgDriver> {
        let host = self.host.unwrap_or_else(|| "127.0.0.1".to_string());
        let port = self.port.unwrap_or(5432);
        let user = self
            .user
            .ok_or_else(|| PgError::Connection("User is required".to_string()))?;
        let database = self
            .database
            .ok_or_else(|| PgError::Connection("Database is required".to_string()))?;

        let password = self.password;
        let options = self.connect_options;

        if let Some(timeout) = self.timeout {
            let options = options.clone();
            tokio::time::timeout(
                timeout,
                PgDriver::connect_with_options(
                    &host,
                    port,
                    &user,
                    &database,
                    password.as_deref(),
                    options,
                ),
            )
            .await
            .map_err(|_| PgError::Timeout(format!("connection after {:?}", timeout)))?
        } else {
            PgDriver::connect_with_options(
                &host,
                port,
                &user,
                &database,
                password.as_deref(),
                options,
            )
            .await
        }
    }
}
