//! Pool configuration, URL parsing, and builder.

use crate::driver::{
    AuthSettings, GssEncMode, GssTokenProvider, GssTokenProviderEx, PgError, PgResult,
    ScramChannelBindingMode, TlsConfig, TlsMode,
};
use std::time::Duration;

/// Configuration for a PostgreSQL connection pool.
///
/// Use the builder pattern to customise settings:
///
/// ```ignore
/// use std::time::Duration;
/// use qail_pg::driver::pool::PoolConfig;
/// let config = PoolConfig::new("localhost", 5432, "app", "mydb")
///     .password("secret")
///     .max_connections(20)
///     .acquire_timeout(Duration::from_secs(5));
/// ```
#[derive(Clone)]
pub struct PoolConfig {
    /// PostgreSQL server hostname or IP address.
    pub host: String,
    /// PostgreSQL server port (default: 5432).
    pub port: u16,
    /// Database role / user name.
    pub user: String,
    /// Target database name.
    pub database: String,
    /// Optional password for authentication.
    pub password: Option<String>,
    /// Hard upper limit on simultaneous connections (default: 10).
    pub max_connections: usize,
    /// Minimum idle connections kept warm in the pool (default: 1).
    pub min_connections: usize,
    /// Close idle connections after this duration (default: 10 min).
    pub idle_timeout: Duration,
    /// Maximum time to wait when acquiring a connection (default: 30s).
    pub acquire_timeout: Duration,
    /// TCP connect timeout for new connections (default: 10s).
    pub connect_timeout: Duration,
    /// Optional maximum lifetime of any connection in the pool.
    pub max_lifetime: Option<Duration>,
    /// Maximum number of leaked-connection cleanup tasks that may run concurrently.
    ///
    /// When a `PooledConnection` is dropped without calling `release()`, the pool
    /// can attempt async reset-and-return. This bound prevents unbounded cleanup fanout.
    pub leaked_cleanup_queue: usize,
    /// When `true`, run a health check (`SELECT 1`) before handing out a connection.
    pub test_on_acquire: bool,
    /// TLS mode for new connections.
    pub tls_mode: TlsMode,
    /// Optional custom CA bundle (PEM) for server certificate validation.
    pub tls_ca_cert_pem: Option<Vec<u8>>,
    /// Optional mTLS client certificate/key configuration.
    pub mtls: Option<TlsConfig>,
    /// Optional callback for Kerberos/GSS/SSPI token generation.
    pub gss_token_provider: Option<GssTokenProvider>,
    /// Optional stateful callback for Kerberos/GSS/SSPI token generation.
    pub gss_token_provider_ex: Option<GssTokenProviderEx>,
    /// Number of retries for transient GSS/Kerberos connection failures.
    pub gss_connect_retries: usize,
    /// Base delay for GSS/Kerberos connect retry backoff.
    pub gss_retry_base_delay: Duration,
    /// Transient GSS failures in one window before opening the local circuit.
    pub gss_circuit_breaker_threshold: usize,
    /// Rolling window used to count transient GSS failures.
    pub gss_circuit_breaker_window: Duration,
    /// Cooldown duration while the local GSS circuit stays open.
    pub gss_circuit_breaker_cooldown: Duration,
    /// Password-auth policy.
    pub auth_settings: AuthSettings,
    /// GSSAPI session encryption mode (`gssencmode` URL parameter).
    pub gss_enc_mode: GssEncMode,
    /// Opt into Linux io_uring for plain TCP transport.
    ///
    /// Disabled by default because some production environments disallow
    /// io_uring for security policy reasons. TLS/mTLS/GSSENC paths ignore this.
    pub io_uring: bool,
}

impl PoolConfig {
    /// Create a new pool configuration with **production-safe** defaults.
    ///
    /// Defaults: `tls_mode = Require`, `auth_settings = scram_only()`.
    /// For local development without TLS, use [`PoolConfig::new_dev`].
    ///
    /// # Arguments
    ///
    /// * `host` — PostgreSQL server hostname or IP.
    /// * `port` — TCP port (typically 5432).
    /// * `user` — PostgreSQL role name.
    /// * `database` — Target database name.
    pub fn new(host: &str, port: u16, user: &str, database: &str) -> Self {
        Self {
            host: host.to_string(),
            port,
            user: user.to_string(),
            database: database.to_string(),
            password: None,
            max_connections: 10,
            min_connections: 1,
            idle_timeout: Duration::from_secs(600), // 10 minutes
            acquire_timeout: Duration::from_secs(30), // 30 seconds
            connect_timeout: Duration::from_secs(10), // 10 seconds
            max_lifetime: None,                     // No limit by default
            leaked_cleanup_queue: 64,               // Bounded cleanup fanout
            test_on_acquire: false,                 // Disabled by default for performance
            tls_mode: TlsMode::Prefer,
            tls_ca_cert_pem: None,
            mtls: None,
            gss_token_provider: None,
            gss_token_provider_ex: None,
            gss_connect_retries: 2,
            gss_retry_base_delay: Duration::from_millis(150),
            gss_circuit_breaker_threshold: 8,
            gss_circuit_breaker_window: Duration::from_secs(30),
            gss_circuit_breaker_cooldown: Duration::from_secs(15),
            auth_settings: AuthSettings::scram_only(),
            gss_enc_mode: GssEncMode::Disable,
            io_uring: false,
        }
    }

    /// Create a pool configuration with **permissive** defaults for local development.
    ///
    /// Defaults: `tls_mode = Disable`, `auth_settings = default()` (accepts any auth).
    /// Do NOT use in production.
    pub fn new_dev(host: &str, port: u16, user: &str, database: &str) -> Self {
        let mut config = Self::new(host, port, user, database);
        config.tls_mode = TlsMode::Disable;
        config.auth_settings = AuthSettings::default();
        config
    }

    /// Set password for authentication.
    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    /// Set maximum simultaneous connections.
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Set minimum idle connections.
    pub fn min_connections(mut self, min: usize) -> Self {
        self.min_connections = min;
        self
    }

    /// Set idle timeout (connections idle longer than this are closed).
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Set acquire timeout (max wait time when getting a connection).
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set connect timeout (max time to establish new connection).
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set maximum lifetime of a connection before recycling.
    pub fn max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = Some(lifetime);
        self
    }

    /// Set max concurrent leaked-connection cleanup tasks.
    ///
    /// Values <= 1 force strict fallback-destroy behavior under burst leaks.
    pub fn leaked_cleanup_queue(mut self, max: usize) -> Self {
        self.leaked_cleanup_queue = max;
        self
    }

    /// Enable connection validation on acquire.
    pub fn test_on_acquire(mut self, enabled: bool) -> Self {
        self.test_on_acquire = enabled;
        self
    }

    /// Set TLS mode for pool connections.
    pub fn tls_mode(mut self, mode: TlsMode) -> Self {
        self.tls_mode = mode;
        self
    }

    /// Set custom CA bundle (PEM) for TLS validation.
    pub fn tls_ca_cert_pem(mut self, ca_cert_pem: Vec<u8>) -> Self {
        self.tls_ca_cert_pem = Some(ca_cert_pem);
        self
    }

    /// Enable mTLS for pool connections.
    pub fn mtls(mut self, config: TlsConfig) -> Self {
        self.mtls = Some(config);
        self.tls_mode = TlsMode::Require;
        self
    }

    /// Set Kerberos/GSS/SSPI token provider callback.
    pub fn gss_token_provider(mut self, provider: GssTokenProvider) -> Self {
        self.gss_token_provider = Some(provider);
        self
    }

    /// Set a stateful Kerberos/GSS/SSPI token provider.
    pub fn gss_token_provider_ex(mut self, provider: GssTokenProviderEx) -> Self {
        self.gss_token_provider_ex = Some(provider);
        self
    }

    /// Set retry count for transient GSS/Kerberos connection failures.
    pub fn gss_connect_retries(mut self, retries: usize) -> Self {
        self.gss_connect_retries = retries;
        self
    }

    /// Set base backoff delay for GSS/Kerberos connection retry.
    pub fn gss_retry_base_delay(mut self, delay: Duration) -> Self {
        self.gss_retry_base_delay = delay;
        self
    }

    /// Set failure threshold for opening local GSS circuit breaker.
    pub fn gss_circuit_breaker_threshold(mut self, threshold: usize) -> Self {
        self.gss_circuit_breaker_threshold = threshold;
        self
    }

    /// Set rolling failure window for GSS circuit breaker.
    pub fn gss_circuit_breaker_window(mut self, window: Duration) -> Self {
        self.gss_circuit_breaker_window = window;
        self
    }

    /// Set cooldown duration for open GSS circuit breaker.
    pub fn gss_circuit_breaker_cooldown(mut self, cooldown: Duration) -> Self {
        self.gss_circuit_breaker_cooldown = cooldown;
        self
    }

    /// Set authentication policy.
    pub fn auth_settings(mut self, settings: AuthSettings) -> Self {
        self.auth_settings = settings;
        self
    }

    /// Opt into Linux io_uring for plain TCP transport.
    ///
    /// This only has an effect on Linux builds compiled with the `io_uring`
    /// feature and when the connection uses plaintext TCP.
    pub fn io_uring(mut self, enabled: bool) -> Self {
        self.io_uring = enabled;
        self
    }

    /// Create a `PoolConfig` from a centralized `QailConfig`.
    ///
    /// Parses `postgres.url` for host/port/user/database/password
    /// and applies pool tuning from `[postgres]` section.
    pub fn from_qail_config(qail: &qail_core::config::QailConfig) -> PgResult<Self> {
        let pg = &qail.postgres;
        let (host, port, user, database, password) = parse_pg_url(&pg.url)?;

        let mut config = PoolConfig::new(&host, port, &user, &database)
            .max_connections(pg.max_connections)
            .min_connections(pg.min_connections)
            .idle_timeout(Duration::from_secs(pg.idle_timeout_secs))
            .acquire_timeout(Duration::from_secs(pg.acquire_timeout_secs))
            .connect_timeout(Duration::from_secs(pg.connect_timeout_secs))
            .test_on_acquire(pg.test_on_acquire)
            .io_uring(pg.io_uring);

        if let Some(ref pw) = password {
            config = config.password(pw);
        }

        // Optional URL query params for enterprise auth/TLS settings.
        if let Some((_, query)) = pg.url.split_once('?') {
            apply_url_query_params(&mut config, query, &host)?;
        }

        Ok(config)
    }

    /// Create a pool configuration directly from a PostgreSQL URL.
    ///
    /// This parses the same URL shape and query parameters as
    /// [`crate::driver::PgDriver::connect_url`], then applies the pool builder
    /// defaults for connection limits and timeouts.
    pub fn from_url(url: &str) -> PgResult<Self> {
        let (host, port, user, database, password) = parse_pg_url(url)?;
        let mut config = PoolConfig::new(&host, port, &user, &database);

        if let Some(ref pw) = password {
            config = config.password(pw);
        }

        if let Some((_, query)) = url.split_once('?') {
            apply_url_query_params(&mut config, query, &host)?;
        }

        Ok(config)
    }
}

/// Apply enterprise auth/TLS query parameters to a `PoolConfig`.
///
/// Shared between `PoolConfig::from_qail_config` and `PgDriver::connect_url`
/// so that both paths support the same set of URL knobs.
#[allow(unused_variables)]
pub(crate) fn apply_url_query_params(
    config: &mut PoolConfig,
    query: &str,
    host: &str,
) -> PgResult<()> {
    let mut sslcert: Option<String> = None;
    let mut sslkey: Option<String> = None;
    let mut gss_provider: Option<String> = None;
    let mut gss_service = "postgres".to_string();
    let mut gss_target: Option<String> = None;

    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let mut kv = pair.splitn(2, '=');
        let key = percent_decode(kv.next().unwrap_or_default().trim())?;
        let value = percent_decode(kv.next().unwrap_or_default().trim())?;

        match key.as_str() {
            "sslmode" => {
                let mode = TlsMode::parse_sslmode(&value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid sslmode value: {}", value))
                })?;
                config.tls_mode = mode;
            }
            "gssencmode" => {
                let mode = GssEncMode::parse_gssencmode(&value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid gssencmode value: {}", value))
                })?;
                config.gss_enc_mode = mode;
            }
            "io_uring" => {
                let enabled = parse_bool_param(&value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid io_uring value: {}", value))
                })?;
                config.io_uring = enabled;
            }
            "sslrootcert" => {
                let ca_pem = std::fs::read(&value).map_err(|e| {
                    PgError::Connection(format!("Failed to read sslrootcert '{}': {}", value, e))
                })?;
                config.tls_ca_cert_pem = Some(ca_pem);
            }
            "sslcert" => sslcert = Some(value.clone()),
            "sslkey" => sslkey = Some(value.clone()),
            "channel_binding" => {
                let mode = ScramChannelBindingMode::parse(&value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid channel_binding value: {}", value))
                })?;
                config.auth_settings.channel_binding = mode;
            }
            "auth_scram" => {
                let enabled = parse_bool_param(&value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid auth_scram value: {}", value))
                })?;
                config.auth_settings.allow_scram_sha_256 = enabled;
            }
            "auth_md5" => {
                let enabled = parse_bool_param(&value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid auth_md5 value: {}", value))
                })?;
                config.auth_settings.allow_md5_password = enabled;
            }
            "auth_cleartext" => {
                let enabled = parse_bool_param(&value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid auth_cleartext value: {}", value))
                })?;
                config.auth_settings.allow_cleartext_password = enabled;
            }
            "auth_kerberos" => {
                let enabled = parse_bool_param(&value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid auth_kerberos value: {}", value))
                })?;
                config.auth_settings.allow_kerberos_v5 = enabled;
            }
            "auth_gssapi" => {
                let enabled = parse_bool_param(&value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid auth_gssapi value: {}", value))
                })?;
                config.auth_settings.allow_gssapi = enabled;
            }
            "auth_sspi" => {
                let enabled = parse_bool_param(&value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid auth_sspi value: {}", value))
                })?;
                config.auth_settings.allow_sspi = enabled;
            }
            "auth_mode" => {
                if value.eq_ignore_ascii_case("scram_only") {
                    config.auth_settings = AuthSettings::scram_only();
                } else if value.eq_ignore_ascii_case("gssapi_only") {
                    config.auth_settings = AuthSettings::gssapi_only();
                } else if value.eq_ignore_ascii_case("compat")
                    || value.eq_ignore_ascii_case("default")
                {
                    config.auth_settings = AuthSettings::default();
                } else {
                    return Err(PgError::Connection(format!(
                        "Invalid auth_mode value: {}",
                        value
                    )));
                }
            }
            "gss_provider" => gss_provider = Some(value.clone()),
            "gss_service" => {
                if value.is_empty() {
                    return Err(PgError::Connection(
                        "gss_service must not be empty".to_string(),
                    ));
                }
                gss_service = value.clone();
            }
            // libpq alias for kerberos service principal name component.
            "krbsrvname" => {
                if value.is_empty() {
                    return Err(PgError::Connection(
                        "gss_service must not be empty".to_string(),
                    ));
                }
                gss_service = value.clone();
            }
            "gss_target" => {
                if value.is_empty() {
                    return Err(PgError::Connection(
                        "gss_target must not be empty".to_string(),
                    ));
                }
                gss_target = Some(value.clone());
            }
            // libpq alias for GSS target hostname override.
            "gsshostname" => {
                if value.is_empty() {
                    return Err(PgError::Connection(
                        "gss_target must not be empty".to_string(),
                    ));
                }
                gss_target = Some(value.clone());
            }
            // libpq compatibility knob; accepted values are validated but
            // provider selection remains controlled by qail `gss_provider`.
            "gsslib" => match value.trim().to_ascii_lowercase().as_str() {
                "gssapi" | "sspi" => {}
                _ => {
                    return Err(PgError::Connection(format!(
                        "Invalid gsslib value: {} (expected gssapi or sspi)",
                        value
                    )));
                }
            },
            "gss_connect_retries" => {
                let retries = value.parse::<usize>().map_err(|_| {
                    PgError::Connection(format!("Invalid gss_connect_retries value: {}", value))
                })?;
                if retries > 20 {
                    return Err(PgError::Connection(
                        "gss_connect_retries must be <= 20".to_string(),
                    ));
                }
                config.gss_connect_retries = retries;
            }
            "gss_retry_base_ms" => {
                let delay_ms = value.parse::<u64>().map_err(|_| {
                    PgError::Connection(format!("Invalid gss_retry_base_ms value: {}", value))
                })?;
                if delay_ms == 0 {
                    return Err(PgError::Connection(
                        "gss_retry_base_ms must be greater than 0".to_string(),
                    ));
                }
                config.gss_retry_base_delay = Duration::from_millis(delay_ms);
            }
            "gss_circuit_threshold" => {
                let threshold = value.parse::<usize>().map_err(|_| {
                    PgError::Connection(format!("Invalid gss_circuit_threshold value: {}", value))
                })?;
                if threshold > 100 {
                    return Err(PgError::Connection(
                        "gss_circuit_threshold must be <= 100".to_string(),
                    ));
                }
                config.gss_circuit_breaker_threshold = threshold;
            }
            "gss_circuit_window_ms" => {
                let window_ms = value.parse::<u64>().map_err(|_| {
                    PgError::Connection(format!("Invalid gss_circuit_window_ms value: {}", value))
                })?;
                if window_ms == 0 {
                    return Err(PgError::Connection(
                        "gss_circuit_window_ms must be greater than 0".to_string(),
                    ));
                }
                config.gss_circuit_breaker_window = Duration::from_millis(window_ms);
            }
            "gss_circuit_cooldown_ms" => {
                let cooldown_ms = value.parse::<u64>().map_err(|_| {
                    PgError::Connection(format!("Invalid gss_circuit_cooldown_ms value: {}", value))
                })?;
                if cooldown_ms == 0 {
                    return Err(PgError::Connection(
                        "gss_circuit_cooldown_ms must be greater than 0".to_string(),
                    ));
                }
                config.gss_circuit_breaker_cooldown = Duration::from_millis(cooldown_ms);
            }
            _ => {}
        }
    }

    match (sslcert.as_deref(), sslkey.as_deref()) {
        (Some(cert_path), Some(key_path)) => {
            let mtls = TlsConfig {
                client_cert_pem: std::fs::read(cert_path).map_err(|e| {
                    PgError::Connection(format!("Failed to read sslcert '{}': {}", cert_path, e))
                })?,
                client_key_pem: std::fs::read(key_path).map_err(|e| {
                    PgError::Connection(format!("Failed to read sslkey '{}': {}", key_path, e))
                })?,
                ca_cert_pem: config.tls_ca_cert_pem.clone(),
            };
            config.mtls = Some(mtls);
            config.tls_mode = TlsMode::Require;
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(PgError::Connection(
                "Both sslcert and sslkey must be provided together".to_string(),
            ));
        }
        (None, None) => {}
    }

    if let Some(provider) = gss_provider {
        if provider.eq_ignore_ascii_case("linux_krb5") || provider.eq_ignore_ascii_case("builtin") {
            #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
            {
                let provider = crate::driver::gss::linux_krb5_token_provider(
                    crate::driver::gss::LinuxKrb5ProviderConfig {
                        host: host.to_string(),
                        service: gss_service,
                        target_name: gss_target,
                    },
                )
                .map_err(PgError::Auth)?;
                config.gss_token_provider_ex = Some(provider);
            }
            #[cfg(not(all(feature = "enterprise-gssapi", target_os = "linux")))]
            {
                let _ = gss_service;
                let _ = gss_target;
                return Err(PgError::Connection(
                    "gss_provider=linux_krb5 requires qail-pg feature enterprise-gssapi on Linux"
                        .to_string(),
                ));
            }
        } else if provider.eq_ignore_ascii_case("callback")
            || provider.eq_ignore_ascii_case("custom")
        {
            // External callback wiring is handled by application code.
        } else {
            return Err(PgError::Connection(format!(
                "Invalid gss_provider value: {}",
                provider
            )));
        }
    }

    Ok(())
}

/// Parse a postgres URL into (host, port, user, database, password).
pub(super) fn parse_pg_url(url: &str) -> PgResult<(String, u16, String, String, Option<String>)> {
    let url = url.split('?').next().unwrap_or(url);
    let url = if let Some(rest) = url.strip_prefix("postgres://") {
        rest
    } else if let Some(rest) = url.strip_prefix("postgresql://") {
        rest
    } else {
        return Err(PgError::Connection(
            "PostgreSQL URL must start with postgres:// or postgresql://".to_string(),
        ));
    };

    let (credentials, host_part) = if let Some(at_pos) = url.rfind('@') {
        let creds = &url[..at_pos];
        let host = &url[at_pos + 1..];
        (Some(creds), host)
    } else {
        (None, url)
    };

    let (host_port, database) = if host_part.contains('/') {
        let mut parts = host_part.splitn(2, '/');
        let host_port = parts.next().unwrap_or("localhost");
        let database = percent_decode(parts.next().unwrap_or("postgres"))?;
        if database.is_empty() {
            return Err(PgError::Connection(
                "Invalid PostgreSQL URL database: missing database name".to_string(),
            ));
        }
        (host_port, database)
    } else {
        (host_part, "postgres".to_string())
    };

    let (host, port) = if host_port.starts_with('[') {
        let end = host_port.find(']').ok_or_else(|| {
            PgError::Connection("Invalid PostgreSQL URL IPv6 host: missing ']'".to_string())
        })?;
        let host = &host_port[..=end];
        if host == "[]" {
            return Err(PgError::Connection(
                "Invalid PostgreSQL URL host: missing host".to_string(),
            ));
        }
        let suffix = &host_port[end + 1..];
        let port = if suffix.is_empty() {
            5432u16
        } else if let Some(port_str) = suffix.strip_prefix(':') {
            if port_str.is_empty() {
                return Err(PgError::Connection(
                    "Invalid PostgreSQL URL port: missing port after ':'".to_string(),
                ));
            }
            let p = port_str.parse::<u16>().map_err(|_| {
                PgError::Connection(format!(
                    "Invalid PostgreSQL URL port '{}': expected a number from 1 to 65535",
                    port_str
                ))
            })?;
            if p == 0 {
                return Err(PgError::Connection(
                    "Invalid PostgreSQL URL port '0': expected a number from 1 to 65535"
                        .to_string(),
                ));
            }
            p
        } else {
            return Err(PgError::Connection(
                "Invalid PostgreSQL URL IPv6 host: unexpected characters after ']'".to_string(),
            ));
        };
        (host.to_string(), port)
    } else if host_port.contains(':') {
        let mut parts = host_port.splitn(2, ':');
        let h = parts.next().unwrap_or("localhost").to_string();
        if h.is_empty() {
            return Err(PgError::Connection(
                "Invalid PostgreSQL URL host: missing host".to_string(),
            ));
        }
        let port_str = parts.next().unwrap_or("");
        if port_str.is_empty() {
            return Err(PgError::Connection(
                "Invalid PostgreSQL URL port: missing port after ':'".to_string(),
            ));
        }
        let p = port_str.parse::<u16>().map_err(|_| {
            PgError::Connection(format!(
                "Invalid PostgreSQL URL port '{}': expected a number from 1 to 65535",
                port_str
            ))
        })?;
        if p == 0 {
            return Err(PgError::Connection(
                "Invalid PostgreSQL URL port '0': expected a number from 1 to 65535".to_string(),
            ));
        }
        (h, p)
    } else {
        if host_port.is_empty() {
            return Err(PgError::Connection(
                "Invalid PostgreSQL URL host: missing host".to_string(),
            ));
        }
        (host_port.to_string(), 5432u16)
    };

    let (user, password) = if let Some(creds) = credentials {
        if creds.contains(':') {
            let mut parts = creds.splitn(2, ':');
            let u = percent_decode(parts.next().unwrap_or("postgres"))?;
            if u.is_empty() {
                return Err(PgError::Connection(
                    "Invalid PostgreSQL URL user: missing user".to_string(),
                ));
            }
            let p = parts.next().map(percent_decode).transpose()?;
            (u, p)
        } else {
            let u = percent_decode(creds)?;
            if u.is_empty() {
                return Err(PgError::Connection(
                    "Invalid PostgreSQL URL user: missing user".to_string(),
                ));
            }
            (u, None)
        }
    } else {
        ("postgres".to_string(), None)
    };

    Ok((host, port, user, database, password))
}

fn percent_decode(s: &str) -> PgResult<String> {
    fn hex_value(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = s.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(PgError::Connection(
                    "Invalid PostgreSQL URL percent-encoding: '%' must be followed by two hex digits"
                        .to_string(),
                ));
            }
            let (Some(hi), Some(lo)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) else {
                return Err(PgError::Connection(
                    "Invalid PostgreSQL URL percent-encoding: '%' must be followed by two hex digits"
                        .to_string(),
                ));
            };
            decoded.push((hi << 4) | lo);
            i += 3;
        } else {
            decoded.push(bytes[i]);
            i += 1;
        }
    }

    String::from_utf8(decoded).map_err(|_| {
        PgError::Connection(
            "Invalid PostgreSQL URL percent-encoding: decoded value is not UTF-8".to_string(),
        )
    })
}

pub(super) fn parse_bool_param(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
