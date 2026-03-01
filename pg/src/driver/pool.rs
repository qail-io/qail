//! PostgreSQL Connection Pool
//!
//! Provides connection pooling for efficient resource management.
//! Connections are reused across queries to avoid reconnection overhead.

use super::{
    AuthSettings, ConnectOptions, GssEncMode, GssTokenProvider, GssTokenProviderEx, PgConnection,
    PgError, PgResult, ResultFormat, ScramChannelBindingMode, TlsConfig, TlsMode,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};

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
            .test_on_acquire(pg.test_on_acquire);

        if let Some(ref pw) = password {
            config = config.password(pw);
        }

        // Optional URL query params for enterprise auth/TLS settings.
        if let Some(query) = pg.url.split('?').nth(1) {
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
        let key = kv.next().unwrap_or_default().trim();
        let value = kv.next().unwrap_or_default().trim();

        match key {
            "sslmode" => {
                let mode = TlsMode::parse_sslmode(value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid sslmode value: {}", value))
                })?;
                config.tls_mode = mode;
            }
            "gssencmode" => {
                let mode = GssEncMode::parse_gssencmode(value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid gssencmode value: {}", value))
                })?;
                config.gss_enc_mode = mode;
            }
            "sslrootcert" => {
                let ca_pem = std::fs::read(value).map_err(|e| {
                    PgError::Connection(format!("Failed to read sslrootcert '{}': {}", value, e))
                })?;
                config.tls_ca_cert_pem = Some(ca_pem);
            }
            "sslcert" => sslcert = Some(value.to_string()),
            "sslkey" => sslkey = Some(value.to_string()),
            "channel_binding" => {
                let mode = ScramChannelBindingMode::parse(value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid channel_binding value: {}", value))
                })?;
                config.auth_settings.channel_binding = mode;
            }
            "auth_scram" => {
                let enabled = parse_bool_param(value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid auth_scram value: {}", value))
                })?;
                config.auth_settings.allow_scram_sha_256 = enabled;
            }
            "auth_md5" => {
                let enabled = parse_bool_param(value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid auth_md5 value: {}", value))
                })?;
                config.auth_settings.allow_md5_password = enabled;
            }
            "auth_cleartext" => {
                let enabled = parse_bool_param(value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid auth_cleartext value: {}", value))
                })?;
                config.auth_settings.allow_cleartext_password = enabled;
            }
            "auth_kerberos" => {
                let enabled = parse_bool_param(value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid auth_kerberos value: {}", value))
                })?;
                config.auth_settings.allow_kerberos_v5 = enabled;
            }
            "auth_gssapi" => {
                let enabled = parse_bool_param(value).ok_or_else(|| {
                    PgError::Connection(format!("Invalid auth_gssapi value: {}", value))
                })?;
                config.auth_settings.allow_gssapi = enabled;
            }
            "auth_sspi" => {
                let enabled = parse_bool_param(value).ok_or_else(|| {
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
            "gss_provider" => gss_provider = Some(value.to_string()),
            "gss_service" => {
                if value.is_empty() {
                    return Err(PgError::Connection(
                        "gss_service must not be empty".to_string(),
                    ));
                }
                gss_service = value.to_string();
            }
            "gss_target" => {
                if value.is_empty() {
                    return Err(PgError::Connection(
                        "gss_target must not be empty".to_string(),
                    ));
                }
                gss_target = Some(value.to_string());
            }
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
                let provider =
                    super::gss::linux_krb5_token_provider(super::gss::LinuxKrb5ProviderConfig {
                        host: host.to_string(),
                        service: gss_service.clone(),
                        target_name: gss_target.clone(),
                    })
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
fn parse_pg_url(url: &str) -> PgResult<(String, u16, String, String, Option<String>)> {
    let url = url.split('?').next().unwrap_or(url);
    let url = url
        .trim_start_matches("postgres://")
        .trim_start_matches("postgresql://");

    let (credentials, host_part) = if url.contains('@') {
        let mut parts = url.splitn(2, '@');
        let creds = parts.next().unwrap_or("");
        let host = parts.next().unwrap_or("localhost/postgres");
        (Some(creds), host)
    } else {
        (None, url)
    };

    let (host_port, database) = if host_part.contains('/') {
        let mut parts = host_part.splitn(2, '/');
        (
            parts.next().unwrap_or("localhost"),
            parts.next().unwrap_or("postgres").to_string(),
        )
    } else {
        (host_part, "postgres".to_string())
    };

    let (host, port) = if host_port.contains(':') {
        let mut parts = host_port.split(':');
        let h = parts.next().unwrap_or("localhost").to_string();
        let p = parts.next().and_then(|s| s.parse().ok()).unwrap_or(5432u16);
        (h, p)
    } else {
        (host_port.to_string(), 5432u16)
    };

    let (user, password) = if let Some(creds) = credentials {
        if creds.contains(':') {
            let mut parts = creds.splitn(2, ':');
            let u = parts.next().unwrap_or("postgres").to_string();
            let p = parts.next().map(|s| s.to_string());
            (u, p)
        } else {
            (creds.to_string(), None)
        }
    } else {
        ("postgres".to_string(), None)
    };

    Ok((host, port, user, database, password))
}

fn parse_bool_param(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Pool statistics for monitoring.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Connections currently checked out by callers.
    pub active: usize,
    /// Connections idle in the pool, ready for reuse.
    pub idle: usize,
    /// Callers waiting for a connection.
    pub pending: usize,
    /// Maximum connections configured
    pub max_size: usize,
    /// Cumulative connections created since pool startup.
    pub total_created: usize,
}

/// A pooled connection with creation timestamp for idle tracking.
struct PooledConn {
    conn: PgConnection,
    created_at: Instant,
    last_used: Instant,
}

/// A pooled connection that returns to the pool when dropped.
///
/// When `rls_dirty` is true (set by `acquire_with_rls`), the connection
/// will automatically reset RLS session variables before returning to
/// the pool. This prevents cross-tenant data leakage.
pub struct PooledConnection {
    conn: Option<PgConnection>,
    pool: Arc<PgPoolInner>,
    rls_dirty: bool,
}

impl PooledConnection {
    /// Get a reference to the underlying connection, returning an error
    /// if the connection has already been released.
    fn conn_ref(&self) -> PgResult<&PgConnection> {
        self.conn
            .as_ref()
            .ok_or_else(|| PgError::Connection("Connection already released back to pool".into()))
    }

    /// Get a mutable reference to the underlying connection, returning an error
    /// if the connection has already been released.
    fn conn_mut(&mut self) -> PgResult<&mut PgConnection> {
        self.conn
            .as_mut()
            .ok_or_else(|| PgError::Connection("Connection already released back to pool".into()))
    }

    /// Get a mutable reference to the underlying connection.
    /// Panics if the connection has been released (use `conn_mut()` for fallible access).
    pub fn get_mut(&mut self) -> &mut PgConnection {
        // SAFETY: Connection is always Some while PooledConnection is in use.
        // Only becomes None after release() or Drop, after which no methods should be called.
        self.conn
            .as_mut()
            .expect("Connection should always be present")
    }

    /// Get a token to cancel the currently running query.
    pub fn cancel_token(&self) -> PgResult<crate::driver::CancelToken> {
        let conn = self.conn_ref()?;
        let (process_id, secret_key) = conn.get_cancel_key();
        Ok(crate::driver::CancelToken {
            host: self.pool.config.host.clone(),
            port: self.pool.config.port,
            process_id,
            secret_key,
        })
    }

    /// Deterministic connection cleanup and pool return.
    ///
    /// This is the **correct** way to return a connection to the pool.
    /// COMMITs the transaction (which auto-resets transaction-local RLS
    /// session variables) and returns the connection to the pool with
    /// prepared statement caches intact.
    ///
    /// If cleanup fails, the connection is destroyed (not returned to pool).
    ///
    /// # Usage
    /// ```ignore
    /// let mut conn = pool.acquire_with_rls(ctx).await?;
    /// let result = conn.fetch_all_cached(&cmd).await;
    /// conn.release().await; // COMMIT + return to pool
    /// result
    /// ```
    pub async fn release(mut self) {
        if let Some(mut conn) = self.conn.take() {
            // COMMIT the transaction opened by acquire_with_rls.
            // Transaction-local set_config values auto-reset on COMMIT,
            // so no explicit RLS cleanup is needed.
            // Prepared statements survive — they are NOT transaction-scoped.
            if let Err(e) = conn.execute_simple(super::rls::reset_sql()).await {
                eprintln!(
                    "[CRITICAL] pool_release_failed: COMMIT failed — \
                     dropping connection to prevent state leak: {}",
                    e
                );
                return; // Connection destroyed — not returned to pool
            }

            self.pool.return_connection(conn).await;
        }
    }

    /// Execute a QAIL command and fetch all rows (UNCACHED).
    /// Returns rows with column metadata for JSON serialization.
    pub async fn fetch_all_uncached(
        &mut self,
        cmd: &qail_core::ast::Qail,
    ) -> PgResult<Vec<super::PgRow>> {
        self.fetch_all_uncached_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute raw SQL with bind parameters and return raw row data.
    ///
    /// Uses the Extended Query Protocol so parameters are never interpolated
    /// into the SQL string. Intended for EXPLAIN or other SQL that can't be
    /// represented as a `Qail` AST but still needs parameterized execution.
    ///
    /// Returns raw column bytes; callers must decode as needed.
    pub async fn query_raw_with_params(
        &mut self,
        sql: &str,
        params: &[Option<Vec<u8>>],
    ) -> PgResult<Vec<Vec<Option<Vec<u8>>>>> {
        let conn = self.conn_mut()?;
        conn.query(sql, params).await
    }

    /// Execute a QAIL command and fetch all rows (UNCACHED) with explicit result format.
    pub async fn fetch_all_uncached_with_format(
        &mut self,
        cmd: &qail_core::ast::Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<super::PgRow>> {
        use super::ColumnInfo;
        use crate::protocol::AstEncoder;

        let conn = self.conn_mut()?;

        AstEncoder::encode_cmd_reuse_into_with_result_format(
            cmd,
            &mut conn.sql_buf,
            &mut conn.params_buf,
            &mut conn.write_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

        conn.flush_write_buf().await?;

        let mut rows: Vec<super::PgRow> = Vec::new();
        let mut column_info: Option<Arc<ColumnInfo>> = None;
        let mut error: Option<PgError> = None;

        loop {
            let msg = conn.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    column_info = Some(Arc::new(ColumnInfo::from_fields(&fields)));
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(super::PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                _ => {}
            }
        }
    }

    /// Execute a QAIL command and fetch all rows (FAST VERSION).
    /// Uses native AST-to-wire encoding and optimized recv_with_data_fast.
    /// Skips column metadata for maximum speed.
    pub async fn fetch_all_fast(
        &mut self,
        cmd: &qail_core::ast::Qail,
    ) -> PgResult<Vec<super::PgRow>> {
        self.fetch_all_fast_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command and fetch all rows (FAST VERSION) with explicit result format.
    pub async fn fetch_all_fast_with_format(
        &mut self,
        cmd: &qail_core::ast::Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<super::PgRow>> {
        use crate::protocol::AstEncoder;

        let conn = self.conn_mut()?;

        AstEncoder::encode_cmd_reuse_into_with_result_format(
            cmd,
            &mut conn.sql_buf,
            &mut conn.params_buf,
            &mut conn.write_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;

        conn.flush_write_buf().await?;

        let mut rows: Vec<super::PgRow> = Vec::with_capacity(32);
        let mut error: Option<PgError> = None;

        loop {
            let res = conn.recv_with_data_fast().await;
            match res {
                Ok((msg_type, data)) => match msg_type {
                    b'D' => {
                        if error.is_none()
                            && let Some(columns) = data
                        {
                            rows.push(super::PgRow {
                                columns,
                                column_info: None,
                            });
                        }
                    }
                    b'Z' => {
                        if let Some(err) = error {
                            return Err(err);
                        }
                        return Ok(rows);
                    }
                    _ => {}
                },
                Err(e) => {
                    if error.is_none() {
                        error = Some(e);
                    }
                }
            }
        }
    }

    /// Execute a QAIL command and fetch all rows (CACHED).
    /// Uses prepared statement caching: Parse+Describe on first call,
    /// then Bind+Execute only on subsequent calls with the same SQL shape.
    /// This matches PostgREST's behavior for fair benchmarks.
    pub async fn fetch_all_cached(
        &mut self,
        cmd: &qail_core::ast::Qail,
    ) -> PgResult<Vec<super::PgRow>> {
        self.fetch_all_cached_with_format(cmd, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command and fetch all rows (CACHED) with explicit result format.
    pub async fn fetch_all_cached_with_format(
        &mut self,
        cmd: &qail_core::ast::Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<super::PgRow>> {
        let mut retried = false;
        loop {
            match self
                .fetch_all_cached_with_format_once(cmd, result_format)
                .await
            {
                Ok(rows) => return Ok(rows),
                Err(err) if !retried && err.is_prepared_statement_retryable() => {
                    retried = true;
                    if let Some(conn) = self.conn.as_mut() {
                        conn.clear_prepared_statement_state();
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn fetch_all_cached_with_format_once(
        &mut self,
        cmd: &qail_core::ast::Qail,
        result_format: ResultFormat,
    ) -> PgResult<Vec<super::PgRow>> {
        use super::ColumnInfo;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let conn = self.conn.as_mut().ok_or_else(|| {
            PgError::Connection("Connection already released back to pool".into())
        })?;

        conn.sql_buf.clear();
        conn.params_buf.clear();

        // Encode SQL + params to reusable buffers
        match cmd.action {
            qail_core::ast::Action::Get | qail_core::ast::Action::With => {
                crate::protocol::ast_encoder::dml::encode_select(
                    cmd,
                    &mut conn.sql_buf,
                    &mut conn.params_buf,
                )?;
            }
            qail_core::ast::Action::Add => {
                crate::protocol::ast_encoder::dml::encode_insert(
                    cmd,
                    &mut conn.sql_buf,
                    &mut conn.params_buf,
                )?;
            }
            qail_core::ast::Action::Set => {
                crate::protocol::ast_encoder::dml::encode_update(
                    cmd,
                    &mut conn.sql_buf,
                    &mut conn.params_buf,
                )?;
            }
            qail_core::ast::Action::Del => {
                crate::protocol::ast_encoder::dml::encode_delete(
                    cmd,
                    &mut conn.sql_buf,
                    &mut conn.params_buf,
                )?;
            }
            _ => {
                // Fallback: unsupported actions go through uncached path
                return self
                    .fetch_all_uncached_with_format(cmd, result_format)
                    .await;
            }
        }

        let mut hasher = DefaultHasher::new();
        conn.sql_buf.hash(&mut hasher);
        let sql_hash = hasher.finish();

        let is_cache_miss = !conn.stmt_cache.contains(&sql_hash);

        conn.write_buf.clear();

        let stmt_name = if let Some(name) = conn.stmt_cache.get(&sql_hash) {
            name
        } else {
            let name = format!("qail_{:x}", sql_hash);

            conn.evict_prepared_if_full();

            let sql_str = std::str::from_utf8(&conn.sql_buf).unwrap_or("");

            use crate::protocol::PgEncoder;
            let parse_msg = PgEncoder::encode_parse(&name, sql_str, &[]);
            let describe_msg = PgEncoder::encode_describe(false, &name);
            conn.write_buf.extend_from_slice(&parse_msg);
            conn.write_buf.extend_from_slice(&describe_msg);

            conn.stmt_cache.put(sql_hash, name.clone());
            conn.prepared_statements
                .insert(name.clone(), sql_str.to_string());

            // Register in global hot-statement registry for cross-connection sharing
            if let Ok(mut hot) = self.pool.hot_statements.write()
                && hot.len() < MAX_HOT_STATEMENTS
            {
                hot.insert(sql_hash, (name.clone(), sql_str.to_string()));
            }

            name
        };

        use crate::protocol::PgEncoder;
        PgEncoder::encode_bind_to_with_result_format(
            &mut conn.write_buf,
            &stmt_name,
            &conn.params_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut conn.write_buf);
        PgEncoder::encode_sync_to(&mut conn.write_buf);

        conn.flush_write_buf().await?;

        let cached_column_info = conn.column_info_cache.get(&sql_hash).cloned();

        let mut rows: Vec<super::PgRow> = Vec::with_capacity(32);
        let mut column_info: Option<Arc<ColumnInfo>> = cached_column_info;
        let mut error: Option<PgError> = None;

        loop {
            let msg = conn.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::ParameterDescription(_) => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    let info = Arc::new(ColumnInfo::from_fields(&fields));
                    if is_cache_miss {
                        conn.column_info_cache.insert(sql_hash, info.clone());
                    }
                    column_info = Some(info);
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(super::PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                _ => {}
            }
        }
    }

    /// Execute a QAIL command with RLS context in a SINGLE roundtrip.
    ///
    /// Pipelines the RLS setup (BEGIN + set_config) and the query
    /// (Parse/Bind/Execute/Sync) into one `write_all` syscall.
    /// PG processes messages in order, so the BEGIN + set_config
    /// completes before the query executes — security is preserved.
    ///
    /// Wire layout:
    /// ```text
    /// [SimpleQuery: "BEGIN; SET LOCAL...; SELECT set_config(...)"]
    /// [Parse (if cache miss)]
    /// [Describe (if cache miss)]
    /// [Bind]
    /// [Execute]
    /// [Sync]
    /// ```
    ///
    /// Response processing: consume 2× ReadyForQuery (SimpleQuery + Sync).
    pub async fn fetch_all_with_rls(
        &mut self,
        cmd: &qail_core::ast::Qail,
        rls_sql: &str,
    ) -> PgResult<Vec<super::PgRow>> {
        self.fetch_all_with_rls_with_format(cmd, rls_sql, ResultFormat::Text)
            .await
    }

    /// Execute a QAIL command with RLS context in a SINGLE roundtrip with explicit result format.
    pub async fn fetch_all_with_rls_with_format(
        &mut self,
        cmd: &qail_core::ast::Qail,
        rls_sql: &str,
        result_format: ResultFormat,
    ) -> PgResult<Vec<super::PgRow>> {
        let mut retried = false;
        loop {
            match self
                .fetch_all_with_rls_with_format_once(cmd, rls_sql, result_format)
                .await
            {
                Ok(rows) => return Ok(rows),
                Err(err) if !retried && err.is_prepared_statement_retryable() => {
                    retried = true;
                    if let Some(conn) = self.conn.as_mut() {
                        conn.clear_prepared_statement_state();
                        let _ = conn.execute_simple("ROLLBACK").await;
                    }
                    self.rls_dirty = false;
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn fetch_all_with_rls_with_format_once(
        &mut self,
        cmd: &qail_core::ast::Qail,
        rls_sql: &str,
        result_format: ResultFormat,
    ) -> PgResult<Vec<super::PgRow>> {
        use super::ColumnInfo;
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let conn = self.conn.as_mut().ok_or_else(|| {
            PgError::Connection("Connection already released back to pool".into())
        })?;

        conn.sql_buf.clear();
        conn.params_buf.clear();

        // Encode SQL + params to reusable buffers
        if cmd.is_raw_sql() {
            // Raw SQL pass-through: write verbatim, RLS context already set above
            conn.sql_buf.clear();
            conn.params_buf.clear();
            conn.sql_buf.extend_from_slice(cmd.table.as_bytes());
        } else {
            match cmd.action {
                qail_core::ast::Action::Get | qail_core::ast::Action::With => {
                    crate::protocol::ast_encoder::dml::encode_select(
                        cmd,
                        &mut conn.sql_buf,
                        &mut conn.params_buf,
                    )?;
                }
                qail_core::ast::Action::Add => {
                    crate::protocol::ast_encoder::dml::encode_insert(
                        cmd,
                        &mut conn.sql_buf,
                        &mut conn.params_buf,
                    )?;
                }
                qail_core::ast::Action::Set => {
                    crate::protocol::ast_encoder::dml::encode_update(
                        cmd,
                        &mut conn.sql_buf,
                        &mut conn.params_buf,
                    )?;
                }
                qail_core::ast::Action::Del => {
                    crate::protocol::ast_encoder::dml::encode_delete(
                        cmd,
                        &mut conn.sql_buf,
                        &mut conn.params_buf,
                    )?;
                }
                _ => {
                    // Fallback: RLS setup must happen synchronously for unsupported actions
                    conn.execute_simple(rls_sql).await?;
                    self.rls_dirty = true;
                    return self
                        .fetch_all_uncached_with_format(cmd, result_format)
                        .await;
                }
            }
        }

        let mut hasher = DefaultHasher::new();
        conn.sql_buf.hash(&mut hasher);
        let sql_hash = hasher.finish();

        let is_cache_miss = !conn.stmt_cache.contains(&sql_hash);

        conn.write_buf.clear();

        // ── Prepend RLS Simple Query message ─────────────────────────
        // This is the key optimization: RLS setup bytes go first in the
        // same buffer as the query messages.
        let rls_msg = crate::protocol::PgEncoder::encode_query_string(rls_sql);
        conn.write_buf.extend_from_slice(&rls_msg);

        // ── Then append the query messages (same as fetch_all_cached) ──
        let stmt_name = if let Some(name) = conn.stmt_cache.get(&sql_hash) {
            name
        } else {
            let name = format!("qail_{:x}", sql_hash);

            conn.evict_prepared_if_full();

            let sql_str = std::str::from_utf8(&conn.sql_buf).unwrap_or("");

            use crate::protocol::PgEncoder;
            let parse_msg = PgEncoder::encode_parse(&name, sql_str, &[]);
            let describe_msg = PgEncoder::encode_describe(false, &name);
            conn.write_buf.extend_from_slice(&parse_msg);
            conn.write_buf.extend_from_slice(&describe_msg);

            conn.stmt_cache.put(sql_hash, name.clone());
            conn.prepared_statements
                .insert(name.clone(), sql_str.to_string());

            if let Ok(mut hot) = self.pool.hot_statements.write()
                && hot.len() < MAX_HOT_STATEMENTS
            {
                hot.insert(sql_hash, (name.clone(), sql_str.to_string()));
            }

            name
        };

        use crate::protocol::PgEncoder;
        PgEncoder::encode_bind_to_with_result_format(
            &mut conn.write_buf,
            &stmt_name,
            &conn.params_buf,
            result_format.as_wire_code(),
        )
        .map_err(|e| PgError::Encode(e.to_string()))?;
        PgEncoder::encode_execute_to(&mut conn.write_buf);
        PgEncoder::encode_sync_to(&mut conn.write_buf);

        // ── Single write_all for RLS + Query ────────────────────────
        conn.flush_write_buf().await?;

        // Mark connection as RLS-dirty (needs COMMIT on release)
        self.rls_dirty = true;

        // ── Phase 1: Consume Simple Query responses (RLS setup) ─────
        // Simple Query produces: CommandComplete × N, then ReadyForQuery.
        // set_config results and BEGIN/SET LOCAL responses are all here.
        let mut rls_error: Option<PgError> = None;
        loop {
            let msg = conn.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    // RLS setup done — break to Extended Query phase
                    if let Some(err) = rls_error {
                        return Err(err);
                    }
                    break;
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if rls_error.is_none() {
                        rls_error = Some(PgError::QueryServer(err.into()));
                    }
                }
                // CommandComplete, DataRow (from set_config), RowDescription — ignore
                _ => {}
            }
        }

        // ── Phase 2: Consume Extended Query responses (actual data) ──
        let cached_column_info = conn.column_info_cache.get(&sql_hash).cloned();

        let mut rows: Vec<super::PgRow> = Vec::with_capacity(32);
        let mut column_info: Option<std::sync::Arc<ColumnInfo>> = cached_column_info;
        let mut error: Option<PgError> = None;

        loop {
            let msg = conn.recv().await?;
            match msg {
                crate::protocol::BackendMessage::ParseComplete
                | crate::protocol::BackendMessage::BindComplete => {}
                crate::protocol::BackendMessage::ParameterDescription(_) => {}
                crate::protocol::BackendMessage::RowDescription(fields) => {
                    let info = std::sync::Arc::new(ColumnInfo::from_fields(&fields));
                    if is_cache_miss {
                        conn.column_info_cache.insert(sql_hash, info.clone());
                    }
                    column_info = Some(info);
                }
                crate::protocol::BackendMessage::DataRow(data) => {
                    if error.is_none() {
                        rows.push(super::PgRow {
                            columns: data,
                            column_info: column_info.clone(),
                        });
                    }
                }
                crate::protocol::BackendMessage::CommandComplete(_) => {}
                crate::protocol::BackendMessage::ReadyForQuery(_) => {
                    if let Some(err) = error {
                        return Err(err);
                    }
                    return Ok(rows);
                }
                crate::protocol::BackendMessage::ErrorResponse(err) => {
                    if error.is_none() {
                        error = Some(PgError::QueryServer(err.into()));
                    }
                }
                _ => {}
            }
        }
    }

    /// Execute multiple QAIL commands in a single PG pipeline round-trip.
    ///
    /// Sends all queries as Parse+Bind+Execute in one write, receives all
    /// responses in one read. Returns raw column data per query per row.
    ///
    /// This is the fastest path for batch operations — amortizes TCP
    /// overhead across N queries into a single syscall pair.
    pub async fn pipeline_ast(
        &mut self,
        cmds: &[qail_core::ast::Qail],
    ) -> PgResult<Vec<Vec<Vec<Option<Vec<u8>>>>>> {
        let conn = self.conn_mut()?;
        conn.pipeline_ast(cmds).await
    }

    /// Run `EXPLAIN (FORMAT JSON)` on a Qail command and return cost estimates.
    ///
    /// Uses `simple_query` under the hood — no additional round-trips beyond
    /// the single EXPLAIN statement. Returns `None` if parsing fails or
    /// the EXPLAIN output is unexpected.
    pub async fn explain_estimate(
        &mut self,
        cmd: &qail_core::ast::Qail,
    ) -> PgResult<Option<super::explain::ExplainEstimate>> {
        use qail_core::transpiler::ToSql;

        let sql = cmd.to_sql();
        let explain_sql = format!("EXPLAIN (FORMAT JSON) {}", sql);

        let rows = self.simple_query(&explain_sql).await?;

        // PostgreSQL returns the JSON plan as a single text column across one or more rows
        let mut json_output = String::new();
        for row in &rows {
            if let Some(Some(val)) = row.columns.first()
                && let Ok(text) = std::str::from_utf8(val)
            {
                json_output.push_str(text);
            }
        }

        Ok(super::explain::parse_explain_json(&json_output))
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if self.conn.is_some() {
            // Safety net: connection was NOT released via `release()`.
            // This happens when:
            //   - Handler panicked
            //   - Early return without calling release()
            //   - Missed release() call (programming error)
            //
            // We DESTROY the connection (don't return to pool) to prevent
            // dirty session state from being reused. But we MUST return the
            // semaphore permit so the pool can create a replacement connection
            // on the next acquire. Without this, leaked connections permanently
            // reduce pool capacity until all slots are consumed.
            //
            // The `conn` field is dropped here, closing the TCP socket.
            eprintln!(
                "[WARN] pool_connection_leaked: PooledConnection dropped without release() — \
                 connection destroyed to prevent state leak (rls_dirty={}). \
                 Use conn.release().await for deterministic cleanup.",
                self.rls_dirty
            );
            // Decrement active count so pool can create a replacement
            self.pool
                .active_count
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            // Return the semaphore permit so the pool slot can be reused.
            // Without this, each leaked connection permanently reduces capacity.
            self.pool.semaphore.add_permits(1);
        }
    }
}

impl std::ops::Deref for PooledConnection {
    type Target = PgConnection;

    fn deref(&self) -> &Self::Target {
        // SAFETY: Connection is always Some while PooledConnection is alive and in use.
        // Only becomes None after release() consumes self, or during Drop.
        self.conn
            .as_ref()
            .expect("PooledConnection::deref called after release — this is a bug")
    }
}

impl std::ops::DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: Connection is always Some while PooledConnection is alive and in use.
        // Only becomes None after release() consumes self, or during Drop.
        self.conn
            .as_mut()
            .expect("PooledConnection::deref_mut called after release — this is a bug")
    }
}

/// Maximum number of hot statements to track globally.
const MAX_HOT_STATEMENTS: usize = 32;

/// Inner pool state (shared across clones).
struct PgPoolInner {
    config: PoolConfig,
    connections: Mutex<Vec<PooledConn>>,
    semaphore: Semaphore,
    closed: AtomicBool,
    active_count: AtomicUsize,
    total_created: AtomicUsize,
    /// Global registry of frequently-used prepared statements.
    /// Maps sql_hash → (stmt_name, sql_text).
    /// New connections pre-prepare these on checkout for instant cache hits.
    hot_statements: std::sync::RwLock<std::collections::HashMap<u64, (String, String)>>,
}

impl PgPoolInner {
    async fn return_connection(&self, conn: PgConnection) {
        self.active_count.fetch_sub(1, Ordering::Relaxed);

        if self.closed.load(Ordering::Relaxed) {
            return;
        }

        let mut connections = self.connections.lock().await;
        if connections.len() < self.config.max_connections {
            connections.push(PooledConn {
                conn,
                created_at: Instant::now(),
                last_used: Instant::now(),
            });
        }

        self.semaphore.add_permits(1);
    }

    /// Get a healthy connection from the pool, or None if pool is empty.
    async fn get_healthy_connection(&self) -> Option<PgConnection> {
        let mut connections = self.connections.lock().await;

        while let Some(pooled) = connections.pop() {
            if pooled.last_used.elapsed() > self.config.idle_timeout {
                // Connection is stale, drop it
                continue;
            }

            if let Some(max_life) = self.config.max_lifetime
                && pooled.created_at.elapsed() > max_life
            {
                // Connection exceeded max lifetime, recycle it
                continue;
            }

            return Some(pooled.conn);
        }

        None
    }
}

/// # Example
/// ```ignore
/// let config = PoolConfig::new("localhost", 5432, "user", "db")
///     .password("secret")
///     .max_connections(20);
/// let pool = PgPool::connect(config).await?;
/// // Get a connection from the pool
/// let mut conn = pool.acquire_raw().await?;
/// conn.simple_query("SELECT 1").await?;
/// ```
#[derive(Clone)]
pub struct PgPool {
    inner: Arc<PgPoolInner>,
}

impl PgPool {
    /// Create a pool from `qail.toml` (loads and parses automatically).
    ///
    /// # Example
    /// ```ignore
    /// let pool = PgPool::from_config().await?;
    /// ```
    pub async fn from_config() -> PgResult<Self> {
        let qail = qail_core::config::QailConfig::load()
            .map_err(|e| PgError::Connection(format!("Config error: {}", e)))?;
        let config = PoolConfig::from_qail_config(&qail)?;
        Self::connect(config).await
    }

    /// Create a new connection pool.
    pub async fn connect(config: PoolConfig) -> PgResult<Self> {
        // Semaphore starts with max_connections permits
        let semaphore = Semaphore::new(config.max_connections);

        let mut initial_connections = Vec::new();
        for _ in 0..config.min_connections {
            let conn = Self::create_connection(&config).await?;
            initial_connections.push(PooledConn {
                conn,
                created_at: Instant::now(),
                last_used: Instant::now(),
            });
        }

        let initial_count = initial_connections.len();

        let inner = Arc::new(PgPoolInner {
            config,
            connections: Mutex::new(initial_connections),
            semaphore,
            closed: AtomicBool::new(false),
            active_count: AtomicUsize::new(0),
            total_created: AtomicUsize::new(initial_count),
            hot_statements: std::sync::RwLock::new(std::collections::HashMap::new()),
        });

        Ok(Self { inner })
    }

    /// Acquire a raw connection from the pool (crate-internal only).
    ///
    /// # Safety (not `unsafe` in the Rust sense, but security-critical)
    ///
    /// This returns a connection with **no RLS context**. All tenant data
    /// queries on this connection will bypass row-level security.
    ///
    /// **Safe usage**: Pair with `fetch_all_with_rls()` for pipelined
    /// RLS+query execution (single roundtrip). Or use `acquire_with_rls()`
    /// / `acquire_with_rls_timeout()` for the 2-roundtrip path.
    ///
    /// **Unsafe usage**: Running queries directly on a raw connection
    /// without RLS context. Every call site MUST include a `// SAFETY:`
    /// comment explaining why raw acquisition is justified.
    pub async fn acquire_raw(&self) -> PgResult<PooledConnection> {
        if self.inner.closed.load(Ordering::Relaxed) {
            return Err(PgError::PoolClosed);
        }

        // Wait for available slot with timeout
        let acquire_timeout = self.inner.config.acquire_timeout;
        let permit = tokio::time::timeout(acquire_timeout, self.inner.semaphore.acquire())
            .await
            .map_err(|_| {
                PgError::Timeout(format!(
                    "pool acquire after {}s ({} max connections)",
                    acquire_timeout.as_secs(),
                    self.inner.config.max_connections
                ))
            })?
            .map_err(|_| PgError::PoolClosed)?;

        // Try to get existing healthy connection
        let mut conn = if let Some(conn) = self.inner.get_healthy_connection().await {
            conn
        } else {
            let conn = Self::create_connection(&self.inner.config).await?;
            self.inner.total_created.fetch_add(1, Ordering::Relaxed);
            conn
        };

        if self.inner.config.test_on_acquire
            && let Err(e) = conn.execute_simple("SELECT 1").await
        {
            eprintln!(
                "[WARN] pool_health_check_failed: checkout probe failed, creating replacement connection: {}",
                e
            );
            conn = Self::create_connection(&self.inner.config).await?;
            self.inner.total_created.fetch_add(1, Ordering::Relaxed);
        }

        // Pre-prepare hot statements that this connection doesn't have yet.
        // Collect data synchronously (guard dropped before async work).
        let missing: Vec<(u64, String, String)> = {
            if let Ok(hot) = self.inner.hot_statements.read() {
                hot.iter()
                    .filter(|(hash, _)| !conn.stmt_cache.contains(hash))
                    .map(|(hash, (name, sql))| (*hash, name.clone(), sql.clone()))
                    .collect()
            } else {
                Vec::new()
            }
        }; // RwLockReadGuard dropped here — safe across .await

        if !missing.is_empty() {
            use crate::protocol::PgEncoder;
            let mut buf = bytes::BytesMut::new();
            for (_, name, sql) in &missing {
                let parse_msg = PgEncoder::encode_parse(name, sql, &[]);
                buf.extend_from_slice(&parse_msg);
            }
            PgEncoder::encode_sync_to(&mut buf);
            if conn.send_bytes(&buf).await.is_ok() {
                // Drain responses (ParseComplete + ReadyForQuery)
                loop {
                    match conn.recv().await {
                        Ok(crate::protocol::BackendMessage::ReadyForQuery(_)) => break,
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
                // Register in local cache
                for (hash, name, sql) in &missing {
                    conn.stmt_cache.put(*hash, name.clone());
                    conn.prepared_statements.insert(name.clone(), sql.clone());
                }
            }
        }

        self.inner.active_count.fetch_add(1, Ordering::Relaxed);
        // Permit is intentionally detached here; returned by `release()` / pool return.
        permit.forget();

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.inner.clone(),
            rls_dirty: false,
        })
    }

    /// Acquire a connection with RLS context pre-configured.
    ///
    /// Sets PostgreSQL session variables for tenant isolation before
    /// returning the connection. When the connection is dropped, it
    /// automatically clears the RLS context before returning to the pool.
    ///
    /// # Example
    /// ```ignore
    /// use qail_core::rls::RlsContext;
    ///
    /// let mut conn = pool.acquire_with_rls(
    ///     RlsContext::operator("550e8400-e29b-41d4-a716-446655440000")
    /// ).await?;
    /// // All queries through `conn` are now scoped to this operator
    /// ```
    pub async fn acquire_with_rls(
        &self,
        ctx: qail_core::rls::RlsContext,
    ) -> PgResult<PooledConnection> {
        // SAFETY: RLS context is set immediately below via context_to_sql().
        let mut conn = self.acquire_raw().await?;

        // Set RLS context on the raw connection
        let sql = super::rls::context_to_sql(&ctx);
        let pg_conn = conn.get_mut();
        pg_conn.execute_simple(&sql).await?;

        // Mark dirty so Drop resets context before pool return
        conn.rls_dirty = true;

        Ok(conn)
    }

    /// Acquire a connection with RLS context AND statement timeout.
    ///
    /// Like `acquire_with_rls()`, but also sets `statement_timeout` to prevent
    /// runaway queries from holding pool connections indefinitely.
    pub async fn acquire_with_rls_timeout(
        &self,
        ctx: qail_core::rls::RlsContext,
        timeout_ms: u32,
    ) -> PgResult<PooledConnection> {
        // SAFETY: RLS context + timeout set immediately below via context_to_sql_with_timeout().
        let mut conn = self.acquire_raw().await?;

        // Set RLS context + statement_timeout atomically
        let sql = super::rls::context_to_sql_with_timeout(&ctx, timeout_ms);
        let pg_conn = conn.get_mut();
        pg_conn.execute_simple(&sql).await?;

        // Mark dirty so Drop resets context + timeout before pool return
        conn.rls_dirty = true;

        Ok(conn)
    }

    /// Acquire a connection with RLS context, statement timeout, AND lock timeout.
    ///
    /// Like `acquire_with_rls_timeout()`, but also sets `lock_timeout` to prevent
    /// queries from blocking indefinitely on row/table locks.
    /// When `lock_timeout_ms` is 0, the lock_timeout clause is omitted.
    pub async fn acquire_with_rls_timeouts(
        &self,
        ctx: qail_core::rls::RlsContext,
        statement_timeout_ms: u32,
        lock_timeout_ms: u32,
    ) -> PgResult<PooledConnection> {
        // SAFETY: RLS context + timeouts set immediately below via context_to_sql_with_timeouts().
        let mut conn = self.acquire_raw().await?;

        let sql =
            super::rls::context_to_sql_with_timeouts(&ctx, statement_timeout_ms, lock_timeout_ms);
        let pg_conn = conn.get_mut();
        pg_conn.execute_simple(&sql).await?;

        conn.rls_dirty = true;

        Ok(conn)
    }

    /// Acquire a connection for system-level operations (no tenant context).
    ///
    /// Sets RLS session variables to maximally restrictive values:
    /// - `app.current_operator_id = ''`
    /// - `app.current_agent_id = ''`  
    /// - `app.is_super_admin = false`
    ///
    /// Use this for startup introspection, migrations, and health checks
    /// that must not operate within any tenant scope.
    pub async fn acquire_system(&self) -> PgResult<PooledConnection> {
        let ctx = qail_core::rls::RlsContext::empty();
        self.acquire_with_rls(ctx).await
    }

    /// Acquire a connection scoped to a specific tenant.
    ///
    /// Shorthand for `acquire_with_rls(RlsContext::tenant(tenant_id))`.
    /// Use this when you already know the tenant UUID and want a
    /// tenant-scoped connection in a single call.
    ///
    /// # Example
    /// ```ignore
    /// let mut conn = pool.acquire_for_tenant("550e8400-...").await?;
    /// // All queries through `conn` are now scoped to this tenant
    /// ```
    pub async fn acquire_for_tenant(&self, tenant_id: &str) -> PgResult<PooledConnection> {
        self.acquire_with_rls(qail_core::rls::RlsContext::tenant(tenant_id))
            .await
    }

    /// Acquire a connection with branch context pre-configured.
    ///
    /// Sets PostgreSQL session variable `app.branch_id` for data virtualization.
    /// When the connection is dropped, it automatically clears the branch context.
    ///
    /// # Example
    /// ```ignore
    /// use qail_core::branch::BranchContext;
    ///
    /// let ctx = BranchContext::branch("feature-auth");
    /// let mut conn = pool.acquire_with_branch(&ctx).await?;
    /// // All queries through `conn` are now branch-aware
    /// ```
    pub async fn acquire_with_branch(
        &self,
        ctx: &qail_core::branch::BranchContext,
    ) -> PgResult<PooledConnection> {
        // SAFETY: Branch context is set immediately below via branch_context_sql().
        let mut conn = self.acquire_raw().await?;

        if let Some(branch_name) = ctx.branch_name() {
            let sql = super::branch_sql::branch_context_sql(branch_name);
            let pg_conn = conn.get_mut();
            pg_conn.execute_simple(&sql).await?;
            conn.rls_dirty = true; // Reuse dirty flag for auto-reset
        }

        Ok(conn)
    }

    /// Get the current number of idle connections.
    pub async fn idle_count(&self) -> usize {
        self.inner.connections.lock().await.len()
    }

    /// Get the number of connections currently in use.
    pub fn active_count(&self) -> usize {
        self.inner.active_count.load(Ordering::Relaxed)
    }

    /// Get the maximum number of connections.
    pub fn max_connections(&self) -> usize {
        self.inner.config.max_connections
    }

    /// Get comprehensive pool statistics.
    pub async fn stats(&self) -> PoolStats {
        let idle = self.inner.connections.lock().await.len();
        PoolStats {
            active: self.inner.active_count.load(Ordering::Relaxed),
            idle,
            pending: self.inner.config.max_connections
                - self.inner.semaphore.available_permits()
                - self.active_count(),
            max_size: self.inner.config.max_connections,
            total_created: self.inner.total_created.load(Ordering::Relaxed),
        }
    }

    /// Check if the pool is closed.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Relaxed)
    }

    /// Close the pool gracefully.
    pub async fn close(&self) {
        self.inner.closed.store(true, Ordering::Relaxed);

        let mut connections = self.inner.connections.lock().await;
        connections.clear();
    }

    /// Create a new connection using the pool configuration.
    async fn create_connection(config: &PoolConfig) -> PgResult<PgConnection> {
        if !config.auth_settings.has_any_password_method()
            && config.mtls.is_none()
            && config.password.is_some()
        {
            return Err(PgError::Auth(
                "Invalid PoolConfig: all password auth methods are disabled".to_string(),
            ));
        }

        let options = ConnectOptions {
            tls_mode: config.tls_mode,
            gss_enc_mode: config.gss_enc_mode,
            tls_ca_cert_pem: config.tls_ca_cert_pem.clone(),
            mtls: config.mtls.clone(),
            gss_token_provider: config.gss_token_provider,
            gss_token_provider_ex: config.gss_token_provider_ex.clone(),
            auth: config.auth_settings,
        };

        if let Some(remaining) = gss_circuit_remaining_open(config) {
            metrics::counter!("qail_pg_gss_circuit_open_total").increment(1);
            tracing::warn!(
                host = %config.host,
                port = config.port,
                user = %config.user,
                db = %config.database,
                remaining_ms = remaining.as_millis() as u64,
                "gss_connect_circuit_open"
            );
            return Err(PgError::Connection(format!(
                "GSS connection circuit is open; retry after {:?}",
                remaining
            )));
        }

        let mut attempt = 0usize;
        loop {
            match PgConnection::connect_with_options(
                &config.host,
                config.port,
                &config.user,
                &config.database,
                config.password.as_deref(),
                options.clone(),
            )
            .await
            {
                Ok(conn) => {
                    gss_circuit_record_success(config);
                    return Ok(conn);
                }
                Err(err) if should_retry_gss_connect_error(config, attempt, &err) => {
                    metrics::counter!("qail_pg_gss_connect_retries_total").increment(1);
                    gss_circuit_record_failure(config);
                    let delay = gss_retry_delay(config.gss_retry_base_delay, attempt);
                    tracing::warn!(
                        host = %config.host,
                        port = config.port,
                        user = %config.user,
                        db = %config.database,
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis() as u64,
                        error = %err,
                        "gss_connect_retry"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(err) => {
                    if should_track_gss_circuit_error(config, &err) {
                        metrics::counter!("qail_pg_gss_connect_failures_total").increment(1);
                        gss_circuit_record_failure(config);
                    }
                    return Err(err);
                }
            }
        }
    }
}

fn should_retry_gss_connect_error(config: &PoolConfig, attempt: usize, err: &PgError) -> bool {
    if attempt >= config.gss_connect_retries {
        return false;
    }

    if !is_gss_auth_enabled(config) {
        return false;
    }

    match err {
        PgError::Auth(msg) | PgError::Connection(msg) => is_transient_gss_message(msg),
        PgError::Timeout(_) => true,
        PgError::Io(io) => matches!(
            io.kind(),
            std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::WouldBlock
        ),
        _ => false,
    }
}

fn is_gss_auth_enabled(config: &PoolConfig) -> bool {
    config.gss_token_provider.is_some()
        || config.gss_token_provider_ex.is_some()
        || config.auth_settings.allow_kerberos_v5
        || config.auth_settings.allow_gssapi
        || config.auth_settings.allow_sspi
}

fn is_transient_gss_message(msg: &str) -> bool {
    let msg = msg.to_ascii_lowercase();
    [
        "temporary",
        "temporarily unavailable",
        "try again",
        "timed out",
        "timeout",
        "connection reset",
        "connection refused",
        "network is unreachable",
        "resource temporarily unavailable",
        "service unavailable",
    ]
    .iter()
    .any(|needle| msg.contains(needle))
}

fn gss_retry_delay(base: Duration, attempt: usize) -> Duration {
    let factor = 1u32 << attempt.min(6);
    let delay = base.saturating_mul(factor).min(Duration::from_secs(5));
    let jitter_cap_ms = ((delay.as_millis() as u64) / 5).clamp(1, 250);
    let jitter_ms = pseudo_random_jitter_ms(jitter_cap_ms);
    delay.saturating_add(Duration::from_millis(jitter_ms))
}

fn pseudo_random_jitter_ms(max_inclusive: u64) -> u64 {
    if max_inclusive == 0 {
        return 0;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    nanos % (max_inclusive + 1)
}

#[derive(Debug, Clone)]
struct GssCircuitState {
    window_started_at: Instant,
    failure_count: usize,
    open_until: Option<Instant>,
}

fn gss_circuit_registry() -> &'static std::sync::Mutex<HashMap<String, GssCircuitState>> {
    static REGISTRY: OnceLock<std::sync::Mutex<HashMap<String, GssCircuitState>>> = OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

fn gss_circuit_key(config: &PoolConfig) -> String {
    format!(
        "{}:{}:{}:{}",
        config.host, config.port, config.user, config.database
    )
}

fn gss_circuit_remaining_open(config: &PoolConfig) -> Option<Duration> {
    if !is_gss_auth_enabled(config)
        || config.gss_circuit_breaker_threshold == 0
        || config.gss_circuit_breaker_window.is_zero()
        || config.gss_circuit_breaker_cooldown.is_zero()
    {
        return None;
    }

    let now = Instant::now();
    let key = gss_circuit_key(config);
    let Ok(mut registry) = gss_circuit_registry().lock() else {
        return None;
    };
    let state = registry.get_mut(&key)?;
    let until = state.open_until?;
    if until > now {
        return Some(until.duration_since(now));
    }
    state.open_until = None;
    state.failure_count = 0;
    state.window_started_at = now;
    None
}

fn should_track_gss_circuit_error(config: &PoolConfig, err: &PgError) -> bool {
    if !is_gss_auth_enabled(config) {
        return false;
    }
    matches!(
        err,
        PgError::Auth(_) | PgError::Connection(_) | PgError::Timeout(_) | PgError::Io(_)
    )
}

fn gss_circuit_record_failure(config: &PoolConfig) {
    if !is_gss_auth_enabled(config)
        || config.gss_circuit_breaker_threshold == 0
        || config.gss_circuit_breaker_window.is_zero()
        || config.gss_circuit_breaker_cooldown.is_zero()
    {
        return;
    }

    let now = Instant::now();
    let key = gss_circuit_key(config);
    let Ok(mut registry) = gss_circuit_registry().lock() else {
        return;
    };
    let state = registry
        .entry(key.clone())
        .or_insert_with(|| GssCircuitState {
            window_started_at: now,
            failure_count: 0,
            open_until: None,
        });

    if now.duration_since(state.window_started_at) > config.gss_circuit_breaker_window {
        state.window_started_at = now;
        state.failure_count = 0;
        state.open_until = None;
    }

    state.failure_count += 1;
    if state.failure_count >= config.gss_circuit_breaker_threshold {
        metrics::counter!("qail_pg_gss_circuit_open_total").increment(1);
        state.open_until = Some(now + config.gss_circuit_breaker_cooldown);
        state.failure_count = 0;
        state.window_started_at = now;
        tracing::warn!(
            host = %config.host,
            port = config.port,
            user = %config.user,
            db = %config.database,
            threshold = config.gss_circuit_breaker_threshold,
            cooldown_ms = config.gss_circuit_breaker_cooldown.as_millis() as u64,
            "gss_connect_circuit_opened"
        );
    }
}

fn gss_circuit_record_success(config: &PoolConfig) {
    if !is_gss_auth_enabled(config) {
        return;
    }
    let key = gss_circuit_key(config);
    if let Ok(mut registry) = gss_circuit_registry().lock() {
        registry.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config() {
        let config = PoolConfig::new("localhost", 5432, "user", "testdb")
            .password("secret123")
            .max_connections(20)
            .min_connections(5);

        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
        assert_eq!(config.user, "user");
        assert_eq!(config.database, "testdb");
        assert_eq!(config.password, Some("secret123".to_string()));
        assert_eq!(config.max_connections, 20);
        assert_eq!(config.min_connections, 5);
    }

    #[test]
    fn test_pool_config_defaults() {
        let config = PoolConfig::new("localhost", 5432, "user", "testdb");
        assert_eq!(config.max_connections, 10);
        assert_eq!(config.min_connections, 1);
        assert_eq!(config.idle_timeout, Duration::from_secs(600));
        assert_eq!(config.acquire_timeout, Duration::from_secs(30));
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert!(config.password.is_none());
        assert_eq!(config.tls_mode, TlsMode::Prefer);
        assert!(config.tls_ca_cert_pem.is_none());
        assert!(config.mtls.is_none());
        assert!(config.auth_settings.allow_scram_sha_256);
        assert!(!config.auth_settings.allow_md5_password);
        assert!(!config.auth_settings.allow_cleartext_password);
        assert_eq!(config.gss_connect_retries, 2);
        assert_eq!(config.gss_retry_base_delay, Duration::from_millis(150));
        assert_eq!(config.gss_circuit_breaker_threshold, 8);
        assert_eq!(config.gss_circuit_breaker_window, Duration::from_secs(30));
        assert_eq!(config.gss_circuit_breaker_cooldown, Duration::from_secs(15));
        assert_eq!(config.gss_enc_mode, GssEncMode::Disable);
    }

    #[test]
    fn test_gss_enc_mode_parse() {
        assert_eq!(
            GssEncMode::parse_gssencmode("disable"),
            Some(GssEncMode::Disable)
        );
        assert_eq!(
            GssEncMode::parse_gssencmode("prefer"),
            Some(GssEncMode::Prefer)
        );
        assert_eq!(
            GssEncMode::parse_gssencmode("require"),
            Some(GssEncMode::Require)
        );
        assert_eq!(
            GssEncMode::parse_gssencmode("PREFER"),
            Some(GssEncMode::Prefer)
        );
        assert_eq!(
            GssEncMode::parse_gssencmode("  Require  "),
            Some(GssEncMode::Require)
        );
        assert_eq!(GssEncMode::parse_gssencmode(""), None);
        assert_eq!(GssEncMode::parse_gssencmode("invalid"), None);
        assert_eq!(GssEncMode::parse_gssencmode("allow"), None);
    }

    #[test]
    fn test_gss_enc_mode_default() {
        assert_eq!(GssEncMode::default(), GssEncMode::Disable);
    }

    #[test]
    fn test_url_gssencmode_disable() {
        let mut config = PoolConfig::new("localhost", 5432, "u", "db");
        apply_url_query_params(&mut config, "gssencmode=disable", "localhost").unwrap();
        assert_eq!(config.gss_enc_mode, GssEncMode::Disable);
    }

    #[test]
    fn test_url_gssencmode_prefer() {
        let mut config = PoolConfig::new("localhost", 5432, "u", "db");
        apply_url_query_params(&mut config, "gssencmode=prefer", "localhost").unwrap();
        assert_eq!(config.gss_enc_mode, GssEncMode::Prefer);
    }

    #[test]
    fn test_url_gssencmode_require() {
        let mut config = PoolConfig::new("localhost", 5432, "u", "db");
        apply_url_query_params(&mut config, "gssencmode=require", "localhost").unwrap();
        assert_eq!(config.gss_enc_mode, GssEncMode::Require);
    }

    #[test]
    fn test_url_gssencmode_invalid() {
        let mut config = PoolConfig::new("localhost", 5432, "u", "db");
        let err = apply_url_query_params(&mut config, "gssencmode=bogus", "localhost");
        assert!(err.is_err());
    }

    #[test]
    fn test_url_gssencmode_with_sslmode() {
        let mut config = PoolConfig::new("localhost", 5432, "u", "db");
        apply_url_query_params(
            &mut config,
            "gssencmode=prefer&sslmode=require",
            "localhost",
        )
        .unwrap();
        assert_eq!(config.gss_enc_mode, GssEncMode::Prefer);
        assert_eq!(config.tls_mode, TlsMode::Require);
    }

    #[test]
    fn test_url_gssencmode_require_sslmode_require_is_valid() {
        // libpq allows this — negotiation resolves precedence, not config parsing.
        let mut config = PoolConfig::new("localhost", 5432, "u", "db");
        apply_url_query_params(
            &mut config,
            "gssencmode=require&sslmode=require",
            "localhost",
        )
        .unwrap();
        assert_eq!(config.gss_enc_mode, GssEncMode::Require);
        assert_eq!(config.tls_mode, TlsMode::Require);
    }

    #[test]
    fn test_pool_config_builder_chaining() {
        let config = PoolConfig::new("db.example.com", 5433, "admin", "prod")
            .password("p@ss")
            .max_connections(50)
            .min_connections(10)
            .idle_timeout(Duration::from_secs(300))
            .acquire_timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(3))
            .max_lifetime(Duration::from_secs(3600))
            .gss_connect_retries(4)
            .gss_retry_base_delay(Duration::from_millis(250))
            .gss_circuit_breaker_threshold(12)
            .gss_circuit_breaker_window(Duration::from_secs(45))
            .gss_circuit_breaker_cooldown(Duration::from_secs(20))
            .test_on_acquire(false);

        assert_eq!(config.host, "db.example.com");
        assert_eq!(config.port, 5433);
        assert_eq!(config.max_connections, 50);
        assert_eq!(config.min_connections, 10);
        assert_eq!(config.idle_timeout, Duration::from_secs(300));
        assert_eq!(config.acquire_timeout, Duration::from_secs(5));
        assert_eq!(config.connect_timeout, Duration::from_secs(3));
        assert_eq!(config.max_lifetime, Some(Duration::from_secs(3600)));
        assert_eq!(config.gss_connect_retries, 4);
        assert_eq!(config.gss_retry_base_delay, Duration::from_millis(250));
        assert_eq!(config.gss_circuit_breaker_threshold, 12);
        assert_eq!(config.gss_circuit_breaker_window, Duration::from_secs(45));
        assert_eq!(config.gss_circuit_breaker_cooldown, Duration::from_secs(20));
        assert!(!config.test_on_acquire);
    }

    #[test]
    fn test_parse_pg_url_strips_query_string() {
        let (host, port, user, db, password) = parse_pg_url(
            "postgresql://alice:secret@db.internal:5433/app?sslmode=require&channel_binding=require",
        )
        .unwrap();
        assert_eq!(host, "db.internal");
        assert_eq!(port, 5433);
        assert_eq!(user, "alice");
        assert_eq!(db, "app");
        assert_eq!(password, Some("secret".to_string()));
    }

    #[test]
    fn test_parse_bool_param_variants() {
        assert_eq!(parse_bool_param("true"), Some(true));
        assert_eq!(parse_bool_param("YES"), Some(true));
        assert_eq!(parse_bool_param("0"), Some(false));
        assert_eq!(parse_bool_param("off"), Some(false));
        assert_eq!(parse_bool_param("invalid"), None);
    }

    #[test]
    fn test_from_qail_config_rejects_invalid_gss_provider() {
        let mut qail = qail_core::config::QailConfig::default();
        qail.postgres.url =
            "postgres://alice:secret@db.internal:5432/app?gss_provider=unknown".to_string();

        let err = match PoolConfig::from_qail_config(&qail) {
            Ok(_) => panic!("expected invalid gss_provider error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("Invalid gss_provider value"));
    }

    #[test]
    fn test_from_qail_config_rejects_empty_gss_service() {
        let mut qail = qail_core::config::QailConfig::default();
        qail.postgres.url = "postgres://alice:secret@db.internal:5432/app?gss_service=".to_string();

        let err = match PoolConfig::from_qail_config(&qail) {
            Ok(_) => panic!("expected empty gss_service error"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("gss_service must not be empty"));
    }

    #[test]
    fn test_from_qail_config_parses_gss_retry_settings() {
        let mut qail = qail_core::config::QailConfig::default();
        qail.postgres.url =
            "postgres://alice@db.internal:5432/app?gss_connect_retries=5&gss_retry_base_ms=400&gss_circuit_threshold=9&gss_circuit_window_ms=60000&gss_circuit_cooldown_ms=12000".to_string();

        let cfg = PoolConfig::from_qail_config(&qail).expect("expected valid config");
        assert_eq!(cfg.gss_connect_retries, 5);
        assert_eq!(cfg.gss_retry_base_delay, Duration::from_millis(400));
        assert_eq!(cfg.gss_circuit_breaker_threshold, 9);
        assert_eq!(cfg.gss_circuit_breaker_window, Duration::from_secs(60));
        assert_eq!(cfg.gss_circuit_breaker_cooldown, Duration::from_secs(12));
    }

    #[test]
    fn test_from_qail_config_rejects_invalid_gss_retry_base() {
        let mut qail = qail_core::config::QailConfig::default();
        qail.postgres.url = "postgres://alice@db.internal:5432/app?gss_retry_base_ms=0".to_string();

        let err = match PoolConfig::from_qail_config(&qail) {
            Ok(_) => panic!("expected invalid gss_retry_base_ms error"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("gss_retry_base_ms must be greater than 0")
        );
    }

    #[test]
    fn test_from_qail_config_rejects_invalid_gss_connect_retries() {
        let mut qail = qail_core::config::QailConfig::default();
        qail.postgres.url =
            "postgres://alice@db.internal:5432/app?gss_connect_retries=100".to_string();

        let err = match PoolConfig::from_qail_config(&qail) {
            Ok(_) => panic!("expected invalid gss_connect_retries error"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("gss_connect_retries must be <= 20")
        );
    }

    #[test]
    fn test_from_qail_config_rejects_invalid_gss_circuit_threshold() {
        let mut qail = qail_core::config::QailConfig::default();
        qail.postgres.url =
            "postgres://alice@db.internal:5432/app?gss_circuit_threshold=500".to_string();

        let err = match PoolConfig::from_qail_config(&qail) {
            Ok(_) => panic!("expected invalid gss_circuit_threshold error"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("gss_circuit_threshold must be <= 100")
        );
    }

    #[test]
    fn test_from_qail_config_rejects_invalid_gss_circuit_window() {
        let mut qail = qail_core::config::QailConfig::default();
        qail.postgres.url =
            "postgres://alice@db.internal:5432/app?gss_circuit_window_ms=0".to_string();

        let err = match PoolConfig::from_qail_config(&qail) {
            Ok(_) => panic!("expected invalid gss_circuit_window_ms error"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("gss_circuit_window_ms must be greater than 0")
        );
    }

    #[test]
    fn test_from_qail_config_rejects_invalid_gss_circuit_cooldown() {
        let mut qail = qail_core::config::QailConfig::default();
        qail.postgres.url =
            "postgres://alice@db.internal:5432/app?gss_circuit_cooldown_ms=0".to_string();

        let err = match PoolConfig::from_qail_config(&qail) {
            Ok(_) => panic!("expected invalid gss_circuit_cooldown_ms error"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("gss_circuit_cooldown_ms must be greater than 0")
        );
    }

    #[cfg(not(all(feature = "enterprise-gssapi", target_os = "linux")))]
    #[test]
    fn test_from_qail_config_linux_krb5_requires_feature_on_linux() {
        let mut qail = qail_core::config::QailConfig::default();
        qail.postgres.url =
            "postgres://alice@db.internal:5432/app?gss_provider=linux_krb5".to_string();

        let err = match PoolConfig::from_qail_config(&qail) {
            Ok(_) => panic!("expected linux_krb5 feature-gate error"),
            Err(e) => e,
        };
        assert!(
            err.to_string()
                .contains("requires qail-pg feature enterprise-gssapi on Linux")
        );
    }

    #[test]
    fn test_timeout_error_display() {
        let err = PgError::Timeout("pool acquire after 30s (10 max connections)".to_string());
        let msg = err.to_string();
        assert!(msg.contains("Timeout"));
        assert!(msg.contains("30s"));
        assert!(msg.contains("10 max connections"));
    }

    #[test]
    fn test_should_retry_gss_connect_error_transient_auth() {
        let config = PoolConfig::new("localhost", 5432, "user", "db")
            .auth_settings(AuthSettings::gssapi_only())
            .gss_connect_retries(3);
        let err = PgError::Auth("temporary kerberos service unavailable".to_string());
        assert!(should_retry_gss_connect_error(&config, 0, &err));
    }

    #[test]
    fn test_should_retry_gss_connect_error_non_transient_auth() {
        let config = PoolConfig::new("localhost", 5432, "user", "db")
            .auth_settings(AuthSettings::gssapi_only())
            .gss_connect_retries(3);
        let err = PgError::Auth(
            "Kerberos V5 authentication requested but no GSS token provider is configured"
                .to_string(),
        );
        assert!(!should_retry_gss_connect_error(&config, 0, &err));
    }

    #[test]
    fn test_should_retry_gss_connect_error_respects_retry_limit() {
        let config = PoolConfig::new("localhost", 5432, "user", "db")
            .auth_settings(AuthSettings::gssapi_only())
            .gss_connect_retries(1);
        let err = PgError::Connection("temporary network is unreachable".to_string());
        assert!(should_retry_gss_connect_error(&config, 0, &err));
        assert!(!should_retry_gss_connect_error(&config, 1, &err));
    }

    #[test]
    fn test_gss_retry_delay_has_bounded_jitter() {
        let delay = gss_retry_delay(Duration::from_millis(100), 2);
        assert!(delay >= Duration::from_millis(400));
        assert!(delay <= Duration::from_millis(480));
    }

    #[test]
    fn test_gss_circuit_opens_and_resets_on_success() {
        let config = PoolConfig::new("circuit.test", 5432, "user", "db_circuit")
            .auth_settings(AuthSettings::gssapi_only())
            .gss_circuit_breaker_threshold(2)
            .gss_circuit_breaker_window(Duration::from_secs(30))
            .gss_circuit_breaker_cooldown(Duration::from_secs(5));

        gss_circuit_record_success(&config);
        assert!(gss_circuit_remaining_open(&config).is_none());

        gss_circuit_record_failure(&config);
        assert!(gss_circuit_remaining_open(&config).is_none());

        gss_circuit_record_failure(&config);
        assert!(gss_circuit_remaining_open(&config).is_some());

        gss_circuit_record_success(&config);
        assert!(gss_circuit_remaining_open(&config).is_none());
    }

    #[test]
    fn test_pool_closed_error_display() {
        let err = PgError::PoolClosed;
        assert_eq!(err.to_string(), "Connection pool is closed");
    }

    #[test]
    fn test_pool_exhausted_error_display() {
        let err = PgError::PoolExhausted { max: 20 };
        let msg = err.to_string();
        assert!(msg.contains("exhausted"));
        assert!(msg.contains("20"));
    }

    #[test]
    fn test_io_error_source_chaining() {
        use std::error::Error;
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "peer reset");
        let pg_err = PgError::Io(io_err);
        // source() should return the inner io::Error
        let source = pg_err.source().expect("Io variant should have source");
        assert!(source.to_string().contains("peer reset"));
    }

    #[test]
    fn test_non_io_errors_have_no_source() {
        use std::error::Error;
        assert!(PgError::Connection("test".into()).source().is_none());
        assert!(PgError::Query("test".into()).source().is_none());
        assert!(PgError::Timeout("test".into()).source().is_none());
        assert!(PgError::PoolClosed.source().is_none());
        assert!(PgError::NoRows.source().is_none());
    }

    #[test]
    fn test_io_error_from_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "broken");
        let pg_err: PgError = io_err.into();
        assert!(matches!(pg_err, PgError::Io(_)));
        assert!(pg_err.to_string().contains("broken"));
    }

    #[test]
    fn test_error_variants_are_distinct() {
        // Ensure we can match on each variant for programmatic error handling
        let errors: Vec<PgError> = vec![
            PgError::Connection("conn".into()),
            PgError::Protocol("proto".into()),
            PgError::Auth("auth".into()),
            PgError::Query("query".into()),
            PgError::QueryServer(crate::driver::PgServerError {
                severity: "ERROR".to_string(),
                code: "23505".to_string(),
                message: "duplicate key value violates unique constraint".to_string(),
                detail: None,
                hint: None,
            }),
            PgError::NoRows,
            PgError::Io(std::io::Error::other("io")),
            PgError::Encode("enc".into()),
            PgError::Timeout("timeout".into()),
            PgError::PoolExhausted { max: 10 },
            PgError::PoolClosed,
        ];
        // All variants produce non-empty display strings
        for err in &errors {
            assert!(!err.to_string().is_empty());
        }
        assert_eq!(errors.len(), 11);
    }
}
