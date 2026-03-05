//! PgDriver — high-level async PostgreSQL driver combining the wire-protocol
//! encoder with connection management (connect, fetch, execute, copy, pipeline, txn, RLS).

use super::auth_types::*;
use super::builder::PgDriverBuilder;
use super::connection::PgConnection;
use super::pool;
use super::rls::RlsContext;
use super::types::*;

/// Combines the pure encoder (Layer 2) with async I/O (Layer 3).
pub struct PgDriver {
    #[allow(dead_code)]
    pub(super) connection: PgConnection,
    /// Current RLS context, if set. Used for multi-tenant data isolation.
    pub(super) rls_context: Option<RlsContext>,
}

impl PgDriver {
    /// Create a new driver with an existing connection.
    pub fn new(connection: PgConnection) -> Self {
        Self {
            connection,
            rls_context: None,
        }
    }

    /// Builder pattern for ergonomic connection configuration.
    /// # Example
    /// ```ignore
    /// let driver = PgDriver::builder()
    ///     .host("localhost")
    ///     .port(5432)
    ///     .user("admin")
    ///     .database("mydb")
    ///     .password("secret")  // Optional
    ///     .connect()
    ///     .await?;
    /// ```
    pub fn builder() -> PgDriverBuilder {
        PgDriverBuilder::new()
    }

    /// Connect to PostgreSQL and create a driver (trust mode, no password).
    ///
    /// # Arguments
    ///
    /// * `host` — PostgreSQL server hostname or IP.
    /// * `port` — TCP port (typically 5432).
    /// * `user` — PostgreSQL role name.
    /// * `database` — Target database name.
    pub async fn connect(host: &str, port: u16, user: &str, database: &str) -> PgResult<Self> {
        let connection = PgConnection::connect(host, port, user, database).await?;
        Ok(Self::new(connection))
    }

    /// Connect to PostgreSQL with password authentication.
    /// Supports server-requested auth flow: cleartext, MD5, or SCRAM-SHA-256.
    pub async fn connect_with_password(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: &str,
    ) -> PgResult<Self> {
        let connection =
            PgConnection::connect_with_password(host, port, user, database, Some(password)).await?;
        Ok(Self::new(connection))
    }

    /// Connect with explicit security options.
    pub async fn connect_with_options(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        options: ConnectOptions,
    ) -> PgResult<Self> {
        let connection =
            PgConnection::connect_with_options(host, port, user, database, password, options)
                .await?;
        Ok(Self::new(connection))
    }

    /// Connect in logical replication mode (`replication=database`).
    ///
    /// This enables replication commands such as `IDENTIFY_SYSTEM` and
    /// `CREATE_REPLICATION_SLOT`.
    pub async fn connect_logical_replication(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
    ) -> PgResult<Self> {
        let options = ConnectOptions::default().with_logical_replication();
        Self::connect_with_options(host, port, user, database, password, options).await
    }

    /// Connect with explicit options and force logical replication mode.
    pub async fn connect_logical_replication_with_options(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: Option<&str>,
        options: ConnectOptions,
    ) -> PgResult<Self> {
        Self::connect_with_options(
            host,
            port,
            user,
            database,
            password,
            options.with_logical_replication(),
        )
        .await
    }

    /// Connect using DATABASE_URL environment variable.
    ///
    /// Parses the URL format: `postgresql://user:password@host:port/database`
    /// or `postgres://user:password@host:port/database`
    ///
    /// # Example
    /// ```ignore
    /// // Set DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
    /// let driver = PgDriver::connect_env().await?;
    /// ```
    pub async fn connect_env() -> PgResult<Self> {
        let url = std::env::var("DATABASE_URL").map_err(|_| {
            PgError::Connection("DATABASE_URL environment variable not set".to_string())
        })?;
        Self::connect_url(&url).await
    }

    /// Connect using a PostgreSQL connection URL.
    ///
    /// Parses the URL format: `postgresql://user:password@host:port/database?params`
    /// or `postgres://user:password@host:port/database?params`
    ///
    /// Supports all enterprise query params (sslmode, auth_mode, gss_provider,
    /// channel_binding, etc.) — same set as `PoolConfig::from_qail_config`.
    ///
    /// # Example
    /// ```ignore
    /// let driver = PgDriver::connect_url("postgresql://user:pass@localhost:5432/mydb?sslmode=require").await?;
    /// ```
    pub async fn connect_url(url: &str) -> PgResult<Self> {
        let (host, port, user, database, password) = Self::parse_database_url(url)?;

        // Parse enterprise query params using the shared helper from pool.rs.
        let mut pool_cfg = pool::PoolConfig::new(&host, port, &user, &database);
        if let Some(pw) = &password {
            pool_cfg = pool_cfg.password(pw);
        }
        if let Some(query) = url.split('?').nth(1) {
            pool::apply_url_query_params(&mut pool_cfg, query, &host)?;
        }

        let mut opts = ConnectOptions {
            tls_mode: pool_cfg.tls_mode,
            gss_enc_mode: pool_cfg.gss_enc_mode,
            tls_ca_cert_pem: pool_cfg.tls_ca_cert_pem,
            mtls: pool_cfg.mtls,
            gss_token_provider: pool_cfg.gss_token_provider,
            gss_token_provider_ex: pool_cfg.gss_token_provider_ex,
            auth: pool_cfg.auth_settings,
            startup_params: Vec::new(),
        };

        // Startup parameters not owned by PoolConfig parser.
        if let Some(query) = url.split('?').nth(1) {
            for pair in query.split('&') {
                let mut kv = pair.splitn(2, '=');
                let key = kv.next().unwrap_or_default().trim();
                let value = kv.next().unwrap_or_default().trim();
                if key.eq_ignore_ascii_case("replication") {
                    let replication_mode = if value.eq_ignore_ascii_case("database") {
                        "database"
                    } else if value.eq_ignore_ascii_case("true")
                        || value.eq_ignore_ascii_case("on")
                        || value == "1"
                    {
                        // Canonicalize legacy truthy values to PostgreSQL's
                        // logical-replication mode value.
                        "database"
                    } else {
                        return Err(PgError::Connection(format!(
                            "Invalid replication startup mode '{}': expected database|true|on|1",
                            value
                        )));
                    };
                    opts = opts.with_startup_param("replication", replication_mode);
                }
            }
        }

        Self::connect_with_options(&host, port, &user, &database, password.as_deref(), opts).await
    }

    /// Parse a PostgreSQL connection URL into components.
    ///
    /// Format: `postgresql://user:password@host:port/database`
    /// or `postgres://user:password@host:port/database`
    ///
    /// URL percent-encoding is automatically decoded for user and password.
    pub(crate) fn parse_database_url(
        url: &str,
    ) -> PgResult<(String, u16, String, String, Option<String>)> {
        // Remove scheme (postgresql:// or postgres://)
        let after_scheme = url.split("://").nth(1).ok_or_else(|| {
            PgError::Connection("Invalid DATABASE_URL: missing scheme".to_string())
        })?;

        // Split into auth@host parts
        let (auth_part, host_db_part) = if let Some(at_pos) = after_scheme.rfind('@') {
            (Some(&after_scheme[..at_pos]), &after_scheme[at_pos + 1..])
        } else {
            (None, after_scheme)
        };

        // Parse auth (user:password)
        let (user, password) = if let Some(auth) = auth_part {
            let parts: Vec<&str> = auth.splitn(2, ':').collect();
            if parts.len() == 2 {
                // URL-decode both user and password
                (
                    Self::percent_decode(parts[0]),
                    Some(Self::percent_decode(parts[1])),
                )
            } else {
                (Self::percent_decode(parts[0]), None)
            }
        } else {
            return Err(PgError::Connection(
                "Invalid DATABASE_URL: missing user".to_string(),
            ));
        };

        // Parse host:port/database (strip query string if present)
        let (host_port, database) = if let Some(slash_pos) = host_db_part.find('/') {
            let raw_db = &host_db_part[slash_pos + 1..];
            // Strip ?query params — they're handled separately by connect_url
            let db = raw_db.split('?').next().unwrap_or(raw_db).to_string();
            (&host_db_part[..slash_pos], db)
        } else {
            return Err(PgError::Connection(
                "Invalid DATABASE_URL: missing database name".to_string(),
            ));
        };

        // Parse host:port
        let (host, port) = if let Some(colon_pos) = host_port.rfind(':') {
            let port_str = &host_port[colon_pos + 1..];
            let port = port_str
                .parse::<u16>()
                .map_err(|_| PgError::Connection(format!("Invalid port: {}", port_str)))?;
            (host_port[..colon_pos].to_string(), port)
        } else {
            (host_port.to_string(), 5432) // Default PostgreSQL port
        };

        Ok((host, port, user, database, password))
    }

    /// Decode URL percent-encoded string.
    /// Handles common encodings: %20 (space), %2B (+), %3D (=), %40 (@), %2F (/), etc.
    pub(crate) fn percent_decode(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        let mut chars = s.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '%' {
                // Try to parse next two chars as hex
                let hex: String = chars.by_ref().take(2).collect();
                if hex.len() == 2
                    && let Ok(byte) = u8::from_str_radix(&hex, 16)
                {
                    result.push(byte as char);
                    continue;
                }
                // If parsing failed, keep original
                result.push('%');
                result.push_str(&hex);
            } else if c == '+' {
                // '+' often represents space in query strings (form encoding)
                // But in path components, keep as-is. PostgreSQL URLs use path encoding.
                result.push('+');
            } else {
                result.push(c);
            }
        }

        result
    }

    /// Connect to PostgreSQL with a connection timeout.
    /// If the connection cannot be established within the timeout, returns an error.
    /// # Example
    /// ```ignore
    /// use std::time::Duration;
    /// let driver = PgDriver::connect_with_timeout(
    ///     "localhost", 5432, "user", "db", "password",
    ///     Duration::from_secs(5)
    /// ).await?;
    /// ```
    pub async fn connect_with_timeout(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        password: &str,
        timeout: std::time::Duration,
    ) -> PgResult<Self> {
        tokio::time::timeout(
            timeout,
            Self::connect_with_password(host, port, user, database, password),
        )
        .await
        .map_err(|_| PgError::Timeout(format!("connection after {:?}", timeout)))?
    }
    /// Clear the prepared statement cache.
    /// Frees memory by removing all cached statements.
    /// Note: Statements remain on the PostgreSQL server until connection closes.
    pub fn clear_cache(&mut self) {
        self.connection.clear_prepared_statement_state();
    }

    /// Get cache statistics.
    /// Returns (current_size, max_capacity).
    pub fn cache_stats(&self) -> (usize, usize) {
        (
            self.connection.stmt_cache.len(),
            self.connection.stmt_cache.cap().get(),
        )
    }
}
