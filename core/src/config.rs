//! Centralized configuration for the Qail ecosystem.
//!
//! Reads `qail.toml` with env-expansion (`${VAR}`, `${VAR:-default}`)
//! and layered priority: Env > TOML > Defaults.
//!
//! # Example
//! ```ignore
//! let config = QailConfig::load()?;
//! let pg_url = config.postgres_url();
//! ```

use std::path::Path;

/// Error type for configuration loading.
#[derive(Debug)]
pub enum ConfigError {
    /// Config file not found.
    NotFound(String),

    /// I/O error reading config.
    Read(std::io::Error),

    /// TOML parse error.
    Parse(toml::de::Error),

    /// Invalid config structure or type.
    Invalid(String),

    /// Required env var not set.
    MissingEnvVar(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(path) => write!(f, "Config file not found: {path}"),
            Self::Read(err) => write!(f, "Failed to read config: {err}"),
            Self::Parse(err) => write!(f, "Failed to parse TOML: {err}"),
            Self::Invalid(msg) => write!(f, "Invalid qail.toml: {msg}"),
            Self::MissingEnvVar(var) => {
                write!(f, "Missing required environment variable: {var}")
            }
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read(err) => Some(err),
            Self::Parse(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ConfigError {
    fn from(value: std::io::Error) -> Self {
        Self::Read(value)
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(value: toml::de::Error) -> Self {
        Self::Parse(value)
    }
}

/// Result alias for config operations.
pub type ConfigResult<T> = Result<T, ConfigError>;

// ────────────────────────────────────────────────────────────
// Top-level config
// ────────────────────────────────────────────────────────────

/// Root config — deserialized from `qail.toml`.
///
/// All sections are optional for backward compatibility.
#[derive(Debug, Clone, Default)]
pub struct QailConfig {
    /// `[project]` section.
    pub project: ProjectConfig,

    /// `[postgres]` section.
    pub postgres: PostgresConfig,

    /// `[qdrant]` section (optional).
    pub qdrant: Option<QdrantConfig>,

    /// `[gateway]` section (optional).
    pub gateway: Option<GatewayConfig>,

    /// `[[sync]]` rules.
    pub sync: Vec<SyncRule>,
}

// ────────────────────────────────────────────────────────────
// Section structs
// ────────────────────────────────────────────────────────────

/// `[project]` — project metadata.
#[derive(Debug, Clone)]
pub struct ProjectConfig {
    /// Project name.
    pub name: String,

    /// Database mode (`postgres`, `hybrid`).
    pub mode: String,

    /// Default `.qail` schema file path.
    pub schema: Option<String>,

    /// Migrations directory override (default: `deltas/`).
    pub migrations_dir: Option<String>,

    /// Enforce strict `_order.qail` manifest coverage by default.
    ///
    /// When true, all modules under `schema/` must be explicitly listed
    /// (directly or via listed directories) in `_order.qail`.
    pub schema_strict_manifest: Option<bool>,
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self {
            name: default_project_name(),
            mode: default_mode(),
            schema: None,
            migrations_dir: None,
            schema_strict_manifest: None,
        }
    }
}

fn default_project_name() -> String {
    "qail-app".to_string()
}

fn default_mode() -> String {
    "postgres".to_string()
}

/// `[postgres]` — PostgreSQL connection and pool settings.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// Connection URL. Supports `${VAR}` expansion.
    pub url: String,

    /// Maximum pool connections.
    pub max_connections: usize,

    /// Minimum idle connections.
    pub min_connections: usize,

    /// Idle connection timeout in seconds.
    pub idle_timeout_secs: u64,

    /// Connection acquire timeout in seconds.
    pub acquire_timeout_secs: u64,

    /// TCP connect timeout in seconds.
    pub connect_timeout_secs: u64,

    /// Whether to test connections on acquire.
    pub test_on_acquire: bool,

    /// RLS defaults.
    pub rls: Option<RlsConfig>,

    /// SSH tunnel host for remote connections (e.g., "myserver" or "user@host").
    pub ssh: Option<String>,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            url: default_pg_url(),
            max_connections: default_max_connections(),
            min_connections: default_min_connections(),
            idle_timeout_secs: default_idle_timeout(),
            acquire_timeout_secs: default_acquire_timeout(),
            connect_timeout_secs: default_connect_timeout(),
            test_on_acquire: false,
            rls: None,
            ssh: None,
        }
    }
}

fn default_pg_url() -> String {
    "postgres://postgres@localhost:5432/postgres".to_string()
}

fn default_max_connections() -> usize {
    10
}

fn default_min_connections() -> usize {
    1
}

fn default_idle_timeout() -> u64 {
    600
}

fn default_acquire_timeout() -> u64 {
    30
}

fn default_connect_timeout() -> u64 {
    10
}

/// `[postgres.rls]` — RLS default settings.
#[derive(Debug, Clone, Default)]
pub struct RlsConfig {
    /// Postgres role used for application connections.
    pub default_role: Option<String>,

    /// Role name that bypasses RLS.
    pub super_admin_role: Option<String>,
}

/// `[qdrant]` — Qdrant connection settings.
#[derive(Debug, Clone)]
pub struct QdrantConfig {
    /// Qdrant HTTP URL.
    pub url: String,

    /// gRPC endpoint (defaults to port 6334).
    pub grpc: Option<String>,

    /// Max connections.
    pub max_connections: usize,

    /// Use TLS for gRPC connections.
    /// - `None` (default) → auto-detect from URL scheme (`https://` = TLS)
    /// - `Some(true)` → force TLS
    /// - `Some(false)` → force plain TCP
    pub tls: Option<bool>,
}

impl Default for QdrantConfig {
    fn default() -> Self {
        Self {
            url: default_qdrant_url(),
            grpc: None,
            max_connections: default_max_connections(),
            tls: None,
        }
    }
}

fn default_qdrant_url() -> String {
    "http://localhost:6333".to_string()
}

/// `[gateway]` — Gateway server settings.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    /// Bind address.
    pub bind: String,

    /// Enable CORS.
    pub cors: bool,

    /// Allowed CORS origins. Empty = allow all.
    pub cors_allowed_origins: Option<Vec<String>>,

    /// Path to policy file.
    pub policy: Option<String>,

    /// Query cache settings.
    pub cache: Option<CacheConfig>,

    /// Maximum number of relations in `?expand=` (default: 4).
    /// Prevents query explosion from unbounded LEFT JOINs.
    pub max_expand_depth: usize,

    /// Tables to block from auto-REST endpoint generation.
    /// Blocked tables will not have any CRUD routes, cannot be referenced
    /// via `?expand=`, and cannot appear as nested route targets.
    /// Use this to hide sensitive tables (e.g., `users`) from the HTTP API.
    pub blocked_tables: Option<Vec<String>>,

    /// Tables to allow for auto-REST endpoint generation (whitelist mode).
    /// When set, ONLY these tables are exposed — all others are blocked.
    /// This is a fail-closed approach: new tables must be explicitly allowed.
    /// Takes precedence over `blocked_tables` if both are set.
    pub allowed_tables: Option<Vec<String>>,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            cors: default_true(),
            cors_allowed_origins: None,
            policy: None,
            cache: None,
            max_expand_depth: default_max_expand_depth(),
            blocked_tables: None,
            allowed_tables: None,
        }
    }
}

fn default_bind() -> String {
    "0.0.0.0:8080".to_string()
}

fn default_true() -> bool {
    true
}

fn default_max_expand_depth() -> usize {
    4
}

/// `[gateway.cache]` — query cache settings.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Whether caching is enabled.
    pub enabled: bool,

    /// Maximum cache entries.
    pub max_entries: usize,

    /// Default TTL in seconds.
    pub ttl_secs: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            max_entries: default_cache_max(),
            ttl_secs: default_cache_ttl(),
        }
    }
}

fn default_cache_max() -> usize {
    1000
}

fn default_cache_ttl() -> u64 {
    60
}

/// `[[sync]]` — Qdrant sync rule (unchanged from existing CLI).
#[derive(Debug, Clone)]
pub struct SyncRule {
    /// PostgreSQL source table.
    pub source_table: String,
    /// Qdrant target collection.
    pub target_collection: String,

    /// Column that triggers re-sync.
    pub trigger_column: Option<String>,

    /// Embedding model for sync.
    pub embedding_model: Option<String>,
}

// ────────────────────────────────────────────────────────────
// Config loading
// ────────────────────────────────────────────────────────────

impl QailConfig {
    /// Load config from `./qail.toml` in the current directory.
    pub fn load() -> ConfigResult<Self> {
        Self::load_from("qail.toml")
    }

    /// Load config from a specific file path.
    pub fn load_from(path: impl AsRef<Path>) -> ConfigResult<Self> {
        let path = path.as_ref();

        if !path.exists() {
            return Err(ConfigError::NotFound(path.display().to_string()));
        }

        let raw = std::fs::read_to_string(path)?;

        // Phase 1: Expand ${VAR} and ${VAR:-default} in raw TOML text
        let expanded = expand_env(&raw)?;

        // Phase 2: Parse TOML table manually (serde-free)
        let mut config = Self::from_toml_str(&expanded)?;

        // Phase 3: Apply env var overrides (highest priority)
        config.apply_env_overrides();

        Ok(config)
    }

    /// Convenience: get the resolved PostgreSQL URL.
    pub fn postgres_url(&self) -> &str {
        &self.postgres.url
    }

    /// Apply env var overrides (env > TOML > defaults).
    fn apply_env_overrides(&mut self) {
        // DATABASE_URL overrides postgres.url
        if let Ok(url) = std::env::var("DATABASE_URL") {
            self.postgres.url = url;
        }

        // QDRANT_URL overrides qdrant.url
        if let (Ok(url), Some(ref mut q)) = (std::env::var("QDRANT_URL"), self.qdrant.as_mut()) {
            q.url = url;
        }

        // QAIL_BIND overrides gateway.bind
        if let (Ok(bind), Some(ref mut gw)) = (std::env::var("QAIL_BIND"), self.gateway.as_mut()) {
            gw.bind = bind;
        }
    }

    fn from_toml_str(input: &str) -> ConfigResult<Self> {
        let value: toml::Value = toml::from_str(input)?;
        let root = value
            .as_table()
            .ok_or_else(|| ConfigError::Invalid("root must be a TOML table".to_string()))?;

        Ok(Self {
            project: parse_project(root)?,
            postgres: parse_postgres(root)?,
            qdrant: parse_qdrant(root)?,
            gateway: parse_gateway(root)?,
            sync: parse_sync(root)?,
        })
    }
}

fn parse_project(root: &toml::Table) -> ConfigResult<ProjectConfig> {
    let mut cfg = ProjectConfig::default();
    let Some(tbl) = subtable(root, "project")? else {
        return Ok(cfg);
    };

    if let Some(v) = opt_string(tbl, "project", "name")? {
        cfg.name = v;
    }
    if let Some(v) = opt_string(tbl, "project", "mode")? {
        cfg.mode = v;
    }
    cfg.schema = opt_string(tbl, "project", "schema")?;
    cfg.migrations_dir = opt_string(tbl, "project", "migrations_dir")?;
    cfg.schema_strict_manifest = opt_bool(tbl, "project", "schema_strict_manifest")?;

    Ok(cfg)
}

fn parse_postgres(root: &toml::Table) -> ConfigResult<PostgresConfig> {
    let mut cfg = PostgresConfig::default();
    let Some(tbl) = subtable(root, "postgres")? else {
        return Ok(cfg);
    };

    if let Some(v) = opt_string(tbl, "postgres", "url")? {
        cfg.url = v;
    }
    if let Some(v) = opt_usize(tbl, "postgres", "max_connections")? {
        cfg.max_connections = v;
    }
    if let Some(v) = opt_usize(tbl, "postgres", "min_connections")? {
        cfg.min_connections = v;
    }
    if let Some(v) = opt_u64(tbl, "postgres", "idle_timeout_secs")? {
        cfg.idle_timeout_secs = v;
    }
    if let Some(v) = opt_u64(tbl, "postgres", "acquire_timeout_secs")? {
        cfg.acquire_timeout_secs = v;
    }
    if let Some(v) = opt_u64(tbl, "postgres", "connect_timeout_secs")? {
        cfg.connect_timeout_secs = v;
    }
    if let Some(v) = opt_bool(tbl, "postgres", "test_on_acquire")? {
        cfg.test_on_acquire = v;
    }
    cfg.ssh = opt_string(tbl, "postgres", "ssh")?;

    cfg.rls = if let Some(rls_tbl) = nested_table(tbl, "postgres", "rls")? {
        Some(RlsConfig {
            default_role: opt_string(rls_tbl, "postgres.rls", "default_role")?,
            super_admin_role: opt_string(rls_tbl, "postgres.rls", "super_admin_role")?,
        })
    } else {
        None
    };

    Ok(cfg)
}

fn parse_qdrant(root: &toml::Table) -> ConfigResult<Option<QdrantConfig>> {
    let Some(tbl) = subtable(root, "qdrant")? else {
        return Ok(None);
    };

    let mut cfg = QdrantConfig::default();

    if let Some(v) = opt_string(tbl, "qdrant", "url")? {
        cfg.url = v;
    }
    cfg.grpc = opt_string(tbl, "qdrant", "grpc")?;
    if let Some(v) = opt_usize(tbl, "qdrant", "max_connections")? {
        cfg.max_connections = v;
    }
    cfg.tls = opt_bool(tbl, "qdrant", "tls")?;

    Ok(Some(cfg))
}

fn parse_gateway(root: &toml::Table) -> ConfigResult<Option<GatewayConfig>> {
    let Some(tbl) = subtable(root, "gateway")? else {
        return Ok(None);
    };

    let mut cfg = GatewayConfig::default();

    if let Some(v) = opt_string(tbl, "gateway", "bind")? {
        cfg.bind = v;
    }
    if let Some(v) = opt_bool(tbl, "gateway", "cors")? {
        cfg.cors = v;
    }
    cfg.cors_allowed_origins = opt_string_vec(tbl, "gateway", "cors_allowed_origins")?;
    cfg.policy = opt_string(tbl, "gateway", "policy")?;
    if let Some(v) = opt_usize(tbl, "gateway", "max_expand_depth")? {
        cfg.max_expand_depth = v;
    }
    cfg.blocked_tables = opt_string_vec(tbl, "gateway", "blocked_tables")?;
    cfg.allowed_tables = opt_string_vec(tbl, "gateway", "allowed_tables")?;

    cfg.cache = if let Some(cache_tbl) = nested_table(tbl, "gateway", "cache")? {
        let mut cache = CacheConfig::default();
        if let Some(v) = opt_bool(cache_tbl, "gateway.cache", "enabled")? {
            cache.enabled = v;
        }
        if let Some(v) = opt_usize(cache_tbl, "gateway.cache", "max_entries")? {
            cache.max_entries = v;
        }
        if let Some(v) = opt_u64(cache_tbl, "gateway.cache", "ttl_secs")? {
            cache.ttl_secs = v;
        }
        Some(cache)
    } else {
        None
    };

    Ok(Some(cfg))
}

fn parse_sync(root: &toml::Table) -> ConfigResult<Vec<SyncRule>> {
    let Some(value) = root.get("sync") else {
        return Ok(Vec::new());
    };

    let arr = value
        .as_array()
        .ok_or_else(|| ConfigError::Invalid("sync must be an array of tables".to_string()))?;

    let mut out = Vec::with_capacity(arr.len());
    for (idx, item) in arr.iter().enumerate() {
        let path = format!("sync[{idx}]");
        let tbl = item
            .as_table()
            .ok_or_else(|| ConfigError::Invalid(format!("{path} must be a table")))?;

        out.push(SyncRule {
            source_table: required_string(tbl, &path, "source_table")?,
            target_collection: required_string(tbl, &path, "target_collection")?,
            trigger_column: opt_string(tbl, &path, "trigger_column")?,
            embedding_model: opt_string(tbl, &path, "embedding_model")?,
        });
    }

    Ok(out)
}

fn subtable<'a>(root: &'a toml::Table, section: &str) -> ConfigResult<Option<&'a toml::Table>> {
    match root.get(section) {
        None => Ok(None),
        Some(value) => value.as_table().map(Some).ok_or_else(|| {
            ConfigError::Invalid(format!("{section} must be a table (e.g. [{section}])"))
        }),
    }
}

fn nested_table<'a>(
    table: &'a toml::Table,
    parent: &str,
    key: &str,
) -> ConfigResult<Option<&'a toml::Table>> {
    match table.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_table()
            .map(Some)
            .ok_or_else(|| ConfigError::Invalid(format!("{parent}.{key} must be a table"))),
    }
}

fn required_string(table: &toml::Table, section: &str, key: &str) -> ConfigResult<String> {
    opt_string(table, section, key)?
        .ok_or_else(|| ConfigError::Invalid(format!("{section}.{key} is required")))
}

fn opt_string(table: &toml::Table, section: &str, key: &str) -> ConfigResult<Option<String>> {
    match table.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_str()
            .map(|s| Some(s.to_string()))
            .ok_or_else(|| ConfigError::Invalid(format!("{section}.{key} must be a string"))),
    }
}

fn opt_bool(table: &toml::Table, section: &str, key: &str) -> ConfigResult<Option<bool>> {
    match table.get(key) {
        None => Ok(None),
        Some(value) => value
            .as_bool()
            .map(Some)
            .ok_or_else(|| ConfigError::Invalid(format!("{section}.{key} must be a boolean"))),
    }
}

fn opt_usize(table: &toml::Table, section: &str, key: &str) -> ConfigResult<Option<usize>> {
    match table.get(key) {
        None => Ok(None),
        Some(value) => {
            let raw = value.as_integer().ok_or_else(|| {
                ConfigError::Invalid(format!("{section}.{key} must be a non-negative integer"))
            })?;
            let converted = usize::try_from(raw).map_err(|_| {
                ConfigError::Invalid(format!("{section}.{key} must be a non-negative integer"))
            })?;
            Ok(Some(converted))
        }
    }
}

fn opt_u64(table: &toml::Table, section: &str, key: &str) -> ConfigResult<Option<u64>> {
    match table.get(key) {
        None => Ok(None),
        Some(value) => {
            let raw = value.as_integer().ok_or_else(|| {
                ConfigError::Invalid(format!("{section}.{key} must be a non-negative integer"))
            })?;
            let converted = u64::try_from(raw).map_err(|_| {
                ConfigError::Invalid(format!("{section}.{key} must be a non-negative integer"))
            })?;
            Ok(Some(converted))
        }
    }
}

fn opt_string_vec(
    table: &toml::Table,
    section: &str,
    key: &str,
) -> ConfigResult<Option<Vec<String>>> {
    let Some(value) = table.get(key) else {
        return Ok(None);
    };

    let arr = value
        .as_array()
        .ok_or_else(|| ConfigError::Invalid(format!("{section}.{key} must be an array")))?;

    let mut out = Vec::with_capacity(arr.len());
    for (idx, item) in arr.iter().enumerate() {
        let Some(s) = item.as_str() else {
            return Err(ConfigError::Invalid(format!(
                "{section}.{key}[{idx}] must be a string"
            )));
        };
        out.push(s.to_string());
    }

    Ok(Some(out))
}

// ────────────────────────────────────────────────────────────
// Env expansion
// ────────────────────────────────────────────────────────────

/// Expand `${VAR}` and `${VAR:-default}` patterns in a string.
///
/// - `${VAR}` — required, errors if not set
/// - `${VAR:-default}` — optional, uses `default` if not set
/// - `$$` — literal `$`
pub fn expand_env(input: &str) -> ConfigResult<String> {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' {
            match chars.peek() {
                Some('$') => {
                    // Escaped: $$ → $
                    chars.next();
                    result.push('$');
                }
                Some('{') => {
                    chars.next(); // consume '{'
                    let mut var_expr = String::new();
                    let mut depth = 1;

                    for c in chars.by_ref() {
                        if c == '{' {
                            depth += 1;
                        } else if c == '}' {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        var_expr.push(c);
                    }

                    // Parse VAR:-default
                    let (var_name, default_val) = if let Some(idx) = var_expr.find(":-") {
                        (&var_expr[..idx], Some(&var_expr[idx + 2..]))
                    } else {
                        (var_expr.as_str(), None)
                    };

                    match std::env::var(var_name) {
                        Ok(val) => result.push_str(&val),
                        Err(_) => {
                            if let Some(default) = default_val {
                                result.push_str(default);
                            } else {
                                return Err(ConfigError::MissingEnvVar(var_name.to_string()));
                            }
                        }
                    }
                }
                _ => {
                    // Plain `$` not followed by `{` or `$`, keep as-is
                    result.push('$');
                }
            }
        } else {
            result.push(ch);
        }
    }

    Ok(result)
}

// ────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: safely set and remove env vars in tests.
    /// SAFETY: Tests run with `--test-threads=1` or use unique var names.
    unsafe fn set_env(key: &str, val: &str) {
        unsafe { std::env::set_var(key, val) };
    }

    unsafe fn unset_env(key: &str) {
        unsafe { std::env::remove_var(key) };
    }

    #[test]
    fn test_expand_env_required_var() {
        unsafe { set_env("QAIL_TEST_VAR", "hello") };
        let result = expand_env("prefix_${QAIL_TEST_VAR}_suffix").unwrap();
        assert_eq!(result, "prefix_hello_suffix");
        unsafe { unset_env("QAIL_TEST_VAR") };
    }

    #[test]
    fn test_expand_env_missing_required() {
        unsafe { unset_env("QAIL_MISSING_VAR_XYZ") };
        let result = expand_env("${QAIL_MISSING_VAR_XYZ}");
        assert!(result.is_err());
        assert!(
            matches!(result, Err(ConfigError::MissingEnvVar(ref v)) if v == "QAIL_MISSING_VAR_XYZ")
        );
    }

    #[test]
    fn test_expand_env_default_value() {
        unsafe { unset_env("QAIL_OPT_VAR") };
        let result = expand_env("${QAIL_OPT_VAR:-fallback}").unwrap();
        assert_eq!(result, "fallback");
    }

    #[test]
    fn test_expand_env_default_empty() {
        unsafe { unset_env("QAIL_OPT_EMPTY") };
        let result = expand_env("${QAIL_OPT_EMPTY:-}").unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_expand_env_set_overrides_default() {
        unsafe { set_env("QAIL_SET_VAR", "real") };
        let result = expand_env("${QAIL_SET_VAR:-fallback}").unwrap();
        assert_eq!(result, "real");
        unsafe { unset_env("QAIL_SET_VAR") };
    }

    #[test]
    fn test_expand_env_escaped_dollar() {
        let result = expand_env("price: $$100").unwrap();
        assert_eq!(result, "price: $100");
    }

    #[test]
    fn test_expand_env_no_expansion() {
        let result = expand_env("plain text no vars").unwrap();
        assert_eq!(result, "plain text no vars");
    }

    #[test]
    fn test_expand_env_postgres_url() {
        unsafe { set_env("QAIL_DB_USER", "admin") };
        unsafe { set_env("QAIL_DB_PASS", "s3cret") };
        let result =
            expand_env("postgres://${QAIL_DB_USER}:${QAIL_DB_PASS}@localhost:5432/mydb").unwrap();
        assert_eq!(result, "postgres://admin:s3cret@localhost:5432/mydb");
        unsafe { unset_env("QAIL_DB_USER") };
        unsafe { unset_env("QAIL_DB_PASS") };
    }

    #[test]
    fn test_parse_minimal_toml() {
        let toml_str = r#"
[project]
name = "test"
mode = "postgres"

[postgres]
url = "postgres://localhost/test"
"#;
        let config = QailConfig::from_toml_str(toml_str).unwrap();
        assert_eq!(config.project.name, "test");
        assert_eq!(config.postgres.url, "postgres://localhost/test");
        assert_eq!(config.postgres.max_connections, 10); // default
        assert!(config.qdrant.is_none());
        assert!(config.gateway.is_none());
    }

    #[test]
    fn test_parse_full_toml() {
        let toml_str = r#"
[project]
name = "fulltest"
mode = "hybrid"
schema = "schema.qail"
migrations_dir = "deltas"
schema_strict_manifest = true

[postgres]
url = "postgres://localhost/test"
max_connections = 25
min_connections = 5
idle_timeout_secs = 300

[postgres.rls]
default_role = "app_user"
super_admin_role = "super_admin"

[qdrant]
url = "http://qdrant:6333"
grpc = "qdrant:6334"
max_connections = 15

[gateway]
bind = "0.0.0.0:9090"
cors = false
policy = "policies.yaml"

[gateway.cache]
enabled = true
max_entries = 5000
ttl_secs = 120

[[sync]]
source_table = "products"
target_collection = "products_search"
trigger_column = "description"
embedding_model = "candle:bert-base"
"#;
        let config = QailConfig::from_toml_str(toml_str).unwrap();
        assert_eq!(config.project.name, "fulltest");
        assert_eq!(config.project.schema_strict_manifest, Some(true));
        assert_eq!(config.postgres.max_connections, 25);
        assert_eq!(config.postgres.min_connections, 5);

        let rls = config.postgres.rls.unwrap();
        assert_eq!(rls.default_role.unwrap(), "app_user");

        let qdrant = config.qdrant.unwrap();
        assert_eq!(qdrant.max_connections, 15);

        let gw = config.gateway.unwrap();
        assert_eq!(gw.bind, "0.0.0.0:9090");
        assert!(!gw.cors);

        let cache = gw.cache.unwrap();
        assert_eq!(cache.max_entries, 5000);

        assert_eq!(config.sync.len(), 1);
        assert_eq!(config.sync[0].source_table, "products");
    }

    #[test]
    fn test_backward_compat_existing_toml() {
        // Existing qail.toml format must still parse
        let toml_str = r#"
[project]
name = "legacy"
mode = "postgres"

[postgres]
url = "postgres://localhost/legacy"
"#;
        let config = QailConfig::from_toml_str(toml_str).unwrap();
        assert_eq!(config.project.name, "legacy");
        assert_eq!(config.postgres.url, "postgres://localhost/legacy");
        // All new fields should have defaults
        assert_eq!(config.postgres.max_connections, 10);
        assert!(config.postgres.rls.is_none());
        assert!(config.qdrant.is_none());
        assert!(config.gateway.is_none());
    }
}
