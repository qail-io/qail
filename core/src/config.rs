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

use serde::Deserialize;
use std::path::Path;

/// Error type for configuration loading.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Config file not found.
    #[error("Config file not found: {0}")]
    NotFound(String),

    /// I/O error reading config.
    #[error("Failed to read config: {0}")]
    Read(#[from] std::io::Error),

    /// TOML parse error.
    #[error("Failed to parse TOML: {0}")]
    Parse(#[from] toml::de::Error),

    /// Required env var not set.
    #[error("Missing required environment variable: {0}")]
    MissingEnvVar(String),
}

/// Result alias for config operations.
pub type ConfigResult<T> = Result<T, ConfigError>;

// ────────────────────────────────────────────────────────────
// Top-level config
// ────────────────────────────────────────────────────────────

/// Root config — deserialized from `qail.toml`.
///
/// All sections are optional for backward compatibility.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct QailConfig {
    /// `[project]` section.
    #[serde(default)]
    pub project: ProjectConfig,

    /// `[postgres]` section.
    #[serde(default)]
    pub postgres: PostgresConfig,

    /// `[qdrant]` section (optional).
    #[serde(default)]
    pub qdrant: Option<QdrantConfig>,

    /// `[gateway]` section (optional).
    #[serde(default)]
    pub gateway: Option<GatewayConfig>,

    /// `[[sync]]` rules.
    #[serde(default)]
    pub sync: Vec<SyncRule>,
}

// ────────────────────────────────────────────────────────────
// Section structs
// ────────────────────────────────────────────────────────────

/// `[project]` — project metadata.
#[derive(Debug, Clone, Deserialize)]
pub struct ProjectConfig {
    /// Project name.
    #[serde(default = "default_project_name")]
    pub name: String,

    /// Database mode (`postgres`, `hybrid`).
    #[serde(default = "default_mode")]
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
#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    /// Connection URL. Supports `${VAR}` expansion.
    #[serde(default = "default_pg_url")]
    pub url: String,

    /// Maximum pool connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Minimum idle connections.
    #[serde(default = "default_min_connections")]
    pub min_connections: usize,

    /// Idle connection timeout in seconds.
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,

    /// Connection acquire timeout in seconds.
    #[serde(default = "default_acquire_timeout")]
    pub acquire_timeout_secs: u64,

    /// TCP connect timeout in seconds.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,

    /// Whether to test connections on acquire.
    #[serde(default)]
    pub test_on_acquire: bool,

    /// RLS defaults.
    #[serde(default)]
    pub rls: Option<RlsConfig>,

    /// SSH tunnel host for remote connections (e.g., "myserver" or "user@host").
    #[serde(default)]
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
#[derive(Debug, Clone, Default, Deserialize)]
pub struct RlsConfig {
    /// Postgres role used for application connections.
    pub default_role: Option<String>,

    /// Role name that bypasses RLS.
    pub super_admin_role: Option<String>,
}

/// `[qdrant]` — Qdrant connection settings.
#[derive(Debug, Clone, Deserialize)]
pub struct QdrantConfig {
    /// Qdrant HTTP URL.
    #[serde(default = "default_qdrant_url")]
    pub url: String,

    /// gRPC endpoint (defaults to port 6334).
    pub grpc: Option<String>,

    /// Max connections.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    /// Use TLS for gRPC connections.
    /// - `None` (default) → auto-detect from URL scheme (`https://` = TLS)
    /// - `Some(true)` → force TLS
    /// - `Some(false)` → force plain TCP
    #[serde(default)]
    pub tls: Option<bool>,
}

fn default_qdrant_url() -> String {
    "http://localhost:6333".to_string()
}

/// `[gateway]` — Gateway server settings.
#[derive(Debug, Clone, Deserialize)]
pub struct GatewayConfig {
    /// Bind address.
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Enable CORS.
    #[serde(default = "default_true")]
    pub cors: bool,

    /// Allowed CORS origins. Empty = allow all.
    #[serde(default)]
    pub cors_allowed_origins: Option<Vec<String>>,

    /// Path to policy file.
    pub policy: Option<String>,

    /// Query cache settings.
    #[serde(default)]
    pub cache: Option<CacheConfig>,

    /// Maximum number of relations in `?expand=` (default: 4).
    /// Prevents query explosion from unbounded LEFT JOINs.
    #[serde(default = "default_max_expand_depth")]
    pub max_expand_depth: usize,

    /// Tables to block from auto-REST endpoint generation.
    /// Blocked tables will not have any CRUD routes, cannot be referenced
    /// via `?expand=`, and cannot appear as nested route targets.
    /// Use this to hide sensitive tables (e.g., `users`) from the HTTP API.
    #[serde(default)]
    pub blocked_tables: Option<Vec<String>>,

    /// Tables to allow for auto-REST endpoint generation (whitelist mode).
    /// When set, ONLY these tables are exposed — all others are blocked.
    /// This is a fail-closed approach: new tables must be explicitly allowed.
    /// Takes precedence over `blocked_tables` if both are set.
    #[serde(default)]
    pub allowed_tables: Option<Vec<String>>,
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
#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    /// Whether caching is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Maximum cache entries.
    #[serde(default = "default_cache_max")]
    pub max_entries: usize,

    /// Default TTL in seconds.
    #[serde(default = "default_cache_ttl")]
    pub ttl_secs: u64,
}

fn default_cache_max() -> usize {
    1000
}
fn default_cache_ttl() -> u64 {
    60
}

/// `[[sync]]` — Qdrant sync rule (unchanged from existing CLI).
#[derive(Debug, Clone, Deserialize)]
pub struct SyncRule {
    /// PostgreSQL source table.
    pub source_table: String,
    /// Qdrant target collection.
    pub target_collection: String,

    /// Column that triggers re-sync.
    #[serde(default)]
    pub trigger_column: Option<String>,

    /// Embedding model for sync.
    #[serde(default)]
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

        // Phase 2: Parse TOML
        let mut config: QailConfig = toml::from_str(&expanded)?;

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
        let config: QailConfig = toml::from_str(toml_str).unwrap();
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
        let config: QailConfig = toml::from_str(toml_str).unwrap();
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
        let config: QailConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.project.name, "legacy");
        assert_eq!(config.postgres.url, "postgres://localhost/legacy");
        // All new fields should have defaults
        assert_eq!(config.postgres.max_connections, 10);
        assert!(config.postgres.rls.is_none());
        assert!(config.qdrant.is_none());
        assert!(config.gateway.is_none());
    }
}
