use crate::config::GatewayConfig;
use crate::error::GatewayError;

use qail_pg::{PoolConfig, TlsConfig};

use super::{gss::apply_gss_provider, parse_bool_query};

pub(super) fn parse_database_url(
    url_str: &str,
    gateway_config: &GatewayConfig,
) -> Result<PoolConfig, GatewayError> {
    use percent_encoding::percent_decode_str;
    use qail_pg::driver::{AuthSettings, ScramChannelBindingMode, TlsMode};

    let url = url::Url::parse(url_str)
        .map_err(|e| GatewayError::Config(format!("Invalid database URL: {}", e)))?;

    let host = url
        .host_str()
        .ok_or_else(|| GatewayError::Config("Missing host in database URL".to_string()))?;

    let port = url.port().unwrap_or(5432);

    let user = if url.username().is_empty() {
        "postgres".to_string()
    } else {
        percent_decode_str(url.username())
            .decode_utf8()
            .map_err(|e| GatewayError::Config(format!("Invalid UTF-8 in username: {}", e)))?
            .into_owned()
    };

    let database = url.path().trim_start_matches('/');
    if database.is_empty() {
        return Err(GatewayError::Config(
            "Missing database name in URL".to_string(),
        ));
    }

    let mut config = PoolConfig::new(host, port, &user, database);

    if let Some(mode) = TlsMode::parse_sslmode(&gateway_config.pg_sslmode) {
        config = config.tls_mode(mode);
    }
    let mut auth_settings: AuthSettings = config.auth_settings;
    if let Some(mode) = ScramChannelBindingMode::parse(&gateway_config.pg_channel_binding) {
        auth_settings.channel_binding = mode;
    }

    if let Some(password) = url.password() {
        let decoded = percent_decode_str(password)
            .decode_utf8()
            .map_err(|e| GatewayError::Config(format!("Invalid UTF-8 in password: {}", e)))?;
        config = config.password(&decoded);
    }

    let mut sslcert_path: Option<String> = None;
    let mut sslkey_path: Option<String> = None;
    let mut gss_provider: Option<String> = None;
    let mut gss_service = "postgres".to_string();
    let mut gss_target: Option<String> = None;

    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "max_connections" => {
                if let Ok(n) = value.parse() {
                    config = config.max_connections(n);
                }
            }
            "min_connections" => {
                if let Ok(n) = value.parse() {
                    config = config.min_connections(n);
                }
            }
            "sslmode" => {
                let mode = TlsMode::parse_sslmode(value.as_ref()).ok_or_else(|| {
                    GatewayError::Config(format!("Invalid sslmode value: {}", value))
                })?;
                if gateway_config.production_strict && mode != TlsMode::Require {
                    return Err(GatewayError::Config(format!(
                        "SECURITY: production_strict=true — URL sslmode='{}' rejected (must be 'require')",
                        value
                    )));
                }
                config = config.tls_mode(mode);
            }
            "sslrootcert" => {
                let ca_pem = std::fs::read(value.as_ref()).map_err(|e| {
                    GatewayError::Config(format!("Failed to read sslrootcert '{}': {}", value, e))
                })?;
                config = config.tls_ca_cert_pem(ca_pem);
            }
            "sslcert" => sslcert_path = Some(value.to_string()),
            "sslkey" => sslkey_path = Some(value.to_string()),
            "channel_binding" => {
                let mode = ScramChannelBindingMode::parse(value.as_ref()).ok_or_else(|| {
                    GatewayError::Config(format!("Invalid channel_binding value: {}", value))
                })?;
                if gateway_config.production_strict && mode != ScramChannelBindingMode::Require {
                    return Err(GatewayError::Config(format!(
                        "SECURITY: production_strict=true — URL channel_binding='{}' rejected (must be 'require')",
                        value
                    )));
                }
                auth_settings.channel_binding = mode;
            }
            "auth_scram" => {
                let enabled = parse_bool_query(value.as_ref()).ok_or_else(|| {
                    GatewayError::Config(format!("Invalid auth_scram value: {}", value))
                })?;
                auth_settings.allow_scram_sha_256 = enabled;
            }
            "auth_md5" => {
                if gateway_config.production_strict {
                    return Err(GatewayError::Config(
                        "SECURITY: production_strict=true — auth_md5 URL param rejected (use SCRAM only)".to_string(),
                    ));
                }
                let enabled = parse_bool_query(value.as_ref()).ok_or_else(|| {
                    GatewayError::Config(format!("Invalid auth_md5 value: {}", value))
                })?;
                auth_settings.allow_md5_password = enabled;
            }
            "auth_cleartext" => {
                if gateway_config.production_strict {
                    return Err(GatewayError::Config(
                        "SECURITY: production_strict=true — auth_cleartext URL param rejected"
                            .to_string(),
                    ));
                }
                let enabled = parse_bool_query(value.as_ref()).ok_or_else(|| {
                    GatewayError::Config(format!("Invalid auth_cleartext value: {}", value))
                })?;
                auth_settings.allow_cleartext_password = enabled;
            }
            "auth_kerberos" => {
                let enabled = parse_bool_query(value.as_ref()).ok_or_else(|| {
                    GatewayError::Config(format!("Invalid auth_kerberos value: {}", value))
                })?;
                auth_settings.allow_kerberos_v5 = enabled;
            }
            "auth_gssapi" => {
                let enabled = parse_bool_query(value.as_ref()).ok_or_else(|| {
                    GatewayError::Config(format!("Invalid auth_gssapi value: {}", value))
                })?;
                auth_settings.allow_gssapi = enabled;
            }
            "auth_sspi" => {
                let enabled = parse_bool_query(value.as_ref()).ok_or_else(|| {
                    GatewayError::Config(format!("Invalid auth_sspi value: {}", value))
                })?;
                auth_settings.allow_sspi = enabled;
            }
            "auth_mode" => {
                if gateway_config.production_strict
                    && !value.eq_ignore_ascii_case("scram_only")
                    && !value.eq_ignore_ascii_case("gssapi_only")
                {
                    return Err(GatewayError::Config(format!(
                        "SECURITY: production_strict=true — auth_mode='{}' rejected (only scram_only or gssapi_only allowed)",
                        value
                    )));
                }
                if value.eq_ignore_ascii_case("scram_only") {
                    auth_settings = AuthSettings::scram_only();
                } else if value.eq_ignore_ascii_case("gssapi_only") {
                    auth_settings = AuthSettings::gssapi_only();
                } else if value.eq_ignore_ascii_case("compat")
                    || value.eq_ignore_ascii_case("default")
                {
                    auth_settings = AuthSettings::default();
                } else {
                    return Err(GatewayError::Config(format!(
                        "Invalid auth_mode value: {}",
                        value
                    )));
                }
            }
            "gss_provider" => gss_provider = Some(value.to_string()),
            "gss_service" => {
                if value.is_empty() {
                    return Err(GatewayError::Config(
                        "gss_service must not be empty".to_string(),
                    ));
                }
                gss_service = value.to_string();
            }
            "gss_target" => {
                if value.is_empty() {
                    return Err(GatewayError::Config(
                        "gss_target must not be empty".to_string(),
                    ));
                }
                gss_target = Some(value.to_string());
            }
            "gss_connect_retries" => {
                let retries = value.parse::<usize>().map_err(|_| {
                    GatewayError::Config(format!("Invalid gss_connect_retries value: {}", value))
                })?;
                if retries > 20 {
                    return Err(GatewayError::Config(
                        "gss_connect_retries must be <= 20".to_string(),
                    ));
                }
                config = config.gss_connect_retries(retries);
            }
            "gss_retry_base_ms" => {
                let delay_ms = value.parse::<u64>().map_err(|_| {
                    GatewayError::Config(format!("Invalid gss_retry_base_ms value: {}", value))
                })?;
                if delay_ms == 0 {
                    return Err(GatewayError::Config(
                        "gss_retry_base_ms must be greater than 0".to_string(),
                    ));
                }
                config = config.gss_retry_base_delay(std::time::Duration::from_millis(delay_ms));
            }
            "gss_circuit_threshold" => {
                let threshold = value.parse::<usize>().map_err(|_| {
                    GatewayError::Config(format!("Invalid gss_circuit_threshold value: {}", value))
                })?;
                if threshold > 100 {
                    return Err(GatewayError::Config(
                        "gss_circuit_threshold must be <= 100".to_string(),
                    ));
                }
                config = config.gss_circuit_breaker_threshold(threshold);
            }
            "gss_circuit_window_ms" => {
                let window_ms = value.parse::<u64>().map_err(|_| {
                    GatewayError::Config(format!("Invalid gss_circuit_window_ms value: {}", value))
                })?;
                if window_ms == 0 {
                    return Err(GatewayError::Config(
                        "gss_circuit_window_ms must be greater than 0".to_string(),
                    ));
                }
                config =
                    config.gss_circuit_breaker_window(std::time::Duration::from_millis(window_ms));
            }
            "gss_circuit_cooldown_ms" => {
                let cooldown_ms = value.parse::<u64>().map_err(|_| {
                    GatewayError::Config(format!(
                        "Invalid gss_circuit_cooldown_ms value: {}",
                        value
                    ))
                })?;
                if cooldown_ms == 0 {
                    return Err(GatewayError::Config(
                        "gss_circuit_cooldown_ms must be greater than 0".to_string(),
                    ));
                }
                config = config
                    .gss_circuit_breaker_cooldown(std::time::Duration::from_millis(cooldown_ms));
            }
            _ => {}
        }
    }

    match (sslcert_path.as_deref(), sslkey_path.as_deref()) {
        (Some(cert_path), Some(key_path)) => {
            let mtls = TlsConfig {
                client_cert_pem: std::fs::read(cert_path).map_err(|e| {
                    GatewayError::Config(format!("Failed to read sslcert '{}': {}", cert_path, e))
                })?,
                client_key_pem: std::fs::read(key_path).map_err(|e| {
                    GatewayError::Config(format!("Failed to read sslkey '{}': {}", key_path, e))
                })?,
                ca_cert_pem: config.tls_ca_cert_pem.clone(),
            };
            config = config.mtls(mtls);
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(GatewayError::Config(
                "Both sslcert and sslkey must be provided together".to_string(),
            ));
        }
        (None, None) => {}
    }

    config = config.auth_settings(auth_settings);
    apply_gss_provider(config, gss_provider, host, gss_service, gss_target)
}
