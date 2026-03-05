use crate::error::GatewayError;
use qail_pg::PoolConfig;

#[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
use qail_pg::{LinuxKrb5ProviderConfig, linux_krb5_preflight, linux_krb5_token_provider};

pub(super) fn apply_gss_provider(
    config: PoolConfig,
    gss_provider: Option<String>,
    host: &str,
    gss_service: String,
    gss_target: Option<String>,
) -> Result<PoolConfig, GatewayError> {
    let Some(provider) = gss_provider else {
        return Ok(config);
    };

    if provider.eq_ignore_ascii_case("linux_krb5") || provider.eq_ignore_ascii_case("builtin") {
        #[cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]
        {
            let mut config = config;
            let gss_config = LinuxKrb5ProviderConfig {
                service: gss_service.clone(),
                host: host.to_string(),
                target_name: gss_target.clone(),
            };
            let report = linux_krb5_preflight(&gss_config).map_err(GatewayError::Config)?;
            for warning in &report.warnings {
                tracing::warn!("Kerberos preflight warning: {}", warning);
            }
            tracing::info!(
                "Kerberos preflight passed (target='{}', warnings={})",
                report.target_name,
                report.warnings.len()
            );

            let provider = linux_krb5_token_provider(gss_config).map_err(GatewayError::Config)?;
            config = config.gss_token_provider_ex(provider);
            return Ok(config);
        }
        #[cfg(not(all(feature = "enterprise-gssapi", target_os = "linux")))]
        {
            let _ = host;
            let _ = gss_service;
            let _ = gss_target;
            return Err(GatewayError::Config(
                "gss_provider=linux_krb5 requires gateway feature enterprise-gssapi on Linux"
                    .to_string(),
            ));
        }
    }

    if provider.eq_ignore_ascii_case("callback") || provider.eq_ignore_ascii_case("custom") {
        // External callback wiring is handled by direct qail-pg integration.
        return Ok(config);
    }

    Err(GatewayError::Config(format!(
        "Invalid gss_provider value: {}",
        provider
    )))
}
