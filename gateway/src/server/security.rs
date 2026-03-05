use super::Gateway;
use crate::error::GatewayError;

impl Gateway {
    /// SECURITY (M2): Refuse to start if dev-mode is enabled with unsafe config.
    ///
    /// Prevents operators from accidentally running header-based auth on a
    /// public interface. Two conditions trigger the guard:
    /// 1. `QAIL_DEV_MODE=true` + bind address is NOT `127.0.0.1` or `localhost`
    /// 2. `QAIL_DEV_MODE=true` + `JWT_SECRET` is not set
    pub(super) fn check_dev_mode_safety(&self) -> Result<(), GatewayError> {
        let dev_mode = std::env::var("QAIL_DEV_MODE")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        if !dev_mode {
            return Ok(());
        }

        let bind = &self.config.bind_address;
        let is_local = bind.starts_with("127.0.0.1")
            || bind.starts_with("localhost")
            || bind.starts_with("[::1]");

        if !is_local {
            return Err(GatewayError::Config(format!(
                "SECURITY: QAIL_DEV_MODE=true but bind_address='{}' is not localhost. \
                 Dev mode allows unauthenticated header-based auth. \
                 Either set bind_address to 127.0.0.1 or unset QAIL_DEV_MODE.",
                bind
            )));
        }

        let jwt_set = std::env::var("JWT_SECRET").is_ok();
        if !jwt_set {
            tracing::warn!(
                "QAIL_DEV_MODE=true and JWT_SECRET is not set. \
                 Header-based auth is active on {}. Do NOT expose this port publicly.",
                bind
            );
        }

        Ok(())
    }

    /// SECURITY (P0-1): Refuse startup when `production_strict=true` and
    /// essential security controls are not configured.
    ///
    /// Checks:
    /// 1. JWT_SECRET or JWKS_URL environment variable is set
    /// 2. Explicit CORS origins are configured
    /// 3. Admin token is set (protects /metrics, /health/internal)
    /// 4. Query allow-list is configured
    pub(super) fn check_production_strict(&self) -> Result<(), GatewayError> {
        if !self.config.production_strict {
            return Ok(());
        }

        let mut violations = Vec::new();

        let jwt_set = std::env::var("JWT_SECRET").is_ok();
        let jwks_set = std::env::var("JWKS_URL").is_ok();
        if !jwt_set && !jwks_set {
            violations.push("JWT_SECRET or JWKS_URL must be set");
        }

        if self.config.cors_allowed_origins.is_empty() {
            violations.push("cors_allowed_origins must be non-empty");
        }

        // SECURITY (P0-R4): Prevent CORS fail-open on parse errors.
        if !self.config.cors_strict {
            violations.push("cors_strict must be true (prevents fail-open on origin parse errors)");
        }

        if self.config.admin_token.is_none() {
            violations.push("admin_token must be set");
        }

        if self.config.allow_list_path.is_none() {
            violations.push("allow_list_path must be set");
        }

        if self.config.rpc_allowlist_path.is_none() {
            violations.push("rpc_allowlist_path must be set");
        }

        if !self.config.rpc_signature_check {
            violations.push("rpc_signature_check must be true");
        }

        if self.config.jwt_allowed_algorithms.is_empty() {
            violations.push("jwt_allowed_algorithms must be non-empty (e.g. [\"RS256\"])");
        }

        if self.config.pg_sslmode != "require" {
            violations.push("pg_sslmode must be \"require\"");
        }

        if self.config.pg_channel_binding != "require" {
            violations.push("pg_channel_binding must be \"require\"");
        }

        // SECURITY: Dev mode header auth allows arbitrary role spoofing.
        if std::env::var("QAIL_DEV_MODE")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false)
        {
            violations.push("QAIL_DEV_MODE must not be set (header auth spoofing risk)");
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(GatewayError::Config(format!(
                "SECURITY: production_strict=true but {} violation(s) found:\n  - {}",
                violations.len(),
                violations.join("\n  - ")
            )))
        }
    }
}
