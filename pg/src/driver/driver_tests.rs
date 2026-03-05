#[cfg(test)]
mod tests {
    use crate::driver::{PgError, PgServerError};

    fn server_error(code: &str, message: &str) -> PgError {
        PgError::QueryServer(PgServerError {
            severity: "ERROR".to_string(),
            code: code.to_string(),
            message: message.to_string(),
            detail: None,
            hint: None,
        })
    }

    #[test]
    fn prepared_statement_missing_is_retryable() {
        let err = server_error("26000", "prepared statement \"s1\" does not exist");
        assert!(err.is_prepared_statement_retryable());
    }

    #[test]
    fn cached_plan_replanned_is_retryable() {
        let err = server_error("0A000", "cached plan must be replanned");
        assert!(err.is_prepared_statement_retryable());
    }

    #[test]
    fn unrelated_server_error_is_not_retryable() {
        let err = server_error("23505", "duplicate key value violates unique constraint");
        assert!(!err.is_prepared_statement_retryable());
    }

    #[test]
    fn prepared_statement_already_exists_is_detected() {
        let err = server_error("42P05", "prepared statement \"s1\" already exists");
        assert!(err.is_prepared_statement_already_exists());
    }

    #[test]
    fn prepared_statement_already_exists_non_matching_code_is_not_detected() {
        let err = server_error("26000", "prepared statement \"s1\" already exists");
        assert!(!err.is_prepared_statement_already_exists());
    }

    // ══════════════════════════════════════════════════════════════════
    // is_transient_server_error
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn serialization_failure_is_transient() {
        let err = server_error("40001", "could not serialize access");
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn deadlock_detected_is_transient() {
        let err = server_error("40P01", "deadlock detected");
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn cannot_connect_now_is_transient() {
        let err = server_error("57P03", "the database system is starting up");
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn admin_shutdown_is_transient() {
        let err = server_error(
            "57P01",
            "terminating connection due to administrator command",
        );
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn connection_exception_class_is_transient() {
        let err = server_error("08006", "connection failure");
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn connection_does_not_exist_is_transient() {
        let err = server_error("08003", "connection does not exist");
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn unique_violation_is_not_transient() {
        let err = server_error("23505", "duplicate key value violates unique constraint");
        assert!(!err.is_transient_server_error());
    }

    #[test]
    fn syntax_error_is_not_transient() {
        let err = server_error("42601", "syntax error at or near \"SELECT\"");
        assert!(!err.is_transient_server_error());
    }

    #[test]
    fn timeout_error_is_transient() {
        let err = PgError::Timeout("query after 30s".to_string());
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn io_connection_reset_is_transient() {
        let err = PgError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "connection reset by peer",
        ));
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn io_permission_denied_is_not_transient() {
        let err = PgError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "permission denied",
        ));
        assert!(!err.is_transient_server_error());
    }

    #[test]
    fn connection_error_is_transient() {
        let err = PgError::Connection("host not found".to_string());
        assert!(err.is_transient_server_error());
    }

    #[test]
    fn prepared_stmt_retryable_counts_as_transient() {
        let err = server_error("26000", "prepared statement \"s1\" does not exist");
        assert!(err.is_transient_server_error());
    }

    // ══════════════════════════════════════════════════════════════════
    // TlsMode parse_sslmode (Phase 1b)
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn tls_mode_parse_disable() {
        assert_eq!(
            crate::driver::TlsMode::parse_sslmode("disable"),
            Some(crate::driver::TlsMode::Disable)
        );
    }

    #[test]
    fn tls_mode_parse_prefer_variants() {
        assert_eq!(
            crate::driver::TlsMode::parse_sslmode("prefer"),
            Some(crate::driver::TlsMode::Prefer)
        );
        assert_eq!(
            crate::driver::TlsMode::parse_sslmode("allow"),
            Some(crate::driver::TlsMode::Prefer),
            "libpq 'allow' maps to Prefer"
        );
    }

    #[test]
    fn tls_mode_parse_require_variants() {
        // All three map to Require — verify-ca and verify-full require
        // TLS but certificate validation is handled at the rustls layer.
        assert_eq!(
            crate::driver::TlsMode::parse_sslmode("require"),
            Some(crate::driver::TlsMode::Require)
        );
        assert_eq!(
            crate::driver::TlsMode::parse_sslmode("verify-ca"),
            Some(crate::driver::TlsMode::Require),
            "verify-ca → Require (CA validation at TLS layer)"
        );
        assert_eq!(
            crate::driver::TlsMode::parse_sslmode("verify-full"),
            Some(crate::driver::TlsMode::Require),
            "verify-full → Require (hostname validation at TLS layer)"
        );
    }

    #[test]
    fn tls_mode_parse_case_insensitive() {
        assert_eq!(
            crate::driver::TlsMode::parse_sslmode("REQUIRE"),
            Some(crate::driver::TlsMode::Require)
        );
        assert_eq!(
            crate::driver::TlsMode::parse_sslmode("Verify-Full"),
            Some(crate::driver::TlsMode::Require)
        );
    }

    #[test]
    fn tls_mode_parse_unknown_returns_none() {
        assert_eq!(crate::driver::TlsMode::parse_sslmode("invalid"), None);
        assert_eq!(crate::driver::TlsMode::parse_sslmode(""), None);
    }

    #[test]
    fn tls_mode_parse_trims_whitespace() {
        assert_eq!(
            crate::driver::TlsMode::parse_sslmode("  require  "),
            Some(crate::driver::TlsMode::Require)
        );
    }

    #[test]
    fn tls_mode_default_is_disable() {
        assert_eq!(
            crate::driver::TlsMode::default(),
            crate::driver::TlsMode::Disable
        );
    }

    // ══════════════════════════════════════════════════════════════════
    // AuthSettings behavior matrix (Phase 1c)
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn auth_default_allows_all_password_methods() {
        let auth = crate::driver::AuthSettings::default();
        assert!(auth.allow_cleartext_password);
        assert!(auth.allow_md5_password);
        assert!(auth.allow_scram_sha_256);
        assert!(auth.has_any_password_method());
    }

    #[test]
    fn auth_default_disables_enterprise_methods() {
        let auth = crate::driver::AuthSettings::default();
        assert!(
            !auth.allow_kerberos_v5,
            "Kerberos V5 should be disabled by default"
        );
        assert!(!auth.allow_gssapi, "GSSAPI should be disabled by default");
        assert!(!auth.allow_sspi, "SSPI should be disabled by default");
    }

    #[test]
    fn auth_scram_only_restricts_to_scram() {
        let auth = crate::driver::AuthSettings::scram_only();
        // Only SCRAM allowed
        assert!(auth.allow_scram_sha_256);
        assert!(!auth.allow_cleartext_password);
        assert!(!auth.allow_md5_password);
        // Enterprise auth still disabled
        assert!(!auth.allow_kerberos_v5);
        assert!(!auth.allow_gssapi);
        assert!(!auth.allow_sspi);
        // Still has a password method
        assert!(auth.has_any_password_method());
    }

    #[test]
    fn auth_gssapi_only_disables_all_passwords() {
        let auth = crate::driver::AuthSettings::gssapi_only();
        // No password methods
        assert!(!auth.allow_cleartext_password);
        assert!(!auth.allow_md5_password);
        assert!(!auth.allow_scram_sha_256);
        assert!(!auth.has_any_password_method());
        // All enterprise methods enabled
        assert!(auth.allow_kerberos_v5);
        assert!(auth.allow_gssapi);
        assert!(auth.allow_sspi);
    }

    #[test]
    fn auth_has_any_password_when_only_cleartext() {
        let auth = crate::driver::AuthSettings {
            allow_cleartext_password: true,
            allow_md5_password: false,
            allow_scram_sha_256: false,
            ..crate::driver::AuthSettings::default()
        };
        assert!(auth.has_any_password_method());
    }

    #[test]
    fn auth_no_password_method_when_all_disabled() {
        let auth = crate::driver::AuthSettings {
            allow_cleartext_password: false,
            allow_md5_password: false,
            allow_scram_sha_256: false,
            ..crate::driver::AuthSettings::default()
        };
        assert!(!auth.has_any_password_method());
    }

    #[test]
    fn auth_enterprise_mechanisms_are_distinct() {
        // Verify the three enterprise mechanisms are distinct values
        assert_ne!(
            crate::driver::EnterpriseAuthMechanism::KerberosV5,
            crate::driver::EnterpriseAuthMechanism::GssApi
        );
        assert_ne!(
            crate::driver::EnterpriseAuthMechanism::GssApi,
            crate::driver::EnterpriseAuthMechanism::Sspi
        );
        assert_ne!(
            crate::driver::EnterpriseAuthMechanism::KerberosV5,
            crate::driver::EnterpriseAuthMechanism::Sspi
        );
    }

    #[test]
    fn auth_channel_binding_default_is_prefer() {
        let auth = crate::driver::AuthSettings::default();
        assert_eq!(
            auth.channel_binding,
            crate::driver::ScramChannelBindingMode::Prefer
        );
    }

    // ══════════════════════════════════════════════════════════════════
    // parse_database_url — query-string stripping
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn parse_database_url_basic() {
        let (host, port, user, db, pw) = crate::driver::PgDriver::parse_database_url(
            "postgresql://admin:secret@localhost:5432/mydb",
        )
        .unwrap();
        assert_eq!(host, "localhost");
        assert_eq!(port, 5432);
        assert_eq!(user, "admin");
        assert_eq!(db, "mydb");
        assert_eq!(pw, Some("secret".to_string()));
    }

    #[test]
    fn parse_database_url_strips_query_params() {
        let (_, _, _, db, _) = crate::driver::PgDriver::parse_database_url(
            "postgresql://user:pass@host:5432/mydb?sslmode=require&auth_mode=scram_only",
        )
        .unwrap();
        assert_eq!(db, "mydb", "query params must not leak into database name");
    }

    #[test]
    fn parse_database_url_strips_single_query_param() {
        let (_, _, _, db, _) = crate::driver::PgDriver::parse_database_url(
            "postgres://u:p@h/testdb?gss_provider=linux_krb5",
        )
        .unwrap();
        assert_eq!(db, "testdb");
    }

    #[test]
    fn parse_database_url_no_query_still_works() {
        let (_, _, _, db, _) =
            crate::driver::PgDriver::parse_database_url("postgresql://user@host:5432/cleandb")
                .unwrap();
        assert_eq!(db, "cleandb");
    }

    #[test]
    fn connect_options_with_logical_replication_replaces_existing_key() {
        let opts = crate::driver::ConnectOptions::default()
            .with_startup_param("Replication", "true")
            .with_startup_param("application_name", "qail")
            .with_logical_replication();

        assert!(
            opts.startup_params
                .iter()
                .any(|(k, v)| k == "replication" && v == "database")
        );
        assert_eq!(
            opts.startup_params
                .iter()
                .filter(|(k, _)| k.eq_ignore_ascii_case("replication"))
                .count(),
            1
        );
        assert!(
            opts.startup_params
                .iter()
                .any(|(k, v)| k == "application_name" && v == "qail")
        );
    }

    #[test]
    fn builder_logical_replication_sets_startup_param() {
        let builder = crate::driver::PgDriverBuilder::new().logical_replication();
        assert!(
            builder
                .connect_options
                .startup_params
                .iter()
                .any(|(k, v)| k == "replication" && v == "database")
        );
    }

    #[test]
    fn connect_options_with_startup_param_replaces_existing_case_insensitively() {
        let opts = crate::driver::ConnectOptions::default()
            .with_startup_param("Application_Name", "qail-a")
            .with_startup_param("application_name", "qail-b");

        assert_eq!(
            opts.startup_params
                .iter()
                .filter(|(k, _)| k.eq_ignore_ascii_case("application_name"))
                .count(),
            1
        );
        assert!(
            opts.startup_params
                .iter()
                .any(|(k, v)| k == "application_name" && v == "qail-b")
        );
    }

    #[test]
    fn builder_startup_param_replaces_existing_case_insensitively() {
        let builder = crate::driver::PgDriverBuilder::new()
            .startup_param("Application_Name", "qail-a")
            .startup_param("application_name", "qail-b");

        assert_eq!(
            builder
                .connect_options
                .startup_params
                .iter()
                .filter(|(k, _)| k.eq_ignore_ascii_case("application_name"))
                .count(),
            1
        );
        assert!(
            builder
                .connect_options
                .startup_params
                .iter()
                .any(|(k, v)| k == "application_name" && v == "qail-b")
        );
    }
}
