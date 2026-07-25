use super::*;
use crate::config::GatewayConfig;
use std::io::Write;

fn default_cfg() -> GatewayConfig {
    GatewayConfig::default()
}

#[test]
fn test_parse_bool_query_variants() {
    assert_eq!(parse_bool_query("true"), Some(true));
    assert_eq!(parse_bool_query("YES"), Some(true));
    assert_eq!(parse_bool_query("0"), Some(false));
    assert_eq!(parse_bool_query("off"), Some(false));
    assert_eq!(parse_bool_query("invalid"), None);
}

#[test]
fn test_load_rpc_allow_list_skips_comments_and_normalizes() {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "qail_rpc_allowlist_{}_{}.txt",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));

    let mut file = std::fs::File::create(&path).expect("create temp allowlist");
    writeln!(file, "api.search_orders").expect("write");
    writeln!(file, "  # comment").expect("write");
    writeln!(file, "public.Ping").expect("write");

    let list = load_rpc_allow_list(&path).expect("load allowlist");
    assert!(list.contains("api.search_orders"));
    assert!(list.contains("public.ping"));
    assert_eq!(list.len(), 2);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_parse_database_url_decodes_user_password_and_database() {
    let cfg = parse_database_url(
        "postgres://us%40er:p%40ss%2Fword@db.internal:5432/my%2Fdb",
        &default_cfg(),
    )
    .expect("expected valid encoded postgres URL");

    assert_eq!(cfg.user, "us@er");
    assert_eq!(cfg.password.as_deref(), Some("p@ss/word"));
    assert_eq!(cfg.database, "my/db");
    assert!(!cfg.io_uring);
}

#[test]
fn test_parse_database_url_uses_gateway_io_uring_default() {
    let mut gateway = default_cfg();
    gateway.pg_io_uring = true;

    let cfg = parse_database_url("postgres://alice@db.internal:5432/app", &gateway)
        .expect("expected valid URL");

    assert!(cfg.io_uring);
}

#[test]
fn test_parse_database_url_io_uring_query_overrides_gateway_default() {
    let mut gateway = default_cfg();
    gateway.pg_io_uring = true;

    let cfg = parse_database_url(
        "postgres://alice@db.internal:5432/app?io_uring=false",
        &gateway,
    )
    .expect("expected valid URL");

    assert!(!cfg.io_uring);

    let cfg = parse_database_url(
        "postgres://alice@db.internal:5432/app?io_uring=true",
        &default_cfg(),
    )
    .expect("expected valid URL");

    assert!(cfg.io_uring);
}

#[test]
fn test_parse_database_url_rejects_invalid_io_uring_query() {
    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?io_uring=auto",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected invalid io_uring bool error"),
        Err(e) => e,
    };

    assert!(err.to_string().contains("Invalid io_uring value"));
}

#[test]
fn test_parse_database_url_rejects_malformed_percent_encoding() {
    let err = match parse_database_url(
        "postgres://alice:bad%ZZ@db.internal:5432/app",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected malformed password percent escape error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("two hex digits"));

    let err = match parse_database_url("postgres://alice@db.internal:5432/app%", &default_cfg()) {
        Ok(_) => panic!("expected trailing database percent escape error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("two hex digits"));
}

#[test]
fn test_parse_database_url_rejects_invalid_gss_provider() {
    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?gss_provider=unknown",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected invalid gss_provider error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("Invalid gss_provider value"));
}

#[test]
fn test_parse_database_url_rejects_empty_gss_service() {
    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?gss_service=",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected empty gss_service error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("gss_service must not be empty"));
}

#[test]
fn test_parse_database_url_parses_gss_retry_settings() {
    let cfg = parse_database_url(
        "postgres://alice@db.internal:5432/app?gss_connect_retries=6&gss_retry_base_ms=350&gss_circuit_threshold=7&gss_circuit_window_ms=45000&gss_circuit_cooldown_ms=9000",
        &default_cfg(),
    )
    .expect("expected valid url");
    assert_eq!(cfg.gss_connect_retries, 6);
    assert_eq!(
        cfg.gss_retry_base_delay,
        std::time::Duration::from_millis(350)
    );
    assert_eq!(cfg.gss_circuit_breaker_threshold, 7);
    assert_eq!(
        cfg.gss_circuit_breaker_window,
        std::time::Duration::from_secs(45)
    );
    assert_eq!(
        cfg.gss_circuit_breaker_cooldown,
        std::time::Duration::from_secs(9)
    );
}

#[test]
fn test_parse_database_url_rejects_invalid_pool_size_params() {
    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?max_connections=not-a-number",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected invalid max_connections error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("Invalid max_connections value"));

    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?max_connections=0",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected zero max_connections error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("max_connections must be greater than 0")
    );

    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?min_connections=5&max_connections=4",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected min/max pool size error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("min_connections (5)"));
}

#[test]
fn test_parse_database_url_rejects_invalid_gss_retry_base() {
    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?gss_retry_base_ms=0",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected invalid gss_retry_base_ms error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("gss_retry_base_ms must be greater than 0")
    );
}

#[test]
fn test_parse_database_url_rejects_invalid_gss_connect_retries() {
    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?gss_connect_retries=99",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected invalid gss_connect_retries error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("gss_connect_retries must be <= 20")
    );
}

#[test]
fn test_parse_database_url_rejects_invalid_gss_circuit_threshold() {
    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?gss_circuit_threshold=101",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected invalid gss_circuit_threshold error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("gss_circuit_threshold must be <= 100")
    );
}

#[test]
fn test_parse_database_url_rejects_invalid_gss_circuit_window() {
    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?gss_circuit_window_ms=0",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected invalid gss_circuit_window_ms error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("gss_circuit_window_ms must be greater than 0")
    );
}

#[test]
fn test_parse_database_url_rejects_invalid_gss_circuit_cooldown() {
    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?gss_circuit_cooldown_ms=0",
        &default_cfg(),
    ) {
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
fn test_parse_database_url_linux_krb5_requires_feature_on_linux() {
    let err = match parse_database_url(
        "postgres://alice@db.internal:5432/app?gss_provider=linux_krb5",
        &default_cfg(),
    ) {
        Ok(_) => panic!("expected linux_krb5 feature-gate error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("requires gateway feature enterprise-gssapi on Linux")
    );
}
