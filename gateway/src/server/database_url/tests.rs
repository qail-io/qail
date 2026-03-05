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
