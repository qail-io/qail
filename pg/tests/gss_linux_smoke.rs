#![cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]

use qail_pg::{PgPool, PoolConfig};

/// End-to-end Kerberos smoke test.
///
/// Requires:
/// - Linux runtime
/// - `enterprise-gssapi` feature
/// - PostgreSQL configured for GSS/Kerberos auth
/// - valid Kerberos credentials for this process
/// - `QAIL_GSS_TEST_DATABASE_URL` set to a URL containing `gss_provider=linux_krb5`
#[tokio::test]
#[ignore = "requires real Linux Kerberos + PostgreSQL GSS environment"]
async fn gss_linux_smoke_connects() {
    let database_url = std::env::var("QAIL_GSS_TEST_DATABASE_URL")
        .expect("set QAIL_GSS_TEST_DATABASE_URL for Kerberos smoke test");

    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = database_url;
    qail.postgres.min_connections = 1;
    qail.postgres.max_connections = 1;

    let config = PoolConfig::from_qail_config(&qail)
        .expect("failed to parse qail postgres URL for gss smoke test");
    let pool = PgPool::connect(config)
        .await
        .expect("failed to establish gss connection pool");

    let mut conn = pool
        .acquire_system()
        .await
        .expect("failed to acquire system connection");
    conn.execute_simple("SELECT 1")
        .await
        .expect("failed to execute smoke query");
    conn.release().await;

    pool.close().await;
}
