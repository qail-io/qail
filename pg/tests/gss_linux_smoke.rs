#![cfg(all(feature = "enterprise-gssapi", target_os = "linux"))]

use qail_pg::{PgError, PgPool, PgResult, PoolConfig};

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
async fn gss_linux_smoke_connects() -> PgResult<()> {
    let database_url = std::env::var("QAIL_GSS_TEST_DATABASE_URL").map_err(|_| {
        PgError::Connection("set QAIL_GSS_TEST_DATABASE_URL for Kerberos smoke test".into())
    })?;

    let mut qail = qail_core::config::QailConfig::default();
    qail.postgres.url = database_url;
    qail.postgres.min_connections = 1;
    qail.postgres.max_connections = 1;

    let config = PoolConfig::from_qail_config(&qail)?;
    let pool = PgPool::connect(config).await?;

    let mut conn = pool.acquire_system().await?;
    conn.get_mut()?.execute_simple("SELECT 1").await?;
    conn.release().await;

    pool.close().await;
    Ok(())
}
