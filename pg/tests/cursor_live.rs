//! Live PostgreSQL regression tests for cursor transaction cleanup.
//!
//! Default local target:
//!   QAIL_TEST_DB_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test cursor_live -- --ignored --nocapture

use qail_core::ast::Qail;
use qail_pg::{PgDriver, PgResult};

fn database_url() -> String {
    std::env::var("QAIL_TEST_DB_URL").unwrap_or_else(|_| {
        "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
    })
}

#[tokio::test]
#[ignore = "Requires local Podman PostgreSQL qail-pg18-lab on 127.0.0.1:55432"]
async fn stream_cmd_rolls_back_after_declare_error() -> PgResult<()> {
    let mut driver = PgDriver::connect_url(&database_url()).await?;
    let cmd = Qail::get("qail_missing_cursor_table").select_all();

    let err = match driver.stream_cmd(&cmd, 10).await {
        Ok(_) => panic!("missing table should fail cursor declaration"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("qail_missing_cursor_table")
            || err.to_string().contains("does not exist"),
        "{err}"
    );

    driver.execute_simple("SELECT 1").await?;
    Ok(())
}
