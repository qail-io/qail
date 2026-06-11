//! Live PostgreSQL validation for access-checked qail-pg APIs.
//!
//! Default local target:
//!   podman start qail-pg18-lab
//!   QAIL_ACCESS_DATABASE_URL=postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab \
//!   cargo test -p qail-pg --test access_checked_live -- --ignored --nocapture

use qail_core::access::{
    AccessContext, AccessOperation, AccessPolicy, ColumnRule, TableAccessPolicy,
};
use qail_core::ast::{Qail, Value};
use qail_core::rls::RlsContext;
use qail_pg::{PgDriver, PgError, PgPool, PgResult, PoolConfig};
use uuid::Uuid;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

fn database_url() -> String {
    std::env::var("QAIL_ACCESS_DATABASE_URL")
        .or_else(|_| std::env::var("QAIL_TEST_DB_URL"))
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgres://qail_lab:qail_lab@127.0.0.1:55432/qail_engine_lab".to_string()
        })
}

fn pool_config_from_url(url: &str) -> PgResult<PoolConfig> {
    let after_scheme = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .ok_or_else(|| PgError::Connection("access live test URL must be postgres://".into()))?;
    let (auth, host_db) = after_scheme
        .rsplit_once('@')
        .ok_or_else(|| PgError::Connection("access live test URL must include user auth".into()))?;
    let (user, password) = auth
        .split_once(':')
        .ok_or_else(|| PgError::Connection("access live test URL must include password".into()))?;
    let (host_port, db_query) = host_db
        .split_once('/')
        .ok_or_else(|| PgError::Connection("access live test URL must include database".into()))?;
    let database = db_query.split('?').next().unwrap_or(db_query);
    let (host, port) = host_port
        .rsplit_once(':')
        .ok_or_else(|| PgError::Connection("access live test URL must include port".into()))?;
    let port = port
        .parse::<u16>()
        .map_err(|err| PgError::Connection(format!("invalid access live test port: {err}")))?;

    Ok(PoolConfig::new_dev(host, port, user, database)
        .password(password)
        .max_connections(2)
        .min_connections(0))
}

fn test_table(prefix: &str) -> String {
    format!("{}_{}", prefix, Uuid::new_v4().simple())
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn connect() -> PgResult<PgDriver> {
    PgDriver::connect_url(&database_url()).await
}

async fn create_access_table(driver: &mut PgDriver, table: &str) -> PgResult<()> {
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {table}"))
        .await?;
    driver
        .execute_simple(&format!(
            "CREATE TABLE {table} (
                id integer PRIMARY KEY,
                name text NOT NULL,
                private_note text,
                tenant_id text
            )"
        ))
        .await
}

async fn drop_table(driver: &mut PgDriver, table: &str) -> PgResult<()> {
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {table}"))
        .await
}

async fn drop_role(driver: &mut PgDriver, role: &str) -> PgResult<()> {
    let role = quote_literal(role);
    driver
        .execute_simple(&format!(
            "DO $qail$
             BEGIN
               IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = {role}) THEN
                 EXECUTE format('DROP OWNED BY %I', {role});
                 EXECUTE format('DROP ROLE %I', {role});
               END IF;
             END
             $qail$"
        ))
        .await
}

fn policy_for(table: &str) -> AccessPolicy {
    AccessPolicy::new().with_table(
        table,
        TableAccessPolicy::new()
            .allow_operations([
                AccessOperation::Read,
                AccessOperation::Create,
                AccessOperation::Update,
                AccessOperation::Delete,
            ])
            .read_columns(ColumnRule::only(["id", "name", "tenant_id"]))
            .write_columns(ColumnRule::only(["id", "name", "tenant_id"]))
            .returning_columns(ColumnRule::only(["id", "name", "tenant_id"])),
    )
}

fn access_ctx() -> AccessContext {
    AccessContext::subject("qa-access-live").with_tenant(TENANT_A)
}

fn query_error_string(err: PgError) -> PgResult<String> {
    match err {
        PgError::Query(message) | PgError::Connection(message) | PgError::Encode(message) => {
            Ok(message)
        }
        other => Err(other),
    }
}

fn text_cell(row: &qail_pg::PgRow, idx: usize, label: &str) -> PgResult<String> {
    row.get_string(idx)
        .ok_or_else(|| PgError::Query(format!("missing {label} cell")))
}

#[tokio::test]
#[ignore = "Requires live PostgreSQL; default local target is qail-pg18-lab on 127.0.0.1:55432"]
async fn checked_driver_fetch_execute_and_batch_preflight_against_postgres() -> PgResult<()> {
    let mut driver = connect().await?;
    let table = test_table("qail_access_driver");
    create_access_table(&mut driver, &table).await?;

    let result = async {
        driver
            .execute_simple(&format!(
                "INSERT INTO {table} (id, name, private_note, tenant_id)
                 VALUES (1, 'seed', 'secret', '{TENANT_A}')"
            ))
            .await?;

        let policy = policy_for(&table);
        let ctx = access_ctx();
        let allowed_read = Qail::get(&table).columns(["id", "name"]).order_asc("id");
        let rows = driver
            .fetch_all_checked(&allowed_read, &ctx, &policy)
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(text_cell(&rows[0], 1, "name")?, "seed");

        let denied_read = Qail::get(&table).columns(["id", "private_note"]);
        let err = driver
            .fetch_all_checked(&denied_read, &ctx, &policy)
            .await
            .err()
            .ok_or_else(|| PgError::Query("private_note read should be denied".into()))?;
        assert!(query_error_string(err)?.contains("private_note"));

        let one = driver
            .fetch_one_checked(
                &Qail::get(&table).columns(["id"]).eq("id", 1),
                &ctx,
                &policy,
            )
            .await?;
        assert_eq!(
            one.get_i32(0)
                .ok_or_else(|| PgError::Query("missing id cell".into()))?,
            1
        );

        let add = Qail::add(&table)
            .columns(["id", "name"])
            .values([Value::Int(2), Value::String("created".into())]);
        assert_eq!(driver.execute_checked(&add, &ctx, &policy).await?, 1);

        let batch_allowed = Qail::add(&table)
            .columns(["id", "name"])
            .values([Value::Int(3), Value::String("batch-should-not-run".into())]);
        let batch_denied = Qail::add(&table)
            .columns(["id", "private_note"])
            .values([Value::Int(4), Value::String("denied".into())]);
        let err = driver
            .execute_batch_checked(&[batch_allowed, batch_denied], &ctx, &policy)
            .await
            .err()
            .ok_or_else(|| PgError::Query("denied batch command should fail".into()))?;
        assert!(query_error_string(err)?.contains("private_note"));

        let skipped = driver
            .fetch_all(&Qail::get(&table).columns(["id"]).eq("id", 3))
            .await?;
        assert!(
            skipped.is_empty(),
            "batch preflight must reject before the first command executes"
        );

        Ok(())
    }
    .await;

    let cleanup = drop_table(&mut driver, &table).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "Requires live PostgreSQL; default local target is qail-pg18-lab on 127.0.0.1:55432"]
async fn checked_copy_and_stream_paths_against_postgres() -> PgResult<()> {
    let mut driver = connect().await?;
    let table = test_table("qail_access_copy");
    create_access_table(&mut driver, &table).await?;

    let result = async {
        let policy = policy_for(&table);
        let ctx = access_ctx();

        let bulk_cmd = Qail::add(&table).columns(["id", "name"]);
        let rows = vec![
            vec![Value::Int(10), Value::String("bulk-a".into())],
            vec![Value::Int(11), Value::String("bulk-b".into())],
        ];
        assert_eq!(
            driver
                .copy_bulk_checked(&bulk_cmd, &rows, &ctx, &policy)
                .await?,
            2
        );

        let denied_bulk = Qail::add(&table).columns(["id", "private_note"]);
        let err = driver
            .copy_bulk_checked(
                &denied_bulk,
                &[vec![Value::Int(12), Value::String("hidden".into())]],
                &ctx,
                &policy,
            )
            .await
            .err()
            .ok_or_else(|| PgError::Query("private_note COPY insert should be denied".into()))?;
        assert!(query_error_string(err)?.contains("private_note"));

        let export_columns = vec!["id".to_string(), "name".to_string()];
        let exported = driver
            .copy_export_table_checked(&table, &export_columns, &ctx, &policy)
            .await?;
        let exported = String::from_utf8(exported)
            .map_err(|err| PgError::Protocol(format!("COPY output was not UTF-8: {err}")))?;
        assert!(exported.contains("bulk-a"));
        assert!(exported.contains("bulk-b"));

        let denied_export_columns = vec!["id".to_string(), "private_note".to_string()];
        let err = driver
            .copy_export_table_checked(&table, &denied_export_columns, &ctx, &policy)
            .await
            .err()
            .ok_or_else(|| PgError::Query("private_note COPY export should be denied".into()))?;
        assert!(query_error_string(err)?.contains("private_note"));

        let mut streamed_rows = Vec::new();
        driver
            .copy_export_cmd_stream_rows_checked(
                &Qail::export(&table).columns(["id", "name"]),
                |row| {
                    streamed_rows.push(row);
                    Ok(())
                },
                &ctx,
                &policy,
            )
            .await?;
        assert_eq!(streamed_rows.len(), 2);

        let batches = driver
            .stream_cmd_checked(
                &Qail::get(&table).columns(["id", "name"]).order_asc("id"),
                1,
                &ctx,
                &policy,
            )
            .await?;
        assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), 2);

        let err = driver
            .stream_cmd_checked(
                &Qail::get(&table).columns(["id", "private_note"]),
                1,
                &ctx,
                &policy,
            )
            .await
            .err()
            .ok_or_else(|| PgError::Query("private_note cursor stream should be denied".into()))?;
        assert!(query_error_string(err)?.contains("private_note"));

        driver.execute_simple("SELECT 1").await?;
        Ok(())
    }
    .await;

    let cleanup = drop_table(&mut driver, &table).await;
    result.and(cleanup)
}

#[tokio::test]
#[ignore = "Requires live PostgreSQL; default local target is qail-pg18-lab on 127.0.0.1:55432"]
async fn checked_pooled_rls_path_applies_vertical_and_tenant_policy() -> PgResult<()> {
    let mut setup = connect().await?;
    let table = test_table("qail_access_rls");
    let role = test_table("qail_access_rls_role");
    let password = format!("pw_{}", Uuid::new_v4().simple());
    create_access_table(&mut setup, &table).await?;

    let result = async {
        let quoted_role = quote_ident(&role);
        setup
            .execute_simple(&format!(
                "INSERT INTO {table} (id, name, private_note, tenant_id)
                 VALUES
                   (1, 'tenant-a-row', 'secret-a', '{TENANT_A}'),
                   (2, 'tenant-b-row', 'secret-b', '{TENANT_B}');
                 ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;
                 ALTER TABLE {table} FORCE ROW LEVEL SECURITY;
                 CREATE POLICY {table}_tenant_policy ON {table}
                   USING (tenant_id = current_setting('app.current_tenant_id', true))
                   WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true));
                 CREATE ROLE {quoted_role}
                   LOGIN PASSWORD {}
                   NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
                 GRANT USAGE ON SCHEMA public TO {quoted_role};
                 GRANT SELECT ON {table} TO {quoted_role}",
                quote_literal(&password)
            ))
            .await?;

        let policy = policy_for(&table);
        let ctx = access_ctx();
        let mut pool_config = pool_config_from_url(&database_url())?;
        pool_config.user = role.clone();
        pool_config.password = Some(password.clone());
        let pool = PgPool::connect(pool_config).await?;
        let rls_sql = qail_pg::rls_sql_with_timeout(&RlsContext::tenant(TENANT_A), 5_000);

        let mut conn = pool.acquire_raw().await?;
        let rows = conn
            .fetch_all_with_rls_checked(
                &Qail::get(&table)
                    .columns(["id", "name", "tenant_id"])
                    .order_asc("id"),
                &rls_sql,
                &ctx,
                &policy,
            )
            .await?;
        conn.release_checked().await?;

        assert_eq!(rows.len(), 1);
        assert_eq!(text_cell(&rows[0], 1, "name")?, "tenant-a-row");
        assert_eq!(text_cell(&rows[0], 2, "tenant_id")?, TENANT_A);

        let mut denied_conn = pool.acquire_raw().await?;
        let denied_result = denied_conn
            .fetch_all_with_rls_checked(
                &Qail::get(&table).columns(["id", "private_note"]),
                &rls_sql,
                &ctx,
                &policy,
            )
            .await;
        let denied_release = denied_conn.release_checked().await;
        let err = denied_result
            .err()
            .ok_or_else(|| PgError::Query("private_note RLS read should be denied".into()))?;
        denied_release?;
        assert!(query_error_string(err)?.contains("private_note"));

        pool.close().await;
        Ok(())
    }
    .await;

    let cleanup = drop_table(&mut setup, &table).await;
    let role_cleanup = drop_role(&mut setup, &role).await;
    result.and(cleanup).and(role_cleanup)
}
