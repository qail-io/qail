use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::RwLock;

use super::super::database_url::parse_database_url;
use crate::config::GatewayConfig;
use crate::error::GatewayError;
use crate::event::EventTriggerEngine;
use crate::policy::PolicyEngine;
use crate::schema::SchemaRegistry;
use qail_pg::PgPool;

pub(super) fn load_policy_engine(config: &GatewayConfig) -> Result<PolicyEngine, GatewayError> {
    let mut policy_engine = PolicyEngine::new();
    if let Some(ref policy_path) = config.policy_path {
        crate::config::validate_config_path(policy_path, config.config_root.as_deref())
            .map_err(GatewayError::Config)?;
        tracing::info!("Loading policies from: {}", policy_path);
        policy_engine.load_from_file(policy_path)?;
    }
    Ok(policy_engine)
}

pub(super) fn load_schema_registry(config: &GatewayConfig) -> Result<SchemaRegistry, GatewayError> {
    let mut schema = SchemaRegistry::new();
    if let Some(ref schema_path) = config.schema_path {
        crate::config::validate_config_path(schema_path, config.config_root.as_deref())
            .map_err(GatewayError::Config)?;
        tracing::info!("Loading schema from: {}", schema_path);
        schema.load_from_file(schema_path)?;
    }
    Ok(schema)
}

pub(super) async fn build_pool(config: &GatewayConfig) -> Result<PgPool, GatewayError> {
    tracing::info!("Creating connection pool...");
    let mut pool_config = parse_database_url(&config.database_url, config)?;

    if let Ok(min) = std::env::var("POOL_MIN_CONNECTIONS")
        && let Ok(n) = min.parse()
    {
        pool_config = pool_config.min_connections(n);
    }
    if let Ok(max) = std::env::var("POOL_MAX_CONNECTIONS")
        && let Ok(n) = max.parse()
    {
        pool_config = pool_config.max_connections(n);
    }

    let pool = PgPool::connect(pool_config)
        .await
        .map_err(|e| GatewayError::Database(e.to_string()))?;

    let stats = pool.stats().await;
    tracing::info!(
        "Connection pool: {} idle, {} max",
        stats.idle,
        stats.max_size
    );

    Ok(pool)
}

pub(super) async fn verify_schema_drift(
    pool: &PgPool,
    schema: &SchemaRegistry,
    statement_timeout_ms: u32,
    lock_timeout_ms: u32,
) -> Result<(), GatewayError> {
    if schema.table_names().is_empty() {
        return Ok(());
    }

    tracing::info!("Verifying schema against database...");
    let mut conn = pool
        .acquire_with_rls_timeouts(
            qail_core::rls::RlsContext::empty(),
            statement_timeout_ms,
            lock_timeout_ms,
        )
        .await
        .map_err(|e| {
            GatewayError::Database(format!("Schema verification connection failed: {}", e))
        })?;

    let cmd = qail_core::ast::Qail::get("information_schema.tables")
        .columns(["table_name"])
        .eq(
            "table_schema",
            qail_core::ast::Value::String("public".into()),
        );

    match conn.fetch_all_uncached(&cmd).await {
        Ok(rows) => {
            let db_tables: HashSet<String> = rows
                .iter()
                .filter_map(|row| {
                    row.try_get_by_name::<String>("table_name")
                        .ok()
                        .or_else(|| row.get_string(0))
                })
                .collect();

            let mut missing = Vec::new();
            for table in schema.table_names() {
                if !db_tables.contains(table) {
                    missing.push(table.to_string());
                }
            }

            if !missing.is_empty() {
                let msg = format!(
                    "Schema drift detected! {} table(s) defined in .qail but missing from database: {}",
                    missing.len(),
                    missing.join(", ")
                );
                tracing::error!("{}", msg);
                conn.release().await;
                return Err(GatewayError::Config(msg));
            }

            tracing::info!(
                "Schema verified: {} tables match ({} in DB)",
                schema.table_names().len(),
                db_tables.len()
            );
        }
        Err(e) => {
            tracing::warn!("Schema verification skipped (query failed): {}", e);
        }
    }

    conn.release().await;
    Ok(())
}

pub(super) fn load_event_engine(
    config: &GatewayConfig,
) -> Result<EventTriggerEngine, GatewayError> {
    let mut event_engine = EventTriggerEngine::new();
    if let Some(ref events_path) = config.events_path {
        crate::config::validate_config_path(events_path, config.config_root.as_deref())
            .map_err(GatewayError::Config)?;
        tracing::info!("Loading event triggers from: {}", events_path);
        event_engine
            .load_from_file(events_path)
            .map_err(GatewayError::Config)?;
    }
    Ok(event_engine)
}

pub(super) async fn load_user_tenant_map(
    pool: &PgPool,
    process_name: &str,
    statement_timeout_ms: u32,
    lock_timeout_ms: u32,
) -> Result<Arc<RwLock<HashMap<String, String>>>, GatewayError> {
    // Legacy variable/function name kept to avoid wide call-site churn.
    // Values are tenant IDs from `users.tenant_id`.
    let user_tenant_map = Arc::new(RwLock::new(HashMap::new()));

    let token = qail_core::rls::SuperAdminToken::for_system_process(process_name);
    let rls = qail_core::rls::RlsContext::super_admin(token);
    let mut conn = pool
        .acquire_with_rls_timeouts(rls, statement_timeout_ms, lock_timeout_ms)
        .await
        .map_err(|e| GatewayError::Database(format!("User lookup connection failed: {}", e)))?;

    let rows_result = conn
        .fetch_all_uncached(
            &qail_core::ast::Qail::get("users")
                .columns(["id", "tenant_id"])
                .limit(10_000),
        )
        .await;

    match rows_result {
        Ok(rows) => {
            let mut map = user_tenant_map.write().await;
            for row in &rows {
                let uid = row
                    .try_get_by_name::<String>("id")
                    .ok()
                    .or_else(|| row.get_string(0));
                let tenant_id = row
                    .try_get_by_name::<String>("tenant_id")
                    .ok()
                    .or_else(|| row.get_string(1));

                if let (Some(uid), Some(tenant_id)) = (uid, tenant_id)
                    && !tenant_id.is_empty()
                {
                    map.insert(uid, tenant_id);
                }
            }
            tracing::info!(
                "Loaded {} user→tenant mappings for JWT resolution",
                map.len()
            );
        }
        Err(e) => {
            tracing::warn!("Could not load user→tenant map (non-fatal): {}", e);
        }
    }

    conn.release().await;
    Ok(user_tenant_map)
}
