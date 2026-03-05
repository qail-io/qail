use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use metrics_exporter_prometheus::PrometheusHandle;
use tokio::sync::RwLock;

use super::super::database_url::load_rpc_allow_list;
use crate::config::GatewayConfig;
use crate::error::GatewayError;
use crate::event::EventTriggerEngine;
use crate::middleware::QueryAllowList;
use crate::policy::PolicyEngine;
use crate::schema::SchemaRegistry;
use qail_pg::PgPool;

pub(super) fn load_policy_engine(config: &GatewayConfig) -> Result<PolicyEngine, GatewayError> {
    let mut policy_engine = PolicyEngine::new();
    if let Some(ref policy_path) = config.policy_path
        && let Ok(canonical) =
            crate::config::validate_config_path(policy_path, config.config_root.as_deref())
    {
        tracing::info!("Loading policies from: {}", canonical.display());
        policy_engine.load_from_file(canonical.to_str().unwrap_or(policy_path))?;
    }
    Ok(policy_engine)
}

pub(super) fn load_schema_registry(config: &GatewayConfig) -> Result<SchemaRegistry, GatewayError> {
    let mut schema = SchemaRegistry::new();
    if let Some(ref schema_path) = config.schema_path
        && let Ok(canonical) =
            crate::config::validate_config_path(schema_path, config.config_root.as_deref())
    {
        tracing::info!("Loading schema from: {}", canonical.display());
        schema.load_from_file(canonical.to_str().unwrap_or(schema_path))?;
    }
    Ok(schema)
}

pub(super) async fn verify_schema_drift(
    pool: &PgPool,
    schema: &SchemaRegistry,
) -> Result<(), GatewayError> {
    if schema.table_names().is_empty() {
        return Ok(());
    }

    let mut conn = pool.acquire_system().await.map_err(|e| {
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
                    "Schema drift detected! {} table(s) missing from database: {}",
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
    if let Some(ref events_path) = config.events_path
        && let Ok(canonical) =
            crate::config::validate_config_path(events_path, config.config_root.as_deref())
    {
        event_engine
            .load_from_file(canonical.to_str().unwrap_or(events_path))
            .map_err(GatewayError::Config)?;
    }
    Ok(event_engine)
}

pub(super) async fn load_user_operator_map(
    pool: &PgPool,
) -> Result<Arc<RwLock<HashMap<String, String>>>, GatewayError> {
    let user_operator_map = Arc::new(RwLock::new(HashMap::new()));
    let token = qail_core::rls::SuperAdminToken::for_system_process("embedded_user_map");
    let rls = qail_core::rls::RlsContext::super_admin(token);
    let mut conn = pool
        .acquire_with_rls(rls)
        .await
        .map_err(|e| GatewayError::Database(format!("User lookup connection failed: {}", e)))?;
    let cmd = qail_core::ast::Qail::get("users")
        .columns(["id", "operator_id"])
        .limit(10_000);
    match conn.fetch_all_uncached(&cmd).await {
        Ok(rows) => {
            let mut map = user_operator_map.write().await;
            for row in &rows {
                let uid = row
                    .try_get_by_name::<String>("id")
                    .ok()
                    .or_else(|| row.get_string(0));
                let oid = row
                    .try_get_by_name::<String>("operator_id")
                    .ok()
                    .or_else(|| row.get_string(1));

                if let (Some(uid), Some(oid)) = (uid, oid)
                    && !oid.is_empty()
                {
                    map.insert(uid, oid);
                }
            }
            tracing::info!(
                "Loaded {} user→operator mappings for JWT resolution",
                map.len()
            );
        }
        Err(e) => {
            tracing::warn!("Could not load user→operator map (non-fatal): {}", e);
        }
    }
    conn.release().await;
    Ok(user_operator_map)
}

pub(super) fn build_prometheus_handle() -> Result<Arc<PrometheusHandle>, GatewayError> {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets(&[
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ])
        .map_err(|e| GatewayError::Config(format!("Failed to set Prometheus buckets: {}", e)))?;
    let recorder = builder.build_recorder();
    let handle = recorder.handle();
    Ok(Arc::new(handle))
}

pub(super) fn load_allow_list(config: &GatewayConfig) -> Result<QueryAllowList, GatewayError> {
    let mut allow_list = QueryAllowList::new();
    if let Some(ref path) = config.allow_list_path
        && let Ok(canonical) =
            crate::config::validate_config_path(path, config.config_root.as_deref())
    {
        allow_list
            .load_from_file(canonical.to_str().unwrap_or(path))
            .map_err(|e| {
                GatewayError::Config(format!("Failed to load allow-list from '{}': {}", path, e))
            })?;
    }
    Ok(allow_list)
}

pub(super) fn load_rpc_allow_list_from_config(
    config: &GatewayConfig,
) -> Result<Option<HashSet<String>>, GatewayError> {
    if let Some(ref path) = config.rpc_allowlist_path {
        if let Ok(canonical) =
            crate::config::validate_config_path(path, config.config_root.as_deref())
        {
            return Ok(Some(load_rpc_allow_list(&canonical)?));
        }
    }
    Ok(None)
}

pub(super) async fn load_jwks_store() -> Option<crate::jwks::JwksKeyStore> {
    if let Some(store) = crate::jwks::JwksKeyStore::from_env() {
        match store.initial_fetch().await {
            Ok(n) => {
                tracing::info!("JWKS: loaded {} keys from endpoint", n);
                store.start_refresh_task();
                Some(store)
            }
            Err(e) => {
                tracing::warn!("JWKS: initial fetch failed (non-fatal): {}", e);
                Some(store)
            }
        }
    } else {
        None
    }
}
