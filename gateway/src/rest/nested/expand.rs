use std::collections::HashMap;
use std::sync::Arc;

use qail_core::ast::{Operator, Value as QailValue};
use serde_json::Value;

use crate::GatewayState;
use crate::handler::row_to_json;
use crate::middleware::ApiError;

use super::{json_to_qail_value, json_value_key};

/// Expand FK relations into nested JSON objects/arrays.
///
/// - **Forward FK** (e.g., `orders?expand=nested:users`):
///   `order.user_id` → `order.user = {id, name, ...}` (nested object)
/// - **Reverse FK** (e.g., `users?expand=nested:orders`):
///   `user` → `user.orders = [{...}, {...}]` (nested array)
///
/// Uses batched WHERE IN queries to avoid N+1.
pub async fn expand_nested(
    state: &Arc<GatewayState>,
    table_name: &str,
    data: &mut [Value],
    relations: &[&str],
    auth: &crate::auth::AuthContext,
) -> Result<(), ApiError> {
    let mut conn = state
        .acquire_with_auth_rls_guarded(auth, Some(table_name))
        .await?;

    for rel in relations {
        // SECURITY: Block nested expansion into inaccessible tables
        let rel_blocked = if !state.allowed_tables.is_empty() {
            !state.allowed_tables.contains(*rel)
        } else {
            state.blocked_tables.contains(*rel)
        };
        if rel_blocked {
            conn.release().await;
            return Err(ApiError::forbidden(format!(
                "Table '{}' is not accessible via REST",
                rel
            )));
        }

        // Try forward FK: this table → rel table
        if let Some((fk_col, ref_col)) = state.schema.relation_for(table_name, rel) {
            // Collect all FK values from data
            let fk_values: Vec<QailValue> = data
                .iter()
                .filter_map(|row| row.get(fk_col).cloned())
                .filter(|v| !v.is_null())
                .map(json_to_qail_value)
                .collect();

            if fk_values.is_empty() {
                continue;
            }

            // Fetch related rows in one query: get rel[ref_col IN (...)]
            let mut cmd = qail_core::ast::Qail::get(*rel).filter(
                ref_col,
                Operator::In,
                QailValue::Array(fk_values),
            );
            if let Err(e) = state.policy_engine.apply_policies(auth, &mut cmd) {
                conn.release().await;
                return Err(ApiError::forbidden(e.to_string()));
            }

            let rows = match conn.fetch_all_uncached(&cmd).await {
                Ok(r) => r,
                Err(e) => {
                    conn.release().await;
                    return Err(ApiError::from_pg_driver_error(&e, Some(rel)));
                }
            };

            // Index by PK
            let related: HashMap<String, Value> = rows
                .iter()
                .map(|row| {
                    let json = row_to_json(row);
                    let key = json.get(ref_col).map(json_value_key).unwrap_or_default();
                    (key, json)
                })
                .collect();

            // Inject nested object
            for row in data.iter_mut() {
                if let Some(fk_val) = row.get(fk_col) {
                    let key = json_value_key(fk_val);
                    if let Some(related_row) = related.get(&key)
                        && let Some(obj) = row.as_object_mut()
                    {
                        obj.insert(rel.to_string(), related_row.clone());
                    }
                }
            }
            continue;
        }

        // Try reverse FK: rel table → this table
        if let Some((fk_col, ref_col)) = state.schema.relation_for(rel, table_name) {
            // Collect all PK values from data
            let pk_values: Vec<QailValue> = data
                .iter()
                .filter_map(|row| row.get(ref_col).cloned())
                .filter(|v| !v.is_null())
                .map(json_to_qail_value)
                .collect();

            if pk_values.is_empty() {
                continue;
            }

            // Fetch all child rows: get rel[fk_col IN (...)]
            let mut cmd = qail_core::ast::Qail::get(*rel).filter(
                fk_col,
                Operator::In,
                QailValue::Array(pk_values),
            );
            if let Err(e) = state.policy_engine.apply_policies(auth, &mut cmd) {
                conn.release().await;
                return Err(ApiError::forbidden(e.to_string()));
            }

            let rows = match conn.fetch_all_uncached(&cmd).await {
                Ok(r) => r,
                Err(e) => {
                    conn.release().await;
                    return Err(ApiError::from_pg_driver_error(&e, Some(rel)));
                }
            };

            // Group by FK value
            let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();
            for row in &rows {
                let json = row_to_json(row);
                let key = json.get(fk_col).map(json_value_key).unwrap_or_default();
                grouped.entry(key).or_default().push(json);
            }

            // Inject nested array
            for row in data.iter_mut() {
                if let Some(pk_val) = row.get(ref_col) {
                    let key = json_value_key(pk_val);
                    let children = grouped.get(&key).cloned().unwrap_or_default();
                    if let Some(obj) = row.as_object_mut() {
                        obj.insert(rel.to_string(), serde_json::json!(children));
                    }
                }
            }
            continue;
        }

        // Release connection before returning error
        conn.release().await;
        return Err(ApiError::parse_error(format!(
            "No relation between '{}' and '{}' for nested expansion",
            table_name, rel
        )));
    }

    // Release connection back to pool
    conn.release().await;

    Ok(())
}
