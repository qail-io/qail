use std::collections::HashMap;
use std::sync::Arc;

use qail_core::ast::Value as QailValue;
use qail_core::optimizer::{NestedRelationKind, plan_nested_batch_fetch};
use qail_core::schema::RelationRegistry;
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

        let relation_registry = relation_registry_for_pair(&state.schema, table_name, rel);
        let parent_key_column = relation_registry
            .get(table_name, rel)
            .map(|(fk_col, _)| fk_col.to_string())
            .or_else(|| {
                relation_registry
                    .get(rel, table_name)
                    .map(|(_, ref_col)| ref_col.to_string())
            });

        let Some(parent_key_column) = parent_key_column else {
            conn.release().await;
            return Err(ApiError::parse_error(format!(
                "No relation between '{}' and '{}' for nested expansion",
                table_name, rel
            )));
        };

        let parent_keys: Vec<QailValue> = data
            .iter()
            .filter_map(|row| row.get(&parent_key_column).cloned())
            .map(json_to_qail_value)
            .collect();

        let plan = match plan_nested_batch_fetch(&relation_registry, table_name, rel, parent_keys) {
            Ok(Some(plan)) => plan,
            Ok(None) => continue,
            Err(_) => {
                conn.release().await;
                return Err(ApiError::parse_error(format!(
                    "No relation between '{}' and '{}' for nested expansion",
                    table_name, rel
                )));
            }
        };

        let mut cmd = plan.to_qail();
        let tenant_scope = crate::rest::tenant_scope_filter_for_table(state.as_ref(), auth, rel);
        if let Some((scope_column, tenant_id)) = tenant_scope.as_ref() {
            cmd = cmd.filter(
                scope_column,
                qail_core::ast::Operator::Eq,
                QailValue::String(tenant_id.clone()),
            );
        }
        state.optimize_qail_for_execution(&mut cmd);
        if let Err(e) = state.policy_engine.apply_policies(auth, &mut cmd) {
            conn.release().await;
            return Err(ApiError::forbidden(e.to_string()));
        }
        if let Err(e) = crate::access::check_access_policy(state.as_ref(), auth, &cmd) {
            conn.release().await;
            return Err(e);
        }

        let rows = match conn.fetch_all_uncached(&cmd).await {
            Ok(r) => r,
            Err(e) => {
                conn.release().await;
                return Err(ApiError::from_pg_driver_error(&e, Some(rel)));
            }
        };

        let related_rows: Vec<Value> = rows.iter().map(row_to_json).collect();
        let related_tenant_column = tenant_scope
            .as_ref()
            .map(|(scope_column, _)| scope_column.clone());
        if let Some((scope_column, tenant_id)) = tenant_scope.as_ref()
            && let Err(v) = crate::tenant_guard::verify_tenant_boundary(
                &related_rows,
                tenant_id,
                scope_column,
                rel,
                "rest_nested_expand",
            )
        {
            tracing::error!("{}", v);
            conn.release().await;
            return Err(ApiError::internal("Data integrity error"));
        }

        match plan.kind {
            NestedRelationKind::ForwardObject => {
                let mut related: HashMap<String, Value> =
                    HashMap::with_capacity(related_rows.len());
                for mut json in related_rows {
                    let key = match related_match_key(&json, &plan.related_match_column, rel) {
                        Ok(key) => key,
                        Err(err) => {
                            conn.release().await;
                            return Err(err);
                        }
                    };
                    if let Some(ref scope_column) = related_tenant_column {
                        strip_json_column(&mut json, scope_column);
                    }
                    related.insert(key, json);
                }

                for row in data.iter_mut() {
                    if let Some(parent_key_value) = row.get(&plan.parent_key_column) {
                        let key = json_value_key(parent_key_value);
                        if let Some(related_row) = related.get(&key)
                            && let Some(obj) = row.as_object_mut()
                        {
                            obj.insert(rel.to_string(), related_row.clone());
                        }
                    }
                }
            }
            NestedRelationKind::ReverseArray => {
                let mut grouped: HashMap<String, Vec<Value>> = HashMap::new();
                for mut json in related_rows {
                    let key = match related_match_key(&json, &plan.related_match_column, rel) {
                        Ok(key) => key,
                        Err(err) => {
                            conn.release().await;
                            return Err(err);
                        }
                    };
                    if let Some(ref scope_column) = related_tenant_column {
                        strip_json_column(&mut json, scope_column);
                    }
                    grouped.entry(key).or_default().push(json);
                }

                for row in data.iter_mut() {
                    if let Some(parent_key_value) = row.get(&plan.parent_key_column) {
                        let key = json_value_key(parent_key_value);
                        let children = grouped.get(&key).cloned().unwrap_or_default();
                        if let Some(obj) = row.as_object_mut() {
                            obj.insert(rel.to_string(), serde_json::json!(children));
                        }
                    }
                }
            }
        }
    }

    // Release connection back to pool
    conn.release().await;

    Ok(())
}

fn strip_json_column(row: &mut Value, column: &str) {
    if let Some(object) = row.as_object_mut() {
        object.remove(column);
    }
}

fn related_match_key(row: &Value, match_column: &str, relation: &str) -> Result<String, ApiError> {
    row.get(match_column).map(json_value_key).ok_or_else(|| {
        ApiError::internal(format!(
            "Nested expansion for relation '{}' returned a row missing match column '{}'",
            relation, match_column
        ))
    })
}

fn relation_registry_for_pair(
    schema: &crate::schema::SchemaRegistry,
    left_table: &str,
    right_table: &str,
) -> RelationRegistry {
    let mut relations = RelationRegistry::new();
    if let Some((fk_col, ref_col)) = schema.relation_for(left_table, right_table) {
        relations.register(left_table, fk_col, right_table, ref_col);
    }
    if let Some((fk_col, ref_col)) = schema.relation_for(right_table, left_table) {
        relations.register(right_table, fk_col, left_table, ref_col);
    }
    relations
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaRegistry;

    #[test]
    fn relation_registry_adapter_registers_forward_relation() {
        let mut schema = SchemaRegistry::new();
        schema
            .load_from_qail_str(
                r#"
table users {
    id uuid primary_key
}

table posts {
    id uuid primary_key
    user_id uuid references users(id)
}
"#,
            )
            .expect("schema should parse");

        let rel = relation_registry_for_pair(&schema, "posts", "users");
        assert_eq!(rel.get("posts", "users"), Some(("user_id", "id")));
    }

    #[test]
    fn relation_registry_adapter_registers_reverse_relation_for_planner() {
        let mut schema = SchemaRegistry::new();
        schema
            .load_from_qail_str(
                r#"
table users {
    id uuid primary_key
}

table posts {
    id uuid primary_key
    user_id uuid references users(id)
}
"#,
            )
            .expect("schema should parse");

        // Asking for users -> posts should still register the underlying
        // posts -> users FK so reverse planning can resolve it.
        let rel = relation_registry_for_pair(&schema, "users", "posts");
        assert_eq!(rel.get("posts", "users"), Some(("user_id", "id")));
    }

    #[test]
    fn related_match_key_rejects_rows_missing_match_column() {
        let row = serde_json::json!({"id": "post-1"});

        let err = related_match_key(&row, "user_id", "posts").unwrap_err();

        assert_eq!(err.code, "INTERNAL_ERROR");
    }

    #[test]
    fn related_match_key_accepts_scalar_match_column() {
        let row = serde_json::json!({"user_id": 42});

        let key = related_match_key(&row, "user_id", "posts").unwrap();

        assert_eq!(key, "42");
    }
}
