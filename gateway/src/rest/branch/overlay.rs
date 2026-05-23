use serde_json::Value;

use crate::middleware::ApiError;

fn json_value_to_pk(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
    }
}

fn ensure_pk_on_overlay_row(row: &mut Value, pk_column: &str, row_pk: &str) {
    if let Some(obj) = row.as_object_mut() {
        obj.entry(pk_column.to_string())
            .or_insert_with(|| Value::String(row_pk.to_string()));
    }
}

pub(crate) fn project_rows_to_selected_columns(data: &mut [Value], selected_columns: &[String]) {
    if selected_columns.is_empty() {
        return;
    }

    for row in data {
        let Some(obj) = row.as_object_mut() else {
            continue;
        };

        let mut projected = serde_json::Map::new();
        for column in selected_columns {
            if let Some(value) = obj.get(column).cloned() {
                projected.insert(column.clone(), value);
            }
        }
        *obj = projected;
    }
}

fn upsert_overlay_row(
    data: &mut Vec<Value>,
    data_map: &mut std::collections::HashMap<String, usize>,
    to_delete: &mut std::collections::HashSet<usize>,
    row_pk: &str,
    mut row: Value,
    pk_column: &str,
) {
    ensure_pk_on_overlay_row(&mut row, pk_column, row_pk);
    if let Some(&idx) = data_map.get(row_pk) {
        data[idx] = row;
        to_delete.remove(&idx);
    } else {
        let idx = data.len();
        data.push(row);
        data_map.insert(row_pk.to_string(), idx);
    }
}

fn patch_overlay_row(
    data: &mut Vec<Value>,
    data_map: &mut std::collections::HashMap<String, usize>,
    to_delete: &mut std::collections::HashSet<usize>,
    row_pk: &str,
    mut patch: Value,
    pk_column: &str,
) {
    if let Some(&idx) = data_map.get(row_pk) {
        to_delete.remove(&idx);
        let existing = &mut data[idx];
        if let (Some(existing_obj), Some(patch_obj)) = (existing.as_object_mut(), patch.as_object())
        {
            for (k, v) in patch_obj {
                existing_obj.insert(k.clone(), v.clone());
            }
        } else {
            ensure_pk_on_overlay_row(&mut patch, pk_column, row_pk);
            *existing = patch;
        }
    } else {
        let idx = data.len();
        ensure_pk_on_overlay_row(&mut patch, pk_column, row_pk);
        data.push(patch);
        data_map.insert(row_pk.to_string(), idx);
    }
}

/// Apply branch overlay to main table data (CoW Read).
///
/// When a branch is active, reads from `_qail_branch_rows` and merges:
/// - `insert` overlays → appended to results
/// - `update` overlays → patch matching PK rows
/// - `delete` overlays → remove matching PK rows
pub(crate) async fn apply_branch_overlay(
    conn: &mut qail_pg::driver::PooledConnection,
    branch_name: &str,
    table_name: &str,
    data: &mut Vec<Value>,
    pk_column: &str,
) {
    let sql = qail_pg::driver::branch_sql::read_overlay_sql(branch_name, table_name);
    let overlay_rows = match conn.get_mut() {
        Ok(pg_conn) => match pg_conn.simple_query(&sql).await {
            Ok(rows) => rows,
            Err(_) => return,
        },
        Err(_) => return,
    };

    if overlay_rows.is_empty() {
        return;
    }

    // Optimization: Index existing results by PK for O(1) lookup during merge.
    // This turns an O(N*M) linear scan into O(N+M).
    let mut data_map: std::collections::HashMap<String, usize> =
        std::collections::HashMap::with_capacity(data.len());
    for (idx, row) in data.iter().enumerate() {
        if let Some(pk) = row.get(pk_column).and_then(json_value_to_pk) {
            data_map.insert(pk, idx);
        }
    }

    let mut to_delete: std::collections::HashSet<usize> = std::collections::HashSet::new();

    for row in &overlay_rows {
        let row_pk = row
            .try_get_by_name::<String>("row_pk")
            .ok()
            .or_else(|| row.get_string(0))
            .unwrap_or_default();
        let operation = row
            .try_get_by_name::<String>("operation")
            .ok()
            .or_else(|| row.get_string(1))
            .unwrap_or_default();
        let row_data_str = row
            .try_get_by_name::<String>("row_data")
            .ok()
            .or_else(|| row.get_string(2))
            .unwrap_or_default();

        match operation.as_str() {
            "insert" => {
                if let Ok(val) = serde_json::from_str::<Value>(&row_data_str) {
                    upsert_overlay_row(
                        data,
                        &mut data_map,
                        &mut to_delete,
                        &row_pk,
                        val,
                        pk_column,
                    );
                }
            }
            "update" => {
                if let Ok(new_val) = serde_json::from_str::<Value>(&row_data_str) {
                    patch_overlay_row(
                        data,
                        &mut data_map,
                        &mut to_delete,
                        &row_pk,
                        new_val,
                        pk_column,
                    );
                }
            }
            "delete" => {
                if let Some(&idx) = data_map.get(&row_pk) {
                    to_delete.insert(idx);
                    data_map.remove(&row_pk);
                }
            }
            _ => {}
        }
    }

    if !to_delete.is_empty() {
        let mut i = 0;
        data.retain(|_| {
            let keep = !to_delete.contains(&i);
            i += 1;
            keep
        });
    }
}

/// Redirect a write to the branch overlay (CoW Write).
///
/// Instead of inserting into the main table, stores the row in `_qail_branch_rows`.
pub(crate) async fn redirect_to_overlay(
    conn: &mut qail_pg::driver::PooledConnection,
    branch_name: &str,
    table_name: &str,
    row_pk: &str,
    operation: &str,
    row_data: &Value,
) -> Result<(), ApiError> {
    let sql =
        qail_pg::driver::branch_sql::write_overlay_sql(branch_name, table_name, row_pk, operation);
    let data_str = serde_json::to_string(row_data)
        .map_err(|e| ApiError::internal(format!("Branch overlay JSON encode failed: {}", e)))?;
    let params = vec![Some(data_str.into_bytes())];
    conn.query_raw_with_params(&sql, &params)
        .await
        .map_err(|e| ApiError::internal(format!("Branch overlay write failed: {}", e)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{patch_overlay_row, project_rows_to_selected_columns, upsert_overlay_row};
    use serde_json::json;

    #[test]
    fn project_rows_to_selected_columns_removes_overlay_extras() {
        let mut rows = vec![json!({
            "id": 1,
            "total": 50,
            "secret": "hidden",
            "tenant_id": "tenant-a"
        })];
        let selected = vec!["id".to_string(), "tenant_id".to_string()];

        project_rows_to_selected_columns(&mut rows, &selected);

        assert_eq!(rows[0], json!({"id": 1, "tenant_id": "tenant-a"}));
    }

    #[test]
    fn chronological_overlay_helpers_patch_inserted_rows_once() {
        let mut rows = Vec::new();
        let mut data_map = std::collections::HashMap::new();
        let mut to_delete = std::collections::HashSet::new();

        upsert_overlay_row(
            &mut rows,
            &mut data_map,
            &mut to_delete,
            "order-1",
            json!({"id": "order-1", "status": "draft"}),
            "id",
        );
        patch_overlay_row(
            &mut rows,
            &mut data_map,
            &mut to_delete,
            "order-1",
            json!({"status": "submitted"}),
            "id",
        );

        assert_eq!(rows, vec![json!({"id": "order-1", "status": "submitted"})]);
    }

    #[test]
    fn chronological_overlay_helpers_allow_reinsert_after_delete() {
        let mut rows = vec![json!({"id": "order-1", "status": "main"})];
        let mut data_map = std::collections::HashMap::from([("order-1".to_string(), 0)]);
        let mut to_delete = std::collections::HashSet::from([0]);
        data_map.remove("order-1");

        upsert_overlay_row(
            &mut rows,
            &mut data_map,
            &mut to_delete,
            "order-1",
            json!({"id": "order-1", "status": "branch"}),
            "id",
        );

        assert!(to_delete.contains(&0));
        assert_eq!(rows[1], json!({"id": "order-1", "status": "branch"}));
    }
}
