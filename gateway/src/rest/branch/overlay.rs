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

fn row_matches_pk(row: &Value, pk_column: &str, row_pk: &str) -> bool {
    row.get(pk_column)
        .and_then(json_value_to_pk)
        .is_some_and(|pk| pk == row_pk)
}

fn ensure_pk_on_overlay_row(row: &mut Value, pk_column: &str, row_pk: &str) {
    if let Some(obj) = row.as_object_mut() {
        obj.entry(pk_column.to_string())
            .or_insert_with(|| Value::String(row_pk.to_string()));
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
                    data.push(val);
                }
            }
            "update" => {
                if let Ok(new_val) = serde_json::from_str::<Value>(&row_data_str) {
                    let mut found = false;
                    for existing in data.iter_mut() {
                        if row_matches_pk(existing, pk_column, &row_pk) {
                            if let (Some(existing_obj), Some(patch_obj)) =
                                (existing.as_object_mut(), new_val.as_object())
                            {
                                for (k, v) in patch_obj {
                                    existing_obj.insert(k.clone(), v.clone());
                                }
                            } else {
                                *existing = new_val.clone();
                            }
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        let mut row = new_val;
                        ensure_pk_on_overlay_row(&mut row, pk_column, &row_pk);
                        data.push(row);
                    }
                }
            }
            "delete" => {
                data.retain(|existing| !row_matches_pk(existing, pk_column, &row_pk));
            }
            _ => {}
        }
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
