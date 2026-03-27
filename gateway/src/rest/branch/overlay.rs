use serde_json::Value;

use crate::middleware::ApiError;

/// Apply branch overlay to main table data (CoW Read).
///
/// When a branch is active, reads from `_qail_branch_rows` and merges:
/// - `insert` overlays → appended to results
/// - `update` overlays → replace matching PK rows
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
                        if let Some(existing_pk) = existing.get(pk_column).and_then(|v| {
                            v.as_str()
                                .map(|s| s.to_string())
                                .or_else(|| Some(v.to_string()))
                        }) && existing_pk == row_pk
                        {
                            *existing = new_val.clone();
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        data.push(new_val);
                    }
                }
            }
            "delete" => {
                data.retain(|existing| {
                    existing
                        .get(pk_column)
                        .and_then(|v| {
                            v.as_str()
                                .map(|s| s.to_string())
                                .or_else(|| Some(v.to_string()))
                        })
                        .map(|pk| pk != row_pk)
                        .unwrap_or(true)
                });
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
