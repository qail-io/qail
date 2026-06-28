use serde_json::Value;

use crate::middleware::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BranchOverlayRowState {
    Absent,
    Visible,
    Deleted,
}

fn json_value_to_pk(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
    }
}

fn ensure_pk_on_overlay_row(
    row: &mut Value,
    pk_column: &str,
    row_pk: &str,
) -> Result<(), ApiError> {
    if let Some(obj) = row.as_object_mut() {
        if let Some(existing) = obj.get(pk_column) {
            if existing
                .as_str()
                .map(str::to_string)
                .or_else(|| json_value_to_pk(existing))
                .as_deref()
                != Some(row_pk)
            {
                return Err(ApiError::internal(format!(
                    "Branch overlay row_data primary key '{}' does not match row_pk '{}'",
                    pk_column, row_pk
                )));
            }
        } else {
            obj.insert(pk_column.to_string(), Value::String(row_pk.to_string()));
        }
    }
    Ok(())
}

pub(crate) fn project_rows_to_selected_columns(data: &mut [Value], selected_columns: &[String]) {
    if selected_columns.is_empty() {
        return;
    }

    for row in data {
        let Some(obj) = row.as_object_mut() else {
            continue;
        };

        let mut source = std::mem::take(obj);
        let mut projected = serde_json::Map::new();
        for column in selected_columns {
            if let Some(value) = source.remove(column) {
                projected.insert(column.clone(), value);
            }
        }
        *obj = projected;
    }
}

async fn ensure_active_branch_exists(
    conn: &mut qail_pg::driver::PooledConnection,
    branch_name: &str,
) -> Result<(), ApiError> {
    let sql = qail_pg::driver::branch_sql::active_branch_exists_sql(branch_name);
    let rows = conn
        .get_mut()
        .map_err(|e| ApiError::internal(format!("Branch connection unavailable: {}", e)))?
        .simple_query(&sql)
        .await
        .map_err(|e| ApiError::internal(format!("Branch lookup failed: {}", e)))?;

    if rows.is_empty() {
        return Err(ApiError::not_found(format!("branch '{}'", branch_name)));
    }

    Ok(())
}

pub(crate) async fn read_branch_overlay_rows(
    conn: &mut qail_pg::driver::PooledConnection,
    branch_name: &str,
    table_name: &str,
) -> Result<Vec<qail_pg::PgRow>, ApiError> {
    ensure_active_branch_exists(conn, branch_name).await?;
    let sql = qail_pg::driver::branch_sql::read_overlay_sql(branch_name, table_name);
    conn.get_mut()
        .map_err(|e| ApiError::internal(format!("Branch connection unavailable: {}", e)))?
        .simple_query(&sql)
        .await
        .map_err(|e| ApiError::internal(format!("Branch overlay read failed: {}", e)))
}

fn required_overlay_string(
    row: &qail_pg::PgRow,
    name: &str,
    idx: usize,
) -> Result<String, ApiError> {
    let value = row
        .try_get_by_name::<String>(name)
        .ok()
        .or_else(|| row.get_string(idx))
        .ok_or_else(|| ApiError::internal(format!("Invalid branch overlay {} metadata", name)))?;
    if value.is_empty() {
        return Err(ApiError::internal(format!(
            "Invalid empty branch overlay {} metadata",
            name
        )));
    }
    Ok(value)
}

fn overlay_row_pk_and_operation_checked(
    row: &qail_pg::PgRow,
) -> Result<(String, String), ApiError> {
    let row_pk = required_overlay_string(row, "row_pk", 0)?;
    let operation = required_overlay_string(row, "operation", 1)?;
    Ok((row_pk, operation))
}

#[cfg(test)]
fn branch_overlay_row_state_from_ops<'a>(
    operations: impl IntoIterator<Item = (&'a str, &'a str)>,
    row_pk: &str,
) -> BranchOverlayRowState {
    let mut state = BranchOverlayRowState::Absent;

    for (overlay_pk, operation) in operations {
        if overlay_pk != row_pk {
            continue;
        }

        state = match operation {
            "insert" | "update" => BranchOverlayRowState::Visible,
            "delete" => BranchOverlayRowState::Deleted,
            _ => state,
        };
    }

    state
}

pub(crate) fn branch_overlay_row_state(
    rows: &[qail_pg::PgRow],
    row_pk: &str,
) -> Result<BranchOverlayRowState, ApiError> {
    let mut state = BranchOverlayRowState::Absent;

    for row in rows {
        let (overlay_pk, operation) = overlay_row_pk_and_operation_checked(row)?;
        if overlay_pk != row_pk {
            continue;
        }

        state = match operation.as_str() {
            "insert" | "update" => BranchOverlayRowState::Visible,
            "delete" => BranchOverlayRowState::Deleted,
            _ => {
                return Err(ApiError::internal(format!(
                    "Unknown branch overlay operation '{}'",
                    operation
                )));
            }
        };
    }

    Ok(state)
}

fn upsert_overlay_row(
    data: &mut Vec<Value>,
    data_map: &mut std::collections::HashMap<String, usize>,
    to_delete: &mut std::collections::HashSet<usize>,
    row_pk: &str,
    mut row: Value,
    pk_column: &str,
) -> Result<(), ApiError> {
    ensure_pk_on_overlay_row(&mut row, pk_column, row_pk)?;
    if let Some(&idx) = data_map.get(row_pk) {
        data[idx] = row;
        to_delete.remove(&idx);
    } else {
        let idx = data.len();
        data.push(row);
        data_map.insert(row_pk.to_string(), idx);
    }
    Ok(())
}

fn patch_overlay_row(
    data: &mut Vec<Value>,
    data_map: &mut std::collections::HashMap<String, usize>,
    to_delete: &mut std::collections::HashSet<usize>,
    row_pk: &str,
    mut patch: Value,
    pk_column: &str,
) -> Result<(), ApiError> {
    ensure_pk_on_overlay_row(&mut patch, pk_column, row_pk)?;
    if let Some(&idx) = data_map.get(row_pk) {
        to_delete.remove(&idx);
        let existing = &mut data[idx];
        match patch {
            Value::Object(patch_obj) => {
                if let Some(existing_obj) = existing.as_object_mut() {
                    existing_obj.extend(patch_obj);
                } else {
                    *existing = Value::Object(patch_obj);
                }
            }
            patch => {
                *existing = patch;
            }
        }
    } else {
        let idx = data.len();
        data.push(patch);
        data_map.insert(row_pk.to_string(), idx);
    }
    Ok(())
}

fn parse_overlay_row_data(row: &qail_pg::PgRow, row_pk: &str) -> Result<Value, ApiError> {
    let row_data_str = required_overlay_string(row, "row_data", 2)?;
    let value = serde_json::from_str::<Value>(&row_data_str).map_err(|e| {
        ApiError::internal(format!(
            "Malformed branch overlay row_data for row '{}': {}",
            row_pk, e
        ))
    })?;
    if !value.is_object() {
        return Err(ApiError::internal(format!(
            "Branch overlay row_data for row '{}' must be a JSON object",
            row_pk
        )));
    }
    Ok(value)
}

pub(crate) fn apply_branch_overlay_rows(
    overlay_rows: &[qail_pg::PgRow],
    data: &mut Vec<Value>,
    pk_column: &str,
) -> Result<(), ApiError> {
    if overlay_rows.is_empty() {
        return Ok(());
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

    for row in overlay_rows {
        let (row_pk, operation) = overlay_row_pk_and_operation_checked(row)?;

        match operation.as_str() {
            "insert" => {
                let val = parse_overlay_row_data(row, &row_pk)?;
                upsert_overlay_row(data, &mut data_map, &mut to_delete, &row_pk, val, pk_column)?;
            }
            "update" => {
                let new_val = parse_overlay_row_data(row, &row_pk)?;
                patch_overlay_row(
                    data,
                    &mut data_map,
                    &mut to_delete,
                    &row_pk,
                    new_val,
                    pk_column,
                )?;
            }
            "delete" => {
                if let Some(&idx) = data_map.get(&row_pk) {
                    to_delete.insert(idx);
                    data_map.remove(&row_pk);
                }
            }
            _ => {
                return Err(ApiError::internal(format!(
                    "Unknown branch overlay operation '{}'",
                    operation
                )));
            }
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

    Ok(())
}

pub(crate) fn apply_branch_overlay_to_single_row(
    overlay_rows: &[qail_pg::PgRow],
    data: Option<Value>,
    pk_column: &str,
    row_pk: &str,
) -> Result<Option<Value>, ApiError> {
    let mut rows: Vec<Value> = data.into_iter().collect();
    apply_branch_overlay_rows(overlay_rows, &mut rows, pk_column)?;
    Ok(rows.into_iter().find(|row| {
        row.get(pk_column)
            .and_then(json_value_to_pk)
            .is_some_and(|pk| pk == row_pk)
    }))
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
) -> Result<(), ApiError> {
    let overlay_rows = read_branch_overlay_rows(conn, branch_name, table_name).await?;
    apply_branch_overlay_rows(&overlay_rows, data, pk_column)
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
    let rows = conn
        .query_raw_with_params(&sql, &params)
        .await
        .map_err(|e| ApiError::internal(format!("Branch overlay write failed: {}", e)))?;
    if rows.is_empty() {
        return Err(ApiError::not_found(format!("branch '{}'", branch_name)));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BranchOverlayRowState, apply_branch_overlay_rows, apply_branch_overlay_to_single_row,
        branch_overlay_row_state, branch_overlay_row_state_from_ops, patch_overlay_row,
        project_rows_to_selected_columns, upsert_overlay_row,
    };
    use crate::middleware::ApiError;
    use serde_json::json;

    fn overlay_row(
        row_pk: Option<&str>,
        operation: Option<&str>,
        row_data: Option<&str>,
    ) -> qail_pg::PgRow {
        qail_pg::PgRow {
            columns: vec![
                row_pk.map(|value| value.as_bytes().to_vec()),
                operation.map(|value| value.as_bytes().to_vec()),
                row_data.map(|value| value.as_bytes().to_vec()),
            ],
            column_info: None,
        }
    }

    fn assert_internal_error(err: ApiError) {
        assert_eq!(err.code, "INTERNAL_ERROR");
        assert_eq!(err.message, "An internal error occurred.");
    }

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
        )
        .unwrap();
        patch_overlay_row(
            &mut rows,
            &mut data_map,
            &mut to_delete,
            "order-1",
            json!({"status": "submitted"}),
            "id",
        )
        .unwrap();

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
        )
        .unwrap();

        assert!(to_delete.contains(&0));
        assert_eq!(rows[1], json!({"id": "order-1", "status": "branch"}));
    }

    #[test]
    fn apply_branch_overlay_rows_replays_valid_rows() {
        let overlay_rows = vec![
            overlay_row(
                Some("order-1"),
                Some("update"),
                Some(r#"{"status":"submitted"}"#),
            ),
            overlay_row(
                Some("order-2"),
                Some("insert"),
                Some(r#"{"id":"order-2","status":"draft"}"#),
            ),
        ];
        let mut rows = vec![json!({"id": "order-1", "status": "draft"})];

        apply_branch_overlay_rows(&overlay_rows, &mut rows, "id").unwrap();

        assert_eq!(
            rows,
            vec![
                json!({"id": "order-1", "status": "submitted"}),
                json!({"id": "order-2", "status": "draft"}),
            ]
        );
    }

    #[test]
    fn apply_branch_overlay_to_single_row_replays_valid_rows() {
        let overlay_rows = vec![
            overlay_row(
                Some("order-2"),
                Some("insert"),
                Some(r#"{"status":"other"}"#),
            ),
            overlay_row(
                Some("order-1"),
                Some("update"),
                Some(r#"{"status":"branch"}"#),
            ),
        ];
        let data = Some(json!({
            "id": "order-1",
            "status": "main",
            "region": "west"
        }));

        let row = apply_branch_overlay_to_single_row(&overlay_rows, data, "id", "order-1")
            .expect("overlay replay should succeed");

        assert_eq!(
            row,
            Some(json!({"id": "order-1", "status": "branch", "region": "west"}))
        );
    }

    #[test]
    fn apply_branch_overlay_to_single_row_filters_other_overlay_inserts() {
        let overlay_rows = vec![overlay_row(
            Some("order-2"),
            Some("insert"),
            Some(r#"{"status":"other"}"#),
        )];

        let row = apply_branch_overlay_to_single_row(&overlay_rows, None, "id", "order-1")
            .expect("overlay replay should succeed");

        assert_eq!(row, None);
    }

    #[test]
    fn apply_branch_overlay_to_single_row_fails_closed_on_malformed_replay() {
        let overlay_rows = vec![overlay_row(
            Some("order-2"),
            Some("insert"),
            Some("{bad-json"),
        )];
        let data = Some(json!({"id": "order-1", "status": "main"}));

        let err =
            apply_branch_overlay_to_single_row(&overlay_rows, data, "id", "order-1").unwrap_err();

        assert_internal_error(err);
    }

    #[test]
    fn apply_branch_overlay_rows_rejects_malformed_row_data() {
        let overlay_rows = vec![overlay_row(
            Some("order-1"),
            Some("insert"),
            Some("{bad-json"),
        )];
        let mut rows = Vec::new();

        let err = apply_branch_overlay_rows(&overlay_rows, &mut rows, "id").unwrap_err();
        assert_internal_error(err);
        assert!(rows.is_empty());
    }

    #[test]
    fn apply_branch_overlay_rows_rejects_non_object_row_data() {
        let overlay_rows = vec![overlay_row(
            Some("order-1"),
            Some("insert"),
            Some(r#""scalar""#),
        )];
        let mut rows = Vec::new();

        let err = apply_branch_overlay_rows(&overlay_rows, &mut rows, "id").unwrap_err();
        assert_internal_error(err);
        assert!(rows.is_empty());
    }

    #[test]
    fn apply_branch_overlay_rows_rejects_insert_pk_drift() {
        let overlay_rows = vec![overlay_row(
            Some("order-1"),
            Some("insert"),
            Some(r#"{"id":"order-2","status":"draft"}"#),
        )];
        let mut rows = Vec::new();

        let err = apply_branch_overlay_rows(&overlay_rows, &mut rows, "id").unwrap_err();

        assert_internal_error(err);
        assert!(rows.is_empty());
    }

    #[test]
    fn apply_branch_overlay_rows_rejects_update_pk_drift() {
        let overlay_rows = vec![overlay_row(
            Some("order-1"),
            Some("update"),
            Some(r#"{"id":"order-2","status":"branch"}"#),
        )];
        let mut rows = vec![json!({"id": "order-1", "status": "main"})];

        let err = apply_branch_overlay_rows(&overlay_rows, &mut rows, "id").unwrap_err();

        assert_internal_error(err);
        assert_eq!(rows, vec![json!({"id": "order-1", "status": "main"})]);
    }

    #[test]
    fn apply_branch_overlay_rows_rejects_unknown_or_incomplete_operation() {
        let mut rows = vec![json!({"id": "order-1", "status": "draft"})];
        let err = apply_branch_overlay_rows(
            &[overlay_row(Some("order-1"), Some("replace"), Some(r#"{}"#))],
            &mut rows,
            "id",
        )
        .unwrap_err();
        assert_internal_error(err);

        let err =
            apply_branch_overlay_rows(&[overlay_row(None, Some("delete"), None)], &mut rows, "id")
                .unwrap_err();
        assert_internal_error(err);
    }

    #[test]
    fn branch_overlay_row_state_replays_last_operation_for_row() {
        let ops = [
            ("order-1", "insert"),
            ("order-2", "delete"),
            ("order-1", "delete"),
            ("order-1", "update"),
        ];

        assert_eq!(
            branch_overlay_row_state_from_ops(ops, "order-1"),
            BranchOverlayRowState::Visible
        );
        assert_eq!(
            branch_overlay_row_state_from_ops(ops, "order-2"),
            BranchOverlayRowState::Deleted
        );
        assert_eq!(
            branch_overlay_row_state_from_ops(ops, "order-3"),
            BranchOverlayRowState::Absent
        );
    }

    #[test]
    fn branch_overlay_row_state_reads_pg_rows_without_metadata() {
        let rows = vec![
            qail_pg::PgRow {
                columns: vec![Some(b"order-1".to_vec()), Some(b"insert".to_vec())],
                column_info: None,
            },
            qail_pg::PgRow {
                columns: vec![Some(b"order-1".to_vec()), Some(b"delete".to_vec())],
                column_info: None,
            },
        ];

        assert_eq!(
            branch_overlay_row_state(&rows, "order-1").unwrap(),
            BranchOverlayRowState::Deleted
        );
    }

    #[test]
    fn branch_overlay_row_state_rejects_malformed_pg_rows() {
        let missing_operation = vec![qail_pg::PgRow {
            columns: vec![Some(b"order-1".to_vec()), None],
            column_info: None,
        }];
        let err = branch_overlay_row_state(&missing_operation, "order-1").unwrap_err();
        assert_eq!(err.code, "INTERNAL_ERROR");

        let unknown_operation = vec![qail_pg::PgRow {
            columns: vec![Some(b"order-1".to_vec()), Some(b"replace".to_vec())],
            column_info: None,
        }];
        let err = branch_overlay_row_state(&unknown_operation, "order-1").unwrap_err();
        assert_eq!(err.code, "INTERNAL_ERROR");
    }
}
