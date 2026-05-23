use super::*;

mod create;
mod delete;
mod update;

pub(crate) use create::create_handler;
pub(crate) use delete::delete_handler;
pub(crate) use update::update_handler;

fn mutation_needs_full_returning(
    response_requested_returning: bool,
    requested_returning: Option<&str>,
    needs_event_row: bool,
) -> bool {
    needs_event_row || (response_requested_returning && requested_returning.is_none())
}

fn project_mutation_returning_rows(
    mut rows: Vec<Value>,
    returning: Option<&str>,
) -> Result<Vec<Value>, ApiError> {
    let Some(returning) = returning else {
        return Ok(rows);
    };

    let selected_columns = parse_select_columns(returning).map_err(ApiError::parse_error)?;
    if selected_columns.len() == 1 && selected_columns[0] == "*" {
        return Ok(rows);
    }

    project_rows_to_selected_columns(&mut rows, &selected_columns);
    Ok(rows)
}

fn ensure_path_mutation_affected(row_count: usize, row_id: &str) -> Result<(), ApiError> {
    if row_count == 0 {
        return Err(ApiError::not_found(format!("row '{}'", row_id)));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        ensure_path_mutation_affected, mutation_needs_full_returning,
        project_mutation_returning_rows,
    };
    use serde_json::json;

    #[test]
    fn mutation_needs_full_returning_when_event_payload_needs_full_row() {
        assert!(mutation_needs_full_returning(false, Some("id"), true));
        assert!(mutation_needs_full_returning(true, None, false));
        assert!(!mutation_needs_full_returning(true, Some("id"), false));
        assert!(!mutation_needs_full_returning(false, None, false));
    }

    #[test]
    fn project_mutation_returning_rows_preserves_event_fetch_and_shapes_response() {
        let rows = vec![json!({
            "id": 7,
            "email": "a@example.test",
            "tenant_id": "tenant-a"
        })];

        let projected = project_mutation_returning_rows(rows, Some("id,email")).unwrap();

        assert_eq!(projected, vec![json!({"id": 7, "email": "a@example.test"})]);
    }

    #[test]
    fn project_mutation_returning_rows_keeps_wildcard_rows() {
        let rows = vec![json!({"id": 7, "email": "a@example.test"})];

        let projected = project_mutation_returning_rows(rows.clone(), Some("*")).unwrap();

        assert_eq!(projected, rows);
    }

    #[test]
    fn ensure_path_mutation_affected_rejects_zero_rows() {
        let err = ensure_path_mutation_affected(0, "missing-row").unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::NOT_FOUND);
        assert!(err.message.contains("missing-row"));
    }

    #[test]
    fn ensure_path_mutation_affected_allows_nonzero_rows() {
        ensure_path_mutation_affected(1, "row-1").unwrap();
    }
}
