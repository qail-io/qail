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

fn apply_table_returning(
    cmd: qail_core::ast::Qail,
    table: &crate::schema::GatewayTable,
) -> qail_core::ast::Qail {
    let columns: Vec<&str> = table
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .filter(|column| crate::rest::filters::is_safe_identifier(column))
        .collect();
    if columns.is_empty() {
        cmd.returning_all()
    } else {
        cmd.returning(columns)
    }
}

fn apply_mutation_returning(
    cmd: qail_core::ast::Qail,
    table: &crate::schema::GatewayTable,
    returning: Option<&str>,
) -> Result<qail_core::ast::Qail, String> {
    let Some(returning) = returning else {
        return Ok(cmd);
    };

    let cols = parse_select_columns(returning)
        .map_err(|msg| format!("Invalid returning parameter: {}", msg))?;
    if cols.len() == 1 && cols[0] == "*" {
        Ok(apply_table_returning(cmd, table))
    } else {
        apply_returning(cmd, Some(returning))
    }
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

fn branch_overlay_write_needs_base_lookup(
    overlay_state: BranchOverlayRowState,
    row_id: &str,
) -> Result<bool, ApiError> {
    match overlay_state {
        BranchOverlayRowState::Visible => Ok(false),
        BranchOverlayRowState::Deleted => Err(ApiError::not_found(format!("row '{}'", row_id))),
        BranchOverlayRowState::Absent => Ok(true),
    }
}

fn identifier_leaf(identifier: &str) -> &str {
    identifier.rsplit('.').next().unwrap_or(identifier)
}

fn identifier_matches_column(identifier: &str, column: &str) -> bool {
    identifier_leaf(identifier).eq_ignore_ascii_case(identifier_leaf(column))
}

#[cfg(test)]
mod tests {
    use super::{
        apply_mutation_returning, apply_table_returning, branch_overlay_write_needs_base_lookup,
        ensure_path_mutation_affected, identifier_matches_column, mutation_needs_full_returning,
        project_mutation_returning_rows,
    };
    use crate::rest::branch::BranchOverlayRowState;
    use crate::schema::{GatewayColumn, GatewayTable};
    use qail_core::ast::{Expr, Qail};
    use serde_json::json;

    fn test_table(columns: &[&str]) -> GatewayTable {
        GatewayTable {
            name: "users".to_string(),
            columns: columns
                .iter()
                .map(|name| GatewayColumn {
                    name: (*name).to_string(),
                    col_type: "string".to_string(),
                    pg_type: "text".to_string(),
                    nullable: true,
                    primary_key: *name == "id",
                    unique: false,
                    has_default: false,
                    foreign_key: None,
                })
                .collect(),
            primary_key: Some("id".to_string()),
        }
    }

    #[test]
    fn mutation_needs_full_returning_when_event_payload_needs_full_row() {
        assert!(mutation_needs_full_returning(false, Some("id"), true));
        assert!(mutation_needs_full_returning(true, None, false));
        assert!(!mutation_needs_full_returning(true, Some("id"), false));
        assert!(!mutation_needs_full_returning(false, None, false));
    }

    #[test]
    fn apply_table_returning_uses_explicit_schema_columns() {
        let table = test_table(&["id", "email", "password_hash"]);

        let cmd = apply_table_returning(Qail::set("users"), &table);

        assert_eq!(
            cmd.returning,
            Some(vec![
                Expr::Named("id".to_string()),
                Expr::Named("email".to_string()),
                Expr::Named("password_hash".to_string())
            ])
        );
    }

    #[test]
    fn apply_mutation_returning_expands_wildcard_to_schema_columns() {
        let table = test_table(&["id", "email"]);

        let cmd = apply_mutation_returning(Qail::add("users"), &table, Some(" * ")).unwrap();

        assert_eq!(
            cmd.returning,
            Some(vec![
                Expr::Named("id".to_string()),
                Expr::Named("email".to_string())
            ])
        );
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

    #[test]
    fn branch_overlay_write_needs_base_lookup_for_absent_rows_only() {
        assert!(
            !branch_overlay_write_needs_base_lookup(BranchOverlayRowState::Visible, "row-1")
                .unwrap()
        );
        assert!(
            branch_overlay_write_needs_base_lookup(BranchOverlayRowState::Absent, "row-1").unwrap()
        );

        let err = branch_overlay_write_needs_base_lookup(BranchOverlayRowState::Deleted, "row-1")
            .unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::NOT_FOUND);
    }

    #[test]
    fn identifier_matching_follows_postgres_unquoted_case_folding() {
        assert!(identifier_matches_column("Tenant_ID", "tenant_id"));
        assert!(identifier_matches_column("orders.Tenant_ID", "tenant_id"));
        assert!(!identifier_matches_column("tenant_id_shadow", "tenant_id"));
    }
}
