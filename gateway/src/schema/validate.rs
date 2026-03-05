use std::collections::HashSet;

use qail_core::ast::{Action, Expr, Qail};

use crate::error::GatewayError;

use super::SchemaRegistry;

pub(super) fn validate_cmd(schema: &SchemaRegistry, cmd: &Qail) -> Result<(), GatewayError> {
    if schema.tables().is_empty() {
        return Ok(());
    }

    match cmd.action {
        Action::Make
        | Action::Drop
        | Action::Alter
        | Action::TxnStart
        | Action::TxnCommit
        | Action::TxnRollback
        | Action::Listen
        | Action::Unlisten
        | Action::Notify => {
            return Ok(());
        }
        _ => {}
    }

    if !schema.table_exists(&cmd.table) {
        return Err(GatewayError::InvalidQuery(format!(
            "Table '{}' not found in schema",
            cmd.table
        )));
    }

    if let Some(table) = schema.table(&cmd.table) {
        let valid_columns: HashSet<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();

        for col_expr in &cmd.columns {
            if let Expr::Named(col_name) = col_expr
                && col_name != "*"
                && !valid_columns.contains(col_name.as_str())
            {
                return Err(GatewayError::InvalidQuery(format!(
                    "Column '{}' not found in table '{}'",
                    col_name, cmd.table
                )));
            }
        }
    }

    Ok(())
}
