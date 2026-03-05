use super::{ApiError, ApiErrorData};

impl ApiError {
    /// Parse a Postgres error string and extract structured hints.
    pub fn from_pg_error(pg_err: &str, table_name: Option<&str>) -> Self {
        let lower = pg_err.to_lowercase();

        if lower.contains("unique") || lower.contains("duplicate key") || lower.contains("23505") {
            tracing::warn!(raw = %pg_err, "unique_violation");
            return Self::new(ApiErrorData {
                code: "CONFLICT".to_string(),
                message: "A record with this value already exists.".to_string(),
                details: None,
                request_id: None,
                hint: Some("Use a different value or update the existing record".to_string()),
                table: table_name.map(|s| s.to_string()),
                column: extract_column_from_constraint(pg_err),
            });
        }

        if lower.contains("foreign key") || lower.contains("23503") {
            tracing::warn!(raw = %pg_err, "fk_violation");
            return Self::new(ApiErrorData {
                code: "VALIDATION_ERROR".to_string(),
                message: "Referenced record does not exist.".to_string(),
                details: None,
                request_id: None,
                hint: Some("Ensure the referenced ID exists before inserting".to_string()),
                table: table_name.map(|s| s.to_string()),
                column: extract_column_from_constraint(pg_err),
            });
        }

        if lower.contains("not-null") || lower.contains("null value") || lower.contains("23502") {
            tracing::warn!(raw = %pg_err, "not_null_violation");
            return Self::new(ApiErrorData {
                code: "VALIDATION_ERROR".to_string(),
                message: "A required field is missing.".to_string(),
                details: None,
                request_id: None,
                hint: Some("Provide all required fields".to_string()),
                table: table_name.map(|s| s.to_string()),
                column: extract_column_from_pg_null_error(pg_err),
            });
        }

        if lower.contains("row-level security") || lower.contains("new row violates") {
            tracing::warn!(raw = %pg_err, "rls_violation");
            return Self::new(ApiErrorData {
                code: "FORBIDDEN".to_string(),
                message: "Access denied by row-level security policy.".to_string(),
                details: None,
                request_id: None,
                hint: Some("Your session does not have permission for this operation".to_string()),
                table: table_name.map(|s| s.to_string()),
                column: None,
            });
        }

        Self::query_error(pg_err)
    }

    /// Map a `qail-pg` driver error into stable API semantics.
    pub fn from_pg_driver_error(err: &qail_pg::PgError, table_name: Option<&str>) -> Self {
        if let Some(server) = err.server_error() {
            return Self::from_pg_server_error(server, table_name);
        }

        match err {
            qail_pg::PgError::Query(msg) => Self::from_pg_error(msg, table_name),
            qail_pg::PgError::Timeout(_) => Self::timeout(),
            qail_pg::PgError::Auth(msg) => {
                tracing::warn!(detail = %msg, "pg_auth_error");
                Self::forbidden("Database authentication failed.")
            }
            qail_pg::PgError::Connection(_)
            | qail_pg::PgError::Io(_)
            | qail_pg::PgError::PoolClosed
            | qail_pg::PgError::PoolExhausted { .. } => Self::connection_error(err.to_string()),
            _ => Self::query_error(err.to_string()),
        }
    }

    fn from_pg_server_error(server: &qail_pg::PgServerError, table_name: Option<&str>) -> Self {
        let sqlstate = server.code.as_str();
        let class = if sqlstate.len() >= 2 {
            &sqlstate[..2]
        } else {
            "??"
        };
        crate::metrics::record_db_error(sqlstate, class);

        match sqlstate {
            "23505" => {
                tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_unique_violation");
                Self::new(ApiErrorData {
                    code: "CONFLICT".to_string(),
                    message: "A record with this value already exists.".to_string(),
                    details: None,
                    request_id: None,
                    hint: Some("Use a different value or update the existing record".to_string()),
                    table: table_name.map(|s| s.to_string()),
                    column: extract_column_from_constraint(&server.message),
                })
            }
            "23503" | "23502" | "23514" => {
                tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_validation_violation");
                let hint = match sqlstate {
                    "23503" => "Ensure the referenced ID exists before inserting",
                    "23502" => "Provide all required fields",
                    "23514" => "Ensure values satisfy database constraints",
                    _ => "Check request payload values",
                };
                let column = if sqlstate == "23502" {
                    extract_column_from_pg_null_error(&server.message)
                } else {
                    extract_column_from_constraint(&server.message)
                };
                Self::new(ApiErrorData {
                    code: "VALIDATION_ERROR".to_string(),
                    message: "Input violates a database constraint.".to_string(),
                    details: None,
                    request_id: None,
                    hint: Some(hint.to_string()),
                    table: table_name.map(|s| s.to_string()),
                    column,
                })
            }
            "42501" => {
                tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_insufficient_privilege");
                Self::new(ApiErrorData {
                    code: "FORBIDDEN".to_string(),
                    message: "Access denied by database policy.".to_string(),
                    details: None,
                    request_id: None,
                    hint: Some(
                        "Your session does not have permission for this operation".to_string(),
                    ),
                    table: table_name.map(|s| s.to_string()),
                    column: None,
                })
            }
            "57014" => {
                tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_query_canceled");
                Self::timeout()
            }
            "57P03" | "53300" | "08000" | "08001" | "08003" | "08004" | "08006" => {
                tracing::error!(sqlstate = %sqlstate, message = %server.message, "pg_connection_failure");
                Self::connection_error(format!("[{}] {}", server.code, server.message))
            }
            _ => match class {
                "22" | "23" => {
                    tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_data_exception");
                    Self::new(ApiErrorData {
                        code: "VALIDATION_ERROR".to_string(),
                        message: "Input violates a database constraint.".to_string(),
                        details: None,
                        request_id: None,
                        hint: Some("Check request payload and field values".to_string()),
                        table: table_name.map(|s| s.to_string()),
                        column: None,
                    })
                }
                "40" => {
                    tracing::warn!(sqlstate = %sqlstate, message = %server.message, "pg_txn_retryable");
                    Self::new(ApiErrorData {
                        code: "QUERY_ERROR".to_string(),
                        message: "Transaction conflict occurred.".to_string(),
                        details: None,
                        request_id: None,
                        hint: Some("Retry the request".to_string()),
                        table: table_name.map(|s| s.to_string()),
                        column: None,
                    })
                }
                _ => Self::query_error(format!("[{}] {}", server.code, server.message)),
            },
        }
    }
}

/// Extract column name from a PG constraint error message.
///
/// Example: `duplicate key value violates unique constraint "users_email_key"` -> `email`.
fn extract_column_from_constraint(err: &str) -> Option<String> {
    if let Some(start) = err.find('"')
        && let Some(end) = err[start + 1..].find('"')
    {
        let constraint = &err[start + 1..start + 1 + end];
        let parts: Vec<&str> = constraint.rsplitn(2, '_').collect();
        if parts.len() == 2 {
            let prefix = parts[1];
            if let Some(col_start) = prefix.find('_') {
                return Some(prefix[col_start + 1..].to_string());
            }
        }
    }
    None
}

/// Extract column name from a PG NOT NULL violation.
///
/// Example: `null value in column "email" of relation "users" violates not-null constraint`.
fn extract_column_from_pg_null_error(err: &str) -> Option<String> {
    let marker = "column \"";
    if let Some(start) = err.find(marker) {
        let rest = &err[start + marker.len()..];
        if let Some(end) = rest.find('"') {
            return Some(rest[..end].to_string());
        }
    }
    None
}
