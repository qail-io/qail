use crate::middleware::ApiError;

/// Reject DDL actions inside transactions. Only DML is allowed.
pub(super) fn reject_ddl_in_transaction(cmd: &qail_core::ast::Qail) -> Result<(), ApiError> {
    use qail_core::ast::Action;
    match cmd.action {
        Action::Get
        | Action::Set
        | Action::Add
        | Action::Del
        | Action::Put
        | Action::With
        | Action::Cnt
        | Action::Over
        | Action::Upsert => Ok(()),
        _ => Err(ApiError::bad_request(
            "UNSUPPORTED_ACTION",
            format!(
                "Action {} is not allowed inside a transaction session. Only DML operations (get/set/add/del/put/with/cnt/over/upsert) are permitted.",
                cmd.action
            ),
        )),
    }
}

/// Convert a `TransactionError` to an `ApiError`.
pub(super) fn txn_err_to_api(err: crate::transaction::TransactionError) -> ApiError {
    use crate::transaction::TransactionError;
    match err {
        TransactionError::SessionLimitReached(_) => {
            ApiError::with_code("TXN_SESSION_LIMIT", err.to_string())
        }
        TransactionError::SessionNotFound => ApiError::not_found("Transaction session"),
        TransactionError::TenantMismatch => ApiError::forbidden(err.to_string()),
        TransactionError::UserMismatch => ApiError::forbidden(err.to_string()),
        TransactionError::Pool(e) => ApiError::connection_error(e),
        TransactionError::Database(e) => ApiError::internal(e),
        TransactionError::Rejected(e) => ApiError::bad_request("TXN_REJECTED", e),
        TransactionError::SessionLifetimeExceeded(_) => {
            ApiError::with_code("TXN_SESSION_EXPIRED", err.to_string())
        }
        TransactionError::StatementLimitReached(_) => {
            ApiError::with_code("TXN_STATEMENT_LIMIT", err.to_string())
        }
        TransactionError::Aborted => ApiError::with_code("TXN_ABORTED", err.to_string()),
    }
}
