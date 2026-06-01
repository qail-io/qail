use crate::middleware::ApiError;
use qail_core::ast::{Condition, Expr, MergeAction, MergeSource, Qail, Value};

fn reject_value_subqueries(value: &Value) -> Result<(), ApiError> {
    match value {
        Value::Array(values) => {
            for value in values {
                reject_value_subqueries(value)?;
            }
        }
        Value::Subquery(query) => reject_ddl_in_transaction(query)?,
        Value::Expr(expr) => reject_expr_subqueries(expr)?,
        _ => {}
    }
    Ok(())
}

fn reject_condition_subqueries(condition: &Condition) -> Result<(), ApiError> {
    reject_expr_subqueries(&condition.left)?;
    reject_value_subqueries(&condition.value)
}

fn reject_expr_subqueries(expr: &Expr) -> Result<(), ApiError> {
    match expr {
        Expr::Aggregate {
            filter: Some(filter),
            ..
        } => {
            for condition in filter {
                reject_condition_subqueries(condition)?;
            }
        }
        Expr::Cast { expr, .. } | Expr::Mod { col: expr, .. } | Expr::Collate { expr, .. } => {
            reject_expr_subqueries(expr)?;
        }
        Expr::Window { params, order, .. } => {
            for expr in params {
                reject_expr_subqueries(expr)?;
            }
            for cage in order {
                for condition in &cage.conditions {
                    reject_condition_subqueries(condition)?;
                }
            }
        }
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            for (condition, then_expr) in when_clauses {
                reject_condition_subqueries(condition)?;
                reject_expr_subqueries(then_expr)?;
            }
            if let Some(expr) = else_value {
                reject_expr_subqueries(expr)?;
            }
        }
        Expr::FunctionCall { args, .. } => {
            for expr in args {
                reject_expr_subqueries(expr)?;
            }
        }
        Expr::SpecialFunction { args, .. } => {
            for (_, expr) in args {
                reject_expr_subqueries(expr)?;
            }
        }
        Expr::Binary { left, right, .. } => {
            reject_expr_subqueries(left)?;
            reject_expr_subqueries(right)?;
        }
        Expr::Literal(value) => reject_value_subqueries(value)?,
        Expr::ArrayConstructor { elements, .. } | Expr::RowConstructor { elements, .. } => {
            for expr in elements {
                reject_expr_subqueries(expr)?;
            }
        }
        Expr::Subscript { expr, index, .. } => {
            reject_expr_subqueries(expr)?;
            reject_expr_subqueries(index)?;
        }
        Expr::FieldAccess { expr, .. } => reject_expr_subqueries(expr)?,
        Expr::Subquery { query, .. } | Expr::Exists { query, .. } => {
            reject_ddl_in_transaction(query)?;
        }
        Expr::Star
        | Expr::Named(_)
        | Expr::Aliased { .. }
        | Expr::Aggregate { filter: None, .. }
        | Expr::Def { .. }
        | Expr::JsonAccess { .. } => {}
    }
    Ok(())
}

fn reject_embedded_subqueries(cmd: &Qail) -> Result<(), ApiError> {
    for expr in &cmd.columns {
        reject_expr_subqueries(expr)?;
    }
    for expr in &cmd.distinct_on {
        reject_expr_subqueries(expr)?;
    }
    if let Some(returning) = &cmd.returning {
        for expr in returning {
            reject_expr_subqueries(expr)?;
        }
    }
    for cage in &cmd.cages {
        for condition in &cage.conditions {
            reject_condition_subqueries(condition)?;
        }
    }
    for condition in &cmd.having {
        reject_condition_subqueries(condition)?;
    }
    for join in &cmd.joins {
        if let Some(conditions) = &join.on {
            for condition in conditions {
                reject_condition_subqueries(condition)?;
            }
        }
    }
    if let Some(on_conflict) = &cmd.on_conflict
        && let qail_core::ast::ConflictAction::DoUpdate { assignments } = &on_conflict.action
    {
        for (_, expr) in assignments {
            reject_expr_subqueries(expr)?;
        }
    }
    if let Some(merge) = &cmd.merge {
        if let MergeSource::Query { query, .. } = &merge.source {
            reject_ddl_in_transaction(query)?;
        }
        for condition in &merge.on {
            reject_condition_subqueries(condition)?;
        }
        for clause in &merge.clauses {
            for condition in &clause.condition {
                reject_condition_subqueries(condition)?;
            }
            match &clause.action {
                MergeAction::Update { assignments } => {
                    for (_, expr) in assignments {
                        reject_expr_subqueries(expr)?;
                    }
                }
                MergeAction::Insert { values, .. } => {
                    for expr in values {
                        reject_expr_subqueries(expr)?;
                    }
                }
                MergeAction::Delete | MergeAction::DoNothing => {}
            }
        }
    }

    Ok(())
}

/// Reject DDL actions inside transactions. Only DML is allowed.
pub(super) fn reject_ddl_in_transaction(cmd: &Qail) -> Result<(), ApiError> {
    use qail_core::ast::Action;
    let action_allowed = matches!(
        cmd.action,
        Action::Get
            | Action::Set
            | Action::Add
            | Action::Del
            | Action::Put
            | Action::With
            | Action::Cnt
            | Action::Over
            | Action::Upsert
            | Action::Merge
    );
    if !action_allowed {
        return Err(ApiError::bad_request(
            "UNSUPPORTED_ACTION",
            format!(
                "Action {} is not allowed inside a transaction session. Only DML operations (get/set/add/del/put/with/cnt/over/upsert/merge) are permitted.",
                cmd.action
            ),
        ));
    }

    for cte in &cmd.ctes {
        reject_ddl_in_transaction(&cte.base_query)?;
        if let Some(ref recursive_query) = cte.recursive_query {
            reject_ddl_in_transaction(recursive_query)?;
        }
    }
    if let Some(ref source_query) = cmd.source_query {
        reject_ddl_in_transaction(source_query)?;
    }
    for (_, set_query) in &cmd.set_ops {
        reject_ddl_in_transaction(set_query)?;
    }
    reject_embedded_subqueries(cmd)?;

    Ok(())
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
        TransactionError::AuthScopeMismatch => ApiError::forbidden(err.to_string()),
        TransactionError::Pool(e) => ApiError::connection_error(e),
        TransactionError::Backpressure(e) => ApiError::with_code("POOL_BACKPRESSURE", e),
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
