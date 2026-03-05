//! Transaction session REST endpoint handlers.
//!
//! Provides multi-statement transaction support via the gateway HTTP API:
//! - `POST /txn/begin` — start a new transaction session
//! - `POST /txn/query` — execute a query within a transaction
//! - `POST /txn/commit` — commit and close a transaction
//! - `POST /txn/rollback` — rollback and close a transaction
//! - `POST /txn/savepoint` — savepoint operations within a transaction

use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::middleware::ApiError;

mod guard;
mod handlers;

pub use handlers::{txn_begin, txn_commit, txn_query, txn_rollback, txn_savepoint};

/// Response from `POST /txn/begin`.
#[derive(Debug, Serialize)]
pub struct TxnBeginResponse {
    /// Unique session ID. Include as `X-Transaction-Id` in subsequent requests.
    pub txn_id: String,
}

/// Response from `POST /txn/commit` or `POST /txn/rollback`.
#[derive(Debug, Serialize)]
pub struct TxnEndResponse {
    /// Action performed: "committed" or "rolled_back".
    pub status: String,
}

/// Request body for `POST /txn/savepoint`.
#[derive(Debug, Deserialize)]
pub struct SavepointRequest {
    /// Savepoint action: "create", "rollback", or "release".
    pub action: String,
    /// Savepoint name.
    pub name: String,
}

/// Response from `POST /txn/savepoint`.
#[derive(Debug, Serialize)]
pub struct SavepointResponse {
    /// Action performed.
    pub action: String,
    /// Savepoint name.
    pub name: String,
}

/// Extract the transaction session ID from headers.
fn extract_txn_id(headers: &HeaderMap) -> Result<String, ApiError> {
    headers
        .get("x-transaction-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| ApiError::bad_request("MISSING_TXN_ID", "Missing X-Transaction-Id header"))
}

use guard::{reject_ddl_in_transaction, txn_err_to_api};

#[cfg(test)]
mod tests;
