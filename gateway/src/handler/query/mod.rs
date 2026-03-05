//! Qail AST query execution handlers.
//!
//! Text, binary, fast, and batch query endpoints.

use axum::{
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{Json, Response},
};
use std::sync::Arc;

use super::convert::{row_to_array, row_to_json};
#[cfg(feature = "qdrant")]
use super::qdrant::execute_qdrant_cmd;
use super::{BatchQueryResult, BatchRequest, BatchResponse, FastQueryResponse, QueryResponse};
use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::middleware::ApiError;
use qail_core::ast::Action;

mod endpoints;
mod rules;
#[cfg(test)]
mod tests;

pub use endpoints::{
    execute_batch, execute_query, execute_query_binary, execute_query_export, execute_query_fast,
};
pub(crate) use rules::reject_dangerous_action;
pub(crate) use rules::{clamp_query_limit, is_query_allowed, query_complexity};
