//! RPC (Remote Procedure Call) handler — invoke PostgreSQL functions via REST.
//!
//! POST /api/rpc/{function} — call PG functions with JSON arguments.
//! Includes overload resolution, signature checking, and name contracts.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::HeaderMap,
    response::Json,
};
use serde_json::{Value, json};

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::handler::row_to_json;
use crate::middleware::ApiError;

use super::{is_safe_ident_segment, quote_ident};

mod handler;
mod name;
mod signature;

pub(crate) use handler::rpc_handler;
pub(super) use name::{
    RpcFunctionName, build_rpc_call_target, build_rpc_sql, enforce_rpc_name_contract,
};
pub(crate) use signature::parse_rpc_input_arg_names;
#[cfg(test)]
pub(super) use signature::{
    matches_positional_signature, select_matching_rpc_signature, signature_matches_call,
};
