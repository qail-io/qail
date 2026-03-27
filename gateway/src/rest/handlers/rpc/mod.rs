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
    RpcBoundQuery, RpcFunctionName, build_rpc_bound_sql, enforce_rpc_name_contract,
};
#[cfg(test)]
pub(super) use name::{build_rpc_probe_sql, build_rpc_sql};
#[cfg(test)]
pub(super) use signature::{
    matches_positional_signature, select_matching_rpc_signature, signature_matches_call,
};
pub(crate) use signature::{
    minimum_required_rpc_args, normalize_pg_type_name, parse_rpc_input_arg_names,
};
