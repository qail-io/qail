//! CRUD handlers for REST endpoints.
//!
//! Split into read/write modules to keep handler files focused.

mod read;
mod write;

pub(crate) use read::{aggregate_handler, get_by_id_handler, list_handler};
pub(crate) use write::{create_handler, delete_handler, update_handler};

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{
        HeaderMap, StatusCode,
        header::{CONTENT_TYPE, HeaderValue},
    },
    response::{IntoResponse, Json, Response},
};
use qail_core::ast::{AggregateFunc, Expr, JoinKind, Operator, Value as QailValue};
use qail_core::transpiler::ToSql;
use serde_json::{Value, json};
use std::sync::Arc;
use uuid::Uuid;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::handler::row_to_json;
use crate::middleware::ApiError;
use crate::policy::OperationType;

use super::super::branch::{apply_branch_overlay, redirect_to_overlay};
use super::super::filters::{
    apply_filters, apply_returning, apply_sorting, json_to_qail_value, parse_filters,
    parse_scalar_value,
};
use super::super::nested::expand_nested;
use super::super::types::*;
use super::super::{debug_sql, extract_branch_from_headers, extract_table_name, is_debug_request};
use super::{check_table_not_blocked, parse_prefer_header, primary_sort_for_cursor};
