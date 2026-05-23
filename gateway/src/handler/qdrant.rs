//! Qdrant vector operation handlers.
//!
//! Routes QAIL vector actions (Search, Upsert, Scroll, CreateCollection,
//! DeleteCollection) to the Qdrant connection pool.

use axum::response::Json;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use super::QueryResponse;
use crate::GatewayState;
use crate::middleware::ApiError;

const ORIGINAL_POINT_ID_PAYLOAD_KEY: &str = "_qail_original_point_id";

/// Execute a Qdrant vector command.
///
/// Routes QAIL vector actions (Search, Upsert, Scroll, etc.) to the
/// Qdrant connection pool. Returns JSON-formatted scored points or
/// operation results.
pub(super) async fn execute_qdrant_cmd(
    state: &Arc<GatewayState>,
    auth: &crate::auth::AuthContext,
    cmd: &qail_core::ast::Qail,
) -> Result<Json<QueryResponse>, ApiError> {
    use qail_core::ast::{Action, CageKind, Distance as CoreDistance};

    let pool = state.qdrant_pool.as_ref().ok_or_else(|| {
        tracing::error!("Qdrant operation requested but no [qdrant] config");
        ApiError::with_code("QDRANT_NOT_CONFIGURED", "Qdrant not configured")
    })?;

    let mut conn = pool.get().await.map_err(|e| {
        tracing::error!("Qdrant pool error: {}", e);
        ApiError::with_code("QDRANT_CONNECTION_ERROR", "Qdrant connection failed")
    })?;

    let mut cmd = cmd.clone();
    let tenant_col = state.config.tenant_column.clone();
    if let Some(tenant_id) = auth.tenant_id.as_deref() {
        inject_qdrant_tenant_scope(&mut cmd, &tenant_col, tenant_id);
    }

    let collection = &cmd.table;
    let (must_conditions, should_groups) = split_filter_conditions(&cmd);

    // Extract limit from CageKind::Limit if present
    let limit_val: u64 = cmd
        .cages
        .iter()
        .find_map(|c| match c.kind {
            CageKind::Limit(n) => Some(n as u64),
            _ => None,
        })
        .unwrap_or(10);

    match cmd.action {
        Action::Search => {
            // Use the dedicated vector field from the Qail AST
            let vector = cmd.vector.as_deref().ok_or_else(|| {
                ApiError::bad_request("MISSING_VECTOR", "Search requires a vector")
            })?;
            let results = if must_conditions.is_empty() && should_groups.is_empty() {
                if let Some(name) = cmd.vector_name.as_deref() {
                    conn.search_named(collection, name, vector, limit_val, cmd.score_threshold)
                        .await
                        .map_err(|e| qdrant_err(e, "search"))?
                } else {
                    conn.search(collection, vector, limit_val, cmd.score_threshold)
                        .await
                        .map_err(|e| qdrant_err(e, "search"))?
                }
            } else {
                conn.search_filtered_grouped_cages(
                    qail_qdrant::encoder::SearchRequest {
                        collection,
                        vector,
                        limit: limit_val,
                        score_threshold: cmd.score_threshold,
                        vector_name: cmd.vector_name.as_deref(),
                    },
                    &must_conditions,
                    &should_groups,
                )
                .await
                .map_err(|e| qdrant_err(e, "search"))?
            };

            let rows: Vec<serde_json::Value> = results.iter().map(scored_point_to_json).collect();
            if let Some(tenant_id) = auth.tenant_id.as_deref() {
                verify_qdrant_tenant_boundary(
                    &rows,
                    tenant_id,
                    &tenant_col,
                    collection,
                    "qdrant_search",
                )?;
            }
            let count = rows.len();

            Ok(Json(QueryResponse {
                rows,
                count,
                metadata: None,
            }))
        }

        Action::Scroll => {
            let result = if must_conditions.is_empty() && should_groups.is_empty() {
                conn.scroll(collection, limit_val as u32, None, cmd.with_vector)
                    .await
                    .map_err(|e| qdrant_err(e, "scroll"))?
            } else {
                conn.scroll_filtered_grouped_cages(
                    collection,
                    limit_val as u32,
                    None,
                    cmd.with_vector,
                    &must_conditions,
                    &should_groups,
                )
                .await
                .map_err(|e| qdrant_err(e, "scroll"))?
            };

            let rows: Vec<serde_json::Value> =
                result.points.iter().map(scored_point_to_json).collect();
            if let Some(tenant_id) = auth.tenant_id.as_deref() {
                verify_qdrant_tenant_boundary(
                    &rows,
                    tenant_id,
                    &tenant_col,
                    collection,
                    "qdrant_scroll",
                )?;
            }
            let count = rows.len();

            Ok(Json(QueryResponse {
                rows,
                count,
                metadata: None,
            }))
        }

        Action::Upsert => {
            let mut point = extract_upsert_point(&cmd)?;
            if let Some(tenant_id) = auth.tenant_id.as_deref() {
                prepare_tenant_scoped_qdrant_upsert_point(&mut point, tenant_id);
            }
            let upsert_filter_cages = qdrant_upsert_filter_cages(&cmd);
            validate_qdrant_upsert_filter_cages(&upsert_filter_cages)?;
            let create_policy_filter_cages = state
                .policy_engine
                .filter_cages_for_operation(auth, collection, crate::policy::OperationType::Create)
                .map_err(|e| ApiError::with_code("POLICY_DENIED", e.to_string()))?;
            let update_policy_filter_cages = state
                .policy_engine
                .filter_cages_for_operation(auth, collection, crate::policy::OperationType::Update)
                .map_err(|e| ApiError::with_code("POLICY_DENIED", e.to_string()))?;
            let request_filter_cages =
                qdrant_request_filter_cages(&upsert_filter_cages, &update_policy_filter_cages);

            if auth.tenant_id.is_some()
                || qdrant_upsert_filter_cages_have_payload_conditions(&request_filter_cages)?
                || !create_policy_filter_cages.is_empty()
                || !update_policy_filter_cages.is_empty()
            {
                let existing = conn
                    .get_points(collection, std::slice::from_ref(&point.id), false)
                    .await
                    .map_err(|e| qdrant_err(e, "get_points"))?;
                if let Some(tenant_id) = auth.tenant_id.as_deref() {
                    verify_existing_qdrant_points_tenant_boundary(
                        &existing,
                        tenant_id,
                        &tenant_col,
                        collection,
                    )?;
                }

                if existing.is_empty() {
                    enforce_qdrant_upsert_outgoing_filters(
                        &point.payload,
                        &request_filter_cages,
                        &create_policy_filter_cages,
                        &update_policy_filter_cages,
                        collection,
                        true,
                    )?;
                } else {
                    for existing_point in &existing {
                        enforce_qdrant_upsert_payload_filters(
                            &existing_point.payload,
                            &request_filter_cages,
                            collection,
                            "existing",
                        )?;
                        enforce_qdrant_upsert_payload_filters(
                            &existing_point.payload,
                            &update_policy_filter_cages,
                            collection,
                            "update_policy",
                        )?;
                    }
                    enforce_qdrant_upsert_outgoing_filters(
                        &point.payload,
                        &request_filter_cages,
                        &create_policy_filter_cages,
                        &update_policy_filter_cages,
                        collection,
                        false,
                    )?;
                }
            }
            conn.upsert(collection, &[point], false)
                .await
                .map_err(|e| qdrant_err(e, "upsert"))?;

            Ok(Json(QueryResponse {
                rows: vec![serde_json::json!({"status": "upsert_ok", "collection": collection})],
                count: 1,
                metadata: None,
            }))
        }

        Action::CreateCollection => {
            let vector_size = cmd.vector_size.ok_or_else(|| {
                ApiError::bad_request(
                    "MISSING_VECTOR_SIZE",
                    "CreateCollection requires vector_size",
                )
            })?;
            let distance = match cmd.distance.unwrap_or(CoreDistance::Cosine) {
                CoreDistance::Cosine => qail_qdrant::Distance::Cosine,
                CoreDistance::Euclid => qail_qdrant::Distance::Euclidean,
                CoreDistance::Dot => qail_qdrant::Distance::Dot,
            };
            let on_disk = cmd.on_disk.unwrap_or(false);

            conn.create_collection(collection, vector_size, distance, on_disk)
                .await
                .map_err(|e| qdrant_err(e, "create_collection"))?;

            Ok(Json(QueryResponse {
                rows: vec![
                    serde_json::json!({"status": "create_collection_ok", "collection": collection}),
                ],
                count: 1,
                metadata: None,
            }))
        }

        Action::DeleteCollection => {
            conn.delete_collection(collection)
                .await
                .map_err(|e| qdrant_err(e, "delete_collection"))?;

            Ok(Json(QueryResponse {
                rows: vec![
                    serde_json::json!({"status": "delete_collection_ok", "collection": collection}),
                ],
                count: 1,
                metadata: None,
            }))
        }

        _ => Err(ApiError::bad_request(
            "UNSUPPORTED_ACTION",
            format!("Unsupported Qdrant action: {:?}", cmd.action),
        )),
    }
}

fn inject_qdrant_tenant_scope(cmd: &mut qail_core::ast::Qail, tenant_col: &str, tenant_id: &str) {
    use qail_core::ast::{Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Value};

    let condition = Condition {
        left: Expr::Named(tenant_col.to_string()),
        op: Operator::Eq,
        value: Value::String(tenant_id.to_string()),
        is_array_unnest: false,
    };

    if matches!(cmd.action, Action::Search | Action::Scroll) {
        if let Some(cage) = cmd
            .cages
            .iter_mut()
            .find(|cage| matches!(cage.kind, CageKind::Filter) && cage.logical_op == LogicalOp::And)
        {
            cage.conditions.push(condition.clone());
        } else {
            cmd.cages.push(Cage {
                kind: CageKind::Filter,
                conditions: vec![condition.clone()],
                logical_op: LogicalOp::And,
            });
        }
    }

    if matches!(cmd.action, Action::Upsert) {
        if let Some(cage) = cmd
            .cages
            .iter_mut()
            .find(|cage| matches!(cage.kind, CageKind::Payload))
        {
            cage.conditions
                .retain(|cond| !matches!(&cond.left, Expr::Named(name) if name == tenant_col));
            cage.conditions.push(condition);
        } else {
            cmd.cages.push(Cage {
                kind: CageKind::Payload,
                conditions: vec![condition],
                logical_op: LogicalOp::And,
            });
        }
    }
}

fn verify_qdrant_tenant_boundary(
    rows: &[serde_json::Value],
    expected_tenant_id: &str,
    tenant_col: &str,
    collection: &str,
    endpoint: &str,
) -> Result<(), ApiError> {
    for (idx, row) in rows.iter().enumerate() {
        let value = row
            .get("payload")
            .and_then(|payload| payload.get(tenant_col))
            .or_else(|| row.get(tenant_col));
        let Some(value) = value else {
            tracing::error!(
                collection = %collection,
                endpoint = %endpoint,
                row = idx,
                tenant_col = %tenant_col,
                "TENANT_BOUNDARY_VIOLATION - Qdrant point missing tenant payload"
            );
            return Err(ApiError::with_code(
                "TENANT_BOUNDARY_VIOLATION",
                "Data integrity error",
            ));
        };
        if value.as_str() != Some(expected_tenant_id) {
            tracing::error!(
                collection = %collection,
                endpoint = %endpoint,
                row = idx,
                tenant_col = %tenant_col,
                expected = %expected_tenant_id,
                actual = %value,
                "TENANT_BOUNDARY_VIOLATION - Qdrant tenant payload mismatch"
            );
            return Err(ApiError::with_code(
                "TENANT_BOUNDARY_VIOLATION",
                "Data integrity error",
            ));
        }
    }
    Ok(())
}

fn split_filter_conditions(
    cmd: &qail_core::ast::Qail,
) -> (
    Vec<qail_core::ast::Condition>,
    Vec<Vec<qail_core::ast::Condition>>,
) {
    use qail_core::ast::{CageKind, LogicalOp};

    let mut must_conditions = Vec::new();
    let mut should_groups = Vec::new();
    for cage in cmd
        .cages
        .iter()
        .filter(|c| matches!(c.kind, CageKind::Filter))
    {
        match cage.logical_op {
            LogicalOp::And => must_conditions.extend(cage.conditions.iter().cloned()),
            LogicalOp::Or => {
                if !cage.conditions.is_empty() {
                    should_groups.push(cage.conditions.to_vec());
                }
            }
        }
    }
    (must_conditions, should_groups)
}

fn verify_existing_qdrant_points_tenant_boundary(
    points: &[qail_qdrant::ScoredPoint],
    expected_tenant_id: &str,
    tenant_col: &str,
    collection: &str,
) -> Result<(), ApiError> {
    for (idx, point) in points.iter().enumerate() {
        let Some(value) = point.payload.get(tenant_col) else {
            tracing::error!(
                collection = %collection,
                row = idx,
                tenant_col = %tenant_col,
                "TENANT_BOUNDARY_VIOLATION - existing Qdrant point missing tenant payload before upsert"
            );
            return Err(ApiError::with_code(
                "TENANT_BOUNDARY_VIOLATION",
                "Data integrity error",
            ));
        };

        if !matches!(value, qail_qdrant::PayloadValue::String(actual) if actual == expected_tenant_id)
        {
            tracing::error!(
                collection = %collection,
                row = idx,
                tenant_col = %tenant_col,
                expected = %expected_tenant_id,
                actual = ?value,
                "TENANT_BOUNDARY_VIOLATION - existing Qdrant point tenant mismatch before upsert"
            );
            return Err(ApiError::with_code(
                "TENANT_BOUNDARY_VIOLATION",
                "Data integrity error",
            ));
        }
    }

    Ok(())
}

fn qdrant_point_id_fingerprint(id: &qail_qdrant::PointId) -> String {
    match id {
        qail_qdrant::PointId::Num(id) => format!("n:{id}"),
        qail_qdrant::PointId::Uuid(id) => format!("u:{id}"),
    }
}

fn tenant_scoped_qdrant_point_id(
    id: &qail_qdrant::PointId,
    tenant_id: &str,
) -> qail_qdrant::PointId {
    let mut hasher = Sha256::new();
    hasher.update(b"qail:qdrant:tenant-point-id:v1");
    hasher.update([0]);
    hasher.update(tenant_id.as_bytes());
    hasher.update([0]);
    hasher.update(qdrant_point_id_fingerprint(id).as_bytes());
    let digest = hasher.finalize();

    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    qail_qdrant::PointId::Uuid(uuid::Uuid::from_bytes(bytes).to_string())
}

fn qdrant_point_id_payload_value(id: &qail_qdrant::PointId) -> qail_qdrant::PayloadValue {
    match id {
        qail_qdrant::PointId::Num(id) if *id <= i64::MAX as u64 => {
            qail_qdrant::PayloadValue::Integer(*id as i64)
        }
        qail_qdrant::PointId::Num(id) => qail_qdrant::PayloadValue::String(id.to_string()),
        qail_qdrant::PointId::Uuid(id) => qail_qdrant::PayloadValue::String(id.clone()),
    }
}

fn prepare_tenant_scoped_qdrant_upsert_point(point: &mut qail_qdrant::Point, tenant_id: &str) {
    let original_id = point.id.clone();
    point
        .payload
        .entry(ORIGINAL_POINT_ID_PAYLOAD_KEY.to_string())
        .or_insert_with(|| qdrant_point_id_payload_value(&original_id));
    point.id = tenant_scoped_qdrant_point_id(&original_id, tenant_id);
}

fn extract_upsert_point(cmd: &qail_core::ast::Qail) -> Result<qail_qdrant::Point, ApiError> {
    use qail_core::ast::{CageKind, Expr};

    let mut id = None;
    let mut vector = cmd.vector.clone();
    let mut payload = qail_qdrant::Payload::new();

    for cage in cmd
        .cages
        .iter()
        .filter(|c| matches!(c.kind, CageKind::Payload | CageKind::Filter))
    {
        let is_payload_cage = matches!(cage.kind, CageKind::Payload);
        for cond in &cage.conditions {
            let field = match &cond.left {
                Expr::Named(name) => name.as_str(),
                Expr::Aliased { name, .. } => name.as_str(),
                _ => continue,
            };

            match field {
                "id" => {
                    id = Some(point_id_from_value(&cond.value).ok_or_else(|| {
                        ApiError::bad_request(
                            "INVALID_POINT_ID",
                            "Upsert point id must be integer or string UUID",
                        )
                    })?);
                }
                "vector" => {
                    vector = Some(vector_from_value(&cond.value).ok_or_else(|| {
                        ApiError::bad_request(
                            "INVALID_VECTOR",
                            "Upsert vector must be an array of numeric values",
                        )
                    })?);
                }
                _ if is_payload_cage => {
                    if let Some(v) = payload_value_from_ast(&cond.value) {
                        payload.insert(field.to_string(), v);
                    }
                }
                _ => {}
            }
        }
    }

    let id = id.ok_or_else(|| {
        ApiError::bad_request(
            "MISSING_POINT_ID",
            "Upsert requires payload/filter field 'id'",
        )
    })?;
    let vector = vector.ok_or_else(|| {
        ApiError::bad_request(
            "MISSING_VECTOR",
            "Upsert requires vector payload field 'vector' or cmd.vector",
        )
    })?;
    if vector.is_empty() {
        return Err(ApiError::bad_request(
            "INVALID_VECTOR",
            "Upsert vector must not be empty",
        ));
    }

    Ok(qail_qdrant::Point {
        id,
        vector,
        payload,
    })
}

fn point_id_from_value(value: &qail_core::ast::Value) -> Option<qail_qdrant::PointId> {
    use qail_core::ast::Value;
    match value {
        Value::Int(n) if *n >= 0 => Some(qail_qdrant::PointId::Num(*n as u64)),
        Value::String(s) => Some(qail_qdrant::PointId::Uuid(s.clone())),
        Value::Uuid(u) => Some(qail_qdrant::PointId::Uuid(u.to_string())),
        _ => None,
    }
}

fn vector_from_value(value: &qail_core::ast::Value) -> Option<Vec<f32>> {
    use qail_core::ast::Value;
    match value {
        Value::Vector(v) => Some(v.clone()),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Int(n) => out.push(*n as f32),
                    Value::Float(f) => out.push(*f as f32),
                    _ => return None,
                }
            }
            Some(out)
        }
        _ => None,
    }
}

fn payload_value_from_ast(value: &qail_core::ast::Value) -> Option<qail_qdrant::PayloadValue> {
    use qail_core::ast::Value;

    match value {
        Value::Null => Some(qail_qdrant::PayloadValue::Null),
        Value::Bool(b) => Some(qail_qdrant::PayloadValue::Bool(*b)),
        Value::Int(n) => Some(qail_qdrant::PayloadValue::Integer(*n)),
        Value::Float(f) => Some(qail_qdrant::PayloadValue::Float(*f)),
        Value::String(s) => Some(qail_qdrant::PayloadValue::String(s.clone())),
        Value::Uuid(u) => Some(qail_qdrant::PayloadValue::String(u.to_string())),
        Value::Json(s) => Some(qail_qdrant::PayloadValue::String(s.clone())),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(payload_value_from_ast(item)?);
            }
            Some(qail_qdrant::PayloadValue::List(out))
        }
        _ => None,
    }
}

fn qdrant_upsert_filter_cages(cmd: &qail_core::ast::Qail) -> Vec<qail_core::ast::Cage> {
    use qail_core::ast::CageKind;

    cmd.cages
        .iter()
        .filter(|cage| matches!(cage.kind, CageKind::Filter))
        .cloned()
        .collect()
}

fn qdrant_request_filter_cages(
    all_filter_cages: &[qail_core::ast::Cage],
    update_policy_filter_cages: &[qail_core::ast::Cage],
) -> Vec<qail_core::ast::Cage> {
    all_filter_cages
        .iter()
        .filter(|cage| {
            !update_policy_filter_cages
                .iter()
                .any(|policy_cage| policy_cage == *cage)
        })
        .cloned()
        .collect()
}

fn qdrant_upsert_filter_payload_field(
    condition: &qail_core::ast::Condition,
) -> Result<Option<&str>, ApiError> {
    use qail_core::ast::Expr;

    let raw = match &condition.left {
        Expr::Named(name) | Expr::Aliased { name, .. } => name.as_str(),
        other => {
            return Err(ApiError::forbidden(format!(
                "Qdrant upsert filter cannot be safely enforced for expression {:?}",
                other
            )));
        }
    };
    let field = raw.rsplit('.').next().unwrap_or(raw).trim_matches('"');
    if field.is_empty() {
        return Err(ApiError::forbidden(
            "Qdrant upsert filter cannot be safely enforced for an empty payload field",
        ));
    }
    if matches!(field, "id" | "vector") {
        return Ok(None);
    }
    Ok(Some(field))
}

fn qdrant_upsert_filter_cages_have_payload_conditions(
    cages: &[qail_core::ast::Cage],
) -> Result<bool, ApiError> {
    for cage in cages {
        for condition in &cage.conditions {
            if qdrant_upsert_filter_payload_field(condition)?.is_some() {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn validate_qdrant_upsert_filter_value(value: &qail_core::ast::Value) -> Result<(), ApiError> {
    use qail_core::ast::Value;

    match value {
        Value::Null
        | Value::NullUuid
        | Value::Bool(_)
        | Value::Int(_)
        | Value::Float(_)
        | Value::String(_)
        | Value::Json(_)
        | Value::Timestamp(_)
        | Value::Uuid(_) => Ok(()),
        Value::Array(items) => {
            for item in items {
                validate_qdrant_upsert_filter_value(item)?;
            }
            Ok(())
        }
        _ => Err(ApiError::forbidden(
            "Qdrant upsert filters support only literal equality values",
        )),
    }
}

fn validate_qdrant_upsert_filter_cages(cages: &[qail_core::ast::Cage]) -> Result<(), ApiError> {
    use qail_core::ast::Operator;

    for cage in cages {
        for condition in &cage.conditions {
            if qdrant_upsert_filter_payload_field(condition)?.is_none() {
                continue;
            }
            if condition.op != Operator::Eq {
                return Err(ApiError::forbidden(
                    "Qdrant upsert filters support only equality conditions",
                ));
            }
            validate_qdrant_upsert_filter_value(&condition.value)?;
        }
    }
    Ok(())
}

fn ast_value_matches_qdrant_payload(
    expected: &qail_core::ast::Value,
    actual: &qail_qdrant::PayloadValue,
) -> Result<bool, ApiError> {
    use qail_core::ast::Value;
    use qail_qdrant::PayloadValue;

    let matches = match expected {
        Value::Null | Value::NullUuid => matches!(actual, PayloadValue::Null),
        Value::Bool(expected) => matches!(actual, PayloadValue::Bool(actual) if actual == expected),
        Value::Int(expected) => match actual {
            PayloadValue::Integer(actual) => actual == expected,
            PayloadValue::Float(actual) => (*actual - *expected as f64).abs() < f64::EPSILON,
            _ => false,
        },
        Value::Float(expected) => match actual {
            PayloadValue::Float(actual) => (*actual - *expected).abs() < f64::EPSILON,
            PayloadValue::Integer(actual) => (*actual as f64 - *expected).abs() < f64::EPSILON,
            _ => false,
        },
        Value::String(expected) | Value::Json(expected) | Value::Timestamp(expected) => {
            matches!(actual, PayloadValue::String(actual) if actual == expected)
        }
        Value::Uuid(expected) => {
            matches!(actual, PayloadValue::String(actual) if actual == &expected.to_string())
        }
        Value::Array(expected_items) => match actual {
            PayloadValue::List(actual_items) if actual_items.len() == expected_items.len() => {
                let mut all_match = true;
                for (expected, actual) in expected_items.iter().zip(actual_items) {
                    if !ast_value_matches_qdrant_payload(expected, actual)? {
                        all_match = false;
                        break;
                    }
                }
                all_match
            }
            _ => false,
        },
        _ => {
            return Err(ApiError::forbidden(
                "Qdrant upsert filters support only literal equality values",
            ));
        }
    };

    Ok(matches)
}

fn qdrant_payload_matches_filter_condition(
    payload: &qail_qdrant::Payload,
    condition: &qail_core::ast::Condition,
) -> Result<Option<bool>, ApiError> {
    use qail_core::ast::Operator;

    let Some(field) = qdrant_upsert_filter_payload_field(condition)? else {
        return Ok(None);
    };
    if condition.op != Operator::Eq {
        return Err(ApiError::forbidden(
            "Qdrant upsert filters support only equality conditions",
        ));
    }
    validate_qdrant_upsert_filter_value(&condition.value)?;

    let Some(actual) = payload.get(field) else {
        return Ok(Some(false));
    };
    Ok(Some(ast_value_matches_qdrant_payload(
        &condition.value,
        actual,
    )?))
}

fn qdrant_payload_matches_filter_cages(
    payload: &qail_qdrant::Payload,
    cages: &[qail_core::ast::Cage],
) -> Result<bool, ApiError> {
    use qail_core::ast::LogicalOp;

    let mut has_or_condition = false;
    let mut any_or_condition_matches = false;

    for cage in cages {
        match cage.logical_op {
            LogicalOp::And => {
                for condition in &cage.conditions {
                    if qdrant_payload_matches_filter_condition(payload, condition)?
                        .is_some_and(|matches| !matches)
                    {
                        return Ok(false);
                    }
                }
            }
            LogicalOp::Or => {
                for condition in &cage.conditions {
                    if let Some(matches) =
                        qdrant_payload_matches_filter_condition(payload, condition)?
                    {
                        has_or_condition = true;
                        if matches {
                            any_or_condition_matches = true;
                        }
                    }
                }
            }
        }
    }

    Ok(!has_or_condition || any_or_condition_matches)
}

fn enforce_qdrant_upsert_payload_filters(
    payload: &qail_qdrant::Payload,
    cages: &[qail_core::ast::Cage],
    collection: &str,
    context: &str,
) -> Result<(), ApiError> {
    if qdrant_payload_matches_filter_cages(payload, cages)? {
        return Ok(());
    }

    tracing::warn!(
        collection = %collection,
        context = %context,
        "Qdrant upsert rejected by payload filter enforcement"
    );
    Err(ApiError::forbidden(
        "Qdrant upsert violates policy filter constraints",
    ))
}

fn enforce_qdrant_upsert_outgoing_filters(
    payload: &qail_qdrant::Payload,
    upsert_filter_cages: &[qail_core::ast::Cage],
    create_policy_filter_cages: &[qail_core::ast::Cage],
    update_policy_filter_cages: &[qail_core::ast::Cage],
    collection: &str,
    is_create: bool,
) -> Result<(), ApiError> {
    enforce_qdrant_upsert_payload_filters(payload, upsert_filter_cages, collection, "outgoing")?;
    if is_create {
        enforce_qdrant_upsert_payload_filters(
            payload,
            create_policy_filter_cages,
            collection,
            "create_policy",
        )
    } else {
        enforce_qdrant_upsert_payload_filters(
            payload,
            update_policy_filter_cages,
            collection,
            "update_policy_outgoing",
        )
    }
}

/// Convert a Qdrant error into an ApiError.
fn qdrant_err(e: qail_qdrant::QdrantError, op: &str) -> ApiError {
    tracing::error!("Qdrant {} error: {}", op, e);
    ApiError::with_code("QDRANT_ERROR", format!("Qdrant {} failed", op))
}

/// Convert a `ScoredPoint` to a JSON value for the response.
fn scored_point_to_json(pt: &qail_qdrant::ScoredPoint) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    let response_id = pt
        .payload
        .get(ORIGINAL_POINT_ID_PAYLOAD_KEY)
        .map(payload_value_to_json)
        .unwrap_or_else(|| serde_json::json!(pt.id));
    obj.insert("id".to_string(), response_id);
    obj.insert("score".to_string(), serde_json::json!(pt.score));

    if !pt.payload.is_empty() {
        let payload: serde_json::Map<String, serde_json::Value> = pt
            .payload
            .iter()
            .filter(|(k, _)| k.as_str() != ORIGINAL_POINT_ID_PAYLOAD_KEY)
            .map(|(k, v)| (k.clone(), payload_value_to_json(v)))
            .collect();
        if !payload.is_empty() {
            obj.insert("payload".to_string(), serde_json::Value::Object(payload));
        }
    }

    if let Some(vector) = &pt.vector {
        obj.insert("vector".to_string(), serde_json::json!(vector));
    }

    serde_json::Value::Object(obj)
}

/// Convert a `PayloadValue` to JSON.
fn payload_value_to_json(v: &qail_qdrant::PayloadValue) -> serde_json::Value {
    match v {
        qail_qdrant::PayloadValue::String(s) => serde_json::json!(s),
        qail_qdrant::PayloadValue::Integer(i) => serde_json::json!(i),
        qail_qdrant::PayloadValue::Float(f) => serde_json::json!(f),
        qail_qdrant::PayloadValue::Bool(b) => serde_json::json!(b),
        qail_qdrant::PayloadValue::Null => serde_json::Value::Null,
        qail_qdrant::PayloadValue::List(arr) => {
            serde_json::Value::Array(arr.iter().map(payload_value_to_json).collect())
        }
        qail_qdrant::PayloadValue::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), payload_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ORIGINAL_POINT_ID_PAYLOAD_KEY, enforce_qdrant_upsert_outgoing_filters,
        enforce_qdrant_upsert_payload_filters, extract_upsert_point,
        prepare_tenant_scoped_qdrant_upsert_point, qdrant_payload_matches_filter_cages,
        qdrant_request_filter_cages, qdrant_upsert_filter_cages, scored_point_to_json,
        split_filter_conditions, tenant_scoped_qdrant_point_id,
        verify_existing_qdrant_points_tenant_boundary,
    };
    use qail_core::ast::{Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value};

    fn cond(name: &str, value: &str) -> Condition {
        Condition {
            left: Expr::Named(name.to_string()),
            op: Operator::Eq,
            value: Value::String(value.to_string()),
            is_array_unnest: false,
        }
    }

    #[test]
    fn split_filter_conditions_keeps_or_groups_separate() {
        let cmd = Qail {
            cages: vec![
                Cage {
                    kind: CageKind::Filter,
                    conditions: vec![cond("tenant_id", "t1")],
                    logical_op: LogicalOp::And,
                },
                Cage {
                    kind: CageKind::Filter,
                    conditions: vec![cond("city", "London"), cond("city", "Paris")],
                    logical_op: LogicalOp::Or,
                },
                Cage {
                    kind: CageKind::Filter,
                    conditions: vec![cond("country", "UK"), cond("country", "FR")],
                    logical_op: LogicalOp::Or,
                },
            ],
            ..Default::default()
        };

        let (must, should_groups) = split_filter_conditions(&cmd);
        assert_eq!(must.len(), 1);
        assert_eq!(should_groups.len(), 2);
        assert_eq!(should_groups[0].len(), 2);
        assert_eq!(should_groups[1].len(), 2);
    }

    #[test]
    fn scored_point_json_includes_returned_vector() {
        let point = qail_qdrant::ScoredPoint {
            id: qail_qdrant::PointId::Num(7),
            score: 0.95,
            payload: qail_qdrant::Payload::new(),
            vector: Some(vec![0.1, 0.2, 0.3]),
        };

        let json = scored_point_to_json(&point);

        let vector = json
            .get("vector")
            .and_then(serde_json::Value::as_array)
            .expect("vector array");
        assert_eq!(vector.len(), 3);
        for (actual, expected) in vector.iter().zip([0.1_f64, 0.2, 0.3]) {
            let actual = actual.as_f64().expect("numeric vector component");
            assert!((actual - expected).abs() < 0.000_001);
        }
    }

    #[test]
    fn tenant_scoped_qdrant_point_id_separates_tenants() {
        let id = qail_qdrant::PointId::Num(7);

        let tenant_a = tenant_scoped_qdrant_point_id(&id, "tenant-a");
        let tenant_b = tenant_scoped_qdrant_point_id(&id, "tenant-b");
        let tenant_a_again = tenant_scoped_qdrant_point_id(&id, "tenant-a");

        assert_eq!(tenant_a, tenant_a_again);
        assert_ne!(tenant_a, tenant_b);
        assert!(matches!(tenant_a, qail_qdrant::PointId::Uuid(_)));
    }

    #[test]
    fn tenant_scoped_qdrant_upsert_preserves_original_id_payload() {
        let mut point = qail_qdrant::Point::new_num(7, vec![0.1, 0.2]);

        prepare_tenant_scoped_qdrant_upsert_point(&mut point, "tenant-a");

        assert_ne!(point.id, qail_qdrant::PointId::Num(7));
        assert_eq!(
            point.payload.get(ORIGINAL_POINT_ID_PAYLOAD_KEY),
            Some(&qail_qdrant::PayloadValue::Integer(7))
        );
    }

    #[test]
    fn scored_point_json_uses_original_id_payload_and_hides_internal_metadata() {
        let mut payload = qail_qdrant::Payload::new();
        payload.insert(
            ORIGINAL_POINT_ID_PAYLOAD_KEY.to_string(),
            qail_qdrant::PayloadValue::Integer(7),
        );
        payload.insert(
            "tenant_id".to_string(),
            qail_qdrant::PayloadValue::String("tenant-a".to_string()),
        );
        let point = qail_qdrant::ScoredPoint {
            id: qail_qdrant::PointId::Uuid("aaaaaaaa-aaaa-5aaa-aaaa-aaaaaaaaaaaa".to_string()),
            score: 0.95,
            payload,
            vector: None,
        };

        let json = scored_point_to_json(&point);

        assert_eq!(json.get("id"), Some(&serde_json::json!(7)));
        let payload = json
            .get("payload")
            .and_then(serde_json::Value::as_object)
            .expect("payload object");
        assert_eq!(
            payload.get("tenant_id"),
            Some(&serde_json::json!("tenant-a"))
        );
        assert!(!payload.contains_key(ORIGINAL_POINT_ID_PAYLOAD_KEY));
    }

    #[test]
    fn extract_upsert_point_ignores_filter_conditions_as_payload() {
        let cmd = Qail::upsert("embeddings")
            .set_value("id", 7)
            .set_value("vector", Value::Vector(vec![0.1, 0.2]))
            .set_value("tenant_id", "tenant-a")
            .filter("region", Operator::Eq, "west");

        let point = extract_upsert_point(&cmd).unwrap();

        assert!(point.payload.contains_key("tenant_id"));
        assert!(!point.payload.contains_key("region"));
    }

    #[test]
    fn qdrant_payload_filter_matches_simple_equality() {
        let mut payload = qail_qdrant::Payload::new();
        payload.insert(
            "region".to_string(),
            qail_qdrant::PayloadValue::String("west".to_string()),
        );
        let cages = vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![cond("region", "west")],
            logical_op: LogicalOp::And,
        }];

        assert!(qdrant_payload_matches_filter_cages(&payload, &cages).unwrap());
    }

    #[test]
    fn qdrant_payload_filter_rejects_mismatch() {
        let mut payload = qail_qdrant::Payload::new();
        payload.insert(
            "region".to_string(),
            qail_qdrant::PayloadValue::String("east".to_string()),
        );
        let cages = vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![cond("region", "west")],
            logical_op: LogicalOp::And,
        }];

        let err = enforce_qdrant_upsert_payload_filters(&payload, &cages, "embeddings", "existing")
            .unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn qdrant_payload_filter_fails_closed_on_unsupported_operator() {
        let mut payload = qail_qdrant::Payload::new();
        payload.insert(
            "region".to_string(),
            qail_qdrant::PayloadValue::String("west".to_string()),
        );
        let cages = vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![Condition {
                left: Expr::Named("region".to_string()),
                op: Operator::Ne,
                value: Value::String("east".to_string()),
                is_array_unnest: false,
            }],
            logical_op: LogicalOp::And,
        }];

        let err = qdrant_payload_matches_filter_cages(&payload, &cages).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn upsert_extraction_and_filter_enforcement_use_outgoing_payload() {
        let cmd = Qail::upsert("embeddings")
            .set_value("id", 7)
            .set_value("vector", Value::Vector(vec![0.1, 0.2]))
            .set_value("region", "west")
            .filter("region", Operator::Eq, "west");
        let point = extract_upsert_point(&cmd).unwrap();
        let cages = qdrant_upsert_filter_cages(&cmd);

        enforce_qdrant_upsert_payload_filters(&point.payload, &cages, "embeddings", "outgoing")
            .unwrap();

        let mismatched_cmd = Qail::upsert("embeddings")
            .set_value("id", 7)
            .set_value("vector", Value::Vector(vec![0.1, 0.2]))
            .set_value("region", "east")
            .filter("region", Operator::Eq, "west");
        let mismatched_point = extract_upsert_point(&mismatched_cmd).unwrap();
        let mismatched_cages = qdrant_upsert_filter_cages(&mismatched_cmd);

        let err = enforce_qdrant_upsert_payload_filters(
            &mismatched_point.payload,
            &mismatched_cages,
            "embeddings",
            "outgoing",
        )
        .unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn qdrant_upsert_create_rejects_outgoing_payload_that_violates_request_filter() {
        let mut payload = qail_qdrant::Payload::new();
        payload.insert(
            "region".to_string(),
            qail_qdrant::PayloadValue::String("east".to_string()),
        );
        let upsert_filters = vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![cond("region", "west")],
            logical_op: LogicalOp::And,
        }];

        let err = enforce_qdrant_upsert_outgoing_filters(
            &payload,
            &upsert_filters,
            &[],
            &[],
            "embeddings",
            true,
        )
        .unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn qdrant_upsert_update_rejects_outgoing_payload_that_violates_update_policy() {
        let mut payload = qail_qdrant::Payload::new();
        payload.insert(
            "operator_id".to_string(),
            qail_qdrant::PayloadValue::String("operator-2".to_string()),
        );
        let update_policy_filters = vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![cond("operator_id", "operator-1")],
            logical_op: LogicalOp::And,
        }];

        let err = enforce_qdrant_upsert_outgoing_filters(
            &payload,
            &[],
            &[],
            &update_policy_filters,
            "embeddings",
            false,
        )
        .unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn qdrant_request_filter_cages_exclude_policy_injected_update_filters() {
        let user_filter = Cage {
            kind: CageKind::Filter,
            conditions: vec![cond("region", "west")],
            logical_op: LogicalOp::And,
        };
        let update_policy_filter = Cage {
            kind: CageKind::Filter,
            conditions: vec![cond("operator_id", "operator-1")],
            logical_op: LogicalOp::Or,
        };
        let all_filters = vec![user_filter.clone(), update_policy_filter.clone()];

        let request_filters = qdrant_request_filter_cages(&all_filters, &[update_policy_filter]);

        assert_eq!(request_filters, vec![user_filter]);
    }

    #[test]
    fn existing_qdrant_point_tenant_boundary_allows_owned_point() {
        let mut payload = qail_qdrant::Payload::new();
        payload.insert(
            "tenant_id".to_string(),
            qail_qdrant::PayloadValue::String("tenant-a".to_string()),
        );
        let points = vec![qail_qdrant::ScoredPoint {
            id: qail_qdrant::PointId::Num(7),
            score: 0.0,
            payload,
            vector: None,
        }];

        verify_existing_qdrant_points_tenant_boundary(
            &points,
            "tenant-a",
            "tenant_id",
            "embeddings",
        )
        .unwrap();
    }

    #[test]
    fn existing_qdrant_point_tenant_boundary_rejects_foreign_point() {
        let mut payload = qail_qdrant::Payload::new();
        payload.insert(
            "tenant_id".to_string(),
            qail_qdrant::PayloadValue::String("tenant-b".to_string()),
        );
        let points = vec![qail_qdrant::ScoredPoint {
            id: qail_qdrant::PointId::Num(7),
            score: 0.0,
            payload,
            vector: None,
        }];

        let err = verify_existing_qdrant_points_tenant_boundary(
            &points,
            "tenant-a",
            "tenant_id",
            "embeddings",
        )
        .unwrap_err();

        assert_eq!(
            err.status_code(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(err.code, "TENANT_BOUNDARY_VIOLATION");
    }

    #[test]
    fn existing_qdrant_point_tenant_boundary_rejects_missing_tenant() {
        let points = vec![qail_qdrant::ScoredPoint {
            id: qail_qdrant::PointId::Num(7),
            score: 0.0,
            payload: qail_qdrant::Payload::new(),
            vector: None,
        }];

        let err = verify_existing_qdrant_points_tenant_boundary(
            &points,
            "tenant-a",
            "tenant_id",
            "embeddings",
        )
        .unwrap_err();

        assert_eq!(err.code, "TENANT_BOUNDARY_VIOLATION");
    }
}
