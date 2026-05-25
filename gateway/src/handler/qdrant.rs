//! Qdrant vector operation handlers.
//!
//! Routes QAIL vector actions (Search, Upsert, Scroll, CreateCollection,
//! DeleteCollection) to the Qdrant connection pool.

use axum::response::Json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

use super::{QueryResponse, ResponseMetadata};
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
    use qail_core::ast::{Action, Distance as CoreDistance};

    ensure_qdrant_collection_management_allowed(auth, &cmd.action)?;

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
    ensure_qdrant_conditions_finite(&must_conditions)?;
    ensure_qdrant_condition_groups_finite(&should_groups)?;
    if matches!(cmd.action, Action::Search | Action::Scroll) {
        validate_qdrant_read_filters(&must_conditions, &should_groups)?;
    }

    // Extract limit from CageKind::Limit if present
    let limit_val = qdrant_limit_from_cmd(&cmd, state.config.max_result_rows)?;

    match cmd.action {
        Action::Search => {
            // Use the dedicated vector field from the Qail AST
            let vector = cmd.vector.as_deref().ok_or_else(|| {
                ApiError::bad_request("MISSING_VECTOR", "Search requires a vector")
            })?;
            ensure_qdrant_vector_finite(vector)?;
            ensure_qdrant_score_threshold_finite(cmd.score_threshold)?;
            let search_request = qail_qdrant::encoder::SearchRequest {
                collection,
                vector,
                limit: limit_val,
                score_threshold: cmd.score_threshold,
                vector_name: cmd.vector_name.as_deref(),
                with_vectors: cmd.with_vector,
            };
            let results = if must_conditions.is_empty() && should_groups.is_empty() {
                conn.search_with_request(search_request)
                    .await
                    .map_err(|e| qdrant_err(e, "search"))?
            } else {
                conn.search_filtered_grouped_cages(search_request, &must_conditions, &should_groups)
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
            let scroll_limit = qdrant_scroll_limit_from_cmd(&cmd, state.config.max_result_rows)?;
            let scroll_offset = qdrant_scroll_offset_from_cmd(&cmd)?;
            let result = if must_conditions.is_empty() && should_groups.is_empty() {
                conn.scroll(
                    collection,
                    scroll_limit,
                    scroll_offset.as_ref(),
                    cmd.with_vector,
                )
                .await
                .map_err(|e| qdrant_err(e, "scroll"))?
            } else {
                conn.scroll_filtered_grouped_cages(
                    collection,
                    scroll_limit,
                    scroll_offset.as_ref(),
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
                metadata: qdrant_scroll_metadata(result.next_offset.as_ref()),
            }))
        }

        Action::Upsert => {
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
            let mut point = extract_upsert_point_with_filter_fallback(&cmd, &request_filter_cages)?;
            if let Some(tenant_id) = auth.tenant_id.as_deref() {
                prepare_tenant_scoped_qdrant_upsert_point(&mut point, tenant_id);
            }

            if auth.tenant_id.is_some()
                || qdrant_upsert_filter_cages_have_enforceable_conditions(&request_filter_cages)?
                || !create_policy_filter_cages.is_empty()
                || !update_policy_filter_cages.is_empty()
            {
                let with_vectors = qdrant_upsert_filter_cages_need_vectors(&request_filter_cages)?
                    || qdrant_upsert_filter_cages_need_vectors(&create_policy_filter_cages)?
                    || qdrant_upsert_filter_cages_need_vectors(&update_policy_filter_cages)?;
                let existing = conn
                    .get_points(collection, std::slice::from_ref(&point.id), with_vectors)
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
                        &point,
                        &request_filter_cages,
                        &create_policy_filter_cages,
                        &update_policy_filter_cages,
                        collection,
                        true,
                    )?;
                } else {
                    for existing_point in &existing {
                        let existing_view = QdrantUpsertPointView {
                            id: &existing_point.id,
                            vector: existing_point.vector.as_deref(),
                            payload: &existing_point.payload,
                        };
                        enforce_qdrant_upsert_point_filters(
                            existing_view,
                            &request_filter_cages,
                            collection,
                            "existing",
                        )?;
                        enforce_qdrant_upsert_point_filters(
                            existing_view,
                            &update_policy_filter_cages,
                            collection,
                            "update_policy",
                        )?;
                    }
                    enforce_qdrant_upsert_outgoing_filters(
                        &point,
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

fn ensure_qdrant_collection_management_allowed(
    auth: &crate::auth::AuthContext,
    action: &qail_core::ast::Action,
) -> Result<(), ApiError> {
    if matches!(
        action,
        qail_core::ast::Action::CreateCollection | qail_core::ast::Action::DeleteCollection
    ) && !auth.is_platform_admin()
    {
        return Err(ApiError::forbidden(
            "Platform administrator role required for Qdrant collection management",
        ));
    }
    Ok(())
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

fn qdrant_limit_from_cmd(
    cmd: &qail_core::ast::Qail,
    max_result_rows: usize,
) -> Result<u64, ApiError> {
    use qail_core::ast::CageKind;

    let requested = cmd
        .cages
        .iter()
        .find_map(|c| match c.kind {
            CageKind::Limit(n) => Some(n),
            _ => None,
        })
        .unwrap_or(10);
    if requested == 0 {
        return Err(ApiError::parse_error(
            "Qdrant limit must be greater than zero",
        ));
    }

    Ok((requested as u64).min(max_result_rows.max(1) as u64))
}

fn qdrant_scroll_limit_from_cmd(
    cmd: &qail_core::ast::Qail,
    max_result_rows: usize,
) -> Result<u32, ApiError> {
    let limit = qdrant_limit_from_cmd(cmd, max_result_rows)?;
    Ok(u32::try_from(limit).unwrap_or(u32::MAX))
}

fn qdrant_scroll_offset_from_cmd(
    cmd: &qail_core::ast::Qail,
) -> Result<Option<qail_qdrant::PointId>, ApiError> {
    use qail_core::ast::CageKind;

    let Some(offset) = cmd.cages.iter().find_map(|c| match c.kind {
        CageKind::Offset(n) => Some(n),
        _ => None,
    }) else {
        return Ok(None);
    };
    let offset = u64::try_from(offset)
        .map_err(|_| ApiError::parse_error("Qdrant scroll offset is too large"))?;
    Ok(Some(qail_qdrant::PointId::Num(offset)))
}

fn qdrant_point_id_to_json(id: &qail_qdrant::PointId) -> serde_json::Value {
    match id {
        qail_qdrant::PointId::Num(id) => serde_json::Value::Number((*id).into()),
        qail_qdrant::PointId::Uuid(id) => serde_json::Value::String(id.clone()),
    }
}

fn qdrant_scroll_metadata(next_offset: Option<&qail_qdrant::PointId>) -> Option<ResponseMetadata> {
    next_offset.map(|offset| ResponseMetadata {
        request_id: String::new(),
        duration_ms: None,
        next_page_offset: Some(qdrant_point_id_to_json(offset)),
    })
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
    point.payload.insert(
        ORIGINAL_POINT_ID_PAYLOAD_KEY.to_string(),
        qdrant_point_id_payload_value(&original_id),
    );
    point.id = tenant_scoped_qdrant_point_id(&original_id, tenant_id);
}

fn set_upsert_point_id(
    id: &mut Option<qail_qdrant::PointId>,
    next: qail_qdrant::PointId,
) -> Result<(), ApiError> {
    if id.as_ref().is_some_and(|existing| existing != &next) {
        return Err(ApiError::bad_request(
            "AMBIGUOUS_POINT_ID",
            "Qdrant upsert received conflicting point id values",
        ));
    }
    *id = Some(next);
    Ok(())
}

fn set_upsert_vector(vector: &mut Option<Vec<f32>>, next: Vec<f32>) -> Result<(), ApiError> {
    if vector.as_ref().is_some_and(|existing| existing != &next) {
        return Err(ApiError::bad_request(
            "AMBIGUOUS_VECTOR",
            "Qdrant upsert received conflicting vector values",
        ));
    }
    *vector = Some(next);
    Ok(())
}

fn extract_upsert_point_with_filter_fallback(
    cmd: &qail_core::ast::Qail,
    id_filter_cages: &[qail_core::ast::Cage],
) -> Result<qail_qdrant::Point, ApiError> {
    use qail_core::ast::{CageKind, Expr, LogicalOp};

    let mut id = None;
    let mut vector = cmd.vector.clone();
    let mut payload = qail_qdrant::Payload::new();

    for cage in cmd
        .cages
        .iter()
        .filter(|c| matches!(c.kind, CageKind::Payload))
    {
        for cond in &cage.conditions {
            let field = match &cond.left {
                Expr::Named(name) => name.as_str(),
                Expr::Aliased { name, .. } => name.as_str(),
                _ => {
                    return Err(ApiError::bad_request(
                        "INVALID_QDRANT_PAYLOAD",
                        "Qdrant payload fields must be named",
                    ));
                }
            };

            match field {
                "id" => {
                    let next = point_id_from_value(&cond.value).ok_or_else(|| {
                        ApiError::bad_request(
                            "INVALID_POINT_ID",
                            "Upsert point id must be integer or string UUID",
                        )
                    })?;
                    set_upsert_point_id(&mut id, next)?;
                }
                "vector" => {
                    let next = vector_from_value(&cond.value).ok_or_else(|| {
                        ApiError::bad_request(
                            "INVALID_VECTOR",
                            "Upsert vector must be an array of numeric values",
                        )
                    })?;
                    set_upsert_vector(&mut vector, next)?;
                }
                field if field == ORIGINAL_POINT_ID_PAYLOAD_KEY => {}
                _ => {
                    payload.insert(field.to_string(), payload_value_from_ast(&cond.value)?);
                }
            }
        }
    }

    for cage in id_filter_cages {
        let can_infer_identity =
            matches!(cage.logical_op, LogicalOp::And) || cage.conditions.len() == 1;
        for cond in &cage.conditions {
            let field = match &cond.left {
                Expr::Named(name) | Expr::Aliased { name, .. } => name.as_str(),
                _ => continue,
            };

            match field.rsplit('.').next().unwrap_or(field).trim_matches('"') {
                "id" => {
                    if !can_infer_identity {
                        if id.is_none() {
                            return Err(ApiError::bad_request(
                                "AMBIGUOUS_POINT_ID",
                                "Upsert point id cannot be inferred from a multi-condition OR filter",
                            ));
                        }
                        continue;
                    }
                    let next = point_id_from_value(&cond.value).ok_or_else(|| {
                        ApiError::bad_request(
                            "INVALID_POINT_ID",
                            "Upsert point id filter must be integer or string UUID",
                        )
                    })?;
                    set_upsert_point_id(&mut id, next)?;
                }
                "vector" => {
                    if !can_infer_identity {
                        if vector.is_none() {
                            return Err(ApiError::bad_request(
                                "AMBIGUOUS_VECTOR",
                                "Upsert vector cannot be inferred from a multi-condition OR filter",
                            ));
                        }
                        continue;
                    }
                    let next = vector_from_value(&cond.value).ok_or_else(|| {
                        ApiError::bad_request(
                            "INVALID_VECTOR",
                            "Upsert vector filter must be an array of numeric values",
                        )
                    })?;
                    set_upsert_vector(&mut vector, next)?;
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

#[cfg(test)]
fn extract_upsert_point(cmd: &qail_core::ast::Qail) -> Result<qail_qdrant::Point, ApiError> {
    let filter_cages = qdrant_upsert_filter_cages(cmd);
    extract_upsert_point_with_filter_fallback(cmd, &filter_cages)
}

fn ensure_qdrant_vector_finite(vector: &[f32]) -> Result<(), ApiError> {
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(ApiError::bad_request(
            "INVALID_VECTOR",
            "Qdrant vector values must be finite numbers",
        ));
    }
    Ok(())
}

fn ensure_qdrant_score_threshold_finite(score_threshold: Option<f32>) -> Result<(), ApiError> {
    if let Some(value) = score_threshold
        && !value.is_finite()
    {
        return Err(ApiError::bad_request(
            "INVALID_SCORE_THRESHOLD",
            "Qdrant score threshold must be a finite number",
        ));
    }
    Ok(())
}

fn ensure_qdrant_value_finite(value: &qail_core::ast::Value) -> Result<(), ApiError> {
    use qail_core::ast::Value;

    match value {
        Value::Float(value) if !value.is_finite() => Err(ApiError::bad_request(
            "INVALID_QDRANT_FILTER",
            "Qdrant filter numeric values must be finite numbers",
        )),
        Value::Vector(values) => ensure_qdrant_vector_finite(values),
        Value::Array(items) => {
            for item in items {
                ensure_qdrant_value_finite(item)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn ensure_qdrant_conditions_finite(
    conditions: &[qail_core::ast::Condition],
) -> Result<(), ApiError> {
    for condition in conditions {
        ensure_qdrant_value_finite(&condition.value)?;
    }
    Ok(())
}

fn ensure_qdrant_condition_groups_finite(
    groups: &[Vec<qail_core::ast::Condition>],
) -> Result<(), ApiError> {
    for group in groups {
        ensure_qdrant_conditions_finite(group)?;
    }
    Ok(())
}

fn validate_qdrant_read_filters(
    conditions: &[qail_core::ast::Condition],
    groups: &[Vec<qail_core::ast::Condition>],
) -> Result<(), ApiError> {
    for condition in conditions {
        validate_qdrant_read_filter_condition(condition)?;
    }
    for group in groups {
        for condition in group {
            validate_qdrant_read_filter_condition(condition)?;
        }
    }
    Ok(())
}

fn validate_qdrant_read_filter_condition(
    condition: &qail_core::ast::Condition,
) -> Result<(), ApiError> {
    use qail_core::ast::{Expr, Operator, Value};

    let field = match &condition.left {
        Expr::Named(name) | Expr::Aliased { name, .. } => name.as_str(),
        other => {
            return Err(ApiError::bad_request(
                "INVALID_QDRANT_FILTER",
                format!("Qdrant read filters require named payload fields, got {other:?}"),
            ));
        }
    };

    if field.trim().is_empty() {
        return Err(ApiError::bad_request(
            "INVALID_QDRANT_FILTER",
            "Qdrant read filter field cannot be empty",
        ));
    }

    match (&condition.op, &condition.value) {
        (Operator::Eq, Value::String(_) | Value::Int(_) | Value::Float(_) | Value::Bool(_)) => {
            Ok(())
        }
        (
            Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte,
            Value::Int(_) | Value::Float(_),
        ) => Ok(()),
        (Operator::Contains | Operator::Like, Value::String(_)) => Ok(()),
        (Operator::IsNull, Value::Null) => Ok(()),
        _ => Err(ApiError::bad_request(
            "INVALID_QDRANT_FILTER",
            format!(
                "Qdrant read filter is not supported by the Qdrant transport: op={:?}, value={:?}",
                condition.op, condition.value
            ),
        )),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QdrantUpsertFilterTarget<'a> {
    Id,
    Vector,
    Payload(&'a str),
}

#[derive(Debug, Clone, Copy)]
struct QdrantUpsertPointView<'a> {
    id: &'a qail_qdrant::PointId,
    vector: Option<&'a [f32]>,
    payload: &'a qail_qdrant::Payload,
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
        Value::Vector(v) if v.iter().all(|value| value.is_finite()) => Some(v.clone()),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Int(n) => out.push(*n as f32),
                    Value::Float(f) => {
                        let value = *f as f32;
                        if !value.is_finite() {
                            return None;
                        }
                        out.push(value);
                    }
                    _ => return None,
                }
            }
            Some(out)
        }
        _ => None,
    }
}

fn payload_value_from_ast(
    value: &qail_core::ast::Value,
) -> Result<qail_qdrant::PayloadValue, ApiError> {
    use qail_core::ast::Value;

    match value {
        Value::Null => Ok(qail_qdrant::PayloadValue::Null),
        Value::Bool(b) => Ok(qail_qdrant::PayloadValue::Bool(*b)),
        Value::Int(n) => Ok(qail_qdrant::PayloadValue::Integer(*n)),
        Value::Float(f) if f.is_finite() => Ok(qail_qdrant::PayloadValue::Float(*f)),
        Value::Float(_) => Err(ApiError::bad_request(
            "INVALID_QDRANT_PAYLOAD",
            "Qdrant float payload values must be finite numbers",
        )),
        Value::String(s) => Ok(qail_qdrant::PayloadValue::String(s.clone())),
        Value::Uuid(u) => Ok(qail_qdrant::PayloadValue::String(u.to_string())),
        Value::Json(s) => payload_value_from_json_str(s),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(payload_value_from_ast(item)?);
            }
            Ok(qail_qdrant::PayloadValue::List(out))
        }
        _ => Err(ApiError::bad_request(
            "INVALID_QDRANT_PAYLOAD",
            "Qdrant payload values support only null, bool, number, string, UUID, JSON string, and arrays",
        )),
    }
}

fn payload_value_from_json_str(json: &str) -> Result<qail_qdrant::PayloadValue, ApiError> {
    let value = serde_json::from_str::<serde_json::Value>(json).map_err(|err| {
        ApiError::bad_request(
            "INVALID_QDRANT_PAYLOAD",
            format!("Qdrant JSON payload value is invalid: {err}"),
        )
    })?;
    Ok(payload_value_from_json(value))
}

fn payload_value_from_json(value: serde_json::Value) -> qail_qdrant::PayloadValue {
    match value {
        serde_json::Value::Null => qail_qdrant::PayloadValue::Null,
        serde_json::Value::Bool(value) => qail_qdrant::PayloadValue::Bool(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                qail_qdrant::PayloadValue::Integer(value)
            } else if let Some(value) = value.as_u64().and_then(|value| i64::try_from(value).ok()) {
                qail_qdrant::PayloadValue::Integer(value)
            } else if let Some(value) = value.as_f64() {
                qail_qdrant::PayloadValue::Float(value)
            } else {
                qail_qdrant::PayloadValue::String(value.to_string())
            }
        }
        serde_json::Value::String(value) => qail_qdrant::PayloadValue::String(value),
        serde_json::Value::Array(values) => qail_qdrant::PayloadValue::List(
            values.into_iter().map(payload_value_from_json).collect(),
        ),
        serde_json::Value::Object(values) => qail_qdrant::PayloadValue::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, payload_value_from_json(value)))
                .collect::<HashMap<_, _>>(),
        ),
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
    let mut consumed_policy_filters = vec![false; update_policy_filter_cages.len()];
    let mut request_filters = Vec::new();

    all_filter_cages.iter().for_each(|cage| {
        if let Some(policy_idx) = update_policy_filter_cages
            .iter()
            .enumerate()
            .position(|(idx, policy_cage)| !consumed_policy_filters[idx] && policy_cage == cage)
        {
            consumed_policy_filters[policy_idx] = true;
        } else {
            request_filters.push(cage.clone());
        }
    });

    request_filters
}

fn qdrant_upsert_filter_target(
    condition: &qail_core::ast::Condition,
) -> Result<QdrantUpsertFilterTarget<'_>, ApiError> {
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
    if field == "id" {
        return Ok(QdrantUpsertFilterTarget::Id);
    }
    if field == "vector" {
        return Ok(QdrantUpsertFilterTarget::Vector);
    }
    Ok(QdrantUpsertFilterTarget::Payload(field))
}

fn qdrant_upsert_filter_cages_have_enforceable_conditions(
    cages: &[qail_core::ast::Cage],
) -> Result<bool, ApiError> {
    if let Some(condition) = cages.iter().flat_map(|cage| cage.conditions.iter()).next() {
        qdrant_upsert_filter_target(condition)?;
        return Ok(true);
    }
    Ok(false)
}

fn qdrant_upsert_filter_cages_need_vectors(
    cages: &[qail_core::ast::Cage],
) -> Result<bool, ApiError> {
    for cage in cages {
        for condition in &cage.conditions {
            if qdrant_upsert_filter_target(condition)? == QdrantUpsertFilterTarget::Vector {
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
        | Value::Timestamp(_)
        | Value::Uuid(_) => Ok(()),
        Value::Json(json) => payload_value_from_json_str(json).map(|_| ()),
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
            if condition.op != Operator::Eq {
                return Err(ApiError::forbidden(
                    "Qdrant upsert filters support only equality conditions",
                ));
            }
            match qdrant_upsert_filter_target(condition)? {
                QdrantUpsertFilterTarget::Id => {
                    if point_id_from_value(&condition.value).is_none() {
                        return Err(ApiError::forbidden(
                            "Qdrant upsert id filters support only integer, string, or UUID equality values",
                        ));
                    }
                }
                QdrantUpsertFilterTarget::Vector => {
                    let Some(vector) = vector_from_value(&condition.value) else {
                        return Err(ApiError::forbidden(
                            "Qdrant upsert vector filters support only vector equality values",
                        ));
                    };
                    if vector.is_empty() {
                        return Err(ApiError::forbidden(
                            "Qdrant upsert vector filters must not be empty",
                        ));
                    }
                }
                QdrantUpsertFilterTarget::Payload(_) => {
                    validate_qdrant_upsert_filter_value(&condition.value)?;
                }
            }
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
        Value::String(expected) | Value::Timestamp(expected) => {
            matches!(actual, PayloadValue::String(actual) if actual == expected)
        }
        Value::Json(expected) => payload_value_from_json_str(expected)? == *actual,
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

fn qdrant_point_id_matches_filter_value(
    id: &qail_qdrant::PointId,
    payload: &qail_qdrant::Payload,
    expected: &qail_core::ast::Value,
) -> Result<bool, ApiError> {
    if let Some(original_id) = payload.get(ORIGINAL_POINT_ID_PAYLOAD_KEY) {
        return ast_value_matches_qdrant_payload(expected, original_id);
    }
    Ok(point_id_from_value(expected).is_some_and(|expected_id| expected_id == *id))
}

fn qdrant_vector_matches_filter_value(
    actual: Option<&[f32]>,
    expected: &qail_core::ast::Value,
) -> Result<bool, ApiError> {
    let Some(expected) = vector_from_value(expected) else {
        return Err(ApiError::forbidden(
            "Qdrant upsert vector filters support only vector equality values",
        ));
    };
    let Some(actual) = actual else {
        return Ok(false);
    };
    if actual.len() != expected.len() {
        return Ok(false);
    }
    Ok(actual
        .iter()
        .zip(expected)
        .all(|(actual, expected)| (*actual - expected).abs() <= f32::EPSILON))
}

fn qdrant_point_matches_filter_condition(
    point: QdrantUpsertPointView<'_>,
    condition: &qail_core::ast::Condition,
) -> Result<Option<bool>, ApiError> {
    use qail_core::ast::Operator;

    if condition.op != Operator::Eq {
        return Err(ApiError::forbidden(
            "Qdrant upsert filters support only equality conditions",
        ));
    }

    match qdrant_upsert_filter_target(condition)? {
        QdrantUpsertFilterTarget::Id => Ok(Some(qdrant_point_id_matches_filter_value(
            point.id,
            point.payload,
            &condition.value,
        )?)),
        QdrantUpsertFilterTarget::Vector => Ok(Some(qdrant_vector_matches_filter_value(
            point.vector,
            &condition.value,
        )?)),
        QdrantUpsertFilterTarget::Payload(field) => {
            validate_qdrant_upsert_filter_value(&condition.value)?;
            let Some(actual) = point.payload.get(field) else {
                return Ok(Some(false));
            };
            Ok(Some(ast_value_matches_qdrant_payload(
                &condition.value,
                actual,
            )?))
        }
    }
}

fn qdrant_point_matches_filter_cages(
    point: QdrantUpsertPointView<'_>,
    cages: &[qail_core::ast::Cage],
) -> Result<bool, ApiError> {
    use qail_core::ast::LogicalOp;

    for cage in cages {
        match cage.logical_op {
            LogicalOp::And => {
                for condition in &cage.conditions {
                    if qdrant_point_matches_filter_condition(point, condition)?
                        .is_some_and(|matches| !matches)
                    {
                        return Ok(false);
                    }
                }
            }
            LogicalOp::Or => {
                if cage.conditions.is_empty() {
                    continue;
                }
                let mut cage_matches = false;
                for condition in &cage.conditions {
                    if let Some(matches) = qdrant_point_matches_filter_condition(point, condition)?
                        && matches
                    {
                        cage_matches = true;
                        break;
                    }
                }
                if !cage_matches {
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

#[cfg(test)]
fn qdrant_payload_matches_filter_cages(
    payload: &qail_qdrant::Payload,
    cages: &[qail_core::ast::Cage],
) -> Result<bool, ApiError> {
    let fallback_id = qail_qdrant::PointId::Num(0);
    qdrant_point_matches_filter_cages(
        QdrantUpsertPointView {
            id: &fallback_id,
            vector: None,
            payload,
        },
        cages,
    )
}

fn enforce_qdrant_upsert_point_filters(
    point: QdrantUpsertPointView<'_>,
    cages: &[qail_core::ast::Cage],
    collection: &str,
    context: &str,
) -> Result<(), ApiError> {
    if qdrant_point_matches_filter_cages(point, cages)? {
        return Ok(());
    }

    tracing::warn!(
        collection = %collection,
        context = %context,
        "Qdrant upsert rejected by point filter enforcement"
    );
    Err(ApiError::forbidden(
        "Qdrant upsert violates policy filter constraints",
    ))
}

#[cfg(test)]
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
    point: &qail_qdrant::Point,
    upsert_filter_cages: &[qail_core::ast::Cage],
    create_policy_filter_cages: &[qail_core::ast::Cage],
    update_policy_filter_cages: &[qail_core::ast::Cage],
    collection: &str,
    is_create: bool,
) -> Result<(), ApiError> {
    let point = QdrantUpsertPointView {
        id: &point.id,
        vector: Some(&point.vector),
        payload: &point.payload,
    };
    enforce_qdrant_upsert_point_filters(point, upsert_filter_cages, collection, "outgoing")?;
    if is_create {
        enforce_qdrant_upsert_point_filters(
            point,
            create_policy_filter_cages,
            collection,
            "create_policy",
        )
    } else {
        enforce_qdrant_upsert_point_filters(
            point,
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
        enforce_qdrant_upsert_payload_filters, ensure_qdrant_collection_management_allowed,
        ensure_qdrant_conditions_finite, ensure_qdrant_score_threshold_finite,
        ensure_qdrant_vector_finite, extract_upsert_point,
        extract_upsert_point_with_filter_fallback, prepare_tenant_scoped_qdrant_upsert_point,
        qdrant_limit_from_cmd, qdrant_payload_matches_filter_cages, qdrant_point_id_to_json,
        qdrant_request_filter_cages, qdrant_scroll_limit_from_cmd, qdrant_scroll_metadata,
        qdrant_scroll_offset_from_cmd, qdrant_upsert_filter_cages, scored_point_to_json,
        split_filter_conditions, tenant_scoped_qdrant_point_id, validate_qdrant_read_filters,
        verify_existing_qdrant_points_tenant_boundary,
    };
    use crate::auth::AuthContext;
    use qail_core::ast::{
        Action, Cage, CageKind, Condition, Expr, LogicalOp, Operator, Qail, Value,
    };

    fn cond(name: &str, value: &str) -> Condition {
        value_cond(name, Value::String(value.to_string()))
    }

    fn value_cond(name: &str, value: Value) -> Condition {
        Condition {
            left: Expr::Named(name.to_string()),
            op: Operator::Eq,
            value,
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
    fn tenant_scoped_qdrant_upsert_overwrites_client_original_id_payload() {
        let mut point = qail_qdrant::Point::new_num(7, vec![0.1, 0.2]);
        point.payload.insert(
            ORIGINAL_POINT_ID_PAYLOAD_KEY.to_string(),
            qail_qdrant::PayloadValue::String("victim-id".to_string()),
        );

        prepare_tenant_scoped_qdrant_upsert_point(&mut point, "tenant-a");

        assert_eq!(
            point.payload.get(ORIGINAL_POINT_ID_PAYLOAD_KEY),
            Some(&qail_qdrant::PayloadValue::Integer(7))
        );
    }

    #[test]
    fn qdrant_limit_is_clamped_to_gateway_max_rows() {
        let cmd = Qail::search("embeddings")
            .vector(vec![0.1, 0.2])
            .limit(50_000);

        assert_eq!(qdrant_limit_from_cmd(&cmd, 1_000).unwrap(), 1_000);
    }

    #[test]
    fn qdrant_limit_defaults_to_ten_within_gateway_max_rows() {
        let cmd = Qail::scroll("embeddings");

        assert_eq!(qdrant_limit_from_cmd(&cmd, 1_000).unwrap(), 10);
    }

    #[test]
    fn qdrant_limit_rejects_non_positive_values() {
        let cmd = Qail::scroll("embeddings").limit(-1);

        let err = qdrant_limit_from_cmd(&cmd, 1_000).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn qdrant_scroll_limit_caps_at_protocol_max_without_wrapping() {
        let requested = i64::from(u32::MAX) + 100;
        let max_rows = u32::MAX as usize + 100;
        let cmd = Qail::scroll("embeddings").limit(requested);

        assert_eq!(
            qdrant_scroll_limit_from_cmd(&cmd, max_rows).unwrap(),
            u32::MAX
        );
    }

    #[test]
    fn qdrant_scroll_offset_uses_numeric_point_offset() {
        let cmd = Qail::scroll("embeddings").offset(42);

        let offset = qdrant_scroll_offset_from_cmd(&cmd)
            .expect("scroll offset should parse")
            .expect("offset should be present");

        assert_eq!(offset, qail_qdrant::PointId::Num(42));
    }

    #[test]
    fn qdrant_scroll_metadata_includes_next_page_offset() {
        let metadata = qdrant_scroll_metadata(Some(&qail_qdrant::PointId::Uuid(
            "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa".to_string(),
        )))
        .expect("metadata should be present when Qdrant returns a next offset");

        assert_eq!(
            metadata.next_page_offset.as_ref(),
            Some(&serde_json::json!("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa"))
        );
    }

    #[test]
    fn qdrant_point_id_to_json_preserves_numeric_offsets() {
        assert_eq!(
            qdrant_point_id_to_json(&qail_qdrant::PointId::Num(7)),
            serde_json::json!(7)
        );
    }

    #[test]
    fn qdrant_collection_management_requires_platform_admin() {
        let tenant_admin = AuthContext {
            user_id: "admin-tenant".to_string(),
            role: "administrator".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            claims: std::collections::HashMap::from([(
                "platform_admin".to_string(),
                serde_json::json!(true),
            )]),
        };

        let err =
            ensure_qdrant_collection_management_allowed(&tenant_admin, &Action::DeleteCollection)
                .unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);

        let platform_admin = AuthContext {
            user_id: "admin-platform".to_string(),
            role: "administrator".to_string(),
            tenant_id: None,
            claims: std::collections::HashMap::from([(
                "platform_admin".to_string(),
                serde_json::json!(true),
            )]),
        };
        assert!(
            ensure_qdrant_collection_management_allowed(&platform_admin, &Action::CreateCollection)
                .is_ok()
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
    fn extract_upsert_point_preserves_json_payload_objects() {
        let cmd = Qail::upsert("embeddings")
            .set_value("id", 7)
            .set_value("vector", Value::Vector(vec![0.1, 0.2]))
            .set_value(
                "metadata",
                Value::Json(r#"{"source":"web","score":3,"flags":["hot",true]}"#.to_string()),
            );

        let point = extract_upsert_point(&cmd).unwrap();
        let metadata = point.payload.get("metadata").expect("metadata payload");
        let qail_qdrant::PayloadValue::Object(metadata) = metadata else {
            panic!("metadata should remain a nested Qdrant object: {metadata:?}");
        };

        assert_eq!(
            metadata.get("source"),
            Some(&qail_qdrant::PayloadValue::String("web".to_string()))
        );
        assert_eq!(
            metadata.get("score"),
            Some(&qail_qdrant::PayloadValue::Integer(3))
        );
        assert!(matches!(
            metadata.get("flags"),
            Some(qail_qdrant::PayloadValue::List(items))
                if items
                    == &vec![
                        qail_qdrant::PayloadValue::String("hot".to_string()),
                        qail_qdrant::PayloadValue::Bool(true),
                    ]
        ));
    }

    #[test]
    fn qdrant_json_payload_filter_matches_nested_object() {
        use std::collections::HashMap;

        let mut metadata = HashMap::new();
        metadata.insert(
            "source".to_string(),
            qail_qdrant::PayloadValue::String("web".to_string()),
        );
        metadata.insert("score".to_string(), qail_qdrant::PayloadValue::Integer(3));

        let mut payload = qail_qdrant::Payload::new();
        payload.insert(
            "metadata".to_string(),
            qail_qdrant::PayloadValue::Object(metadata),
        );
        let cages = vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![value_cond(
                "metadata",
                Value::Json(r#"{"source":"web","score":3}"#.to_string()),
            )],
            logical_op: LogicalOp::And,
        }];

        enforce_qdrant_upsert_payload_filters(&payload, &cages, "embeddings", "outgoing").unwrap();
    }

    #[test]
    fn extract_upsert_point_rejects_invalid_json_payload() {
        let cmd = Qail::upsert("embeddings")
            .set_value("id", 7)
            .set_value("vector", Value::Vector(vec![0.1, 0.2]))
            .set_value("metadata", Value::Json("{bad".to_string()));

        let err = extract_upsert_point(&cmd).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "INVALID_QDRANT_PAYLOAD");
    }

    #[test]
    fn extract_upsert_point_uses_request_filter_id_only_as_fallback() {
        let cmd = Qail::upsert("embeddings")
            .set_value("vector", Value::Vector(vec![0.1, 0.2]))
            .filter("id", Operator::Eq, 7);
        let request_filters = qdrant_upsert_filter_cages(&cmd);

        let point = extract_upsert_point_with_filter_fallback(&cmd, &request_filters).unwrap();

        assert_eq!(point.id, qail_qdrant::PointId::Num(7));
    }

    #[test]
    fn extract_upsert_point_rejects_ambiguous_filter_fallback_ids() {
        let cmd = Qail {
            action: Action::Upsert,
            table: "embeddings".to_string(),
            vector: Some(vec![0.1, 0.2]),
            cages: vec![Cage {
                kind: CageKind::Filter,
                conditions: vec![
                    value_cond("id", Value::Int(7)),
                    value_cond("id", Value::Int(8)),
                ],
                logical_op: LogicalOp::Or,
            }],
            ..Default::default()
        };
        let request_filters = qdrant_upsert_filter_cages(&cmd);

        let err = extract_upsert_point_with_filter_fallback(&cmd, &request_filters).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "AMBIGUOUS_POINT_ID");
    }

    #[test]
    fn extract_upsert_point_rejects_or_filter_id_fallback() {
        let cmd = Qail {
            action: Action::Upsert,
            table: "embeddings".to_string(),
            vector: Some(vec![0.1, 0.2]),
            cages: vec![Cage {
                kind: CageKind::Filter,
                conditions: vec![value_cond("id", Value::Int(7)), cond("region", "west")],
                logical_op: LogicalOp::Or,
            }],
            ..Default::default()
        };
        let request_filters = qdrant_upsert_filter_cages(&cmd);

        let err = extract_upsert_point_with_filter_fallback(&cmd, &request_filters).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "AMBIGUOUS_POINT_ID");
    }

    #[test]
    fn extract_upsert_point_rejects_filter_id_that_conflicts_with_payload_id() {
        let cmd = Qail::upsert("embeddings")
            .set_value("id", 7)
            .set_value("vector", Value::Vector(vec![0.1, 0.2]))
            .filter("id", Operator::Eq, 8);
        let request_filters = qdrant_upsert_filter_cages(&cmd);

        let err = extract_upsert_point_with_filter_fallback(&cmd, &request_filters).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "AMBIGUOUS_POINT_ID");
    }

    #[test]
    fn extract_upsert_point_rejects_ambiguous_filter_fallback_vectors() {
        let cmd = Qail {
            action: Action::Upsert,
            table: "embeddings".to_string(),
            cages: vec![
                Cage {
                    kind: CageKind::Payload,
                    conditions: vec![value_cond("id", Value::Int(7))],
                    logical_op: LogicalOp::And,
                },
                Cage {
                    kind: CageKind::Filter,
                    conditions: vec![
                        value_cond("vector", Value::Vector(vec![0.1, 0.2])),
                        value_cond("vector", Value::Vector(vec![0.3, 0.4])),
                    ],
                    logical_op: LogicalOp::Or,
                },
            ],
            ..Default::default()
        };
        let request_filters = qdrant_upsert_filter_cages(&cmd);

        let err = extract_upsert_point_with_filter_fallback(&cmd, &request_filters).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "AMBIGUOUS_VECTOR");
    }

    #[test]
    fn extract_upsert_point_does_not_let_policy_filter_retarget_payload_id() {
        let cmd = Qail::upsert("embeddings")
            .set_value("id", "client-id")
            .set_value("vector", Value::Vector(vec![0.1, 0.2]))
            .filter("id", Operator::Eq, "policy-id");
        let all_filters = qdrant_upsert_filter_cages(&cmd);
        let request_filters = qdrant_request_filter_cages(&all_filters, &all_filters);

        let point = extract_upsert_point_with_filter_fallback(&cmd, &request_filters).unwrap();

        assert_eq!(
            point.id,
            qail_qdrant::PointId::Uuid("client-id".to_string())
        );
    }

    #[test]
    fn extract_upsert_point_rejects_conflicting_payload_ids() {
        let cmd = Qail::upsert("embeddings")
            .set_value("id", 7)
            .set_value("id", 8)
            .set_value("vector", Value::Vector(vec![0.1, 0.2]));

        let err = extract_upsert_point(&cmd).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "AMBIGUOUS_POINT_ID");
    }

    #[test]
    fn extract_upsert_point_drops_reserved_original_id_payload() {
        let cmd = Qail::upsert("embeddings")
            .set_value("id", 7)
            .set_value("vector", Value::Vector(vec![0.1, 0.2]))
            .set_value(ORIGINAL_POINT_ID_PAYLOAD_KEY, "victim-id");

        let point = extract_upsert_point(&cmd).unwrap();

        assert!(!point.payload.contains_key(ORIGINAL_POINT_ID_PAYLOAD_KEY));
    }

    #[test]
    fn extract_upsert_point_rejects_unsupported_payload_values() {
        let cmd = Qail::upsert("embeddings")
            .set_value("id", 7)
            .set_value("vector", Value::Vector(vec![0.1, 0.2]))
            .set_value("blob", Value::Bytes(vec![1, 2, 3]));

        let err = extract_upsert_point(&cmd).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn qdrant_gateway_rejects_non_finite_vectors_and_thresholds() {
        let err = ensure_qdrant_vector_finite(&[0.1, f32::NAN]).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);

        let err = ensure_qdrant_score_threshold_finite(Some(f32::INFINITY)).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);

        let conditions = vec![Condition {
            left: Expr::Named("score".to_string()),
            op: Operator::Gt,
            value: Value::Float(f64::NEG_INFINITY),
            is_array_unnest: false,
        }];
        let err = ensure_qdrant_conditions_finite(&conditions).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn qdrant_read_filters_accept_encoder_supported_matrix() {
        let conditions = vec![
            Condition {
                left: Expr::Named("status".to_string()),
                op: Operator::Eq,
                value: Value::String("open".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("priority".to_string()),
                op: Operator::Gte,
                value: Value::Int(3),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("summary".to_string()),
                op: Operator::Contains,
                value: Value::String("refund".to_string()),
                is_array_unnest: false,
            },
        ];
        let groups = vec![vec![
            Condition {
                left: Expr::Named("archived".to_string()),
                op: Operator::Eq,
                value: Value::Bool(false),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("deleted_at".to_string()),
                op: Operator::IsNull,
                value: Value::Null,
                is_array_unnest: false,
            },
        ]];

        validate_qdrant_read_filters(&conditions, &groups).unwrap();
    }

    #[test]
    fn qdrant_read_filters_reject_unsupported_in_before_driver() {
        let conditions = vec![Condition {
            left: Expr::Named("status".to_string()),
            op: Operator::In,
            value: Value::Array(vec![Value::String("open".to_string())]),
            is_array_unnest: false,
        }];

        let err = validate_qdrant_read_filters(&conditions, &[]).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "INVALID_QDRANT_FILTER");
    }

    #[test]
    fn qdrant_read_filters_reject_is_not_null_until_transport_supports_it() {
        let conditions = vec![Condition {
            left: Expr::Named("deleted_at".to_string()),
            op: Operator::IsNotNull,
            value: Value::Null,
            is_array_unnest: false,
        }];

        let err = validate_qdrant_read_filters(&conditions, &[]).unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(err.code, "INVALID_QDRANT_FILTER");
    }

    #[test]
    fn extract_upsert_point_rejects_non_finite_numbers() {
        let bad_vector = Qail::upsert("embeddings")
            .set_value("id", 7)
            .set_value("vector", Value::Vector(vec![0.1, f32::NAN]));
        let err = extract_upsert_point(&bad_vector).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);

        let bad_payload = Qail::upsert("embeddings")
            .set_value("id", 7)
            .set_value("vector", Value::Vector(vec![0.1, 0.2]))
            .set_value("rank", Value::Float(f64::INFINITY));
        let err = extract_upsert_point(&bad_payload).unwrap_err();
        assert_eq!(err.status_code(), axum::http::StatusCode::BAD_REQUEST);
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
    fn qdrant_payload_filter_requires_each_or_cage_to_match() {
        let mut payload = qail_qdrant::Payload::new();
        payload.insert(
            "city".to_string(),
            qail_qdrant::PayloadValue::String("London".to_string()),
        );
        payload.insert(
            "country".to_string(),
            qail_qdrant::PayloadValue::String("DE".to_string()),
        );
        let cages = vec![
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
        ];

        let err = enforce_qdrant_upsert_payload_filters(&payload, &cages, "embeddings", "outgoing")
            .unwrap_err();

        assert_eq!(err.status_code(), axum::http::StatusCode::FORBIDDEN);
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
        let point = qail_qdrant::Point {
            id: qail_qdrant::PointId::Num(7),
            vector: vec![0.1, 0.2],
            payload,
        };
        let upsert_filters = vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![cond("region", "west")],
            logical_op: LogicalOp::And,
        }];

        let err = enforce_qdrant_upsert_outgoing_filters(
            &point,
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
    fn qdrant_upsert_create_rejects_outgoing_id_that_violates_request_filter() {
        let point = qail_qdrant::Point::new_num(8, vec![0.1, 0.2]);
        let upsert_filters = vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![value_cond("id", Value::Int(7))],
            logical_op: LogicalOp::And,
        }];

        let err = enforce_qdrant_upsert_outgoing_filters(
            &point,
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
    fn qdrant_upsert_id_filter_honors_tenant_original_point_id() {
        let mut point = qail_qdrant::Point::new_num(7, vec![0.1, 0.2]);
        prepare_tenant_scoped_qdrant_upsert_point(&mut point, "tenant-a");
        let upsert_filters = vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![value_cond("id", Value::Int(7))],
            logical_op: LogicalOp::And,
        }];

        enforce_qdrant_upsert_outgoing_filters(
            &point,
            &upsert_filters,
            &[],
            &[],
            "embeddings",
            true,
        )
        .unwrap();
    }

    #[test]
    fn qdrant_upsert_create_rejects_outgoing_vector_that_violates_policy_filter() {
        let point = qail_qdrant::Point::new_num(7, vec![0.9, 0.8]);
        let create_policy_filters = vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![value_cond("vector", Value::Vector(vec![0.1, 0.2]))],
            logical_op: LogicalOp::And,
        }];

        let err = enforce_qdrant_upsert_outgoing_filters(
            &point,
            &[],
            &create_policy_filters,
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
        let point = qail_qdrant::Point {
            id: qail_qdrant::PointId::Num(7),
            vector: vec![0.1, 0.2],
            payload,
        };
        let update_policy_filters = vec![Cage {
            kind: CageKind::Filter,
            conditions: vec![cond("operator_id", "operator-1")],
            logical_op: LogicalOp::And,
        }];

        let err = enforce_qdrant_upsert_outgoing_filters(
            &point,
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
    fn qdrant_request_filter_cages_preserve_user_duplicate_of_policy_filter() {
        let user_filter = Cage {
            kind: CageKind::Filter,
            conditions: vec![cond("operator_id", "operator-1")],
            logical_op: LogicalOp::And,
        };
        let update_policy_filter = user_filter.clone();
        let all_filters = vec![user_filter.clone(), update_policy_filter.clone()];

        let request_filters = qdrant_request_filter_cages(&all_filters, &[update_policy_filter]);

        assert_eq!(
            request_filters,
            vec![user_filter],
            "subtract only the policy-injected copy, not every equal user filter"
        );
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
