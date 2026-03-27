use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::handler::row_to_json;

/// POST /api/_branch — Create a new branch.
pub(crate) async fn branch_create_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let auth = match authenticate_request(state.as_ref(), &headers).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };
    if !auth.is_authenticated() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Authentication required"})),
        )
            .into_response();
    }
    if !auth.can_use_branching() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Platform administrator role required for branch operations"})),
        )
            .into_response();
    }

    let name = match body.get("name").and_then(|v| v.as_str()) {
        Some(n) => n,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Missing 'name' field"})),
            )
                .into_response();
        }
    };

    let parent = body.get("parent").and_then(|v| v.as_str());

    let mut conn = match state.acquire_with_auth_rls_guarded(&auth, None).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    let ddl = qail_pg::driver::branch_sql::create_branch_tables_sql();
    if let Ok(pg_conn) = conn.get_mut()
        && let Err(e) = pg_conn.execute_simple(ddl).await
    {
        tracing::warn!("Branch DDL bootstrap (may already exist): {}", e);
    }

    let sql = qail_pg::driver::branch_sql::create_branch_sql(name, parent);
    let result = match conn.get_mut() {
        Ok(pg_conn) => match pg_conn.execute_simple(&sql).await {
            Ok(_) => (
                StatusCode::CREATED,
                Json(json!({"branch": name, "status": "created"})),
            )
                .into_response(),
            Err(e) => {
                tracing::error!("Failed to create branch '{}': {}", name, e);
                (
                    StatusCode::CONFLICT,
                    Json(json!({"error": "Failed to create branch (may already exist)"})),
                )
                    .into_response()
            }
        },
        Err(e) => {
            tracing::error!("Branch connection released unexpectedly: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Database connection unavailable"})),
            )
                .into_response()
        }
    };
    conn.release().await;
    result
}

/// GET /api/_branch — List all branches.
pub(crate) async fn branch_list_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let auth = match authenticate_request(state.as_ref(), &headers).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };
    if !auth.is_authenticated() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Authentication required"})),
        )
            .into_response();
    }
    if !auth.can_use_branching() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Platform administrator role required for branch operations"})),
        )
            .into_response();
    }

    let mut conn = match state.acquire_with_auth_rls_guarded(&auth, None).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    let sql = qail_pg::driver::branch_sql::list_branches_sql();
    let result = match conn.get_mut() {
        Ok(pg_conn) => match pg_conn.simple_query(sql).await {
            Ok(rows) => {
                let branches: Vec<Value> = rows.iter().map(row_to_json).collect();
                Json(json!({"branches": branches})).into_response()
            }
            Err(_) => Json(json!({"branches": []})).into_response(),
        },
        Err(e) => {
            tracing::error!("Branch connection released unexpectedly: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Database connection unavailable"})),
            )
                .into_response()
        }
    };
    conn.release().await;
    result
}

/// DELETE /api/_branch/:name — Soft-delete a branch.
pub(crate) async fn branch_delete_handler(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    let auth = match authenticate_request(state.as_ref(), &headers).await {
        Ok(auth) => auth,
        Err(e) => return e.into_response(),
    };
    if !auth.is_authenticated() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Authentication required"})),
        )
            .into_response();
    }
    if !auth.can_use_branching() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Platform administrator role required for branch operations"})),
        )
            .into_response();
    }

    let mut conn = match state.acquire_with_auth_rls_guarded(&auth, None).await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    let sql = qail_pg::driver::branch_sql::delete_branch_sql(&name);
    let result = match conn.get_mut() {
        Ok(pg_conn) => match pg_conn.execute_simple(&sql).await {
            Ok(_) => Json(json!({"branch": name, "status": "deleted"})).into_response(),
            Err(e) => {
                tracing::error!("Failed to delete branch '{}': {}", name, e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "Failed to delete branch"})),
                )
                    .into_response()
            }
        },
        Err(e) => {
            tracing::error!("Branch connection released unexpectedly: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "Database connection unavailable"})),
            )
                .into_response()
        }
    };
    conn.release().await;
    result
}
