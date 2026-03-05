use axum::{extract::State, response::Json};
use serde_json::{Value, json};
use std::sync::Arc;

use crate::GatewayState;
use crate::auth::authenticate_request;
use crate::middleware::ApiError;

use super::types::pg_type_to_openapi;

/// GET /api/_openapi — Auto-generated OpenAPI 3.0.3 spec
///
/// Generates a complete OpenAPI specification from the schema registry.
pub(crate) async fn openapi_spec_handler(
    headers: axum::http::HeaderMap,
    State(state): State<Arc<GatewayState>>,
) -> Result<Json<Value>, ApiError> {
    let auth = authenticate_request(state.as_ref(), &headers).await?;
    if !auth.is_authenticated() {
        return Err(ApiError::auth_error(
            "Authentication required for OpenAPI spec",
        ));
    }
    let tables = state.schema.tables();
    let mut paths = serde_json::Map::new();
    let mut schemas = serde_json::Map::new();

    for (name, table) in tables {
        let mut properties = serde_json::Map::new();
        let mut required_cols = Vec::new();

        for col in &table.columns {
            let oas_type = pg_type_to_openapi(&col.pg_type);
            properties.insert(col.name.clone(), json!(oas_type));
            if !col.nullable && !col.has_default {
                required_cols.push(Value::String(col.name.clone()));
            }
        }

        schemas.insert(
            name.clone(),
            json!({
                "type": "object",
                "properties": properties,
                "required": required_cols,
            }),
        );

        let list_path = format!("/api/{}", name);
        paths.insert(list_path, json!({
            "get": {
                "summary": format!("List {}", name),
                "tags": [name],
                "parameters": [
                    {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 50}},
                    {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0}},
                    {"name": "sort", "in": "query", "schema": {"type": "string"}, "description": "col:asc,col:desc"},
                    {"name": "select", "in": "query", "schema": {"type": "string"}, "description": "col1,col2"},
                    {"name": "expand", "in": "query", "schema": {"type": "string"}, "description": "FK relation to expand"},
                    {"name": "distinct", "in": "query", "schema": {"type": "string"}, "description": "col1,col2"},
                ],
                "responses": {
                    "200": {
                        "description": "Success",
                        "content": {"application/json": {"schema": {
                            "type": "object",
                            "properties": {
                                "data": {"type": "array", "items": {"$ref": format!("#/components/schemas/{}", name)}},
                                "count": {"type": "integer"},
                                "limit": {"type": "integer"},
                                "offset": {"type": "integer"},
                            }
                        }}}
                    },
                    "503": {"$ref": "#/components/responses/PoolBackpressure"}
                }
            },
            "post": {
                "summary": format!("Create {}", name),
                "tags": [name],
                "requestBody": {
                    "content": {"application/json": {"schema": {"$ref": format!("#/components/schemas/{}", name)}}}
                },
                "parameters": [
                    {"name": "returning", "in": "query", "schema": {"type": "string"}, "description": "* or col1,col2"},
                    {"name": "on_conflict", "in": "query", "schema": {"type": "string"}, "description": "Upsert conflict column"},
                ],
                "responses": {
                    "201": {"description": "Created"},
                    "503": {"$ref": "#/components/responses/PoolBackpressure"},
                }
            }
        }));

        if let Some(ref pk) = table.primary_key {
            let id_path = format!("/api/{}/{{{}}}", name, pk);
            paths.insert(id_path, json!({
                "get": {
                    "summary": format!("Get {} by {}", name, pk),
                    "tags": [name],
                    "parameters": [
                        {"name": pk, "in": "path", "required": true, "schema": {"type": "string"}}
                    ],
                    "responses": {
                        "200": {"description": "Success"},
                        "503": {"$ref": "#/components/responses/PoolBackpressure"}
                    }
                },
                "patch": {
                    "summary": format!("Update {} by {}", name, pk),
                    "tags": [name],
                    "parameters": [
                        {"name": pk, "in": "path", "required": true, "schema": {"type": "string"}}
                    ],
                    "requestBody": {
                        "content": {"application/json": {"schema": {"$ref": format!("#/components/schemas/{}", name)}}}
                    },
                    "responses": {
                        "200": {"description": "Updated"},
                        "503": {"$ref": "#/components/responses/PoolBackpressure"}
                    }
                },
                "delete": {
                    "summary": format!("Delete {} by {}", name, pk),
                    "tags": [name],
                    "parameters": [
                        {"name": pk, "in": "path", "required": true, "schema": {"type": "string"}}
                    ],
                    "responses": {
                        "204": {"description": "Deleted"},
                        "503": {"$ref": "#/components/responses/PoolBackpressure"}
                    }
                }
            }));
        }
    }

    paths.insert(
        "/api/rpc/{function}".to_string(),
        json!({
            "post": {
                "summary": "Invoke function (RPC)",
                "tags": ["rpc"],
                "parameters": [
                    {
                        "name": "function",
                        "in": "path",
                        "required": true,
                        "schema": {"type": "string"},
                        "description": "Function name ([schema.]function). Enforce schema-qualified names via gateway config."
                    },
                    {
                        "name": "x-qail-result-format",
                        "in": "header",
                        "required": false,
                        "schema": {"type": "string", "enum": ["text", "binary"]},
                        "description": "Optional result wire format. `binary` reduces text decode overhead."
                    }
                ],
                "requestBody": {
                    "required": false,
                    "content": {
                        "application/json": {
                            "schema": {
                                "oneOf": [
                                    {"type": "object"},
                                    {"type": "array"},
                                    {"type": "string"},
                                    {"type": "number"},
                                    {"type": "boolean"},
                                    {"type": "null"}
                                ]
                            }
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "Function result rows",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": {"type": "array", "items": {"type": "object"}},
                                        "count": {"type": "integer"},
                                        "function": {"type": "string"},
                                        "result_format": {"type": "string", "enum": ["text", "binary"]}
                                    }
                                }
                            }
                        }
                    },
                    "400": {"description": "Invalid arguments or ambiguous overload"},
                    "403": {"description": "Blocked by policy or RPC allow-list"},
                    "503": {"$ref": "#/components/responses/PoolBackpressure"}
                }
            }
        }),
    );

    paths.insert(
        "/api/_rpc/contracts".to_string(),
        json!({
            "get": {
                "summary": "List callable RPC function contracts",
                "tags": ["rpc", "devex"],
                "responses": {
                    "200": {"description": "Success"},
                    "401": {"description": "Authentication required"},
                    "503": {"$ref": "#/components/responses/PoolBackpressure"}
                }
            }
        }),
    );

    Ok(Json(json!({
        "openapi": "3.0.3",
        "info": {
            "title": "QAIL Gateway API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Auto-generated REST API from QAIL schema. Under database acquire pressure, endpoints may return 503 POOL_BACKPRESSURE with Retry-After + x-qail-backpressure-* headers; clients should retry with exponential backoff and jitter."
        },
        "paths": paths,
        "components": {
            "schemas": schemas,
            "headers": {
                "RetryAfter": {
                    "description": "Minimum wait (seconds) before retrying this request.",
                    "schema": {"type": "integer", "minimum": 0},
                    "example": 1
                },
                "XQailBackpressureScope": {
                    "description": "Where shedding occurred.",
                    "schema": {"type": "string", "enum": ["global", "tenant", "tenant_map", "unknown"]},
                    "example": "tenant"
                },
                "XQailBackpressureReason": {
                    "description": "Stable machine-readable reason for 503 shedding.",
                    "schema": {"type": "string", "enum": ["global_waiters_exceeded", "tenant_waiters_exceeded", "tenant_tracker_saturated", "queue_saturated"]},
                    "example": "tenant_waiters_exceeded"
                }
            },
            "responses": {
                "PoolBackpressure": {
                    "description": "Database acquire queue is saturated. Respect Retry-After and retry with exponential backoff + jitter (full-jitter recommended).",
                    "headers": {
                        "Retry-After": {"$ref": "#/components/headers/RetryAfter"},
                        "X-Qail-Backpressure-Scope": {"$ref": "#/components/headers/XQailBackpressureScope"},
                        "X-Qail-Backpressure-Reason": {"$ref": "#/components/headers/XQailBackpressureReason"}
                    },
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "code": {"type": "string", "example": "POOL_BACKPRESSURE"},
                                    "message": {"type": "string", "example": "Tenant database acquire queue is saturated"}
                                },
                                "required": ["code", "message"]
                            }
                        }
                    }
                }
            },
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT"
                }
            }
        },
        "security": [{"bearerAuth": []}],
    })))
}
