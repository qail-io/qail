use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};

use crate::GatewayState;

use super::{branch, devex, explain, handlers, nested};

pub(super) fn auto_rest_routes(state: Arc<GatewayState>) -> Router<Arc<GatewayState>> {
    let mut router: Router<Arc<GatewayState>> = Router::new();
    let table_names: Vec<String> = state
        .schema
        .table_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    // ── Manifest collection ─────────────────────────────────────────
    let mut manifest_tables: Vec<serde_json::Value> = Vec::new();

    for table_name in &table_names {
        // SECURITY: Check table accessibility (allowlist takes precedence over blocklist)
        if !state.allowed_tables.is_empty() {
            // Allowlist mode: only allow listed tables
            if !state.allowed_tables.contains(table_name as &str) {
                tracing::info!(
                    "  AUTO-REST: {} → BLOCKED (not in allowed_tables)",
                    table_name
                );
                manifest_tables.push(serde_json::json!({
                    "table": table_name,
                    "status": "BLOCKED",
                    "reason": "not in allowed_tables",
                    "endpoints": [],
                }));
                continue;
            }
        } else if state.blocked_tables.contains(table_name as &str) {
            // Blocklist mode: block listed tables
            tracing::info!("  AUTO-REST: {} → BLOCKED (in blocked_tables)", table_name);
            manifest_tables.push(serde_json::json!({
                "table": table_name,
                "status": "BLOCKED",
                "reason": "blocked_tables",
                "endpoints": [],
            }));
            continue;
        }

        let table = match state.schema.table(table_name) {
            Some(t) => t,
            None => continue,
        };
        let has_pk = table.primary_key.is_some();
        let path = format!("/api/{}", table_name);

        tracing::info!(
            "  AUTO-REST: {} → GET/POST{}",
            path,
            if has_pk {
                " + GET/PATCH/DELETE :id"
            } else {
                ""
            }
        );

        // Collect endpoints for this table
        let mut endpoints: Vec<serde_json::Value> = vec![
            serde_json::json!({ "path": &path, "methods": ["GET", "POST"] }),
            serde_json::json!({ "path": format!("{}/aggregate", path), "methods": ["GET"] }),
            serde_json::json!({ "path": format!("{}/_explain", path), "methods": ["GET"] }),
        ];

        // GET /api/{table} — list
        // POST /api/{table} — create
        router = router.route(
            &path,
            get(handlers::list_handler).post(handlers::create_handler),
        );

        // GET /api/{table}/aggregate — aggregation
        let agg_path = format!("/api/{}/aggregate", table_name);

        // GET /api/{table}/_explain — explain query plan
        let explain_path = format!("/api/{}/_explain", table_name);
        router = router.route(&explain_path, get(explain::explain_handler));
        router = router.route(&agg_path, get(handlers::aggregate_handler));

        if has_pk {
            let id_path = format!("/api/{}/{{id}}", table_name);
            endpoints.push(serde_json::json!({
                "path": format!("/api/{}/:id", table_name),
                "methods": ["GET", "PATCH", "DELETE"],
            }));

            // GET /api/{table}/:id — get by PK
            // PATCH /api/{table}/:id — update
            // DELETE /api/{table}/:id — delete
            router = router.route(
                &id_path,
                get(handlers::get_by_id_handler)
                    .patch(handlers::update_handler)
                    .delete(handlers::delete_handler),
            );

            // Nested routes: GET /api/{parent}/:id/{child}
            let children = state.schema.children_of(table_name);
            let mut seen_nested = std::collections::HashSet::new();
            for (child_table, _fk_col, _pk_col) in &children {
                // SECURITY: Skip nested routes where child table is not accessible
                let child_blocked = if !state.allowed_tables.is_empty() {
                    !state.allowed_tables.contains(child_table as &str)
                } else {
                    state.blocked_tables.contains(child_table as &str)
                };
                if child_blocked {
                    let reason = if !state.allowed_tables.is_empty() {
                        "child not in allowed_tables"
                    } else {
                        "child in blocked_tables"
                    };
                    tracing::info!(
                        "  AUTO-REST nested: /api/{}/{{id}}/{} → BLOCKED ({})",
                        table_name,
                        child_table,
                        reason
                    );
                    endpoints.push(serde_json::json!({
                        "path": format!("/api/{}/:id/{}", table_name, child_table),
                        "methods": ["GET"],
                        "status": "BLOCKED",
                        "reason": reason,
                    }));
                    continue;
                }
                let nested_path = format!("/api/{}/{{id}}/{}", table_name, child_table);
                if !seen_nested.insert(nested_path.clone()) {
                    tracing::debug!(
                        "  AUTO-REST nested: {} → skipped (duplicate FK)",
                        nested_path
                    );
                    continue;
                }
                tracing::info!("  AUTO-REST nested: {} → GET", nested_path);
                endpoints.push(serde_json::json!({
                    "path": format!("/api/{}/:id/{}", table_name, child_table),
                    "methods": ["GET"],
                }));
                router = router.route(&nested_path, get(nested::nested_list_handler));
            }
        }

        manifest_tables.push(serde_json::json!({
            "table": table_name,
            "status": "ALLOWED",
            "has_pk": has_pk,
            "endpoints": endpoints,
        }));
    }

    // DevEx endpoints
    router = router
        // POST /api/rpc/{function} — function-as-RPC
        .route("/api/rpc/{function}", post(handlers::rpc_handler))
        // GET /api/_schema — Schema introspection
        .route("/api/_schema", get(devex::schema_introspection_handler))
        // GET /api/_schema/typescript — TypeScript interfaces from schema
        .route(
            "/api/_schema/typescript",
            get(devex::typescript_types_handler),
        )
        // GET /api/_rpc/contracts — RPC function signature contracts
        .route("/api/_rpc/contracts", get(devex::rpc_contracts_handler))
        // GET /api/_openapi — Auto-generated OpenAPI 3.0 spec
        .route("/api/_openapi", get(devex::openapi_spec_handler));

    // Branch management endpoints
    router = router
        .route(
            "/api/_branch",
            axum::routing::post(branch::branch_create_handler).get(branch::branch_list_handler),
        )
        .route(
            "/api/_branch/{name}",
            axum::routing::delete(branch::branch_delete_handler),
        )
        .route(
            "/api/_branch/{name}/merge",
            axum::routing::post(branch::branch_merge_handler),
        );

    tracing::info!("  RPC: POST /api/rpc/:function → invoke database function");
    tracing::info!("  DEVEX: GET /api/_schema → Schema introspection");
    tracing::info!("  DEVEX: GET /api/_schema/typescript → TypeScript interfaces");
    tracing::info!("  DEVEX: GET /api/_rpc/contracts → RPC contracts");
    tracing::info!("  DEVEX: GET /api/_openapi → OpenAPI 3.0 spec");
    tracing::info!(
        "  BRANCH: POST/GET /api/_branch, DELETE /api/_branch/:name, POST /api/_branch/:name/merge"
    );

    // ── Write REST manifest to disk ─────────────────────────────────
    let allowed = manifest_tables
        .iter()
        .filter(|t| t["status"] == "ALLOWED")
        .count();
    let blocked = manifest_tables
        .iter()
        .filter(|t| t["status"] == "BLOCKED")
        .count();

    let manifest = serde_json::json!({
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "summary": {
            "total_tables": table_names.len(),
            "allowed": allowed,
            "blocked": blocked,
        },
        "system_endpoints": [
            { "path": "/api/rpc/:function", "methods": ["POST"] },
            { "path": "/api/_schema", "methods": ["GET"] },
            { "path": "/api/_schema/typescript", "methods": ["GET"] },
            { "path": "/api/_rpc/contracts", "methods": ["GET"] },
            { "path": "/api/_openapi", "methods": ["GET"] },
            { "path": "/api/_branch", "methods": ["GET", "POST"] },
            { "path": "/api/_branch/:name", "methods": ["DELETE"] },
            { "path": "/api/_branch/:name/merge", "methods": ["POST"] },
        ],
        "tables": manifest_tables,
    });

    // Write manifest to config root (alongside qail.toml, policies.yaml, etc.)
    if let Some(ref config_root) = state.config.config_root {
        let manifest_path = std::path::Path::new(config_root).join("rest_manifest.json");
        match serde_json::to_string_pretty(&manifest) {
            Ok(json) => match std::fs::write(&manifest_path, &json) {
                Ok(()) => tracing::info!(
                    "  MANIFEST: Written to {} ({} allowed, {} blocked)",
                    manifest_path.display(),
                    allowed,
                    blocked
                ),
                Err(e) => tracing::warn!(
                    "  MANIFEST: Failed to write {}: {}",
                    manifest_path.display(),
                    e
                ),
            },
            Err(e) => tracing::warn!("  MANIFEST: Failed to serialize: {}", e),
        }
    } else {
        tracing::debug!("  MANIFEST: No config_root set, skipping manifest file write");
    }

    router
}
