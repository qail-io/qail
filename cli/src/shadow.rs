//! Shadow Database (Blue-Green) Migrations
//!
//! Provides zero-downtime migration capabilities by:
//! 1. Creating a shadow database with new schema
//! 2. Syncing data from primary to shadow
//! 3. Validating shadow before switch
//! 4. Promoting shadow to primary or aborting
//!
//! This is Phase 3 of the data-safe migration system.

use crate::colors::*;
use anyhow::{Result, anyhow};
use qail_core::ast::{Action, Constraint, Expr, JoinKind, Qail};
use qail_pg::driver::PgDriver;

use crate::introspection::{
    IntrospectedConstraintIdentity, IntrospectedForeignKey, IntrospectedForeignKeyReference,
    IntrospectedKeyColumn, IntrospectedUniqueConstraint, introspected_column_generation,
    parse_index_parts, parse_pg_constraint_fk_action, resolve_introspected_foreign_key,
    resolve_introspected_unique_constraint, resolve_qualified_introspected_foreign_key,
    sort_introspected_key_columns, sort_qualified_introspected_key_columns,
};
use crate::util::{parse_pg_url, redact_url};

fn required_shadow_metadata_string(
    row: &qail_pg::PgRow,
    idx: usize,
    label: &str,
) -> Result<String> {
    row.get_string(idx)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("Invalid shadow introspection metadata: missing {}", label))
}

fn required_shadow_metadata_i32(row: &qail_pg::PgRow, idx: usize, label: &str) -> Result<i32> {
    let value = required_shadow_metadata_string(row, idx, label)?;
    value
        .parse::<i32>()
        .map_err(|_| anyhow!("Invalid shadow introspection metadata: malformed {}", label))
}

fn parse_pg_attnum_array(raw: &str, label: &str) -> Result<Vec<i32>> {
    let trimmed = raw.trim();
    let Some(inner) = trimmed.strip_prefix('{').and_then(|s| s.strip_suffix('}')) else {
        return Err(anyhow!(
            "Invalid shadow introspection metadata: malformed {}",
            label
        ));
    };
    if inner.trim().is_empty() {
        return Ok(Vec::new());
    }
    inner
        .split(',')
        .map(|part| {
            part.trim()
                .parse::<i32>()
                .map_err(|_| anyhow!("Invalid shadow introspection metadata: malformed {}", label))
        })
        .collect()
}

fn public_rls_status_cmd(public_namespace_oid: String) -> Qail {
    Qail::get("pg_catalog.pg_class")
        .columns(["relname", "relrowsecurity", "relforcerowsecurity"])
        .filter("relkind", qail_core::ast::Operator::Eq, "r")
        .filter(
            "relnamespace",
            qail_core::ast::Operator::Eq,
            public_namespace_oid,
        )
}

/// Shadow database state
#[derive(Debug, Clone)]
pub struct ShadowState {
    /// Primary database URL
    pub primary_url: String,
    /// Shadow database name (derived from primary)
    pub shadow_name: String,
    /// Shadow database URL
    pub shadow_url: String,
    pub is_ready: bool,
    pub tables_synced: u64,
    pub rows_synced: u64,
}

impl ShadowState {
    pub fn new(primary_url: &str) -> Result<Self> {
        let (host, port, user, _password, database) = parse_pg_url(primary_url)?;
        let shadow_name = format!("{}_shadow", database);

        let shadow_url = format!("postgres://{}@{}:{}/{}", user, host, port, shadow_name);
        let primary_url = redact_url(primary_url);

        Ok(Self {
            primary_url,
            shadow_name,
            shadow_url,
            is_ready: false,
            tables_synced: 0,
            rows_synced: 0,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shadow State Persistence
// ─────────────────────────────────────────────────────────────────────────────

/// Ensure _qail_shadow_state table exists in primary database
async fn ensure_shadow_state_table(driver: &mut PgDriver) -> Result<()> {
    let exists_cmd = Qail::get("information_schema.tables")
        .column("1")
        .where_eq("table_schema", "public")
        .where_eq("table_name", "_qail_shadow_state")
        .limit(1);
    let exists = driver
        .fetch_all(&exists_cmd)
        .await
        .map_err(|e| anyhow!("Failed to check shadow state table: {}", e))?;

    if exists.is_empty() {
        let create_cmd = Qail {
            action: Action::Make,
            table: "_qail_shadow_state".to_string(),
            columns: vec![
                Expr::Def {
                    name: "id".to_string(),
                    data_type: "serial".to_string(),
                    constraints: vec![Constraint::PrimaryKey],
                },
                Expr::Def {
                    name: "shadow_name".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "primary_url".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "diff_cmds".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "diff_checksum".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "old_schema_path".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "new_schema_path".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "created_at".to_string(),
                    data_type: "timestamptz".to_string(),
                    constraints: vec![
                        Constraint::Nullable,
                        Constraint::Default("now()".to_string()),
                    ],
                },
                Expr::Def {
                    name: "status".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![
                        Constraint::Nullable,
                        Constraint::Default("'pending'".to_string()),
                    ],
                },
            ],
            ..Default::default()
        };
        driver
            .execute(&create_cmd)
            .await
            .map_err(|e| anyhow!("Failed to create shadow state table: {}", e))?;
    }
    Ok(())
}

/// Stable checksum for a migration command sequence.
pub fn diff_cmds_checksum(diff_cmds: &[Qail]) -> String {
    crate::migrations::stable_cmds_checksum(diff_cmds)
}

fn persisted_primary_url(state: &ShadowState) -> String {
    redact_url(&state.primary_url)
}

fn loaded_shadow_state(shadow_name: String, primary_url: String) -> ShadowState {
    ShadowState {
        primary_url: redact_url(&primary_url),
        shadow_name,
        shadow_url: String::new(), // Will be reconstructed by caller-supplied URLs when needed
        is_ready: true,
        tables_synced: 0,
        rows_synced: 0,
    }
}

/// Save shadow state to _qail_shadow_state table (for promote/abort recovery)
async fn save_shadow_state(
    driver: &mut PgDriver,
    state: &ShadowState,
    diff_cmds: &[Qail],
    old_path: &str,
    new_path: &str,
) -> Result<()> {
    ensure_shadow_state_table(driver).await?;

    // Serialize diff commands as QAIL wire text (serde-free for AST).
    let diff_json = qail_core::wire::encode_cmds_text(diff_cmds);
    let diff_checksum = diff_cmds_checksum(diff_cmds);

    // Clear any existing pending state
    let clear_cmd = Qail::del("_qail_shadow_state").in_vals("status", ["pending", "verified"]);
    let _ = driver.execute(&clear_cmd).await;

    // Insert new state
    let insert_cmd = Qail::add("_qail_shadow_state")
        .set_value("shadow_name", state.shadow_name.as_str())
        .set_value("primary_url", persisted_primary_url(state))
        .set_value("diff_cmds", diff_json)
        .set_value("diff_checksum", diff_checksum)
        .set_value("old_schema_path", old_path)
        .set_value("new_schema_path", new_path)
        .set_value("status", "verified");
    driver
        .execute(&insert_cmd)
        .await
        .map_err(|e| anyhow!("Failed to save shadow state: {}", e))?;

    Ok(())
}

/// Load pending shadow state from _qail_shadow_state table
async fn load_shadow_state(driver: &mut PgDriver) -> Result<Option<(ShadowState, Vec<Qail>)>> {
    ensure_shadow_state_table(driver).await?;

    let cmd_verified = Qail::get("_qail_shadow_state")
        .columns(["shadow_name", "primary_url", "diff_cmds"])
        .filter("status", qail_core::ast::Operator::Eq, "verified")
        .limit(1);

    let mut rows = driver
        .fetch_all(&cmd_verified)
        .await
        .map_err(|e| anyhow!("Failed to load shadow state: {}", e))?;

    if rows.is_empty() {
        let cmd_pending = Qail::get("_qail_shadow_state")
            .columns(["shadow_name", "primary_url", "diff_cmds"])
            .filter("status", qail_core::ast::Operator::Eq, "pending")
            .limit(1);
        rows = driver
            .fetch_all(&cmd_pending)
            .await
            .map_err(|e| anyhow!("Failed to load shadow state: {}", e))?;
    }

    if rows.is_empty() {
        return Ok(None);
    }

    let row = &rows[0];
    let shadow_name = row
        .get_string(0)
        .ok_or_else(|| anyhow!("Missing shadow_name"))?;
    let primary_url = row
        .get_string(1)
        .ok_or_else(|| anyhow!("Missing primary_url"))?;
    let diff_json = row
        .get_string(2)
        .ok_or_else(|| anyhow!("Missing diff_cmds"))?;

    let diff_cmds = qail_core::wire::decode_cmds_text(&diff_json)
        .map_err(|e| anyhow!("Failed to decode diff commands: {}", e))?;

    let state = loaded_shadow_state(shadow_name, primary_url);

    Ok(Some((state, diff_cmds)))
}

/// Update shadow state status (pending → promoted/aborted)
async fn update_shadow_state_status(driver: &mut PgDriver, new_status: &str) -> Result<()> {
    let sql = Qail::set("_qail_shadow_state")
        .set_value("status", new_status)
        .in_vals("status", ["pending", "verified"]);
    driver
        .execute(&sql)
        .await
        .map_err(|e| anyhow!("Failed to update shadow state: {}", e))?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    #[test]
    fn shadow_state_new_redacts_primary_url_and_omits_shadow_password() {
        let state = ShadowState::new("postgres://admin:s3cret@db.example.com:5432/app").unwrap();

        assert_eq!(
            state.primary_url,
            "postgres://admin:***@db.example.com:5432/app"
        );
        assert!(!state.primary_url.contains("s3cret"));
        assert_eq!(
            state.shadow_url,
            "postgres://admin@db.example.com:5432/app_shadow"
        );
        assert!(!state.shadow_url.contains("s3cret"));
    }

    #[test]
    fn persisted_primary_url_redacts_raw_state_url() {
        let state = ShadowState {
            primary_url: "postgres://admin:s3cret@db.example.com:5432/app".to_string(),
            shadow_name: "app_shadow".to_string(),
            shadow_url: "postgres://admin:s3cret@db.example.com:5432/app_shadow".to_string(),
            is_ready: false,
            tables_synced: 0,
            rows_synced: 0,
        };

        let persisted = persisted_primary_url(&state);

        assert_eq!(persisted, "postgres://admin:***@db.example.com:5432/app");
        assert!(!persisted.contains("s3cret"));
    }

    #[test]
    fn loaded_shadow_state_redacts_legacy_raw_primary_url() {
        let state = loaded_shadow_state(
            "app_shadow".to_string(),
            "postgres://admin:s3cret@db.example.com:5432/app".to_string(),
        );

        assert_eq!(
            state.primary_url,
            "postgres://admin:***@db.example.com:5432/app"
        );
        assert!(!state.primary_url.contains("s3cret"));
    }

    #[test]
    fn passwordless_primary_url_remains_readable() {
        let state = ShadowState::new("postgres://admin@db.example.com:5432/app").unwrap();

        assert_eq!(
            state.primary_url,
            "postgres://admin@db.example.com:5432/app"
        );
    }

    #[test]
    fn extract_index_columns_ignores_partial_predicate_parentheses() {
        let def = "CREATE INDEX idx_docs_expr ON documents USING btree (regexp_replace(title, ')', '', 'g'), lower(slug)) WHERE (notes <> 'keep WHERE literal')";

        assert_eq!(
            extract_index_columns(def),
            vec![
                "regexp_replace(title, ')', '', 'g')".to_string(),
                "lower(slug)".to_string()
            ]
        );
    }

    #[test]
    fn resolves_all_columns_for_composite_primary_key() {
        let pk_constraints =
            std::collections::HashSet::from([("orders".to_string(), "orders_pkey".to_string())]);
        let constraint_columns = std::collections::HashMap::from([(
            ("orders".to_string(), "orders_pkey".to_string()),
            vec![
                IntrospectedKeyColumn::new("orders".to_string(), "tenant_id".to_string(), 1),
                IntrospectedKeyColumn::new("orders".to_string(), "order_no".to_string(), 2),
            ],
        )]);

        let primary_key_columns =
            resolve_introspected_primary_key_columns(&pk_constraints, &constraint_columns);

        assert_eq!(
            primary_key_columns,
            std::collections::HashSet::from([
                ("orders".to_string(), "tenant_id".to_string()),
                ("orders".to_string(), "order_no".to_string()),
            ])
        );
    }

    #[test]
    fn primary_key_resolution_keeps_same_named_constraints_table_scoped() {
        let pk_constraints =
            std::collections::HashSet::from([("orders".to_string(), "pkey".to_string())]);
        let constraint_columns = std::collections::HashMap::from([
            (
                ("orders".to_string(), "pkey".to_string()),
                vec![IntrospectedKeyColumn::new(
                    "orders".to_string(),
                    "order_no".to_string(),
                    1,
                )],
            ),
            (
                ("line_items".to_string(), "pkey".to_string()),
                vec![IntrospectedKeyColumn::new(
                    "line_items".to_string(),
                    "line_no".to_string(),
                    1,
                )],
            ),
        ]);

        let primary_key_columns =
            resolve_introspected_primary_key_columns(&pk_constraints, &constraint_columns);

        assert_eq!(
            primary_key_columns,
            std::collections::HashSet::from([("orders".to_string(), "order_no".to_string(),)])
        );
    }

    #[test]
    fn parse_column_type_preserves_unknown_and_user_defined_types() {
        assert_eq!(
            parse_column_type("USER-DEFINED", Some("booking_status")),
            ColumnType::Range("BOOKING_STATUS".to_string())
        );
        assert_eq!(
            parse_column_type("ltree", None),
            ColumnType::Range("LTREE".to_string())
        );
        assert_eq!(
            parse_column_type("ARRAY", Some("_int4")),
            ColumnType::Array(Box::new(ColumnType::Int))
        );
        assert_ne!(parse_column_type("ltree", None), ColumnType::Text);
    }

    #[test]
    fn shadow_metadata_parsing_fails_closed() {
        let valid = qail_pg::PgRow {
            columns: vec![Some(b"7".to_vec())],
            column_info: None,
        };
        assert_eq!(
            required_shadow_metadata_i32(&valid, 0, "ordinal_position").unwrap(),
            7
        );

        let missing = qail_pg::PgRow {
            columns: vec![None],
            column_info: None,
        };
        assert!(required_shadow_metadata_string(&missing, 0, "column_name").is_err());
        assert!(required_shadow_metadata_i32(&missing, 0, "ordinal_position").is_err());

        let malformed = qail_pg::PgRow {
            columns: vec![Some(b"not-an-int".to_vec())],
            column_info: None,
        };
        assert!(required_shadow_metadata_i32(&malformed, 0, "ordinal_position").is_err());
    }

    #[test]
    fn shadow_rls_status_query_is_scoped_to_public_namespace() {
        let cmd = public_rls_status_cmd("2200".to_string());

        assert!(cmd.cages.iter().any(|cage| {
            cage.conditions.iter().any(|condition| {
                matches!(&condition.left, Expr::Named(name) if name == "relnamespace")
                    && condition.value == qail_core::ast::Value::String("2200".to_string())
            })
        }));
    }

    #[test]
    fn shadow_receipt_verification_requires_decodable_matching_payload() {
        let diff_cmds = vec![Qail::get("users")];
        let diff_json = qail_core::wire::encode_cmds_text(&diff_cmds);
        let checksum = diff_cmds_checksum(&diff_cmds);

        let valid = qail_pg::PgRow {
            columns: vec![
                Some(diff_json.as_bytes().to_vec()),
                Some(checksum.as_bytes().to_vec()),
            ],
            column_info: None,
        };
        assert!(shadow_receipt_row_matches_expected(&valid, &checksum));

        let malformed_payload = qail_pg::PgRow {
            columns: vec![
                Some(b"not qail wire text".to_vec()),
                Some(checksum.as_bytes().to_vec()),
            ],
            column_info: None,
        };
        assert!(!shadow_receipt_row_matches_expected(
            &malformed_payload,
            &checksum
        ));

        let checksum_drift = qail_pg::PgRow {
            columns: vec![
                Some(diff_json.as_bytes().to_vec()),
                Some(b"different-checksum".to_vec()),
            ],
            column_info: None,
        };
        assert!(!shadow_receipt_row_matches_expected(
            &checksum_drift,
            &checksum
        ));
    }

    #[test]
    fn shadow_receipt_lookup_uses_verified_status_only() {
        let cmd = shadow_receipt_lookup_cmd();

        assert!(cmd.cages.iter().any(|cage| {
            cage.conditions.iter().any(|condition| {
                matches!(&condition.left, Expr::Named(name) if name == "status")
                    && condition.value == qail_core::ast::Value::String("verified".to_string())
            })
        }));
        assert!(!cmd.cages.iter().any(|cage| {
            cage.conditions.iter().any(|condition| {
                condition.value == qail_core::ast::Value::String("pending".to_string())
            })
        }));
    }
}

fn shadow_receipt_lookup_cmd() -> Qail {
    Qail::get("_qail_shadow_state")
        .columns(["diff_cmds", "diff_checksum"])
        .filter("status", qail_core::ast::Operator::Eq, "verified")
        .limit(5)
}

fn shadow_receipt_row_matches_expected(row: &qail_pg::PgRow, expected_checksum: &str) -> bool {
    let Some(diff_json) = row.get_string(0).filter(|value| !value.trim().is_empty()) else {
        return false;
    };
    let Ok(diff_cmds) = qail_core::wire::decode_cmds_text(&diff_json) else {
        return false;
    };
    if diff_cmds_checksum(&diff_cmds) != expected_checksum {
        return false;
    }

    match row.get_string(1).filter(|value| !value.trim().is_empty()) {
        Some(stored_checksum) => stored_checksum == expected_checksum,
        None => true,
    }
}

/// Verify an active shadow receipt by SQL checksum.
pub async fn has_verified_shadow_receipt_with_driver(
    driver: &mut PgDriver,
    expected_checksum: &str,
) -> Result<bool> {
    ensure_shadow_state_table(driver).await?;

    let cmd = shadow_receipt_lookup_cmd();
    let rows = driver
        .fetch_all(&cmd)
        .await
        .map_err(|e| anyhow!("Failed to query shadow receipts: {}", e))?;

    for row in rows {
        if shadow_receipt_row_matches_expected(&row, expected_checksum) {
            return Ok(true);
        }
    }

    Ok(false)
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema Introspection (Zero-Dep)
// ─────────────────────────────────────────────────────────────────────────────

use qail_core::migrate::{Column, ColumnType, Index, IndexMethod, Schema, Table};

/// Introspect the live database schema from information_schema.
/// Returns a Schema struct that represents the current state of the database.
/// This is used for drift detection - comparing live schema vs file schema.
pub async fn introspect_schema(driver: &mut PgDriver) -> Result<Schema> {
    use qail_core::ast::Operator;

    let mut schema = Schema::default();

    let public_ns_cmd = Qail::get("pg_catalog.pg_namespace")
        .columns(["oid"])
        .filter("nspname", Operator::Eq, "public");
    let public_ns_rows = driver
        .fetch_all(&public_ns_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query public namespace OID: {}", e))?;
    let public_namespace_oid = public_ns_rows
        .first()
        .map(|row| required_shadow_metadata_string(row, 0, "public namespace oid"))
        .transpose()?
        .ok_or_else(|| anyhow!("Public schema not found in pg_namespace"))?;

    let (single_unique_columns, unique_constraint_indexes, unique_constraint_names) =
        introspect_unique_constraints(driver).await?;
    let primary_key_columns = introspect_primary_key_columns(driver).await?;

    // 1. Query all tables
    let tables_cmd = Qail::get("information_schema.tables")
        .column("table_name")
        .filter("table_schema", Operator::Eq, "public")
        .filter("table_type", Operator::Eq, "BASE TABLE");

    let table_rows = driver
        .fetch_all(&tables_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query tables: {}", e))?;

    let table_names: Vec<String> = table_rows
        .iter()
        .filter_map(|r| r.get_string(0))
        .filter(|t| !t.starts_with("_qail")) // Skip internal tables
        .collect();

    // 2. For each table, query columns
    for table_name in &table_names {
        let cols_cmd = Qail::get("information_schema.columns")
            .columns([
                "column_name",
                "data_type",
                "is_nullable",
                "column_default",
                "is_identity",
                "identity_generation",
                "is_generated",
                "generation_expression",
                "udt_name",
            ])
            .filter("table_schema", Operator::Eq, "public")
            .filter("table_name", Operator::Eq, table_name.clone());

        let col_rows = driver
            .fetch_all(&cols_cmd)
            .await
            .map_err(|e| anyhow!("Failed to query columns for {}: {}", table_name, e))?;

        let mut columns = Vec::new();
        for row in &col_rows {
            let col_name = required_shadow_metadata_string(row, 0, "column_name")?;
            let data_type_str = required_shadow_metadata_string(row, 1, "data_type")?;
            let is_nullable = required_shadow_metadata_string(row, 2, "is_nullable")? == "YES";
            let raw_default = row.get_string(3);
            // is_identity: 'YES' for identity columns (GENERATED ALWAYS/BY DEFAULT AS IDENTITY)
            let is_identity = row.get_string(4).map(|s| s == "YES").unwrap_or(false);
            let identity_generation = row.get_string(5);
            let is_generated = row.get_string(6);
            let generation_expression = row.get_string(7);
            let udt_name = row.get_string(8);

            // Parse data type to ColumnType
            let data_type = parse_column_type(&data_type_str, udt_name.as_deref());
            let generated = introspected_column_generation(
                is_identity,
                identity_generation.as_deref(),
                is_generated.as_deref(),
                generation_expression.as_deref(),
            );

            // Strip defaults for SERIAL and IDENTITY columns (auto-generated)
            // nextval() for SERIAL, identity columns handle their own generation
            let default = match &raw_default {
                Some(d) if d.starts_with("nextval(") => None,
                _ if generated.is_some() => None, // Generated columns don't need explicit default
                other => other.clone(),
            };

            let is_pk = primary_key_columns.contains(&(table_name.clone(), col_name.clone()));

            let is_unique = single_unique_columns.contains(&(table_name.clone(), col_name.clone()));

            columns.push(Column {
                name: col_name,
                data_type,
                nullable: is_nullable,
                primary_key: is_pk,
                unique: is_unique,
                default,
                foreign_key: None, // Will be filled below after FK query
                check: None,
                generated,
            });
        }

        schema.tables.insert(
            table_name.clone(),
            Table {
                name: table_name.clone(),
                columns,
                multi_column_fks: vec![],
                enable_rls: false,
                force_rls: false,
            },
        );
    }

    // 3. Query indexes
    let idx_cmd = Qail::get("pg_indexes")
        .columns(["indexname", "tablename", "indexdef"])
        .filter("schemaname", Operator::Eq, "public");

    let idx_rows = driver
        .fetch_all(&idx_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query indexes: {}", e))?;

    let unique_constraint_index_names: std::collections::HashSet<String> =
        unique_constraint_indexes
            .iter()
            .map(|index| index.name.clone())
            .collect();
    schema.indexes.extend(unique_constraint_indexes);

    for row in &idx_rows {
        let idx_name = required_shadow_metadata_string(row, 0, "indexname")?;
        let table_name = required_shadow_metadata_string(row, 1, "tablename")?;
        let indexdef = required_shadow_metadata_string(row, 2, "indexdef")?;

        // Skip primary key indexes (they're implicit)
        if idx_name.ends_with("_pkey") {
            continue;
        }

        // Skip constraint-backed unique indexes; they are represented as
        // column-level unique flags or explicit composite unique indexes.
        if idx_name.ends_with("_key")
            || unique_constraint_names.contains(&idx_name)
            || unique_constraint_index_names.contains(&idx_name)
        {
            continue;
        }

        // Parse columns from indexdef (simple extraction)
        let cols = extract_index_columns(&indexdef);
        let is_unique = indexdef.contains("UNIQUE");

        schema.indexes.push(Index {
            name: idx_name,
            table: table_name,
            columns: cols,
            unique: is_unique,
            method: IndexMethod::BTree,
            where_clause: None,
            include: vec![],
            concurrently: false,
            expressions: vec![],
        });
    }

    let attnum_cmd = Qail::get("pg_catalog.pg_attribute")
        .table_alias("a")
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_class c",
            "c.oid",
            "a.attrelid",
        )
        .columns(["c.relname", "a.attnum", "a.attname"])
        .filter(
            "c.relnamespace",
            qail_core::ast::Operator::Eq,
            public_namespace_oid.clone(),
        )
        .filter("a.attnum", qail_core::ast::Operator::Gt, 0)
        .filter("a.attisdropped", qail_core::ast::Operator::Eq, false);
    let attnum_rows = driver
        .fetch_all(&attnum_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query attribute ordinals: {}", e))?;
    let mut attnum_columns = std::collections::HashMap::<(String, i32), String>::new();
    for row in attnum_rows {
        let table = required_shadow_metadata_string(&row, 0, "attrel table")?;
        let attnum = required_shadow_metadata_i32(&row, 1, "attnum")?;
        let column = required_shadow_metadata_string(&row, 2, "attname")?;
        attnum_columns.insert((table, attnum), column);
    }

    // 4. Query FK constraints (batch approach, not N+1)
    let fk_ref_cmd = Qail::get("information_schema.referential_constraints")
        .columns(["constraint_name", "unique_constraint_name"])
        .filter("constraint_schema", Operator::Eq, "public");

    let fk_ref_rows = driver
        .fetch_all(&fk_ref_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query FK refs: {}", e))?;

    // Build bare FK constraint name → candidate referenced constraints. Source
    // table identity is added from pg_constraint before resolving.
    let mut fk_ref_candidates: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for row in fk_ref_rows {
        let fk_name = required_shadow_metadata_string(&row, 0, "constraint_name")?;
        if let Some(ref_name) = row.get_string(1).filter(|value| !value.trim().is_empty()) {
            fk_ref_candidates.entry(fk_name).or_default().push(ref_name);
        }
    }

    let fk_catalog_cmd = Qail::get("pg_catalog.pg_constraint")
        .table_alias("con")
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_class src",
            "src.oid",
            "con.conrelid",
        )
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_class ref",
            "ref.oid",
            "con.confrelid",
        )
        .join(
            JoinKind::Inner,
            "pg_catalog.pg_namespace ns",
            "ns.oid",
            "con.connamespace",
        )
        .columns([
            "con.conname",
            "src.relname",
            "ref.relname",
            "con.confdeltype",
            "con.confupdtype",
            "con.conkey",
            "con.confkey",
        ])
        .filter("con.contype", Operator::Eq, "f")
        .filter("ns.nspname", Operator::Eq, "public");

    let fk_catalog_rows = driver
        .fetch_all(&fk_catalog_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query FK constraint metadata: {}", e))?;

    let mut fk_catalog_metadata = Vec::new();
    for row in fk_catalog_rows {
        let constraint_name = required_shadow_metadata_string(&row, 0, "constraint_name")?;
        let source_table = required_shadow_metadata_string(&row, 1, "source_table")?;
        let ref_table = required_shadow_metadata_string(&row, 2, "referenced_table")?;
        let on_delete = required_shadow_metadata_string(&row, 3, "delete_action")?;
        let on_update = required_shadow_metadata_string(&row, 4, "update_action")?;
        let source_attnums = parse_pg_attnum_array(
            &required_shadow_metadata_string(&row, 5, "conkey")?,
            "conkey",
        )?;
        let ref_attnums = parse_pg_attnum_array(
            &required_shadow_metadata_string(&row, 6, "confkey")?,
            "confkey",
        )?;
        fk_catalog_metadata.push((
            constraint_name,
            source_table,
            ref_table,
            parse_pg_constraint_fk_action(&on_delete),
            parse_pg_constraint_fk_action(&on_update),
            source_attnums,
            ref_attnums,
        ));
    }

    // Batch query key_column_usage for FK resolution
    let kcu_cmd = Qail::get("information_schema.key_column_usage")
        .columns([
            "table_name",
            "column_name",
            "constraint_name",
            "ordinal_position",
        ])
        .filter("table_schema", Operator::Eq, "public");

    let kcu_rows = driver
        .fetch_all(&kcu_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query key columns: {}", e))?;

    let mut constraint_cols: std::collections::HashMap<
        IntrospectedConstraintIdentity,
        Vec<IntrospectedKeyColumn>,
    > = std::collections::HashMap::new();
    for row in &kcu_rows {
        let table = required_shadow_metadata_string(row, 0, "table_name")?;
        let column = required_shadow_metadata_string(row, 1, "column_name")?;
        let constraint = required_shadow_metadata_string(row, 2, "constraint_name")?;
        let ordinal_position = required_shadow_metadata_i32(row, 3, "ordinal_position")?;
        constraint_cols
            .entry(IntrospectedConstraintIdentity::new(
                table.clone(),
                constraint,
            ))
            .or_default()
            .push(IntrospectedKeyColumn::new(table, column, ordinal_position));
    }
    sort_qualified_introspected_key_columns(&mut constraint_cols);

    let mut fk_references = Vec::new();
    for (
        constraint_name,
        source_table,
        ref_table,
        on_delete,
        on_update,
        source_attnums,
        ref_attnums,
    ) in fk_catalog_metadata
    {
        if !schema.tables.contains_key(&source_table) {
            continue;
        }
        if let Some(candidates) = fk_ref_candidates.get(&constraint_name)
            && let Some(ref_constraint) = candidates.iter().find(|ref_constraint| {
                constraint_cols.contains_key(&IntrospectedConstraintIdentity::new(
                    ref_table.clone(),
                    (*ref_constraint).clone(),
                ))
            })
        {
            fk_references.push(IntrospectedForeignKeyReference {
                constraint: IntrospectedConstraintIdentity::new(source_table, constraint_name),
                referenced_constraint: IntrospectedConstraintIdentity::new(
                    ref_table,
                    ref_constraint.clone(),
                ),
                on_delete,
                on_update,
                deferrable: qail_core::migrate::schema::Deferrable::NotDeferrable,
            });
            continue;
        }

        let source_cols = source_attnums
            .iter()
            .enumerate()
            .filter_map(|(idx, attnum)| {
                attnum_columns
                    .get(&(source_table.clone(), *attnum))
                    .map(|column| {
                        IntrospectedKeyColumn::new(source_table.clone(), column.clone(), idx as i32)
                    })
            })
            .collect::<Vec<_>>();
        let ref_cols = ref_attnums
            .iter()
            .enumerate()
            .filter_map(|(idx, attnum)| {
                attnum_columns
                    .get(&(ref_table.clone(), *attnum))
                    .map(|column| {
                        IntrospectedKeyColumn::new(ref_table.clone(), column.clone(), idx as i32)
                    })
            })
            .collect::<Vec<_>>();
        if let Some(resolved) = resolve_introspected_foreign_key(
            &constraint_name,
            &source_cols,
            &ref_cols,
            &on_delete,
            &on_update,
            qail_core::migrate::schema::Deferrable::NotDeferrable,
        ) {
            match resolved {
                IntrospectedForeignKey::Single {
                    table,
                    column,
                    foreign_key,
                } => {
                    if let Some(table_def) = schema.tables.get_mut(&table)
                        && let Some(col) = table_def.columns.iter_mut().find(|c| c.name == column)
                    {
                        col.foreign_key = Some(foreign_key);
                    }
                }
                IntrospectedForeignKey::Multi { table, foreign_key } => {
                    if let Some(table_def) = schema.tables.get_mut(&table) {
                        table_def.multi_column_fks.push(foreign_key);
                    }
                }
            }
        }
    }

    // Resolve FKs
    for fk_reference in &fk_references {
        match resolve_qualified_introspected_foreign_key(fk_reference, &constraint_cols) {
            Some(IntrospectedForeignKey::Single {
                table,
                column,
                foreign_key,
            }) => {
                if let Some(table) = schema.tables.get_mut(table.as_str()) {
                    for col in table.columns.iter_mut() {
                        if col.name == column {
                            col.foreign_key = Some(foreign_key.clone());
                        }
                    }
                }
            }
            Some(IntrospectedForeignKey::Multi { table, foreign_key }) => {
                if let Some(table) = schema.tables.get_mut(table.as_str()) {
                    table.multi_column_fks.push(foreign_key);
                }
            }
            None => {}
        }
    }

    // 5. Query RLS status from pg_class
    let rls_cmd = public_rls_status_cmd(public_namespace_oid);

    let rls_rows = driver
        .fetch_all(&rls_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query RLS: {}", e))?;

    for row in rls_rows {
        let tbl_name = row.text(0);
        let enable = row.text(1) == "t";
        let force = row.text(2) == "t";
        if (enable || force)
            && let Some(table) = schema.tables.get_mut(&tbl_name)
        {
            table.enable_rls = enable;
            table.force_rls = force;
        }
    }

    Ok(schema)
}

async fn introspect_primary_key_columns(
    driver: &mut PgDriver,
) -> Result<std::collections::HashSet<(String, String)>> {
    use qail_core::ast::Operator;

    let pk_cmd = Qail::get("information_schema.table_constraints")
        .columns(["table_name", "constraint_name"])
        .filter("table_schema", Operator::Eq, "public")
        .filter("constraint_type", Operator::Eq, "PRIMARY KEY");

    let pk_rows = driver
        .fetch_all(&pk_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query PK constraints: {}", e))?;

    let mut pk_constraints = std::collections::HashSet::new();
    for row in &pk_rows {
        let table = required_shadow_metadata_string(row, 0, "table_name")?;
        if table.starts_with("_qail") {
            continue;
        }
        pk_constraints.insert((
            table,
            required_shadow_metadata_string(row, 1, "constraint_name")?,
        ));
    }

    if pk_constraints.is_empty() {
        return Ok(std::collections::HashSet::new());
    }

    let kcu_cmd = Qail::get("information_schema.key_column_usage")
        .columns([
            "table_name",
            "column_name",
            "constraint_name",
            "ordinal_position",
        ])
        .filter("table_schema", Operator::Eq, "public");

    let kcu_rows = driver
        .fetch_all(&kcu_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query PK columns: {}", e))?;

    let mut constraint_columns: std::collections::HashMap<
        (String, String),
        Vec<IntrospectedKeyColumn>,
    > = std::collections::HashMap::new();
    for row in &kcu_rows {
        let table = required_shadow_metadata_string(row, 0, "table_name")?;
        let column = required_shadow_metadata_string(row, 1, "column_name")?;
        let constraint = required_shadow_metadata_string(row, 2, "constraint_name")?;
        let ordinal_position = required_shadow_metadata_i32(row, 3, "ordinal_position")?;
        constraint_columns
            .entry((table.clone(), constraint))
            .or_default()
            .push(IntrospectedKeyColumn::new(table, column, ordinal_position));
    }

    Ok(resolve_introspected_primary_key_columns(
        &pk_constraints,
        &constraint_columns,
    ))
}

fn resolve_introspected_primary_key_columns(
    pk_constraints: &std::collections::HashSet<(String, String)>,
    constraint_columns: &std::collections::HashMap<(String, String), Vec<IntrospectedKeyColumn>>,
) -> std::collections::HashSet<(String, String)> {
    let mut primary_key_columns = std::collections::HashSet::new();

    for (table, constraint) in pk_constraints {
        if let Some(columns) = constraint_columns.get(&(table.clone(), constraint.clone())) {
            for column in columns {
                primary_key_columns.insert((table.clone(), column.column.clone()));
            }
        }
    }

    primary_key_columns
}

async fn introspect_unique_constraints(
    driver: &mut PgDriver,
) -> Result<(
    std::collections::HashSet<(String, String)>,
    Vec<Index>,
    std::collections::HashSet<String>,
)> {
    use qail_core::ast::Operator;

    let unique_cmd = Qail::get("information_schema.table_constraints")
        .columns(["constraint_name", "table_name"])
        .filter("table_schema", Operator::Eq, "public")
        .filter("constraint_type", Operator::Eq, "UNIQUE");

    let unique_rows = driver
        .fetch_all(&unique_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query unique constraints: {}", e))?;

    let kcu_cmd = Qail::get("information_schema.key_column_usage")
        .columns([
            "table_name",
            "column_name",
            "constraint_name",
            "ordinal_position",
        ])
        .filter("table_schema", Operator::Eq, "public");

    let kcu_rows = driver
        .fetch_all(&kcu_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query key columns: {}", e))?;

    let mut constraint_columns: std::collections::HashMap<String, Vec<IntrospectedKeyColumn>> =
        std::collections::HashMap::new();
    for row in &kcu_rows {
        let table = required_shadow_metadata_string(row, 0, "table_name")?;
        let column = required_shadow_metadata_string(row, 1, "column_name")?;
        let constraint = required_shadow_metadata_string(row, 2, "constraint_name")?;
        let ordinal_position = required_shadow_metadata_i32(row, 3, "ordinal_position")?;
        constraint_columns
            .entry(constraint)
            .or_default()
            .push(IntrospectedKeyColumn::new(table, column, ordinal_position));
    }
    sort_introspected_key_columns(&mut constraint_columns);

    let mut unique_columns = std::collections::HashSet::new();
    let mut unique_indexes = Vec::new();
    let mut unique_constraint_names = std::collections::HashSet::new();

    for row in unique_rows {
        let constraint_name = required_shadow_metadata_string(&row, 0, "constraint_name")?;
        let table_name = required_shadow_metadata_string(&row, 1, "table_name")?;
        if table_name.starts_with("_qail") {
            continue;
        }
        unique_constraint_names.insert(constraint_name.clone());

        if let Some(cols) = constraint_columns.get(&constraint_name)
            && let Some(unique) =
                resolve_introspected_unique_constraint(&constraint_name, &table_name, cols)
        {
            match unique {
                IntrospectedUniqueConstraint::Single { table, column } => {
                    unique_columns.insert((table, column));
                }
                IntrospectedUniqueConstraint::Multi(index) => unique_indexes.push(index),
            }
        }
    }

    Ok((unique_columns, unique_indexes, unique_constraint_names))
}

/// Parse PostgreSQL data type metadata to ColumnType.
fn parse_column_type(data_type: &str, udt_name: Option<&str>) -> ColumnType {
    if data_type.eq_ignore_ascii_case("array")
        && let Some(array_inner) = udt_name.and_then(|name| name.strip_prefix('_'))
    {
        return ColumnType::Array(Box::new(parse_column_type(array_inner, None)));
    }

    let raw_type = if data_type.eq_ignore_ascii_case("user-defined") {
        udt_name.unwrap_or(data_type)
    } else {
        data_type
    };

    match raw_type.to_lowercase().as_str() {
        "integer" | "int" | "int4" => ColumnType::Int,
        "bigint" | "int8" => ColumnType::BigInt,
        "smallint" | "int2" => ColumnType::Range("SMALLINT".to_string()),
        "text" => ColumnType::Text,
        "character varying" | "varchar" => ColumnType::Varchar(None),
        "boolean" | "bool" => ColumnType::Bool,
        "timestamp without time zone" | "timestamp" => ColumnType::Timestamp,
        "timestamp with time zone" | "timestamptz" => ColumnType::Timestamptz,
        "date" => ColumnType::Date,
        "time" => ColumnType::Time,
        "uuid" => ColumnType::Uuid,
        "jsonb" | "json" => ColumnType::Jsonb,
        "real" | "float4" | "double precision" | "float8" => ColumnType::Float,
        "numeric" | "decimal" => ColumnType::Decimal(None),
        "bytea" => ColumnType::Bytea,
        "interval" => ColumnType::Interval,
        "inet" => ColumnType::Inet,
        "cidr" => ColumnType::Cidr,
        "macaddr" => ColumnType::MacAddr,
        _ => raw_type
            .parse()
            .unwrap_or_else(|_| ColumnType::Range(raw_type.to_uppercase())),
    }
}

/// Extract column names from CREATE INDEX definition
fn extract_index_columns(indexdef: &str) -> Vec<String> {
    parse_index_parts(indexdef).0
}

/// Create a shadow database for blue-green migration
pub async fn create_shadow_database(primary_url: &str) -> Result<ShadowState> {
    println!();
    println!("{}", "🔄 Shadow Migration Mode".cyan().bold());
    println!("{}", "━".repeat(40).dimmed());

    let state = ShadowState::new(primary_url)?;

    println!(
        "  {} Creating shadow database: {}",
        "[1/4]".cyan(),
        state.shadow_name.yellow()
    );

    // Connect to postgres database (not the target) to create new database
    let (host, port, user, password, _database) = parse_pg_url(primary_url)?;

    let mut admin_driver = if let Some(pwd) = password.clone() {
        PgDriver::connect_with_password(&host, port, &user, "postgres", &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, "postgres")
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    };

    let check_cmd = Qail::get("pg_database")
        .column("datname")
        .where_eq("datname", state.shadow_name.clone());

    let existing = admin_driver
        .fetch_all(&check_cmd)
        .await
        .map_err(|e| anyhow!("Failed to check existing database: {}", e))?;

    if !existing.is_empty() {
        println!("    {} Shadow database already exists", "⚠".yellow());
    } else {
        // Note: CREATE DATABASE cannot be in a transaction.
        let create_db = Qail::create_database(state.shadow_name.clone());
        admin_driver
            .execute(&create_db)
            .await
            .map_err(|e| anyhow!("Failed to create shadow database: {}", e))?;

        println!("    {} Created", "✓".green());
    }

    Ok(state)
}

/// Apply migrations to shadow database
pub async fn apply_migrations_to_shadow(
    primary_url: &str,
    state: &mut ShadowState,
    cmds: &[Qail],
) -> Result<()> {
    println!("  {} Applying migration to shadow...", "[2/4]".cyan());

    let (host, port, user, password, _) = parse_pg_url(primary_url)?;

    let mut shadow_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &state.shadow_name, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &state.shadow_name)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    };

    for (i, cmd) in cmds.iter().enumerate() {
        shadow_driver
            .execute(cmd)
            .await
            .map_err(|e| anyhow!("Migration {} failed on shadow: {}", i + 1, e))?;
    }

    println!("    {} {} migrations applied", "✓".green(), cmds.len());

    Ok(())
}

/// Sync data from primary to shadow using COPY streaming (zero-dependency).
/// Uses COPY TO STDOUT → raw bytes → COPY FROM STDIN for maximum performance.
pub async fn sync_data_to_shadow(primary_url: &str, state: &mut ShadowState) -> Result<()> {
    println!(
        "  {} Syncing data from primary to shadow...",
        "[3/4]".cyan()
    );

    let (host, port, user, password, database) = parse_pg_url(primary_url)?;

    // Connect to primary
    let mut primary_driver = if let Some(pwd) = password.clone() {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    };

    // Connect to shadow
    let mut shadow_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &state.shadow_name, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &state.shadow_name)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    };

    // Get list of tables in SHADOW (not primary, since shadow may have different schema)
    use qail_core::ast::Operator;
    let tables_cmd = Qail::get("information_schema.tables")
        .column("table_name")
        .filter("table_schema", Operator::Eq, "public")
        .filter("table_type", Operator::Eq, "BASE TABLE");

    let table_rows = shadow_driver
        .fetch_all(&tables_cmd)
        .await
        .map_err(|e| anyhow!("Failed to list shadow tables: {}", e))?;

    let tables: Vec<String> = table_rows
        .iter()
        .filter_map(|r| r.get_string(0))
        .filter(|t| !t.starts_with("_qail")) // Skip internal tables
        .collect();

    state.tables_synced = tables.len() as u64;

    for table in &tables {
        // Get column names for this table in shadow
        let cols_cmd = Qail::get("information_schema.columns")
            .column("column_name")
            .filter("table_schema", Operator::Eq, "public")
            .filter("table_name", Operator::Eq, table.clone());

        let col_rows = shadow_driver
            .fetch_all(&cols_cmd)
            .await
            .map_err(|e| anyhow!("Failed to get columns for {}: {}", table, e))?;

        let shadow_columns: Vec<String> = col_rows.iter().filter_map(|r| r.get_string(0)).collect();

        if shadow_columns.is_empty() {
            continue;
        }

        // Check if table exists in primary (it might not after migration diff)
        let check_cmd = Qail::get("information_schema.tables")
            .column("table_name")
            .filter("table_schema", Operator::Eq, "public")
            .filter("table_name", Operator::Eq, table.clone());

        let exists = primary_driver
            .fetch_all(&check_cmd)
            .await
            .map_err(|e| anyhow!("Failed to check table {} in primary: {}", table, e))?;

        if exists.is_empty() {
            // Table doesn't exist in primary (new table in migration)
            println!("    {} {} (new table, no data)", "⊕".blue(), table.cyan());
            continue;
        }

        // Get columns that exist in PRIMARY to find intersection
        let primary_cols_cmd = Qail::get("information_schema.columns")
            .column("column_name")
            .filter("table_schema", Operator::Eq, "public")
            .filter("table_name", Operator::Eq, table.clone());

        let primary_col_rows = primary_driver
            .fetch_all(&primary_cols_cmd)
            .await
            .map_err(|e| anyhow!("Failed to get primary columns for {}: {}", table, e))?;

        let primary_columns: std::collections::HashSet<String> = primary_col_rows
            .iter()
            .filter_map(|r| r.get_string(0))
            .collect();

        // Use intersection: columns that exist in BOTH shadow AND primary
        let columns: Vec<String> = shadow_columns
            .into_iter()
            .filter(|c| primary_columns.contains(c))
            .collect();

        if columns.is_empty() {
            println!("    {} {} (no common columns)", "⊕".blue(), table.cyan());
            continue;
        }

        // Use COPY streaming: export from primary, import to shadow
        let copy_data = primary_driver
            .copy_export_table(table, &columns)
            .await
            .map_err(|e| anyhow!("Failed to export {}: {}", table, e))?;

        let row_count = copy_data.iter().filter(|&&b| b == b'\n').count();

        if !copy_data.is_empty() {
            // Build Qail::Add for copy_bulk_bytes
            let mut add_cmd = Qail::add(table);
            for col in &columns {
                add_cmd = add_cmd.column(col);
            }

            shadow_driver
                .copy_bulk_bytes(&add_cmd, &copy_data)
                .await
                .map_err(|e| anyhow!("Failed to import {}: {}", table, e))?;
        }

        state.rows_synced += row_count as u64;
        println!("    {} {} ({} rows)", "✓".green(), table.cyan(), row_count);
    }

    println!(
        "    {} Synced {} tables, {} rows",
        "✓".green().bold(),
        state.tables_synced,
        state.rows_synced
    );

    Ok(())
}

/// Display shadow status and available commands
pub fn display_shadow_status(state: &ShadowState) {
    println!("  {} Shadow ready for validation", "[4/4]".cyan());
    println!();
    println!("{}", "━".repeat(40).dimmed());
    println!("  Shadow URL: {}", redact_url(&state.shadow_url).yellow());
    println!(
        "  Tables: {}, Rows: {}",
        state.tables_synced.to_string().cyan(),
        state.rows_synced.to_string().cyan()
    );
    println!();
    println!("  {}", "Available Commands:".bold());
    println!(
        "    {} → Run tests against shadow",
        "qail shadow test".green()
    );
    println!(
        "    {} → Switch traffic to shadow",
        "qail shadow promote".green().bold()
    );
    println!(
        "    {} → Drop shadow, keep primary",
        "qail shadow abort".red()
    );
    println!();
}

/// Promote shadow to primary (Option B: apply migration to primary, then cleanup)
///
/// Workflow:
/// 1. Load diff commands from _qail_shadow_state table
/// 2. Apply migration to PRIMARY database (not swap!)
/// 3. Drop shadow database
/// 4. Update state: status = 'promoted'
pub async fn promote_shadow(primary_url: &str) -> Result<()> {
    let state = ShadowState::new(primary_url)?;

    println!();
    println!("{}", "🚀 Promoting Shadow to Primary".green().bold());
    println!("{}", "━".repeat(40).dimmed());

    let (host, port, user, password, database) = parse_pg_url(primary_url)?;

    // Connect to primary to load state
    let mut primary_driver = if let Some(pwd) = password.clone() {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    };

    // Load stored state (diff commands)
    println!("  [1/4] Loading migration state...");
    let state_option = load_shadow_state(&mut primary_driver).await?;

    let (_, diff_cmds) = state_option.ok_or_else(|| {
        anyhow!("No pending shadow migration found. Run 'qail migrate shadow' first.")
    })?;

    println!(
        "    {} {} migration commands loaded",
        "✓".green(),
        diff_cmds.len()
    );

    // Data Drift Warning (documented edge case)
    println!();
    println!(
        "    {} Changes on primary since shadow sync may cause failure.",
        "⚠️".yellow()
    );
    println!();

    // Apply migration to PRIMARY (wrapped in transaction for atomic rollback)
    println!("  [2/4] Applying migration to primary...");

    // BEGIN transaction for atomic rollback
    primary_driver
        .begin()
        .await
        .map_err(|e| anyhow!("Failed to begin transaction: {}", e))?;

    let mut migration_failed = false;
    let mut failure_reason = String::new();

    for (i, cmd) in diff_cmds.iter().enumerate() {
        if let Err(e) = primary_driver.execute(cmd).await {
            migration_failed = true;
            failure_reason = format!("Migration {} failed: {} (cmd: {:?})", i + 1, e, cmd.action);
            break;
        }
    }

    if migration_failed {
        // ROLLBACK on failure - atomic rollback!
        primary_driver
            .rollback()
            .await
            .map_err(|e| anyhow!("Failed to rollback: {}", e))?;
        println!(
            "    {} Transaction rolled back - primary unchanged!",
            "↩️".yellow()
        );
        return Err(anyhow!(failure_reason));
    }

    // COMMIT on success
    primary_driver
        .commit()
        .await
        .map_err(|e| anyhow!("Failed to commit: {}", e))?;

    println!(
        "    {} {} migrations applied to primary",
        "✓".green(),
        diff_cmds.len()
    );

    // Drop shadow database
    println!("  [3/4] Dropping shadow database...");
    let mut admin_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, "postgres", &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, "postgres")
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    };

    let drop_db = Qail::drop_database(state.shadow_name.clone());
    admin_driver
        .execute(&drop_db)
        .await
        .map_err(|e| anyhow!("Failed to drop shadow: {}", e))?;
    println!("    {} Shadow database dropped", "✓".green());

    // Update state: promoted
    println!("  [4/4] Updating migration status...");
    update_shadow_state_status(&mut primary_driver, "promoted").await?;
    println!("    {} Status: promoted", "✓".green());

    println!();
    println!("{}", "✓ Shadow promoted successfully!".green().bold());
    println!("  Migration applied to: {}", database.cyan());
    println!("  Shadow {} dropped", state.shadow_name.dimmed());

    Ok(())
}

/// Abort shadow migration (drop shadow database)
pub async fn abort_shadow(primary_url: &str) -> Result<()> {
    let state = ShadowState::new(primary_url)?;

    println!();
    println!("{}", "🛑 Aborting Shadow Migration".red().bold());
    println!("{}", "━".repeat(40).dimmed());

    let (host, port, user, password, database) = parse_pg_url(primary_url)?;

    // Connect to postgres for admin operations
    let mut admin_driver = if let Some(pwd) = password.clone() {
        PgDriver::connect_with_password(&host, port, &user, "postgres", &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, "postgres")
            .await
            .map_err(|e| anyhow!("Failed to connect to postgres: {}", e))?
    };

    println!("  Dropping shadow database: {}", state.shadow_name.yellow());

    let drop_db = Qail::drop_database(state.shadow_name.clone());
    admin_driver
        .execute(&drop_db)
        .await
        .map_err(|e| anyhow!("Failed to drop shadow: {}", e))?;

    // Update state: aborted
    let mut primary_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    };

    let _ = update_shadow_state_status(&mut primary_driver, "aborted").await;

    println!(
        "{}",
        "✓ Shadow database dropped. Primary unchanged.".green()
    );

    Ok(())
}

pub async fn run_shadow_migration(
    primary_url: &str,
    old_cmds: &[Qail],
    diff_cmds: &[Qail],
    old_path: &str,
    new_path: &str,
) -> Result<ShadowState> {
    let mut state = create_shadow_database(primary_url).await?;

    // Step 1: Apply OLD schema to create base tables
    apply_base_schema_to_shadow(primary_url, &mut state, old_cmds).await?;

    // Step 2: Apply DIFF commands (migrations)
    apply_migrations_to_shadow(primary_url, &mut state, diff_cmds).await?;

    sync_data_to_shadow(primary_url, &mut state).await?;

    // Step 3: Save state for promote/abort (Enterprise feature)
    let (host, port, user, password, database) = parse_pg_url(primary_url)?;
    let mut primary_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    };

    save_shadow_state(&mut primary_driver, &state, diff_cmds, old_path, new_path).await?;

    state.is_ready = true;

    display_shadow_status(&state);

    Ok(state)
}

/// Run shadow migration with LIVE introspection (catches drift!)
/// Instead of using old.qail file, introspects the live primary database.
/// This fixes the "False Confidence" trap where file schema differs from production.
pub async fn run_shadow_migration_live(
    primary_url: &str,
    new_schema_path: &str,
) -> Result<ShadowState> {
    use qail_core::migrate::{diff_schemas_checked, parse_qail_file, schema_to_commands};

    println!();
    println!(
        "{}",
        "🔄 Shadow Migration Mode (Live Introspection)"
            .cyan()
            .bold()
    );
    println!("{}", "━".repeat(40).dimmed());

    // Step 0: Connect to primary and introspect live schema
    println!("  {} Introspecting live database schema...", "[0/4]".cyan());

    let (host, port, user, password, database) = parse_pg_url(primary_url)?;
    let mut primary_driver = if let Some(pwd) = password.clone() {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    };

    let live_schema = introspect_schema(&mut primary_driver).await?;
    println!(
        "    {} {} tables, {} indexes introspected",
        "✓".green(),
        live_schema.tables.len(),
        live_schema.indexes.len()
    );

    // Step 1: Parse new schema from file
    let new_schema = parse_qail_file(new_schema_path)
        .map_err(|e| anyhow!("Failed to parse new schema: {}", e))?;

    // Step 2: Generate diff between LIVE schema and new schema
    let old_cmds = schema_to_commands(&live_schema);
    let diff_cmds = diff_schemas_checked(&live_schema, &new_schema).map_err(|e| {
        anyhow!(
            "State-based diff unsupported for live shadow migration '{}': {}",
            new_schema_path,
            e
        )
    })?;

    println!(
        "    {} {} migration commands generated",
        "✓".green(),
        diff_cmds.len()
    );

    // Step 3: Create shadow database
    let mut state = create_shadow_database(primary_url).await?;

    // Step 4: Apply LIVE schema to shadow (not file schema!)
    apply_base_schema_to_shadow(primary_url, &mut state, &old_cmds).await?;

    // Step 5: Apply DIFF commands (migrations)
    apply_migrations_to_shadow(primary_url, &mut state, &diff_cmds).await?;

    // Step 6: Sync data
    sync_data_to_shadow(primary_url, &mut state).await?;

    // Step 7: Save state
    let mut primary_reconnect = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect to primary: {}", e))?
    };

    save_shadow_state(
        &mut primary_reconnect,
        &state,
        &diff_cmds,
        "[introspected]",
        new_schema_path,
    )
    .await?;

    state.is_ready = true;
    display_shadow_status(&state);

    Ok(state)
}

/// Apply base schema to shadow (CREATE TABLEs from old.qail)
async fn apply_base_schema_to_shadow(
    primary_url: &str,
    state: &mut ShadowState,
    cmds: &[Qail],
) -> Result<()> {
    println!("  {} Applying base schema to shadow...", "[1.5/4]".cyan());

    let (host, port, user, password, _) = parse_pg_url(primary_url)?;

    let mut shadow_driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &state.shadow_name, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &state.shadow_name)
            .await
            .map_err(|e| anyhow!("Failed to connect to shadow: {}", e))?
    };

    for (i, cmd) in cmds.iter().enumerate() {
        shadow_driver
            .execute(cmd)
            .await
            .map_err(|e| anyhow!("Base schema {} failed on shadow: {}", i + 1, e))?;
    }

    println!("    {} {} tables/indexes created", "✓".green(), cmds.len());

    Ok(())
}
