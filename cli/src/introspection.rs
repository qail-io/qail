//! Database Schema Introspection
//!
//! Extracts schema from live databases into QAIL format.
//! Uses purely AST-native queries via `Qail::get()` — zero raw SQL.

use anyhow::{Result, anyhow};
use crate::colors::*;
use qail_core::ast::{Operator, Qail};
use qail_core::migrate::{Column, Schema, Table, to_qail_string, parse_policy_expr};
use qail_core::migrate::schema::{ViewDef, SchemaFunctionDef, SchemaTriggerDef};
use qail_core::migrate::policy::{RlsPolicy, PolicyTarget, PolicyPermissiveness};
use qail_pg::driver::PgDriver;

use crate::util::parse_pg_url;

/// Output format for schema generation
#[derive(Clone, Default)]
pub enum SchemaOutputFormat {
    #[default]
    Qail,
}

pub async fn pull_schema(url_str: &str, _format: SchemaOutputFormat) -> Result<()> {
    println!("{} {}", "→ Connecting to:".dimmed(), url_str.yellow());

    let scheme = url_str.split("://").next().unwrap_or("");

    let schema = match scheme {
        "postgres" | "postgresql" => inspect_postgres(url_str).await?,
        "mysql" | "mariadb" => {
            return Err(anyhow!("MySQL introspection not yet migrated to qail-pg"));
        }
        _ => return Err(anyhow!("Unsupported database scheme: {}", scheme)),
    };

    // Always output .qail format now
    let qail = to_qail_string(&schema);
    std::fs::write("schema.qail", &qail)?;
    println!("{}", "✓ Schema synced to schema.qail".green().bold());
    println!("  Tables: {}", schema.tables.len());

    Ok(())
}

async fn inspect_postgres(url: &str) -> Result<Schema> {
    let (host, port, user, password, database) = parse_pg_url(url)?;

    let mut driver = if let Some(pwd) = password {
        PgDriver::connect_with_password(&host, port, &user, &database, &pwd)
            .await
            .map_err(|e| anyhow!("Failed to connect: {}", e))?
    } else {
        PgDriver::connect(&host, port, &user, &database)
            .await
            .map_err(|e| anyhow!("Failed to connect: {}", e))?
    };

    // ── 0. Enums (must be before columns to resolve enum column types) ──
    let enum_cmd = Qail::get("pg_catalog.pg_type")
        .columns(["typname", "oid"])
        .filter("typtype", Operator::Eq, "e");  // 'e' = enum type

    let enum_rows = driver
        .fetch_all(&enum_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query enum types: {}", e))?;

    let mut enum_types: Vec<qail_core::migrate::EnumType> = Vec::new();
    let mut enum_names: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for row in &enum_rows {
        let type_name = row.text(0);
        let oid = row.text(1);

        let values_cmd = Qail::get("pg_catalog.pg_enum")
            .columns(["enumlabel"])
            .filter("enumtypid", Operator::Eq, oid.clone());

        let val_rows = driver
            .fetch_all(&values_cmd)
            .await
            .map_err(|e| anyhow!("Failed to query enum values for {}: {}", type_name, e))?;

        let values: Vec<String> = val_rows.iter().map(|r| r.text(0)).collect();
        enum_names.insert(type_name.clone(), values.clone());
        enum_types.push(qail_core::migrate::EnumType {
            name: type_name,
            values,
        });
    }

    // ── 1. Columns + Defaults (AST-native) ──────────────────────────────
    let columns_cmd = Qail::get("information_schema.columns")
        .columns(["table_name", "column_name", "udt_name", "is_nullable", "column_default"])
        .filter("table_schema", Operator::Eq, "public");

    let rows = driver
        .fetch_all(&columns_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query columns: {}", e))?;

    let mut tables: std::collections::HashMap<String, Vec<Column>> =
        std::collections::HashMap::new();

    for row in rows {
        let table_name = row.text(0);
        let col_name = row.text(1);
        let udt_name = row.text(2);
        let is_nullable_str = row.text(3);
        let is_nullable = is_nullable_str == "YES";
        let column_default_raw = row.get_string(4);

        // Map PostgreSQL type to QAIL ColumnType
        // Check if this is a known enum type first
        let col_type = if let Some(values) = enum_names.get(&udt_name) {
            qail_core::migrate::ColumnType::Enum {
                name: udt_name.clone(),
                values: values.clone(),
            }
        } else {
            let col_type_str = map_pg_type(&udt_name);
            col_type_str
                .parse()
                .unwrap_or(qail_core::migrate::ColumnType::Text)
        };

        let mut col = Column::new(&col_name, col_type);
        col.nullable = is_nullable;

        // Parse default value (skip nextval sequences — those are serial types)
        if let Some(ref default_str) = column_default_raw {
            let d = default_str.trim();
            if !d.is_empty() && !d.starts_with("nextval(") {
                // Strip type casts like ::text, ::integer for cleaner output
                let clean = if let Some(pos) = d.find("::") {
                    d[..pos].trim().to_string()
                } else {
                    d.to_string()
                };
                // Strip surrounding single quotes from string literals
                let clean = if clean.starts_with('\'') && clean.ends_with('\'') {
                    format!("'{}'", &clean[1..clean.len()-1])
                } else {
                    clean
                };
                col.default = Some(clean);
            }
        }

        tables.entry(table_name).or_default().push(col);
    }

    // ── 2. Primary Keys (AST-native) ────────────────────────────────────
    let pk_cmd = Qail::get("information_schema.table_constraints")
        .columns(["table_name", "constraint_name", "constraint_type"])
        .filter("table_schema", Operator::Eq, "public")
        .filter("constraint_type", Operator::Eq, "PRIMARY KEY");

    let pk_rows = driver
        .fetch_all(&pk_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query primary keys: {}", e))?;

    let pk_constraint_names: std::collections::HashSet<String> = pk_rows
        .iter()
        .map(|r| r.text(1))  // constraint_name
        .collect();

    // ── 3. Key Column Usage (for PK + Unique + FK) (AST-native) ─────────
    let kcu_cmd = Qail::get("information_schema.key_column_usage")
        .columns(["table_name", "column_name", "constraint_name"])
        .filter("table_schema", Operator::Eq, "public");

    let kcu_rows = driver
        .fetch_all(&kcu_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query key columns: {}", e))?;

    let mut pk_columns: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();
    // Track constraint → columns for uniqueness and FK detection
    let mut constraint_columns: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new();
    for row in &kcu_rows {
        let table = row.text(0);
        let column = row.text(1);
        let constraint = row.text(2);

        if pk_constraint_names.contains(&constraint) {
            pk_columns.insert((table.clone(), column.clone()));
        }
        constraint_columns
            .entry(constraint)
            .or_default()
            .push((table, column));
    }

    // Mark primary key columns
    for (table_name, columns) in tables.iter_mut() {
        for col in columns.iter_mut() {
            if pk_columns.contains(&(table_name.clone(), col.name.clone())) {
                col.primary_key = true;
            }
        }
    }

    // ── 4. Unique Constraints (AST-native) ──────────────────────────────
    let unique_cmd = Qail::get("information_schema.table_constraints")
        .columns(["constraint_name", "table_name"])
        .filter("table_schema", Operator::Eq, "public")
        .filter("constraint_type", Operator::Eq, "UNIQUE");

    let unique_rows = driver
        .fetch_all(&unique_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query unique constraints: {}", e))?;

    let mut unique_columns: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();
    for row in unique_rows {
        let constraint_name = row.text(0);
        let table_name = row.text(1);
        // Look up which columns this constraint covers
        if let Some(cols) = constraint_columns.get(&constraint_name) {
            // Only mark single-column uniques on the column itself
            if cols.len() == 1 {
                unique_columns.insert((table_name, cols[0].1.clone()));
            }
        }
    }

    for (table_name, columns) in tables.iter_mut() {
        for col in columns.iter_mut() {
            if unique_columns.contains(&(table_name.clone(), col.name.clone())) {
                col.unique = true;
            }
        }
    }

    // ── 4b. CHECK Constraints (AST-native) ──────────────────────────────
    let check_cmd = Qail::get("information_schema.check_constraints")
        .columns(["constraint_name", "check_clause"])
        .filter("constraint_schema", Operator::Eq, "public");

    let check_rows = driver
        .fetch_all(&check_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query check constraints: {}", e))?;

    // Get constraint-to-column mapping
    let ccu_cmd = Qail::get("information_schema.constraint_column_usage")
        .columns(["table_name", "column_name", "constraint_name"])
        .filter("table_schema", Operator::Eq, "public");

    let ccu_rows = driver
        .fetch_all(&ccu_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query constraint column usage: {}", e))?;

    let mut check_column_map: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();
    for row in &ccu_rows {
        let table = row.text(0);
        let column = row.text(1);
        let constraint = row.text(2);
        check_column_map.insert(constraint, (table, column));
    }

    for row in &check_rows {
        let constraint_name = row.text(0);
        let check_clause = row.text(1);

        // Skip NOT NULL checks (auto-generated by PG)
        if check_clause.contains("IS NOT NULL") {
            continue;
        }

        if let Some((table_name, col_name)) = check_column_map.get(&constraint_name)
            && let Some(columns) = tables.get_mut(table_name.as_str())
            && let Some(expr) = parse_check_expr(&check_clause, col_name) {
                for col in columns.iter_mut() {
                    if col.name == *col_name {
                        col.check = Some(qail_core::migrate::CheckConstraint {
                            expr: expr.clone(),
                            name: Some(constraint_name.clone()),
                        });
                    }
                }
        }
    }

    // Get FK constraint names, referenced constraint, and ON DELETE/UPDATE rules
    let fk_ref_cmd = Qail::get("information_schema.referential_constraints")
        .columns(["constraint_name", "unique_constraint_name", "delete_rule", "update_rule"])
        .filter("constraint_schema", Operator::Eq, "public");

    let fk_ref_rows = driver
        .fetch_all(&fk_ref_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query foreign key refs: {}", e))?;

    // Map FK constraint_name → (referenced constraint_name, on_delete, on_update)
    let mut fk_to_ref: std::collections::HashMap<String, (String, qail_core::migrate::schema::FkAction, qail_core::migrate::schema::FkAction)> =
        std::collections::HashMap::new();
    for row in fk_ref_rows {
        let fk_constraint = row.text(0);
        let unique_constraint = row.text(1);
        let delete_rule = parse_fk_action(&row.text(2));
        let update_rule = parse_fk_action(&row.text(3));
        fk_to_ref.insert(fk_constraint, (unique_constraint, delete_rule, update_rule));
    }

    // ── 5b. Deferrable FK Detection ──────────────────────────────────────
    let defer_cmd = Qail::get("pg_catalog.pg_constraint")
        .columns(["conname", "condeferrable", "condeferred"])
        .filter("contype", Operator::Eq, "f");

    let defer_rows = driver
        .fetch_all(&defer_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query deferrable constraints: {}", e))?;

    let mut deferrable_map: std::collections::HashMap<String, qail_core::migrate::schema::Deferrable> =
        std::collections::HashMap::new();
    for row in defer_rows {
        let conname = row.text(0);
        let condeferrable = row.text(1) == "t";
        let condeferred = row.text(2) == "t";
        let status = if condeferred {
            qail_core::migrate::schema::Deferrable::InitiallyDeferred
        } else if condeferrable {
            qail_core::migrate::schema::Deferrable::Deferrable
        } else {
            qail_core::migrate::schema::Deferrable::NotDeferrable
        };
        deferrable_map.insert(conname, status);
    }

    // Resolve FK source column → referenced table.column with actions
    for (fk_constraint, (ref_constraint, on_delete, on_update)) in &fk_to_ref {
        let fk_cols = constraint_columns.get(fk_constraint.as_str());
        let ref_cols = constraint_columns.get(ref_constraint.as_str());

        if let (Some(fk_list), Some(ref_list)) = (fk_cols, ref_cols)
            && fk_list.len() == 1 && ref_list.len() == 1 {
                let (fk_table, fk_col) = &fk_list[0];
                let (ref_table, ref_col) = &ref_list[0];

                if let Some(columns) = tables.get_mut(fk_table.as_str()) {
                    for col in columns.iter_mut() {
                        if col.name == *fk_col {
                            // Look up deferrable status from pg_constraint
                            let def_status = deferrable_map.get(fk_constraint.as_str())
                                .cloned()
                                .unwrap_or(qail_core::migrate::schema::Deferrable::NotDeferrable);
                            col.foreign_key = Some(qail_core::migrate::ForeignKey {
                                table: ref_table.clone(),
                                column: ref_col.clone(),
                                on_delete: on_delete.clone(),
                                on_update: on_update.clone(),
                                deferrable: def_status,
                            });
                        }
                    }
                }
            }
    }

    // ── 6. RLS Status (AST-native) ──────────────────────────────────────
    let rls_cmd = Qail::get("pg_catalog.pg_class")
        .columns(["relname", "relrowsecurity", "relforcerowsecurity"])
        .filter("relkind", Operator::Eq, "r");  // 'r' = ordinary table

    let rls_rows = driver
        .fetch_all(&rls_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query RLS status: {}", e))?;

    let mut rls_map: std::collections::HashMap<String, (bool, bool)> =
        std::collections::HashMap::new();
    for row in rls_rows {
        let table_name = row.text(0);
        let enable_rls = row.text(1) == "t";
        let force_rls = row.text(2) == "t";
        if enable_rls || force_rls {
            rls_map.insert(table_name, (enable_rls, force_rls));
        }
    }

    // ── 7. Indexes (AST-native) ─────────────────────────────────────────
    let idx_cmd = Qail::get("pg_indexes")
        .columns(["indexname", "tablename", "indexdef"])
        .filter("schemaname", Operator::Eq, "public");

    let index_rows = driver
        .fetch_all(&idx_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query indexes: {}", e))?;


    // ── 9. Extensions (AST-native) ──────────────────────────────────────
    let ext_cmd = Qail::get("pg_catalog.pg_extension")
        .columns(["extname", "extversion"]);

    let ext_rows = driver
        .fetch_all(&ext_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query extensions: {}", e))?;

    let mut extensions: Vec<qail_core::migrate::Extension> = Vec::new();
    for row in ext_rows {
        let name = row.text(0);
        // Skip built-in plpgsql extension
        if name == "plpgsql" {
            continue;
        }
        let version = row.get_string(1);
        extensions.push(qail_core::migrate::Extension {
            name,
            schema: None,
            version,
        });
    }

    // ── 10. Sequences (AST-native) ──────────────────────────────────────
    let seq_cmd = Qail::get("information_schema.sequences")
        .columns(["sequence_name", "start_value", "increment", "minimum_value", "maximum_value"])
        .filter("sequence_schema", Operator::Eq, "public");

    let seq_rows = driver
        .fetch_all(&seq_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query sequences: {}", e))?;

    let mut sequences: Vec<qail_core::migrate::Sequence> = Vec::new();
    for row in seq_rows {
        let name = row.text(0);
        // Skip sequences owned by serial columns (auto-generated)
        if name.ends_with("_seq") {
            continue;
        }
        let start = row.get_string(1).and_then(|s| s.parse::<i64>().ok());
        let increment = row.get_string(2).and_then(|s| s.parse::<i64>().ok());
        let min_value = row.get_string(3).and_then(|s| s.parse::<i64>().ok());
        let max_value = row.get_string(4).and_then(|s| s.parse::<i64>().ok());

        sequences.push(qail_core::migrate::Sequence {
            name,
            data_type: None,
            start,
            increment,
            min_value,
            max_value,
            cache: None,
            cycle: false,
            owned_by: None,
        });
    }

    // ── 11. Views (AST-native) ───────────────────────────────────────────
    let view_cmd = Qail::get("pg_views")
        .columns(["viewname", "definition"])
        .filter("schemaname", Operator::Eq, "public");

    let view_rows = driver
        .fetch_all(&view_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query views: {}", e))?;

    let mut views: Vec<ViewDef> = Vec::new();
    for row in view_rows {
        let name = row.text(0);
        let query = row.text(1).trim().trim_end_matches(';').to_string();
        views.push(ViewDef::new(&name, query));
    }

    // ── 12. Materialized Views (AST-native) ─────────────────────────────
    let matview_cmd = Qail::get("pg_matviews")
        .columns(["matviewname", "definition"])
        .filter("schemaname", Operator::Eq, "public");

    let matview_rows = driver
        .fetch_all(&matview_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query materialized views: {}", e))?;

    for row in matview_rows {
        let name = row.text(0);
        let query = row.text(1).trim().trim_end_matches(';').to_string();
        views.push(ViewDef::new(&name, query).materialized());
    }

    // ── 13. Functions (AST-native) ────────────────────────────────────────
    // 13a: Function metadata from information_schema.routines
    let routine_cmd = Qail::get("information_schema.routines")
        .columns(["routine_name", "specific_name", "routine_definition", "external_language", "data_type"])
        .filter("routine_schema", Operator::Eq, "public")
        .filter("routine_type", Operator::Eq, "FUNCTION");

    let routine_rows = driver
        .fetch_all(&routine_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query routines: {}", e))?;

    // 13b: Function parameters from information_schema.parameters
    let param_cmd = Qail::get("information_schema.parameters")
        .columns(["specific_name", "parameter_name", "udt_name", "parameter_mode", "ordinal_position"])
        .filter("specific_schema", Operator::Eq, "public");

    let param_rows = driver
        .fetch_all(&param_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query parameters: {}", e))?;

    // Build parameter map: specific_name → [(ordinal, "name type")]
    let mut param_map: std::collections::HashMap<String, Vec<(i32, String)>> =
        std::collections::HashMap::new();
    for row in param_rows {
        let specific = row.text(0);
        let pname = row.text(1);
        let ptype = row.text(2);
        let mode = row.text(3);
        let ordinal: i32 = row.get_string(4).and_then(|s| s.parse().ok()).unwrap_or(0);

        // Only include IN parameters (skip OUT/INOUT for now)
        if mode != "IN" { continue; }

        let arg_str = if pname.is_empty() {
            ptype.clone()
        } else {
            format!("{} {}", pname, ptype)
        };

        param_map.entry(specific).or_default().push((ordinal, arg_str));
    }

    // 13c: Volatility from pg_proc (AST-native — no function calls needed)
    let vol_cmd = Qail::get("pg_catalog.pg_proc")
        .columns(["proname", "provolatile"])
        .filter("pronamespace", Operator::Eq, "(SELECT oid FROM pg_namespace WHERE nspname = 'public')");

    let vol_rows = driver
        .fetch_all(&vol_cmd)
        .await;

    let mut volatility_map: std::collections::HashMap<String, Option<String>> =
        std::collections::HashMap::new();
    if let Ok(rows) = vol_rows {
        for row in rows {
            let name = row.text(0);
            let vol = match row.text(1).as_str() {
                "i" => Some("immutable".to_string()),
                "s" => Some("stable".to_string()),
                _ => None,
            };
            volatility_map.insert(name, vol);
        }
    }

    // Build functions
    let mut functions: Vec<SchemaFunctionDef> = Vec::new();
    for row in routine_rows {
        let name = row.text(0);
        let specific = row.text(1);
        let body = row.get_string(2).unwrap_or_default().trim().to_string();
        let language = row.text(3);
        let returns = row.text(4);

        // Skip functions without bodies (e.g. C functions)
        if body.is_empty() { continue; }

        // Assemble sorted args from param_map
        let args = if let Some(mut params) = param_map.remove(&specific) {
            params.sort_by_key(|(ord, _)| *ord);
            params.into_iter().map(|(_, s)| s).collect()
        } else {
            Vec::new()
        };

        let volatility = volatility_map.get(&name).cloned().flatten();

        let mut func = SchemaFunctionDef::new(&name, &returns, body);
        func.language = language;
        func.args = args;
        func.volatility = volatility;
        functions.push(func);
    }

    // ── 14. Triggers (AST-native) ───────────────────────────────────────
    let trig_cmd = Qail::get("information_schema.triggers")
        .columns(["trigger_name", "event_object_table", "action_timing", "event_manipulation", "action_statement"])
        .filter("trigger_schema", Operator::Eq, "public");

    let trig_rows = driver
        .fetch_all(&trig_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query triggers: {}", e))?;

    // Group by (trigger_name, table) since each event is a separate row
    let mut trigger_map: std::collections::HashMap<(String, String), (String, Vec<String>, String)> =
        std::collections::HashMap::new();
    for row in trig_rows {
        let name = row.text(0);
        let table = row.text(1);
        let timing = row.text(2);
        let event = row.text(3);
        let action = row.text(4);

        let entry = trigger_map
            .entry((name, table))
            .or_insert_with(|| (timing, Vec::new(), action));
        entry.1.push(event);
    }

    let mut triggers: Vec<SchemaTriggerDef> = Vec::new();
    for ((name, table), (timing, events, action_stmt)) in trigger_map {
        // Extract function name from "EXECUTE FUNCTION func_name()" or "EXECUTE PROCEDURE func_name()"
        let exec_fn = action_stmt
            .replace("EXECUTE FUNCTION ", "")
            .replace("EXECUTE PROCEDURE ", "")
            .trim_end_matches("()")
            .trim()
            .to_string();

        let mut trig = SchemaTriggerDef::new(&name, &table, &exec_fn);
        trig.timing = timing;
        trig.events = events;
        triggers.push(trig);
    }

    // ── 15. RLS Policies (AST-native) ──────────────────────────────────
    let policy_cmd = Qail::get("pg_policies")
        .columns(["policyname", "tablename", "cmd", "permissive", "roles", "qual", "with_check"])
        .filter("schemaname", Operator::Eq, "public");

    let policy_rows = driver
        .fetch_all(&policy_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query RLS policies: {}", e))?;


    let mut policies: Vec<RlsPolicy> = Vec::new();
    for row in policy_rows {
        let name = row.text(0);
        let table = row.text(1);
        let cmd_str = row.text(2);
        let permissive_str = row.text(3);  // "PERMISSIVE" or "RESTRICTIVE"
        let roles_str = row.text(4);       // e.g. "{app_user}" or "{public}"
        let qual = row.get_string(5);      // USING expression (raw SQL)
        let with_check = row.get_string(6); // WITH CHECK expression (raw SQL)


        let target = match cmd_str.as_str() {
            "ALL" => PolicyTarget::All,
            "SELECT" => PolicyTarget::Select,
            "INSERT" => PolicyTarget::Insert,
            "UPDATE" => PolicyTarget::Update,
            "DELETE" => PolicyTarget::Delete,
            _ => PolicyTarget::All,
        };

        let permissiveness = if permissive_str == "RESTRICTIVE" {
            PolicyPermissiveness::Restrictive
        } else {
            PolicyPermissiveness::Permissive
        };

        // Parse roles: "{app_user}" → Some("app_user"), "{public}" → None
        let role = {
            let r = roles_str.trim_matches(|c| c == '{' || c == '}');
            if r.eq_ignore_ascii_case("public") {
                None
            } else {
                Some(r.to_string())
            }
        };

        let using_expr = qual.map(|s| parse_policy_expr(&s));
        let with_check_expr = with_check.map(|s| parse_policy_expr(&s));

        let mut policy = RlsPolicy::create(&name, &table);
        policy.target = target;
        policy.permissiveness = permissiveness;
        policy.role = role;
        policy.using = using_expr;
        policy.with_check = with_check_expr;
        policies.push(policy);
    }

    // ── Build Schema ────────────────────────────────────────────────────
    let mut schema = Schema::new();
    schema.enums = enum_types;
    schema.extensions = extensions;
    schema.sequences = sequences;
    schema.views = views;
    schema.functions = functions;
    schema.triggers = triggers;
    schema.policies = policies;

    for (name, columns) in tables {
        let mut table = Table::new(&name);
        table.columns = columns;
        // Apply RLS status
        if let Some((enable, force)) = rls_map.get(&name) {
            table.enable_rls = *enable;
            table.force_rls = *force;
        }
        schema.add_table(table);
    }

    for row in index_rows {
        let name = row.text(0);
        let table = row.text(1);
        let def = row.text(2);

        // Skip primary key and unique constraint indexes (already captured)
        if name.ends_with("_pkey") || name.ends_with("_key") {
            continue;
        }

        let is_unique = def.to_uppercase().contains("UNIQUE");
        let cols = parse_index_columns(&def);

        let mut index = qail_core::migrate::Index::new(&name, &table, cols);
        if is_unique {
            index.unique = true;
        }
        schema.add_index(index);
    }

    Ok(schema)
}

fn map_pg_type(udt_name: &str) -> &'static str {
    match udt_name {
        "int4" => "int",
        "int8" | "bigint" => "bigint",
        "serial" => "serial",
        "bigserial" => "bigserial",
        "float4" | "float8" | "numeric" => "float",
        "bool" => "bool",
        "json" | "jsonb" => "jsonb",
        "timestamp" => "timestamp",
        "timestamptz" => "timestamptz",
        "date" => "date",
        "uuid" => "uuid",
        "text" => "text",
        "varchar" | "character varying" => "varchar",
        _ => "text",
    }
}

fn parse_index_columns(def: &str) -> Vec<String> {
    if let Some(start) = def.rfind('(')
        && let Some(end) = def.rfind(')')
    {
        let cols_str = &def[start + 1..end];
        return cols_str.split(',').map(|s| s.trim().to_string()).collect();
    }
    vec![]
}

/// Map information_schema FK rule string to FkAction enum
fn parse_fk_action(rule: &str) -> qail_core::migrate::schema::FkAction {
    match rule {
        "CASCADE" => qail_core::migrate::schema::FkAction::Cascade,
        "SET NULL" => qail_core::migrate::schema::FkAction::SetNull,
        "SET DEFAULT" => qail_core::migrate::schema::FkAction::SetDefault,
        "RESTRICT" => qail_core::migrate::schema::FkAction::Restrict,
        _ => qail_core::migrate::schema::FkAction::NoAction,
    }
}

/// Parse a PostgreSQL check constraint expression into a CheckExpr.
/// Handles patterns like:
///   ((age >= 0) AND (age <= 200))  → Between
///   ((score >= 0))                 → GreaterOrEqual
///   ((col > 0))                    → GreaterThan
fn parse_check_expr(
    clause: &str,
    _col_name: &str,
) -> Option<qail_core::migrate::schema::CheckExpr> {
    use qail_core::migrate::schema::CheckExpr;

    // Strip outer parens and whitespace
    let s = clause.replace(['(', ')'], "").trim().to_string();

    // Try BETWEEN-style: "col >= low AND col <= high"
    if let Some(and_pos) = s.find(" AND ") {
        let left = s[..and_pos].trim();
        let right = s[and_pos + 5..].trim();

        if let (Some(l), Some(r)) = (parse_simple_cmp(left), parse_simple_cmp(right)) {
            // col >= low AND col <= high → Between
            if l.0 == r.0
                && matches!(l.1, CmpOp::Gte)
                && matches!(r.1, CmpOp::Lte)
            {
                return Some(CheckExpr::Between {
                    column: l.0,
                    low: l.2,
                    high: r.2,
                });
            }
            // col >= low AND col <= high but reversed
            if l.0 == r.0
                && matches!(l.1, CmpOp::Lte)
                && matches!(r.1, CmpOp::Gte)
            {
                return Some(CheckExpr::Between {
                    column: l.0,
                    low: r.2,
                    high: l.2,
                });
            }
            // Two separate comparisons on same column → AND
            if l.0 == r.0 {
                let left_expr = cmp_to_check_expr(l);
                let right_expr = cmp_to_check_expr(r);
                if let (Some(le), Some(re)) = (left_expr, right_expr) {
                    return Some(CheckExpr::And(Box::new(le), Box::new(re)));
                }
            }
        }
    }

    // Single comparison: "col >= val", "col > val", "col <= val", "col < val"
    if let Some(cmp) = parse_simple_cmp(&s) {
        return cmp_to_check_expr(cmp);
    }

    None
}

#[derive(Debug)]
enum CmpOp {
    Gte,
    Gt,
    Lte,
    Lt,
}

fn parse_simple_cmp(s: &str) -> Option<(String, CmpOp, i64)> {
    // Try >=, <=, >, < in order (longer first)
    let ops: &[(&str, CmpOp)] = &[
        (">=", CmpOp::Gte),
        ("<=", CmpOp::Lte),
        (">", CmpOp::Gt),
        ("<", CmpOp::Lt),
    ];

    for (op_str, op) in ops {
        if let Some(pos) = s.find(op_str) {
            let col = s[..pos].trim().to_string();
            let val_str = s[pos + op_str.len()..].trim();
            // Strip type casts like ::numeric, ::integer
            let val_clean = if let Some(cast_pos) = val_str.find("::") {
                val_str[..cast_pos].trim()
            } else {
                val_str
            };
            if let Ok(val) = val_clean.parse::<i64>() {
                return Some((col, match op {
                    CmpOp::Gte => CmpOp::Gte,
                    CmpOp::Gt => CmpOp::Gt,
                    CmpOp::Lte => CmpOp::Lte,
                    CmpOp::Lt => CmpOp::Lt,
                }, val));
            }
        }
    }
    None
}

fn cmp_to_check_expr(
    (col, op, val): (String, CmpOp, i64),
) -> Option<qail_core::migrate::schema::CheckExpr> {
    use qail_core::migrate::schema::CheckExpr;
    match op {
        CmpOp::Gte => Some(CheckExpr::GreaterOrEqual { column: col, value: val }),
        CmpOp::Gt => Some(CheckExpr::GreaterThan { column: col, value: val }),
        CmpOp::Lte => Some(CheckExpr::LessOrEqual { column: col, value: val }),
        CmpOp::Lt => Some(CheckExpr::LessThan { column: col, value: val }),
    }
}

