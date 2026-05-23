//! Database Schema Introspection
//!
//! Extracts schema from live databases into QAIL format.
//! Uses purely AST-native queries via `Qail::get()` — zero raw SQL.

use crate::colors::*;
use anyhow::{Result, anyhow};
use qail_core::ast::{Operator, Qail};
use qail_core::migrate::policy::{PolicyPermissiveness, PolicyTarget, RlsPolicy};
use qail_core::migrate::schema::{Deferrable, FkAction};
use qail_core::migrate::schema::{SchemaFunctionDef, SchemaTriggerDef, ViewDef};
use qail_core::migrate::{
    Column, ForeignKey, Generated, Index, MultiColumnForeignKey, Schema, Table, to_qail_string,
};
use qail_pg::driver::PgDriver;

use crate::util::{parse_pg_url, redact_url};

#[derive(Debug, Clone)]
pub(crate) struct IntrospectedKeyColumn {
    pub table: String,
    pub column: String,
    ordinal_position: i32,
}

impl IntrospectedKeyColumn {
    pub(crate) fn new(table: String, column: String, ordinal_position: i32) -> Self {
        Self {
            table,
            column,
            ordinal_position,
        }
    }
}

pub(crate) enum IntrospectedForeignKey {
    Single {
        table: String,
        column: String,
        foreign_key: ForeignKey,
    },
    Multi {
        table: String,
        foreign_key: MultiColumnForeignKey,
    },
}

pub(crate) enum IntrospectedUniqueConstraint {
    Single { table: String, column: String },
    Multi(Index),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct IntrospectedConstraintIdentity {
    pub table: String,
    pub name: String,
}

impl IntrospectedConstraintIdentity {
    pub(crate) fn new(table: String, name: String) -> Self {
        Self { table, name }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct IntrospectedForeignKeyReference {
    pub constraint: IntrospectedConstraintIdentity,
    pub referenced_constraint: IntrospectedConstraintIdentity,
    pub on_delete: FkAction,
    pub on_update: FkAction,
    pub deferrable: Deferrable,
}

/// Output format for schema generation
#[derive(Clone, Default)]
pub enum SchemaOutputFormat {
    #[default]
    Qail,
}

pub async fn pull_schema(url_str: &str, _format: SchemaOutputFormat) -> Result<()> {
    println!(
        "{} {}",
        "→ Connecting to:".dimmed(),
        redact_url(url_str).yellow()
    );

    let scheme = url_str.split("://").next().unwrap_or("");

    let schema = match scheme {
        "postgres" | "postgresql" => inspect_postgres(url_str).await?,
        _ => {
            return Err(anyhow!(
                "Unsupported database scheme: {} (CLI introspection supports postgres:// and postgresql:// only)",
                scheme
            ));
        }
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

    // Resolve public namespace OID once; OID columns cannot be filtered with subquery text.
    let public_ns_cmd = Qail::get("pg_catalog.pg_namespace")
        .columns(["oid"])
        .filter("nspname", Operator::Eq, "public");
    let public_ns_rows = driver
        .fetch_all(&public_ns_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query public namespace OID: {}", e))?;
    let public_namespace_oid = public_ns_rows
        .first()
        .map(|r| r.text(0))
        .ok_or_else(|| anyhow!("Public schema not found in pg_namespace"))?;

    // ── 0. Enums (must be before columns to resolve enum column types) ──
    let enum_cmd = Qail::get("pg_catalog.pg_type")
        .columns(["typname", "oid"])
        .filter("typtype", Operator::Eq, "e"); // 'e' = enum type

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

    // ── 0b. Base Tables (exclude views/materialized views) ──────────────
    let base_tables_cmd = Qail::get("information_schema.tables")
        .columns(["table_name", "table_type"])
        .filter("table_schema", Operator::Eq, "public");
    let base_table_rows = driver
        .fetch_all(&base_tables_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query table list: {}", e))?;
    let mut base_tables: std::collections::HashSet<String> = std::collections::HashSet::new();
    for row in base_table_rows {
        let table_name = row.text(0);
        let table_type = row.text(1);
        if table_type.eq_ignore_ascii_case("BASE TABLE") && !is_internal_qail_relation(&table_name)
        {
            base_tables.insert(table_name);
        }
    }

    // ── 1. Columns + Defaults (AST-native) ──────────────────────────────
    let columns_cmd = Qail::get("information_schema.columns")
        .columns([
            "table_name",
            "column_name",
            "udt_name",
            "data_type",
            "character_maximum_length",
            "numeric_precision",
            "numeric_scale",
            "is_nullable",
            "column_default",
            "is_identity",
            "identity_generation",
            "is_generated",
            "generation_expression",
        ])
        .filter("table_schema", Operator::Eq, "public");

    let rows = driver
        .fetch_all(&columns_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query columns: {}", e))?;

    let mut tables: std::collections::HashMap<String, Vec<Column>> =
        std::collections::HashMap::new();

    for row in rows {
        let table_name = row.text(0);
        if !base_tables.contains(&table_name) {
            continue;
        }
        let col_name = row.text(1);
        let udt_name = row.text(2);
        let data_type = row.text(3);
        let char_max_len = row.get_string(4);
        let numeric_precision = row.get_string(5);
        let numeric_scale = row.get_string(6);
        let is_nullable_str = row.text(7);
        let is_nullable = is_nullable_str == "YES";
        let column_default_raw = row.get_string(8);
        let is_identity = row.get_string(9).is_some_and(|s| s == "YES");
        let identity_generation = row.get_string(10);
        let is_generated = row.get_string(11);
        let generation_expression = row.get_string(12);

        let is_nextval_default = column_default_raw
            .as_deref()
            .map(|d| d.trim_start().starts_with("nextval("))
            .unwrap_or(false);
        let col_type = map_pg_column_type(
            &udt_name,
            &data_type,
            char_max_len.as_deref(),
            numeric_precision.as_deref(),
            numeric_scale.as_deref(),
            is_nextval_default,
            &enum_names,
        );

        let mut col = Column::new(&col_name, col_type);
        col.nullable = is_nullable;
        col.generated = introspected_column_generation(
            is_identity,
            identity_generation.as_deref(),
            is_generated.as_deref(),
            generation_expression.as_deref(),
        );

        // Parse default value (skip nextval sequences — those are serial types)
        if let Some(ref default_str) = column_default_raw {
            let d = default_str.trim();
            if !d.is_empty() {
                // For serial/bigserial we intentionally omit explicit nextval()
                // because type already implies sequence-backed default.
                if col.generated.is_none()
                    && !(d.starts_with("nextval(")
                        && matches!(
                            col.data_type,
                            qail_core::migrate::ColumnType::Serial
                                | qail_core::migrate::ColumnType::BigSerial
                        ))
                {
                    // Keep default expression as-is to preserve casts/functions.
                    col.default = Some(d.to_string());
                }
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
        .map(|r| r.text(1)) // constraint_name
        .collect();

    // ── 3. Key Column Usage (for PK + Unique + FK) (AST-native) ─────────
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

    let mut pk_columns: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();
    // Track constraint → columns for uniqueness and FK detection
    let mut constraint_columns: std::collections::HashMap<String, Vec<IntrospectedKeyColumn>> =
        std::collections::HashMap::new();
    let mut qualified_constraint_columns: std::collections::HashMap<
        IntrospectedConstraintIdentity,
        Vec<IntrospectedKeyColumn>,
    > = std::collections::HashMap::new();
    for row in &kcu_rows {
        let table = row.text(0);
        let column = row.text(1);
        let constraint = row.text(2);
        let ordinal_position = row
            .get_string(3)
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);

        if pk_constraint_names.contains(&constraint) {
            pk_columns.insert((table.clone(), column.clone()));
        }
        constraint_columns
            .entry(constraint.clone())
            .or_default()
            .push(IntrospectedKeyColumn::new(
                table.clone(),
                column.clone(),
                ordinal_position,
            ));
        qualified_constraint_columns
            .entry(IntrospectedConstraintIdentity::new(
                table.clone(),
                constraint,
            ))
            .or_default()
            .push(IntrospectedKeyColumn::new(table, column, ordinal_position));
    }
    sort_introspected_key_columns(&mut constraint_columns);
    sort_qualified_introspected_key_columns(&mut qualified_constraint_columns);

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
    let mut unique_constraint_indexes = Vec::new();
    for row in unique_rows {
        let constraint_name = row.text(0);
        let table_name = row.text(1);
        if !base_tables.contains(&table_name) {
            continue;
        }
        // Look up which columns this constraint covers
        if let Some(cols) = constraint_columns.get(&constraint_name)
            && let Some(unique) =
                resolve_introspected_unique_constraint(&constraint_name, &table_name, cols)
        {
            match unique {
                IntrospectedUniqueConstraint::Single { table, column } => {
                    unique_columns.insert((table, column));
                }
                IntrospectedUniqueConstraint::Multi(index) => unique_constraint_indexes.push(index),
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

    let check_table_cmd = Qail::get("information_schema.table_constraints")
        .columns(["constraint_name", "table_name"])
        .filter("table_schema", Operator::Eq, "public")
        .filter("constraint_type", Operator::Eq, "CHECK");
    let check_table_rows = driver
        .fetch_all(&check_table_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query check constraint table mapping: {}", e))?;
    let mut check_table_map: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for row in check_table_rows {
        check_table_map.insert(row.text(0), row.text(1));
    }

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

        // Skip trivial auto-generated NOT NULL checks only.
        if is_trivial_not_null_check(&check_clause) {
            continue;
        }

        let mut applied = false;
        if let Some((table_name, col_name)) = check_column_map.get(&constraint_name)
            && let Some(columns) = tables.get_mut(table_name.as_str())
            && let Some(expr) = parse_check_expr(&check_clause, col_name)
        {
            for col in columns.iter_mut() {
                if col.name == *col_name {
                    col.check = Some(qail_core::migrate::CheckConstraint {
                        expr: expr.clone(),
                        name: Some(constraint_name.clone()),
                    });
                    applied = true;
                }
            }
        }

        if !applied
            && let Some(table_name) = check_table_map.get(&constraint_name)
            && let Some(columns) = tables.get_mut(table_name.as_str())
            && let Some(col) = columns.iter_mut().find(|c| c.check.is_none())
        {
            col.check = Some(qail_core::migrate::CheckConstraint {
                expr: qail_core::migrate::schema::CheckExpr::Sql(check_clause.clone()),
                name: Some(constraint_name.clone()),
            });
        }
    }

    // Get FK constraint names and referenced constraints. Actions come from
    // pg_constraint below because this view is keyed by bare constraint name.
    let fk_ref_cmd = Qail::get("information_schema.referential_constraints")
        .columns(["constraint_name", "unique_constraint_name"])
        .filter("constraint_schema", Operator::Eq, "public");

    let fk_ref_rows = driver
        .fetch_all(&fk_ref_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query foreign key refs: {}", e))?;

    // Map bare FK constraint name → candidate referenced constraints. PostgreSQL
    // allows duplicate FK names on different tables, so source table identity is
    // added from pg_constraint before resolving.
    let mut fk_ref_candidates: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for row in fk_ref_rows {
        let fk_constraint = row.text(0);
        let unique_constraint = row.text(1);
        fk_ref_candidates
            .entry(fk_constraint)
            .or_default()
            .push(unique_constraint);
    }

    // ── 5b. Qualified FK source/reference tables + deferrability ─────────
    let fk_catalog_cmd = Qail::get(
        "pg_catalog.pg_constraint con \
         JOIN pg_catalog.pg_class src ON src.oid = con.conrelid \
         JOIN pg_catalog.pg_class ref ON ref.oid = con.confrelid",
    )
    .columns([
        "con.conname",
        "src.relname",
        "ref.relname",
        "con.condeferrable",
        "con.condeferred",
        "con.confdeltype",
        "con.confupdtype",
    ])
    .filter("con.contype", Operator::Eq, "f")
    .filter(
        "con.connamespace",
        Operator::Eq,
        public_namespace_oid.clone(),
    );

    let fk_catalog_rows = driver
        .fetch_all(&fk_catalog_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query FK constraint metadata: {}", e))?;

    let mut fk_references = Vec::new();
    for row in fk_catalog_rows {
        let constraint_name = row.text(0);
        let source_table = row.text(1);
        let ref_table = row.text(2);
        if !base_tables.contains(&source_table) {
            continue;
        }
        let condeferrable = row.text(3) == "t";
        let condeferred = row.text(4) == "t";
        let on_delete = parse_pg_constraint_fk_action(&row.text(5));
        let on_update = parse_pg_constraint_fk_action(&row.text(6));
        let status = if condeferred {
            qail_core::migrate::schema::Deferrable::InitiallyDeferred
        } else if condeferrable {
            qail_core::migrate::schema::Deferrable::Deferrable
        } else {
            qail_core::migrate::schema::Deferrable::NotDeferrable
        };

        let Some(candidates) = fk_ref_candidates.get(&constraint_name) else {
            continue;
        };
        let Some(ref_constraint) = candidates.iter().find(|ref_constraint| {
            qualified_constraint_columns.contains_key(&IntrospectedConstraintIdentity::new(
                ref_table.clone(),
                (*ref_constraint).clone(),
            ))
        }) else {
            continue;
        };

        fk_references.push(IntrospectedForeignKeyReference {
            constraint: IntrospectedConstraintIdentity::new(source_table, constraint_name),
            referenced_constraint: IntrospectedConstraintIdentity::new(
                ref_table,
                ref_constraint.clone(),
            ),
            on_delete,
            on_update,
            deferrable: status,
        });
    }

    let mut table_multi_column_fks: std::collections::HashMap<String, Vec<MultiColumnForeignKey>> =
        std::collections::HashMap::new();

    // Resolve FK source column → referenced table.column with actions
    for fk_reference in &fk_references {
        match resolve_qualified_introspected_foreign_key(
            fk_reference,
            &qualified_constraint_columns,
        ) {
            Some(IntrospectedForeignKey::Single {
                table,
                column,
                foreign_key,
            }) => {
                if let Some(columns) = tables.get_mut(table.as_str()) {
                    for col in columns.iter_mut() {
                        if col.name == column {
                            col.foreign_key = Some(foreign_key.clone());
                        }
                    }
                }
            }
            Some(IntrospectedForeignKey::Multi { table, foreign_key }) => {
                table_multi_column_fks
                    .entry(table)
                    .or_default()
                    .push(foreign_key);
            }
            None => {}
        }
    }

    // ── 6. RLS Status (AST-native) ──────────────────────────────────────
    let rls_cmd = Qail::get("pg_catalog.pg_class")
        .columns(["relname", "relrowsecurity", "relforcerowsecurity"])
        .filter("relkind", Operator::Eq, "r"); // 'r' = ordinary table

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

    // Index OID -> name map (public schema)
    let idx_class_cmd = Qail::get("pg_catalog.pg_class")
        .columns(["oid", "relname"])
        .filter("relkind", Operator::Eq, "i")
        .filter("relnamespace", Operator::Eq, public_namespace_oid.clone());
    let idx_class_rows = driver
        .fetch_all(&idx_class_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query index class metadata: {}", e))?;
    let mut index_oid_to_name: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for row in idx_class_rows {
        index_oid_to_name.insert(row.text(0), row.text(1));
    }

    // Constraint-backed index names (PK/UNIQUE/EXCLUSION) should not be
    // re-emitted as plain indexes; those are represented by constraints.
    let conidx_cmd = Qail::get("pg_catalog.pg_constraint")
        .columns(["conindid", "contype"])
        .filter("connamespace", Operator::Eq, public_namespace_oid.clone());
    let conidx_rows = driver
        .fetch_all(&conidx_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query constraint index metadata: {}", e))?;
    let mut constraint_index_names: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for row in conidx_rows {
        let conindid = row.text(0);
        let contype = row.text(1);
        if matches!(contype.as_str(), "p" | "u" | "x")
            && conindid != "0"
            && let Some(name) = index_oid_to_name.get(&conindid)
        {
            constraint_index_names.insert(name.clone());
        }
    }

    // ── 9. Extensions (AST-native) ──────────────────────────────────────
    let ext_cmd = Qail::get("pg_catalog.pg_extension").columns(["extname", "extversion"]);

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
        .columns([
            "sequence_name",
            "start_value",
            "increment",
            "minimum_value",
            "maximum_value",
        ])
        .filter("sequence_schema", Operator::Eq, "public");

    let seq_rows = driver
        .fetch_all(&seq_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query sequences: {}", e))?;

    // Sequence OID map for ownership detection.
    let seq_class_cmd = Qail::get("pg_catalog.pg_class")
        .columns(["oid", "relname"])
        .filter("relkind", Operator::Eq, "S")
        .filter("relnamespace", Operator::Eq, public_namespace_oid.clone());
    let seq_class_rows = driver
        .fetch_all(&seq_class_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query sequence class metadata: {}", e))?;
    let mut seq_name_to_oid: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for row in seq_class_rows {
        seq_name_to_oid.insert(row.text(1), row.text(0));
    }

    // deptype='a' marks auto dependency (owned by table column / serial identity).
    let dep_cmd = Qail::get("pg_catalog.pg_depend").columns(["objid", "deptype"]);
    let dep_rows = driver
        .fetch_all(&dep_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query sequence dependencies: {}", e))?;
    let mut owned_sequence_oids: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    for row in dep_rows {
        if row.text(1) == "a" {
            owned_sequence_oids.insert(row.text(0));
        }
    }

    let mut sequences: Vec<qail_core::migrate::Sequence> = Vec::new();
    for row in seq_rows {
        let name = row.text(0);
        if is_internal_qail_relation(&name) {
            continue;
        }
        // Skip sequences owned by table columns (auto-generated serial/identity).
        if let Some(oid) = seq_name_to_oid.get(&name)
            && owned_sequence_oids.contains(oid)
        {
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
        .columns([
            "routine_name",
            "specific_name",
            "routine_definition",
            "external_language",
            "data_type",
        ])
        .filter("routine_schema", Operator::Eq, "public")
        .filter("routine_type", Operator::Eq, "FUNCTION");

    let routine_rows = driver
        .fetch_all(&routine_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query routines: {}", e))?;

    // 13b: Function parameters from information_schema.parameters
    let param_cmd = Qail::get("information_schema.parameters")
        .columns([
            "specific_name",
            "parameter_name",
            "udt_name",
            "parameter_mode",
            "ordinal_position",
            "parameter_default",
        ])
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
        let default = row.get_string(5);

        // Only include IN parameters (skip OUT/INOUT for now)
        if mode != "IN" {
            continue;
        }

        let mut arg_str = if pname.is_empty() {
            ptype.clone()
        } else {
            format!("{} {}", pname, ptype)
        };
        if let Some(default) = default {
            let d = default.trim();
            if !d.is_empty() {
                arg_str.push_str(" DEFAULT ");
                arg_str.push_str(d);
            }
        }

        param_map
            .entry(specific)
            .or_default()
            .push((ordinal, arg_str));
    }

    // 13c: Volatility from pg_proc (AST-native — no function calls needed)
    let vol_cmd = Qail::get("pg_catalog.pg_proc")
        .columns(["proname", "provolatile"])
        .filter("pronamespace", Operator::Eq, public_namespace_oid.clone());

    let vol_rows = driver.fetch_all(&vol_cmd).await;

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

        // Skip extension/internal functions that are not user-authored routine bodies.
        if body.is_empty() || language.eq_ignore_ascii_case("c") {
            continue;
        }

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
        .columns([
            "trigger_name",
            "event_object_table",
            "action_timing",
            "event_manipulation",
            "action_statement",
        ])
        .filter("trigger_schema", Operator::Eq, "public");

    let trig_rows = driver
        .fetch_all(&trig_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query triggers: {}", e))?;

    let trig_update_cols_cmd = Qail::get("information_schema.triggered_update_columns")
        .columns(["trigger_name", "event_object_table", "event_object_column"])
        .filter("trigger_schema", Operator::Eq, "public");

    let trig_update_rows = driver
        .fetch_all(&trig_update_cols_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query trigger update columns: {}", e))?;

    let mut trig_update_cols_map: std::collections::HashMap<(String, String), Vec<String>> =
        std::collections::HashMap::new();
    for row in trig_update_rows {
        let trig_name = row.text(0);
        let table = row.text(1);
        if !base_tables.contains(&table) {
            continue;
        }
        let col = row.text(2);
        if col.is_empty() {
            continue;
        }
        trig_update_cols_map
            .entry((trig_name, table))
            .or_default()
            .push(col);
    }

    // Group by (trigger_name, table) since each event is a separate row
    let mut trigger_map: std::collections::HashMap<
        (String, String),
        (String, Vec<String>, String),
    > = std::collections::HashMap::new();
    for row in trig_rows {
        let name = row.text(0);
        let table = row.text(1);
        if !base_tables.contains(&table) {
            continue;
        }
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
        trig.events = events.clone();
        if events.iter().any(|e| e.eq_ignore_ascii_case("UPDATE"))
            && let Some(mut cols) = trig_update_cols_map.remove(&(name.clone(), table.clone()))
        {
            cols.sort();
            cols.dedup();
            trig.update_columns = cols;
        }
        triggers.push(trig);
    }

    // ── 15. RLS Policies (AST-native) ──────────────────────────────────
    let policy_cmd = Qail::get("pg_policies")
        .columns([
            "policyname",
            "tablename",
            "cmd",
            "permissive",
            "roles",
            "qual",
            "with_check",
        ])
        .filter("schemaname", Operator::Eq, "public");

    let policy_rows = driver
        .fetch_all(&policy_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query RLS policies: {}", e))?;

    let mut policies: Vec<RlsPolicy> = Vec::new();
    for row in policy_rows {
        let name = row.text(0);
        let table = row.text(1);
        if !base_tables.contains(&table) {
            continue;
        }
        let cmd_str = row.text(2);
        let permissive_str = row.text(3); // "PERMISSIVE" or "RESTRICTIVE"
        let roles_str = row.text(4); // e.g. "{app_user}" or "{public}"
        let qual = row.get_string(5); // USING expression (raw SQL)
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

        // Preserve policy predicates as raw SQL expressions from pg_policies.
        // Parsing/re-serializing can mutate semantics for complex predicates.
        let using_expr = qual.map(qail_core::ast::Expr::Named);
        let with_check_expr = with_check.map(qail_core::ast::Expr::Named);

        let mut policy = RlsPolicy::create(&name, &table);
        policy.target = target;
        policy.permissiveness = permissiveness;
        policy.role = role;
        policy.using = using_expr;
        policy.with_check = with_check_expr;
        policies.push(policy);
    }

    // ── 16. Table/Column comments (AST-native joins) ───────────────────
    let table_comment_cmd = Qail::get(
        "pg_catalog.pg_class c \
         LEFT JOIN pg_catalog.pg_description d \
           ON d.objoid = c.oid AND d.objsubid = 0",
    )
    .columns(["c.relname", "c.relkind", "d.description"])
    .filter("c.relnamespace", Operator::Eq, public_namespace_oid.clone());

    let table_comment_rows = driver
        .fetch_all(&table_comment_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query table comments: {}", e))?;

    let col_comment_cmd = Qail::get(
        "pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_class c ON a.attrelid = c.oid \
         LEFT JOIN pg_catalog.pg_description d \
           ON d.objoid = c.oid AND d.objsubid = a.attnum",
    )
    .columns([
        "c.relname",
        "a.attname",
        "a.attnum",
        "a.attisdropped",
        "d.description",
    ])
    .filter("c.relnamespace", Operator::Eq, public_namespace_oid.clone());

    let col_comment_rows = driver
        .fetch_all(&col_comment_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query column comments: {}", e))?;

    let mut comments: Vec<qail_core::migrate::schema::Comment> = Vec::new();
    for row in table_comment_rows {
        let table = row.text(0);
        let relkind = row.text(1);
        let text = normalize_comment_text(&row.get_string(2).unwrap_or_default());
        if relkind == "r" && base_tables.contains(&table) && !text.trim().is_empty() {
            comments.push(qail_core::migrate::schema::Comment::on_table(table, text));
        }
    }
    for row in col_comment_rows {
        let table = row.text(0);
        let column = row.text(1);
        let attnum = row
            .get_string(2)
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(0);
        let attisdropped = row.text(3) == "t";
        let text = normalize_comment_text(&row.get_string(4).unwrap_or_default());
        if attnum > 0
            && !attisdropped
            && base_tables.contains(&table)
            && !text.trim().is_empty()
            && !is_internal_qail_relation(&table)
        {
            comments.push(qail_core::migrate::schema::Comment::on_column(
                table, column, text,
            ));
        }
    }

    let fn_comment_cmd = Qail::get(
        "pg_catalog.pg_proc p \
         LEFT JOIN pg_catalog.pg_description d \
           ON d.objoid = p.oid AND d.objsubid = 0",
    )
    .columns([
        "p.proname",
        "pg_catalog.pg_get_function_identity_arguments(p.oid)",
        "d.description",
    ])
    .filter("p.pronamespace", Operator::Eq, public_namespace_oid.clone());

    let fn_comment_rows = driver
        .fetch_all(&fn_comment_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query function comments: {}", e))?;
    for row in fn_comment_rows {
        let name = row.text(0);
        let args = row.get_string(1).unwrap_or_default();
        let text = normalize_comment_text(&row.get_string(2).unwrap_or_default());
        if !text.trim().is_empty() {
            comments.push(qail_core::migrate::schema::Comment::on_raw(
                format!("function public.{}({})", name, args),
                text,
            ));
        }
    }

    let type_comment_cmd = Qail::get(
        "pg_catalog.pg_type t \
         LEFT JOIN pg_catalog.pg_description d \
           ON d.objoid = t.oid AND d.objsubid = 0",
    )
    .columns(["t.typname", "d.description"])
    .filter("t.typnamespace", Operator::Eq, public_namespace_oid.clone());

    let type_comment_rows = driver
        .fetch_all(&type_comment_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query type comments: {}", e))?;
    for row in type_comment_rows {
        let typname = row.text(0);
        let text = normalize_comment_text(&row.get_string(1).unwrap_or_default());
        if !text.trim().is_empty() {
            comments.push(qail_core::migrate::schema::Comment::on_raw(
                format!("type public.{}", typname),
                text,
            ));
        }
    }

    let policy_comment_cmd = Qail::get(
        "pg_catalog.pg_policy p \
         JOIN pg_catalog.pg_class c ON c.oid = p.polrelid \
         LEFT JOIN pg_catalog.pg_description d \
           ON d.objoid = p.oid AND d.objsubid = 0",
    )
    .columns(["p.polname", "c.relname", "d.description"])
    .filter("c.relnamespace", Operator::Eq, public_namespace_oid.clone());

    let policy_comment_rows = driver
        .fetch_all(&policy_comment_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query policy comments: {}", e))?;
    for row in policy_comment_rows {
        let name = row.text(0);
        let table = row.text(1);
        let text = normalize_comment_text(&row.get_string(2).unwrap_or_default());
        if !text.trim().is_empty() && base_tables.contains(&table) {
            comments.push(qail_core::migrate::schema::Comment::on_raw(
                format!("policy {} on public.{}", name, table),
                text,
            ));
        }
    }

    let constraint_comment_cmd = Qail::get(
        "pg_catalog.pg_constraint con \
         JOIN pg_catalog.pg_class c ON c.oid = con.conrelid \
         LEFT JOIN pg_catalog.pg_description d \
           ON d.objoid = con.oid AND d.objsubid = 0",
    )
    .columns(["con.conname", "c.relname", "d.description"])
    .filter("c.relnamespace", Operator::Eq, public_namespace_oid);

    let constraint_comment_rows = driver
        .fetch_all(&constraint_comment_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query constraint comments: {}", e))?;
    for row in constraint_comment_rows {
        let name = row.text(0);
        let table = row.text(1);
        let text = normalize_comment_text(&row.get_string(2).unwrap_or_default());
        if !text.trim().is_empty() && base_tables.contains(&table) {
            comments.push(qail_core::migrate::schema::Comment::on_raw(
                format!("constraint {} on public.{}", name, table),
                text,
            ));
        }
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
    schema.comments = comments;

    for (name, columns) in tables {
        let mut table = Table::new(&name);
        table.columns = columns;
        table.multi_column_fks = table_multi_column_fks.remove(&name).unwrap_or_default();
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

        if !base_tables.contains(&table) {
            continue;
        }

        // Skip indexes owned by constraints (PK/UNIQUE/EXCLUSION).
        if constraint_index_names.contains(&name) {
            continue;
        }

        let is_unique = def.to_uppercase().contains("UNIQUE");
        let (cols, where_clause, method) = parse_index_parts(&def);
        let has_expressions = cols.iter().any(|c| !is_simple_index_column(c));

        let mut index = if has_expressions {
            qail_core::migrate::Index::expression(&name, &table, cols)
        } else {
            qail_core::migrate::Index::new(&name, &table, cols)
        };
        if is_unique {
            index.unique = true;
        }
        if let Some(predicate) = where_clause {
            index.where_clause = Some(qail_core::migrate::schema::CheckExpr::Sql(predicate));
        }
        index.method = method;
        schema.add_index(index);
    }
    for index in unique_constraint_indexes {
        schema.add_index(index);
    }

    Ok(schema)
}

fn map_pg_column_type(
    udt_name: &str,
    data_type: &str,
    char_max_len: Option<&str>,
    numeric_precision: Option<&str>,
    numeric_scale: Option<&str>,
    nextval_default: bool,
    enum_names: &std::collections::HashMap<String, Vec<String>>,
) -> qail_core::migrate::ColumnType {
    if let Some(values) = enum_names.get(udt_name) {
        return qail_core::migrate::ColumnType::Enum {
            name: udt_name.to_string(),
            values: values.clone(),
        };
    }

    if let Some(array_inner) = udt_name.strip_prefix('_') {
        let inner = map_pg_column_type(array_inner, data_type, None, None, None, false, enum_names);
        return qail_core::migrate::ColumnType::Array(Box::new(inner));
    }

    let lower_udt = udt_name.to_ascii_lowercase();
    let lower_data_type = data_type.to_ascii_lowercase();
    match lower_udt.as_str() {
        "int2" | "smallint" => qail_core::migrate::ColumnType::Range("SMALLINT".to_string()),
        "int4" | "integer" => {
            if nextval_default {
                qail_core::migrate::ColumnType::Serial
            } else {
                qail_core::migrate::ColumnType::Int
            }
        }
        "int8" | "bigint" => {
            if nextval_default {
                qail_core::migrate::ColumnType::BigSerial
            } else {
                qail_core::migrate::ColumnType::BigInt
            }
        }
        "varchar" | "bpchar" => {
            let len = char_max_len.and_then(|s| s.parse::<u16>().ok());
            qail_core::migrate::ColumnType::Varchar(len)
        }
        "numeric" => {
            let p = numeric_precision.and_then(|s| s.parse::<u8>().ok());
            let s = numeric_scale.and_then(|v| v.parse::<u8>().ok());
            qail_core::migrate::ColumnType::Decimal(match (p, s) {
                (Some(p), Some(s)) => Some((p, s)),
                _ => None,
            })
        }
        "float4" | "float8" | "real" => qail_core::migrate::ColumnType::Float,
        "bool" | "boolean" => qail_core::migrate::ColumnType::Bool,
        "json" | "jsonb" => qail_core::migrate::ColumnType::Jsonb,
        "timestamp" | "timestamp without time zone" => qail_core::migrate::ColumnType::Timestamp,
        "timestamptz" | "timestamp with time zone" => qail_core::migrate::ColumnType::Timestamptz,
        "time" | "time without time zone" => qail_core::migrate::ColumnType::Time,
        "date" => qail_core::migrate::ColumnType::Date,
        "uuid" => qail_core::migrate::ColumnType::Uuid,
        "text" => qail_core::migrate::ColumnType::Text,
        "bytea" => qail_core::migrate::ColumnType::Bytea,
        "interval" => qail_core::migrate::ColumnType::Interval,
        "inet" => qail_core::migrate::ColumnType::Inet,
        "cidr" => qail_core::migrate::ColumnType::Cidr,
        "macaddr" => qail_core::migrate::ColumnType::MacAddr,
        _ => {
            let raw = map_pg_base_type_fallback(&lower_udt, &lower_data_type);
            raw.parse()
                .unwrap_or_else(|_| qail_core::migrate::ColumnType::Range(raw.to_uppercase()))
        }
    }
}

fn map_pg_base_type_fallback(udt_name_lower: &str, data_type_lower: &str) -> String {
    match udt_name_lower {
        "character" | "char" | "character varying" => "varchar".to_string(),
        _ if data_type_lower == "character varying" => "varchar".to_string(),
        _ => udt_name_lower.to_string(),
    }
}

fn parse_index_parts(
    def: &str,
) -> (
    Vec<String>,
    Option<String>,
    qail_core::migrate::schema::IndexMethod,
) {
    let upper = def.to_uppercase();
    let where_pos = upper.find(" WHERE ");
    let (main, where_clause) = if let Some(pos) = where_pos {
        (def[..pos].trim(), Some(def[pos + 7..].trim().to_string()))
    } else {
        (def.trim(), None)
    };

    let Some(start) = main.find('(') else {
        return (
            Vec::new(),
            where_clause,
            qail_core::migrate::schema::IndexMethod::BTree,
        );
    };

    let method = if let Some(using_pos) = main.to_ascii_uppercase().find(" USING ") {
        let method_chunk = main[using_pos + 7..start].trim().to_ascii_lowercase();
        match method_chunk.as_str() {
            "hash" => qail_core::migrate::schema::IndexMethod::Hash,
            "gin" => qail_core::migrate::schema::IndexMethod::Gin,
            "gist" => qail_core::migrate::schema::IndexMethod::Gist,
            "brin" => qail_core::migrate::schema::IndexMethod::Brin,
            "spgist" => qail_core::migrate::schema::IndexMethod::SpGist,
            _ => qail_core::migrate::schema::IndexMethod::BTree,
        }
    } else {
        qail_core::migrate::schema::IndexMethod::BTree
    };

    let mut depth = 0_i32;
    let mut end = None;
    for (idx, ch) in main.char_indices().skip(start) {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(idx);
                    break;
                }
            }
            _ => {}
        }
    }

    let Some(end_idx) = end else {
        return (Vec::new(), where_clause, method);
    };

    let inner = &main[start + 1..end_idx];
    (split_top_level_csv(inner), where_clause, method)
}

fn split_top_level_csv(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut depth = 0_i32;

    for ch in s.chars() {
        match ch {
            '(' => {
                depth += 1;
                cur.push(ch);
            }
            ')' => {
                depth -= 1;
                cur.push(ch);
            }
            ',' if depth == 0 => {
                let piece = cur.trim();
                if !piece.is_empty() {
                    out.push(piece.to_string());
                }
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }

    let tail = cur.trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    out
}

fn is_simple_index_column(s: &str) -> bool {
    let t = s.trim();
    !t.is_empty()
        && t.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

fn is_internal_qail_relation(name: &str) -> bool {
    name.starts_with("_qail_")
}

fn normalize_comment_text(s: &str) -> String {
    s.replace(['\r', '\n'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_trivial_not_null_check(check_clause: &str) -> bool {
    let normalized = check_clause
        .replace(['(', ')'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();

    normalized.ends_with("IS NOT NULL")
        && !normalized.contains(" AND ")
        && !normalized.contains(" OR ")
        && normalized.split_whitespace().count() <= 4
}

pub(crate) fn parse_pg_constraint_fk_action(code: &str) -> FkAction {
    match code {
        "c" => FkAction::Cascade,
        "n" => FkAction::SetNull,
        "d" => FkAction::SetDefault,
        "r" => FkAction::Restrict,
        _ => FkAction::NoAction,
    }
}

pub(crate) fn sort_introspected_key_columns(
    constraints: &mut std::collections::HashMap<String, Vec<IntrospectedKeyColumn>>,
) {
    for cols in constraints.values_mut() {
        cols.sort_by_key(|col| col.ordinal_position);
    }
}

pub(crate) fn sort_qualified_introspected_key_columns(
    constraints: &mut std::collections::HashMap<
        IntrospectedConstraintIdentity,
        Vec<IntrospectedKeyColumn>,
    >,
) {
    for cols in constraints.values_mut() {
        cols.sort_by_key(|col| col.ordinal_position);
    }
}

pub(crate) fn identity_generation_to_generated(
    is_identity: bool,
    identity_generation: Option<&str>,
) -> Option<Generated> {
    if !is_identity {
        return None;
    }

    match identity_generation
        .unwrap_or("ALWAYS")
        .trim()
        .to_ascii_uppercase()
        .as_str()
    {
        "BY DEFAULT" => Some(Generated::ByDefaultIdentity),
        _ => Some(Generated::AlwaysIdentity),
    }
}

pub(crate) fn introspected_column_generation(
    is_identity: bool,
    identity_generation: Option<&str>,
    is_generated: Option<&str>,
    generation_expression: Option<&str>,
) -> Option<Generated> {
    if let Some(generated) = identity_generation_to_generated(is_identity, identity_generation) {
        return Some(generated);
    }

    let is_stored_generated = is_generated
        .map(|value| value.trim().eq_ignore_ascii_case("ALWAYS"))
        .unwrap_or(false);
    if !is_stored_generated {
        return None;
    }

    let expression = generation_expression?.trim();
    if expression.is_empty() {
        return None;
    }

    Some(Generated::AlwaysStored(expression.to_string()))
}

pub(crate) fn resolve_introspected_unique_constraint(
    constraint_name: &str,
    table_name: &str,
    columns: &[IntrospectedKeyColumn],
) -> Option<IntrospectedUniqueConstraint> {
    if columns.is_empty() {
        return None;
    }

    let key_table = same_key_column_table(columns)?;
    if key_table != table_name {
        return None;
    }

    if columns.len() == 1 {
        return Some(IntrospectedUniqueConstraint::Single {
            table: table_name.to_string(),
            column: columns[0].column.clone(),
        });
    }

    Some(IntrospectedUniqueConstraint::Multi(
        Index::new(
            constraint_name,
            table_name,
            columns.iter().map(|col| col.column.clone()).collect(),
        )
        .unique(),
    ))
}

pub(crate) fn resolve_introspected_foreign_key(
    constraint_name: &str,
    fk_list: &[IntrospectedKeyColumn],
    ref_list: &[IntrospectedKeyColumn],
    on_delete: &FkAction,
    on_update: &FkAction,
    deferrable: Deferrable,
) -> Option<IntrospectedForeignKey> {
    if fk_list.is_empty() || fk_list.len() != ref_list.len() {
        return None;
    }

    let fk_table = same_key_column_table(fk_list)?;
    let ref_table = same_key_column_table(ref_list)?;

    if fk_list.len() == 1 {
        return Some(IntrospectedForeignKey::Single {
            table: fk_table.to_string(),
            column: fk_list[0].column.clone(),
            foreign_key: ForeignKey {
                table: ref_table.to_string(),
                column: ref_list[0].column.clone(),
                on_delete: on_delete.clone(),
                on_update: on_update.clone(),
                deferrable,
            },
        });
    }

    Some(IntrospectedForeignKey::Multi {
        table: fk_table.to_string(),
        foreign_key: MultiColumnForeignKey {
            columns: fk_list.iter().map(|col| col.column.clone()).collect(),
            ref_table: ref_table.to_string(),
            ref_columns: ref_list.iter().map(|col| col.column.clone()).collect(),
            on_delete: on_delete.clone(),
            on_update: on_update.clone(),
            deferrable,
            name: if constraint_name.is_empty() {
                None
            } else {
                Some(constraint_name.to_string())
            },
        },
    })
}

pub(crate) fn resolve_qualified_introspected_foreign_key(
    reference: &IntrospectedForeignKeyReference,
    constraint_columns: &std::collections::HashMap<
        IntrospectedConstraintIdentity,
        Vec<IntrospectedKeyColumn>,
    >,
) -> Option<IntrospectedForeignKey> {
    let fk_list = constraint_columns.get(&reference.constraint)?;
    let ref_list = constraint_columns.get(&reference.referenced_constraint)?;

    resolve_introspected_foreign_key(
        &reference.constraint.name,
        fk_list,
        ref_list,
        &reference.on_delete,
        &reference.on_update,
        reference.deferrable.clone(),
    )
}

fn same_key_column_table(cols: &[IntrospectedKeyColumn]) -> Option<&str> {
    let table = cols.first()?.table.as_str();
    cols.iter().all(|col| col.table == table).then_some(table)
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

    let s = strip_wrapping_parens(clause.trim()).trim().to_string();
    if s.is_empty() {
        return None;
    }

    // Try BETWEEN-style: "col >= low AND col <= high"
    if let Some(and_pos) = s.find(" AND ") {
        let left = strip_wrapping_parens(s[..and_pos].trim());
        let right = strip_wrapping_parens(s[and_pos + 5..].trim());

        if let (Some(l), Some(r)) = (parse_simple_cmp(left), parse_simple_cmp(right)) {
            // col >= low AND col <= high → Between
            if l.0 == r.0 && matches!(l.1, CmpOp::Gte) && matches!(r.1, CmpOp::Lte) {
                return Some(CheckExpr::Between {
                    column: l.0,
                    low: l.2,
                    high: r.2,
                });
            }
            // col >= low AND col <= high but reversed
            if l.0 == r.0 && matches!(l.1, CmpOp::Lte) && matches!(r.1, CmpOp::Gte) {
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

    Some(CheckExpr::Sql(s))
}

fn strip_wrapping_parens(mut s: &str) -> &str {
    loop {
        let trimmed = s.trim();
        if !(trimmed.starts_with('(') && trimmed.ends_with(')')) {
            return trimmed;
        }

        let mut depth = 0_i32;
        let mut wraps_all = true;
        for (idx, ch) in trimmed.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 && idx != trimmed.len() - 1 {
                        wraps_all = false;
                        break;
                    }
                }
                _ => {}
            }
            if depth < 0 {
                wraps_all = false;
                break;
            }
        }

        if wraps_all && depth == 0 {
            s = &trimmed[1..trimmed.len() - 1];
            continue;
        }
        return trimmed;
    }
}

#[derive(Debug)]
enum CmpOp {
    Gte,
    Gt,
    Lte,
    Lt,
}

fn parse_simple_cmp(s: &str) -> Option<(String, CmpOp, i64)> {
    let s = strip_wrapping_parens(s.trim());
    // Try >=, <=, >, < in order (longer first)
    let ops: &[(&str, CmpOp)] = &[
        (">=", CmpOp::Gte),
        ("<=", CmpOp::Lte),
        (">", CmpOp::Gt),
        ("<", CmpOp::Lt),
    ];

    for (op_str, op) in ops {
        if let Some(pos) = s.find(op_str) {
            let col = strip_wrapping_parens(s[..pos].trim()).to_string();
            let val_str = strip_wrapping_parens(s[pos + op_str.len()..].trim());
            // Strip type casts like ::numeric, ::integer
            let val_clean = if let Some(cast_pos) = val_str.find("::") {
                val_str[..cast_pos].trim()
            } else {
                val_str
            };
            if let Ok(val) = val_clean.parse::<i64>() {
                return Some((
                    col,
                    match op {
                        CmpOp::Gte => CmpOp::Gte,
                        CmpOp::Gt => CmpOp::Gt,
                        CmpOp::Lte => CmpOp::Lte,
                        CmpOp::Lt => CmpOp::Lt,
                    },
                    val,
                ));
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
        CmpOp::Gte => Some(CheckExpr::GreaterOrEqual {
            column: col,
            value: val,
        }),
        CmpOp::Gt => Some(CheckExpr::GreaterThan {
            column: col,
            value: val,
        }),
        CmpOp::Lte => Some(CheckExpr::LessOrEqual {
            column: col,
            value: val,
        }),
        CmpOp::Lt => Some(CheckExpr::LessThan {
            column: col,
            value: val,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(table: &str, column: &str, ordinal_position: i32) -> IntrospectedKeyColumn {
        IntrospectedKeyColumn::new(table.to_string(), column.to_string(), ordinal_position)
    }

    #[test]
    fn maps_identity_generation_to_generated_variants() {
        assert!(matches!(
            identity_generation_to_generated(true, Some("ALWAYS")),
            Some(Generated::AlwaysIdentity)
        ));
        assert!(matches!(
            identity_generation_to_generated(true, Some("BY DEFAULT")),
            Some(Generated::ByDefaultIdentity)
        ));
    }

    #[test]
    fn ignores_identity_generation_when_column_is_not_identity() {
        assert!(identity_generation_to_generated(false, Some("BY DEFAULT")).is_none());
    }

    #[test]
    fn maps_stored_generated_expression_to_generated_column() {
        let generated = introspected_column_generation(
            false,
            None,
            Some("ALWAYS"),
            Some("first_name || ' ' || last_name"),
        );

        assert!(matches!(
            generated,
            Some(Generated::AlwaysStored(expr)) if expr == "first_name || ' ' || last_name"
        ));
    }

    #[test]
    fn identity_generation_takes_precedence_over_stored_generation_metadata() {
        let generated = introspected_column_generation(
            true,
            Some("BY DEFAULT"),
            Some("ALWAYS"),
            Some("ignored_expr"),
        );

        assert!(matches!(generated, Some(Generated::ByDefaultIdentity)));
    }

    #[test]
    fn resolves_duplicate_named_foreign_keys_with_qualified_identity() {
        let mut constraint_columns = std::collections::HashMap::from([
            (
                IntrospectedConstraintIdentity::new("orders".to_string(), "tenant_fk".to_string()),
                vec![key("orders", "tenant_id", 1)],
            ),
            (
                IntrospectedConstraintIdentity::new(
                    "invoices".to_string(),
                    "tenant_fk".to_string(),
                ),
                vec![key("invoices", "tenant_id", 1)],
            ),
            (
                IntrospectedConstraintIdentity::new(
                    "tenants".to_string(),
                    "tenants_pkey".to_string(),
                ),
                vec![key("tenants", "id", 1)],
            ),
        ]);
        sort_qualified_introspected_key_columns(&mut constraint_columns);

        let references = [
            IntrospectedForeignKeyReference {
                constraint: IntrospectedConstraintIdentity::new(
                    "orders".to_string(),
                    "tenant_fk".to_string(),
                ),
                referenced_constraint: IntrospectedConstraintIdentity::new(
                    "tenants".to_string(),
                    "tenants_pkey".to_string(),
                ),
                on_delete: FkAction::Cascade,
                on_update: FkAction::NoAction,
                deferrable: Deferrable::NotDeferrable,
            },
            IntrospectedForeignKeyReference {
                constraint: IntrospectedConstraintIdentity::new(
                    "invoices".to_string(),
                    "tenant_fk".to_string(),
                ),
                referenced_constraint: IntrospectedConstraintIdentity::new(
                    "tenants".to_string(),
                    "tenants_pkey".to_string(),
                ),
                on_delete: FkAction::Restrict,
                on_update: FkAction::NoAction,
                deferrable: Deferrable::NotDeferrable,
            },
        ];

        let resolved: Vec<_> = references
            .iter()
            .map(|reference| {
                resolve_qualified_introspected_foreign_key(reference, &constraint_columns)
                    .expect("duplicate named FK should resolve by table-qualified identity")
            })
            .collect();

        let IntrospectedForeignKey::Single {
            table,
            column,
            foreign_key,
        } = &resolved[0]
        else {
            panic!("expected orders single-column FK");
        };
        assert_eq!(table, "orders");
        assert_eq!(column, "tenant_id");
        assert_eq!(foreign_key.table, "tenants");
        assert_eq!(foreign_key.column, "id");
        assert_eq!(foreign_key.on_delete, FkAction::Cascade);

        let IntrospectedForeignKey::Single {
            table,
            column,
            foreign_key,
        } = &resolved[1]
        else {
            panic!("expected invoices single-column FK");
        };
        assert_eq!(table, "invoices");
        assert_eq!(column, "tenant_id");
        assert_eq!(foreign_key.table, "tenants");
        assert_eq!(foreign_key.column, "id");
        assert_eq!(foreign_key.on_delete, FkAction::Restrict);
    }

    #[test]
    fn resolves_single_column_foreign_key() {
        let resolved = resolve_introspected_foreign_key(
            "trips_schedule_fk",
            &[key("trips", "schedule_id", 1)],
            &[key("schedules", "id", 1)],
            &FkAction::Cascade,
            &FkAction::Restrict,
            Deferrable::InitiallyDeferred,
        )
        .expect("single-column FK should resolve");

        let IntrospectedForeignKey::Single {
            table,
            column,
            foreign_key,
        } = resolved
        else {
            panic!("expected single-column FK");
        };

        assert_eq!(table, "trips");
        assert_eq!(column, "schedule_id");
        assert_eq!(foreign_key.table, "schedules");
        assert_eq!(foreign_key.column, "id");
        assert_eq!(foreign_key.on_delete, FkAction::Cascade);
        assert_eq!(foreign_key.on_update, FkAction::Restrict);
        assert_eq!(foreign_key.deferrable, Deferrable::InitiallyDeferred);
    }

    #[test]
    fn resolves_composite_foreign_key_in_ordinal_order() {
        let mut constraints = std::collections::HashMap::from([
            (
                "trips_schedule_fk".to_string(),
                vec![key("trips", "schedule_id", 2), key("trips", "route_id", 1)],
            ),
            (
                "schedules_route_schedule_key".to_string(),
                vec![
                    key("schedules", "schedule_id", 2),
                    key("schedules", "route_id", 1),
                ],
            ),
        ]);
        sort_introspected_key_columns(&mut constraints);

        let resolved = resolve_introspected_foreign_key(
            "trips_schedule_fk",
            &constraints["trips_schedule_fk"],
            &constraints["schedules_route_schedule_key"],
            &FkAction::Cascade,
            &FkAction::Restrict,
            Deferrable::Deferrable,
        )
        .expect("composite FK should resolve");

        let IntrospectedForeignKey::Multi { table, foreign_key } = resolved else {
            panic!("expected composite FK");
        };

        assert_eq!(table, "trips");
        assert_eq!(foreign_key.name.as_deref(), Some("trips_schedule_fk"));
        assert_eq!(foreign_key.columns, ["route_id", "schedule_id"]);
        assert_eq!(foreign_key.ref_table, "schedules");
        assert_eq!(foreign_key.ref_columns, ["route_id", "schedule_id"]);
        assert_eq!(foreign_key.on_delete, FkAction::Cascade);
        assert_eq!(foreign_key.on_update, FkAction::Restrict);
        assert_eq!(foreign_key.deferrable, Deferrable::Deferrable);
    }

    #[test]
    fn skips_malformed_composite_foreign_key_lengths() {
        let resolved = resolve_introspected_foreign_key(
            "bad_fk",
            &[key("trips", "route_id", 1), key("trips", "schedule_id", 2)],
            &[key("schedules", "route_id", 1)],
            &FkAction::NoAction,
            &FkAction::NoAction,
            Deferrable::NotDeferrable,
        );

        assert!(resolved.is_none());
    }

    #[test]
    fn resolves_single_column_unique_constraint_as_column_flag() {
        let resolved = resolve_introspected_unique_constraint(
            "users_email_key",
            "users",
            &[key("users", "email", 1)],
        )
        .expect("single-column unique should resolve");

        let IntrospectedUniqueConstraint::Single { table, column } = resolved else {
            panic!("expected single-column unique");
        };

        assert_eq!(table, "users");
        assert_eq!(column, "email");
    }

    #[test]
    fn resolves_composite_unique_constraint_as_ordered_unique_index() {
        let mut constraints = std::collections::HashMap::from([(
            "schedules_route_schedule_key".to_string(),
            vec![
                key("schedules", "schedule_id", 2),
                key("schedules", "route_id", 1),
            ],
        )]);
        sort_introspected_key_columns(&mut constraints);

        let resolved = resolve_introspected_unique_constraint(
            "schedules_route_schedule_key",
            "schedules",
            &constraints["schedules_route_schedule_key"],
        )
        .expect("composite unique should resolve");

        let IntrospectedUniqueConstraint::Multi(index) = resolved else {
            panic!("expected composite unique index");
        };

        assert_eq!(index.name, "schedules_route_schedule_key");
        assert_eq!(index.table, "schedules");
        assert_eq!(index.columns, ["route_id", "schedule_id"]);
        assert!(index.unique);
    }

    #[test]
    fn skips_malformed_unique_constraint_mixed_tables() {
        let resolved = resolve_introspected_unique_constraint(
            "bad_unique",
            "schedules",
            &[
                key("schedules", "route_id", 1),
                key("other_table", "schedule_id", 2),
            ],
        );

        assert!(resolved.is_none());
    }
}
