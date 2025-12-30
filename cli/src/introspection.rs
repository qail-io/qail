//! Database Schema Introspection
//!
//! Extracts schema from live databases into QAIL format.
//! Uses pure AST-native queries via Qail (no raw SQL).

use anyhow::{Result, anyhow};
use colored::*;
use qail_core::ast::{Operator, Qail};
use qail_core::migrate::{Column, Schema, Table, to_qail_string};
use qail_pg::driver::PgDriver;
use url::Url;

use crate::util::parse_pg_url;

/// Output format for schema generation
#[derive(Clone, Default)]
pub enum SchemaOutputFormat {
    #[default]
    Qail,
}

pub async fn pull_schema(url_str: &str, _format: SchemaOutputFormat) -> Result<()> {
    println!("{} {}", "→ Connecting to:".dimmed(), url_str.yellow());

    let url = Url::parse(url_str)?;
    let scheme = url.scheme();

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

    // Query columns from information_schema (AST-native)
    let columns_cmd = Qail::get("information_schema.columns")
        .columns(["table_name", "column_name", "udt_name", "is_nullable"])
        .filter("table_schema", Operator::Eq, "public");

    let rows = driver
        .fetch_all(&columns_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query columns: {}", e))?;

    let mut tables: std::collections::HashMap<String, Vec<Column>> =
        std::collections::HashMap::new();

    for row in rows {
        let table_name = row.get_string_by_name("table_name").unwrap_or_default();
        let col_name = row.get_string_by_name("column_name").unwrap_or_default();
        let udt_name = row.get_string_by_name("udt_name").unwrap_or_default();
        let is_nullable_str = row.get_string_by_name("is_nullable").unwrap_or_default();
        let is_nullable = is_nullable_str == "YES";

        // Map PostgreSQL type to QAIL ColumnType
        let col_type_str = map_pg_type(&udt_name);
        let col_type: qail_core::migrate::ColumnType = col_type_str
            .parse()
            .unwrap_or(qail_core::migrate::ColumnType::Text);

        let mut col = Column::new(&col_name, col_type);
        col.nullable = is_nullable;

        tables.entry(table_name).or_default().push(col);
    }

    // Query primary keys (AST-native)
    let pk_cmd = Qail::get("information_schema.table_constraints")
        .columns(["table_name", "constraint_type"])
        .filter("table_schema", Operator::Eq, "public")
        .filter("constraint_type", Operator::Eq, "PRIMARY KEY");

    let pk_rows = driver
        .fetch_all(&pk_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query primary keys: {}", e))?;

    let pk_tables: std::collections::HashSet<String> = pk_rows
        .iter()
        .filter_map(|r| r.get_string_by_name("table_name"))
        .collect();

    // Query key column usage for primary key columns (AST-native)
    let kcu_cmd = Qail::get("information_schema.key_column_usage")
        .columns(["table_name", "column_name", "constraint_name"])
        .filter("table_schema", Operator::Eq, "public");

    let kcu_rows = driver
        .fetch_all(&kcu_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query key columns: {}", e))?;

    let mut pk_columns: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();
    for row in kcu_rows {
        let table = row.get_string_by_name("table_name").unwrap_or_default();
        let column = row.get_string_by_name("column_name").unwrap_or_default();
        let constraint = row
            .get_string_by_name("constraint_name")
            .unwrap_or_default();

        // Primary key constraints typically end with _pkey
        if constraint.ends_with("_pkey") || pk_tables.contains(&table) {
            pk_columns.insert((table, column));
        }
    }

    // Mark primary key columns
    for (table_name, columns) in tables.iter_mut() {
        for col in columns.iter_mut() {
            if pk_columns.contains(&(table_name.clone(), col.name.clone())) {
                col.primary_key = true;
            }
        }
    }

    // Query indexes from pg_indexes (AST-native)
    let idx_cmd = Qail::get("pg_indexes")
        .columns(["indexname", "tablename", "indexdef"])
        .filter("schemaname", Operator::Eq, "public");

    let index_rows = driver
        .fetch_all(&idx_cmd)
        .await
        .map_err(|e| anyhow!("Failed to query indexes: {}", e))?;

    let mut schema = Schema::new();

    for (name, columns) in tables {
        let mut table = Table::new(&name);
        table.columns = columns;
        schema.add_table(table);
    }

    for row in index_rows {
        let name = row.get_string_by_name("indexname").unwrap_or_default();
        let table = row.get_string_by_name("tablename").unwrap_or_default();
        let def = row.get_string_by_name("indexdef").unwrap_or_default();

        // Skip primary key indexes
        if name.ends_with("_pkey") {
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
