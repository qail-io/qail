//! Database Schema Introspection
//!
//! Extracts schema from live databases into QAIL format.

use sqlx::postgres::PgPoolOptions;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::Row;
use qail_core::migrate::{Schema, Table, Column, to_qail_string};
use anyhow::{Result, anyhow};
use colored::*;
use url::Url;

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
        "mysql" | "mariadb" => inspect_mysql(url_str).await?,
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
    let pool = PgPoolOptions::new()
        .connect(url)
        .await?;

    // Query columns with primary key info
    let rows = sqlx::query(
        "SELECT c.table_name, c.column_name, c.udt_name, c.is_nullable,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary
         FROM information_schema.columns c
         LEFT JOIN (
             SELECT kcu.table_name, kcu.column_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu 
               ON tc.constraint_name = kcu.constraint_name
             WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
         ) pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name
         WHERE c.table_schema = 'public'
         ORDER BY c.table_name, c.ordinal_position"
    )
    .fetch_all(&pool)
    .await?;

    let mut tables: std::collections::HashMap<String, Vec<Column>> = std::collections::HashMap::new();

    for row in rows {
        let table_name: String = row.get("table_name");
        let col_name: String = row.get("column_name");
        let udt_name: String = row.get("udt_name");
        let is_nullable_str: String = row.get("is_nullable");
        let is_nullable = is_nullable_str == "YES";
        let is_primary: bool = row.get("is_primary");
        
        let col_type = map_pg_type(&udt_name);

        let mut col = Column::new(&col_name, col_type);
        col.nullable = is_nullable;
        col.primary_key = is_primary;

        tables.entry(table_name).or_default().push(col);
    }

    // Query indexes
    let index_rows = sqlx::query(
        "SELECT indexname, tablename, indexdef
         FROM pg_indexes
         WHERE schemaname = 'public'
         AND indexname NOT LIKE '%_pkey'"
    )
    .fetch_all(&pool)
    .await?;

    let mut schema = Schema::new();
    
    for (name, columns) in tables {
        let mut table = Table::new(&name);
        table.columns = columns;
        schema.add_table(table);
    }

    for row in index_rows {
        let name: String = row.get("indexname");
        let table: String = row.get("tablename");
        let def: String = row.get("indexdef");
        
        let is_unique = def.to_uppercase().contains("UNIQUE");
        // Parse columns from index definition
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
    // Parse "(col1, col2)" from index definition
    if let Some(start) = def.rfind('(') {
        if let Some(end) = def.rfind(')') {
            let cols_str = &def[start + 1..end];
            return cols_str.split(',').map(|s| s.trim().to_string()).collect();
        }
    }
    vec![]
}

async fn inspect_mysql(url: &str) -> Result<Schema> {
    let pool = MySqlPoolOptions::new()
        .connect(url)
        .await?;

    let url_parsed = Url::parse(url)?;
    let db_name = url_parsed.path().trim_start_matches('/');
    
    let rows = sqlx::query(
        "SELECT table_name, column_name, data_type, is_nullable, column_key
         FROM information_schema.columns 
         WHERE table_schema = ? 
         ORDER BY table_name, ordinal_position"
    )
    .bind(db_name)
    .fetch_all(&pool)
    .await?;

    let mut tables: std::collections::HashMap<String, Vec<Column>> = std::collections::HashMap::new();

    for row in rows {
        let table_name: String = row.get("table_name");
        let col_name: String = row.get("column_name");
        let data_type: String = row.get("data_type");
        let is_nullable_str: String = row.get("is_nullable");
        let column_key: String = row.get("column_key");
        
        let is_nullable = is_nullable_str == "YES";
        let is_primary = column_key == "PRI";
        
        let col_type = map_mysql_type(&data_type);

        let mut col = Column::new(&col_name, col_type);
        col.nullable = is_nullable;
        col.primary_key = is_primary;

        tables.entry(table_name).or_default().push(col);
    }

    let mut schema = Schema::new();
    for (name, columns) in tables {
        let mut table = Table::new(&name);
        table.columns = columns;
        schema.add_table(table);
    }

    Ok(schema)
}

fn map_mysql_type(data_type: &str) -> &'static str {
    match data_type {
        "int" | "tinyint" | "smallint" | "mediumint" => "int",
        "bigint" => "bigint",
        "float" | "double" | "decimal" => "float",
        "boolean" | "bool" => "bool",
        "json" => "json",
        "datetime" | "timestamp" => "timestamp",
        "date" => "date",
        "varchar" | "char" => "varchar",
        _ => "text",
    }
}
