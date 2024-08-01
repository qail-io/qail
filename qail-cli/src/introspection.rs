use sqlx::postgres::PgPoolOptions;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::Row;
use qail_core::schema::{Schema, TableDef, ColumnDef};
use anyhow::{Result, anyhow};
use colored::*;

use url::Url;

pub async fn pull_schema(url_str: &str) -> Result<()> {
    println!("{} {}", "→ Connecting to:".dimmed(), url_str.yellow());

    let url = Url::parse(url_str)?;
    let scheme = url.scheme();

    let schema = match scheme {
        "postgres" | "postgresql" => inspect_postgres(url_str).await?,
        "mysql" | "mariadb" => inspect_mysql(url_str).await?,
        // "sqlite" => inspect_sqlite(url_str).await?, // Future
        _ => return Err(anyhow!("Unsupported database scheme: {}", scheme)),
    };

    // Serialize and save
    let json = serde_json::to_string_pretty(&schema)?;
    std::fs::write("qail.schema.json", json)?;

    println!("{}", "✓ Schema synced to qail.schema.json".green().bold());
    println!("  Tables: {}", schema.tables.len());
    
    Ok(())
}

async fn inspect_postgres(url: &str) -> Result<Schema> {
    let pool = PgPoolOptions::new()
        .connect(url)
        .await?;

    // Query columns
    let rows = sqlx::query(
        "SELECT table_name, column_name, udt_name, is_nullable 
         FROM information_schema.columns 
         WHERE table_schema = 'public' 
         ORDER BY table_name, ordinal_position"
    )
    .fetch_all(&pool)
    .await?;

    let mut tables: std::collections::HashMap<String, Vec<ColumnDef>> = std::collections::HashMap::new();

    for row in rows {
        let table_name: String = row.get("table_name");
        let col_name: String = row.get("column_name");
        let udt_name: String = row.get("udt_name");
        let is_nullable_str: String = row.get("is_nullable");
        let is_nullable = is_nullable_str == "YES";
        
        let col_type = match udt_name.as_str() {
            "int4" | "int8" | "serial" | "bigserial" => "int",
            "float4" | "float8" | "numeric" => "float",
            "bool" => "bool",
            "json" | "jsonb" => "json",
            "timestamp" | "date" | "timestamptz" => "date",
            _ => "string",
        }.to_string();

        let col_def = ColumnDef {
            name: col_name,
            typ: col_type,
            nullable: is_nullable,
            primary_key: false, // Naive for now
        };

        tables.entry(table_name).or_default().push(col_def);
    }

    let mut table_defs = Vec::new();
    for (name, columns) in tables {
        table_defs.push(TableDef {
            name,
            columns,
        });
    }
    
    // Sort tables by name for stability
    table_defs.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(Schema {
        tables: table_defs,
    })
}

async fn inspect_mysql(url: &str) -> Result<Schema> {
    let pool = MySqlPoolOptions::new()
        .connect(url)
        .await?;

    // MySQL needs database name filtering, usually part of URL path
    let url_parsed = Url::parse(url)?;
    let db_name = url_parsed.path().trim_start_matches('/');
    
    let rows = sqlx::query(
        "SELECT table_name, column_name, data_type, is_nullable 
         FROM information_schema.columns 
         WHERE table_schema = ? 
         ORDER BY table_name, ordinal_position"
    )
    .bind(db_name)
    .fetch_all(&pool)
    .await?;

    let mut tables: std::collections::HashMap<String, Vec<ColumnDef>> = std::collections::HashMap::new();

    for row in rows {
        let table_name: String = row.get("table_name");
        let col_name: String = row.get("column_name");
        let data_type: String = row.get("data_type");
        let is_nullable_str: String = row.get("is_nullable");
        let is_nullable = is_nullable_str == "YES";
        
        let col_type = match data_type.as_str() {
            "int" | "bigint" | "tinyint" | "smallint" => "int",
            "float" | "double" | "decimal" => "float",
            "boolean" => "bool",
            "json" => "json",
            "datetime" | "timestamp" | "date" => "date",
            _ => "string",
        }.to_string();

        let col_def = ColumnDef {
            name: col_name,
            typ: col_type,
            nullable: is_nullable,
            primary_key: false, // Naive for now
        };

        tables.entry(table_name).or_default().push(col_def);
    }

    let mut table_defs = Vec::new();
    for (name, columns) in tables {
        table_defs.push(TableDef {
            name,
            columns,
        });
    }
    table_defs.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(Schema {
        tables: table_defs,
    })
}
