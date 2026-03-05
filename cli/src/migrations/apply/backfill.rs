//! Chunked backfill system and contract safety enforcement.

use super::types::{BackfillRun, BackfillSpec, BACKFILL_CHECKPOINT_TABLE_SCHEMA};
use super::discovery::{is_valid_ident, parse_drop_targets, quote_ident};
use crate::colors::*;
use anyhow::{Context, Result, anyhow, bail};
use qail_core::analyzer::{CodebaseScanner, QueryType};
use std::collections::BTreeMap;
use std::path::Path;

pub(super) fn parse_backfill_spec(content: &str, default_chunk_size: usize) -> Result<Option<BackfillSpec>> {
    let mut entries = BTreeMap::<String, String>::new();

    for line in content.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("-- @backfill.") else {
            continue;
        };
        let Some((raw_key, raw_val)) = rest.split_once(':') else {
            bail!(
                "Invalid backfill directive '{}'. Expected '-- @backfill.<key>: <value>'",
                trimmed
            );
        };
        let key = raw_key.trim().to_ascii_lowercase();
        let val = raw_val.trim().to_string();
        if !val.is_empty() {
            entries.insert(key, val);
        }
    }

    if entries.is_empty() {
        return Ok(None);
    }

    // Enforce directive-only: reject files that mix directives with SQL/QAIL body.
    // Non-directive, non-comment, non-blank lines indicate a body that would be silently skipped.
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") || trimmed.starts_with('#') {
            continue;
        }
        bail!(
            "Backfill directive file must only contain `-- @backfill.*` directives and comments, \
             but found non-directive body: '{}'. Move schema/data SQL to a separate expand or \
             contract migration.",
            if trimmed.len() > 80 {
                &trimmed[..80]
            } else {
                trimmed
            }
        );
    }

    let table = entries
        .remove("table")
        .ok_or_else(|| anyhow!("Missing backfill directive: -- @backfill.table: <table>"))?;
    let pk_column = entries
        .remove("pk")
        .ok_or_else(|| anyhow!("Missing backfill directive: -- @backfill.pk: <pk_column>"))?;
    let set_clause = entries
        .remove("set")
        .ok_or_else(|| anyhow!("Missing backfill directive: -- @backfill.set: <col = expr>"))?;
    let where_clause = entries.remove("where");

    let chunk_size = if let Some(raw_chunk) = entries.remove("chunk_size") {
        raw_chunk
            .parse::<usize>()
            .map_err(|_| anyhow!("Invalid -- @backfill.chunk_size: '{}'", raw_chunk))?
    } else if let Some(raw_chunk) = entries.remove("chunk") {
        raw_chunk
            .parse::<usize>()
            .map_err(|_| anyhow!("Invalid -- @backfill.chunk: '{}'", raw_chunk))?
    } else {
        default_chunk_size.max(1)
    };

    if !entries.is_empty() {
        let unknown = entries.keys().cloned().collect::<Vec<_>>().join(", ");
        bail!("Unknown backfill directive(s): {}", unknown);
    }

    if !is_valid_ident(&table) {
        bail!("Invalid -- @backfill.table identifier '{}'", table);
    }
    if !is_valid_ident(&pk_column) {
        bail!("Invalid -- @backfill.pk identifier '{}'", pk_column);
    }
    if set_clause.trim().is_empty() {
        bail!("-- @backfill.set cannot be empty");
    }

    Ok(Some(BackfillSpec {
        table,
        pk_column,
        set_clause,
        where_clause,
        chunk_size: chunk_size.max(1),
    }))
}

async fn ensure_backfill_checkpoint_table(pg: &mut qail_pg::PgDriver) -> Result<()> {
    pg.execute_raw(BACKFILL_CHECKPOINT_TABLE_SCHEMA)
        .await
        .context("Failed to ensure _qail_backfill_checkpoints table")?;
    Ok(())
}

/// Split a potentially schema-qualified table name into (schema, table).
/// Defaults to `"public"` when no schema prefix is present.
pub(super) fn split_schema_table(table: &str) -> (&str, &str) {
    match table.split_once('.') {
        Some((schema, name)) => (schema, name),
        None => ("public", table),
    }
}

async fn ensure_integer_backfill_pk(
    pg: &mut qail_pg::PgDriver,
    table: &str,
    pk_column: &str,
) -> Result<()> {
    let (schema, table_name) = split_schema_table(table);
    let schema_escaped = schema.replace('\'', "''");
    let table_escaped = table_name.replace('\'', "''");
    let pk_escaped = pk_column.replace('\'', "''");
    let sql = format!(
        r#"
        SELECT format_type(a.atttypid, a.atttypmod) AS typ
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = '{schema}'
          AND c.relname = '{table}'
          AND a.attname = '{pk}'
          AND a.attnum > 0
          AND NOT a.attisdropped
        LIMIT 1
        "#,
        schema = schema_escaped,
        table = table_escaped,
        pk = pk_escaped
    );

    let rows = pg.fetch_raw(&sql).await.map_err(|e| {
        anyhow!(
            "Failed to inspect backfill PK column '{}.{}': {}",
            table,
            pk_column,
            e
        )
    })?;

    let Some(row) = rows.first() else {
        bail!(
            "Backfill PK column '{}.{}' not found in '{}' schema",
            table,
            pk_column,
            schema
        );
    };

    let typ = row.get_string(0).unwrap_or_default().to_ascii_lowercase();
    let supported = ["smallint", "integer", "bigint"];
    if !supported.iter().any(|t| typ.contains(t)) {
        bail!(
            "Backfill checkpoint runner requires integer PK (smallint/int/bigint). Found '{}.{}' type '{}'",
            table,
            pk_column,
            typ
        );
    }

    Ok(())
}

pub(super) async fn run_chunked_backfill(
    pg: &mut qail_pg::PgDriver,
    migration_version: &str,
    spec: &BackfillSpec,
) -> Result<BackfillRun> {
    ensure_backfill_checkpoint_table(pg).await?;
    ensure_integer_backfill_pk(pg, &spec.table, &spec.pk_column).await?;

    let migration_escaped = migration_version.replace('\'', "''");
    let table_escaped = spec.table.replace('\'', "''");
    let pk_escaped = spec.pk_column.replace('\'', "''");

    let init_sql = format!(
        "INSERT INTO _qail_backfill_checkpoints \
         (migration_version, table_name, pk_column, chunk_size) \
         VALUES ('{mig}', '{table}', '{pk}', {chunk}) \
         ON CONFLICT (migration_version) DO NOTHING",
        mig = migration_escaped,
        table = table_escaped,
        pk = pk_escaped,
        chunk = spec.chunk_size
    );
    pg.execute_raw(&init_sql)
        .await
        .context("Failed to initialize backfill checkpoint")?;

    let status_sql = format!(
        "SELECT last_pk, rows_processed, finished_at IS NOT NULL \
         FROM _qail_backfill_checkpoints WHERE migration_version = '{}'",
        migration_escaped
    );
    let status_rows = pg
        .fetch_raw(&status_sql)
        .await
        .context("Failed to read backfill checkpoint")?;
    let Some(status_row) = status_rows.first() else {
        bail!(
            "Backfill checkpoint row missing after init for '{}'",
            migration_version
        );
    };

    let mut last_pk = status_row.get_i64(0).unwrap_or(0);
    let mut rows_updated = status_row.get_i64(1).unwrap_or(0);
    let already_finished = status_row.get_bool(2).unwrap_or(false);
    if already_finished {
        println!(
            "{}",
            format!(
                "↳ backfill checkpoint already complete (rows={})",
                rows_updated
            )
            .dimmed()
        );
        return Ok(BackfillRun {
            resumed: false,
            rows_updated,
            chunks: 0,
        });
    }

    let resumed = last_pk > 0 || rows_updated > 0;
    if resumed {
        println!(
            "{}",
            format!(
                "↳ resuming checkpoint from last_pk={} rows_done={}",
                last_pk, rows_updated
            )
            .dimmed()
        );
    }

    let table_ident = quote_ident(&spec.table);
    let pk_ident = quote_ident(&spec.pk_column);
    let where_sql = spec.where_clause.as_deref().unwrap_or("TRUE");

    let mut chunks = 0i64;
    loop {
        let chunk_sql = format!(
            r#"
            WITH batch AS (
                SELECT {pk} AS pk
                FROM {table}
                WHERE {pk} > {last_pk}
                  AND ({where_clause})
                ORDER BY {pk}
                LIMIT {chunk}
            ),
            updated AS (
                UPDATE {table} AS t
                SET {set_clause}
                FROM batch
                WHERE t.{pk} = batch.pk
                RETURNING batch.pk
            )
            SELECT COALESCE(MAX(pk), {last_pk})::bigint AS max_pk,
                   COUNT(*)::bigint AS updated_rows
            FROM updated
            "#,
            pk = pk_ident,
            table = table_ident,
            last_pk = last_pk,
            where_clause = where_sql,
            chunk = spec.chunk_size,
            set_clause = spec.set_clause,
        );

        let rows = pg
            .fetch_raw(&chunk_sql)
            .await
            .map_err(|e| anyhow!("Chunked backfill execution failed: {}", e))?;
        let Some(row) = rows.first() else {
            bail!("Chunked backfill returned no status row");
        };

        let next_pk = row.get_i64(0).unwrap_or(last_pk);
        let updated = row.get_i64(1).unwrap_or(0);
        if updated <= 0 {
            break;
        }

        last_pk = next_pk;
        rows_updated = rows_updated.saturating_add(updated);
        chunks += 1;

        let checkpoint_sql = format!(
            "UPDATE _qail_backfill_checkpoints \
             SET last_pk = {last_pk}, rows_processed = {rows}, updated_at = now() \
             WHERE migration_version = '{mig}'",
            last_pk = last_pk,
            rows = rows_updated,
            mig = migration_escaped
        );
        pg.execute_raw(&checkpoint_sql)
            .await
            .context("Failed to update backfill checkpoint")?;
    }

    let finish_sql = format!(
        "UPDATE _qail_backfill_checkpoints \
         SET finished_at = now(), updated_at = now(), rows_processed = {rows}, last_pk = {last_pk} \
         WHERE migration_version = '{mig}'",
        rows = rows_updated,
        last_pk = last_pk,
        mig = migration_escaped
    );
    pg.execute_raw(&finish_sql)
        .await
        .context("Failed to finalize backfill checkpoint")?;

    Ok(BackfillRun {
        resumed,
        rows_updated,
        chunks,
    })
}

pub(super) fn enforce_contract_safety(
    migration_name: &str,
    sql: &str,
    codebase: Option<&str>,
    allow_contract_with_references: bool,
) -> Result<()> {
    let (drop_tables, drop_columns) = parse_drop_targets(sql);
    if drop_tables.is_empty() && drop_columns.is_empty() {
        return Ok(());
    }

    let Some(codebase_path) = codebase else {
        if allow_contract_with_references {
            println!(
                "{}",
                "⚠️  Skipping contract reference guard (no --codebase provided) due to --allow-contract-with-references".yellow()
            );
            return Ok(());
        }
        bail!(
            "Contract migration '{}' requires code reference checks.\n\
             Re-run with --codebase <path> or explicitly override with --allow-contract-with-references.",
            migration_name
        );
    };

    let code_path = Path::new(codebase_path);
    if !code_path.exists() {
        bail!(
            "Contract migration '{}' blocked: codebase path not found: {}",
            migration_name,
            codebase_path
        );
    }

    let scanner = CodebaseScanner::new();
    let refs = scanner.scan(code_path);

    let drop_table_set = drop_tables
        .into_iter()
        .map(|t| t.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let drop_col_set = drop_columns
        .into_iter()
        .map(|(t, c)| (t.to_ascii_lowercase(), c.to_ascii_lowercase()))
        .collect::<std::collections::HashSet<_>>();

    let mut hits = Vec::<String>::new();
    for r in refs {
        let table = r.table.to_ascii_lowercase();
        if drop_table_set.contains(&table) {
            let kind = if matches!(r.query_type, QueryType::RawSql) {
                "RAW SQL"
            } else {
                "QAIL"
            };
            hits.push(format!(
                "{}:{} [{}] references dropped table '{}': {}",
                r.file.display(),
                r.line,
                kind,
                table,
                r.snippet
            ));
            continue;
        }
        for col in &r.columns {
            let normalized_col = col.trim_matches('"').to_ascii_lowercase();
            if drop_col_set.contains(&(table.clone(), normalized_col.clone()))
                || (col == "*" && drop_col_set.iter().any(|(t, _)| t == &table))
            {
                let kind = if matches!(r.query_type, QueryType::RawSql) {
                    "RAW SQL"
                } else {
                    "QAIL"
                };
                hits.push(format!(
                    "{}:{} [{}] references dropped column '{}.{}': {}",
                    r.file.display(),
                    r.line,
                    kind,
                    table,
                    normalized_col,
                    r.snippet
                ));
            }
        }
    }

    if hits.is_empty() {
        return Ok(());
    }

    if allow_contract_with_references {
        println!(
            "{}",
            format!(
                "⚠️  Contract reference guard bypassed for '{}' with {} hit(s) due to --allow-contract-with-references",
                migration_name,
                hits.len()
            )
            .yellow()
        );
        return Ok(());
    }

    let sample = hits.into_iter().take(8).collect::<Vec<_>>().join("\n  - ");
    bail!(
        "Contract migration '{}' blocked: detected live references to dropped fields/tables.\n  - {}",
        migration_name,
        sample
    );
}
