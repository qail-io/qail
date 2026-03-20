//! Chunked backfill system and contract safety enforcement.

use super::discovery::{is_valid_ident, parse_drop_targets};
use super::types::{BackfillRun, BackfillSpec, BackfillTransform, BackfillTransformOp};
use crate::colors::*;
use crate::migrations::maybe_failpoint;
use anyhow::{Context, Result, anyhow, bail};
use qail_core::analyzer::{CodebaseScanner, QueryType};
use qail_core::ast::{Action, Constraint, Expr, JoinKind, Qail};
use std::collections::BTreeMap;
use std::path::Path;

pub(crate) fn parse_backfill_spec(
    content: &str,
    default_chunk_size: usize,
) -> Result<Option<BackfillSpec>> {
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
    let (set_column, source_column, transform) = if entries.contains_key("set") {
        if entries.contains_key("set_column")
            || entries.contains_key("set_source")
            || entries.contains_key("set_transform")
        {
            bail!(
                "Use either legacy -- @backfill.set or structured set directives (-- @backfill.set_column / set_source / set_transform), not both"
            );
        }
        let set_clause = entries
            .remove("set")
            .ok_or_else(|| anyhow!("Missing backfill directive: -- @backfill.set: <col = expr>"))?;
        parse_set_clause(&set_clause)?
    } else {
        let set_column = entries.remove("set_column").ok_or_else(|| {
            anyhow!("Missing backfill directive: -- @backfill.set_column: <column>")
        })?;
        let source_column = entries.remove("set_source").ok_or_else(|| {
            anyhow!("Missing backfill directive: -- @backfill.set_source: <column>")
        })?;
        let transform = parse_transform(
            entries
                .remove("set_transform")
                .as_deref()
                .unwrap_or("identity"),
        )?;
        (set_column, source_column, transform)
    };

    let where_null_column = if let Some(raw_where_null) = entries.remove("where_null") {
        parse_where_null(&raw_where_null)?
    } else if let Some(raw_where) = entries.remove("where") {
        parse_where_is_null(&raw_where)?
    } else {
        None
    };

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
    if !is_valid_ident(&set_column) {
        bail!("Invalid backfill set target column '{}'", set_column);
    }
    if !is_valid_ident(&source_column) {
        bail!("Invalid backfill set source column '{}'", source_column);
    }
    if let Some(where_col) = &where_null_column
        && !is_valid_ident(where_col)
    {
        bail!("Invalid backfill where_null column '{}'", where_col);
    }

    Ok(Some(BackfillSpec {
        table,
        pk_column,
        set_column,
        source_column,
        transform,
        where_null_column,
        chunk_size: chunk_size.max(1),
    }))
}

fn parse_set_clause(raw: &str) -> Result<(String, String, BackfillTransform)> {
    let (lhs, rhs) = raw
        .split_once('=')
        .ok_or_else(|| anyhow!("Invalid -- @backfill.set format. Expected '<col> = <expr>'"))?;
    let target = lhs.trim().to_string();
    let expr = rhs.trim();

    if expr.is_empty() {
        bail!("Invalid -- @backfill.set: expression cannot be empty");
    }

    let (source, transform) = parse_set_expr(expr)?;
    Ok((target, source, transform))
}

fn parse_set_expr(expr: &str) -> Result<(String, BackfillTransform)> {
    let mut current = expr.trim().to_string();
    let mut pipeline = Vec::<BackfillTransformOp>::new();

    loop {
        if is_valid_ident(&current) {
            pipeline.reverse();
            return Ok((current, collapse_pipeline_to_transform(pipeline)));
        }

        let (func, arg) = parse_unary_call(&current)
            .ok_or_else(|| anyhow!("Unsupported -- @backfill.set expression '{}'", expr))?;
        let op = parse_transform_op(func)?;
        pipeline.push(op);
        current = arg;
    }
}

fn parse_unary_call(expr: &str) -> Option<(&str, String)> {
    let open = expr.find('(')?;
    if !expr.ends_with(')') {
        return None;
    }

    let func = expr[..open].trim();
    if !is_valid_ident(func) {
        return None;
    }

    let mut depth = 0i32;
    let mut close_idx = None;
    for (idx, ch) in expr.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close_idx = Some(idx);
                    break;
                }
            }
            _ => {}
        }
    }
    let close = close_idx?;
    if close != expr.len() - 1 {
        return None;
    }

    let inner = expr[open + 1..close].trim().to_string();
    if inner.is_empty() {
        return None;
    }

    Some((func, inner))
}

fn parse_transform(raw: &str) -> Result<BackfillTransform> {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(BackfillTransform::Identity);
    }

    if normalized == "identity" || normalized == "copy" {
        return Ok(BackfillTransform::Identity);
    }

    let mut ops = Vec::<BackfillTransformOp>::new();
    for token in normalized.split('|') {
        ops.push(parse_transform_op(token.trim())?);
    }
    Ok(collapse_pipeline_to_transform(ops))
}

fn parse_transform_op(raw: &str) -> Result<BackfillTransformOp> {
    match raw {
        "lower" => Ok(BackfillTransformOp::Lower),
        "upper" => Ok(BackfillTransformOp::Upper),
        "trim" => Ok(BackfillTransformOp::Trim),
        "initcap" => Ok(BackfillTransformOp::Initcap),
        other => bail!(
            "Unsupported backfill transform '{}'. Allowed: identity, lower, upper, trim, initcap, or pipelines like lower|trim",
            other
        ),
    }
}

fn collapse_pipeline_to_transform(ops: Vec<BackfillTransformOp>) -> BackfillTransform {
    match ops.as_slice() {
        [] => BackfillTransform::Identity,
        [BackfillTransformOp::Lower] => BackfillTransform::Lower,
        [BackfillTransformOp::Upper] => BackfillTransform::Upper,
        [BackfillTransformOp::Trim] => BackfillTransform::Trim,
        [BackfillTransformOp::Initcap] => BackfillTransform::Initcap,
        _ => BackfillTransform::Pipeline(ops),
    }
}

fn parse_where_null(raw: &str) -> Result<Option<String>> {
    let col = raw.trim();
    if col.is_empty() {
        return Ok(None);
    }
    if !is_valid_ident(col) {
        bail!("Invalid -- @backfill.where_null identifier '{}'", col);
    }
    Ok(Some(col.to_string()))
}

fn parse_where_is_null(raw: &str) -> Result<Option<String>> {
    let parts: Vec<&str> = raw.split_whitespace().collect();
    if parts.len() == 3
        && parts[1].eq_ignore_ascii_case("is")
        && parts[2].eq_ignore_ascii_case("null")
    {
        return parse_where_null(parts[0]);
    }
    bail!(
        "Unsupported -- @backfill.where expression '{}'. Use '<column> IS NULL' or -- @backfill.where_null: <column>",
        raw
    )
}

async fn ensure_backfill_checkpoint_table(pg: &mut qail_pg::PgDriver) -> Result<()> {
    let exists_cmd = Qail::get("information_schema.tables")
        .column("1")
        .where_eq("table_schema", "public")
        .where_eq("table_name", "_qail_backfill_checkpoints")
        .limit(1);
    let exists = pg
        .fetch_all(&exists_cmd)
        .await
        .context("Failed to inspect _qail_backfill_checkpoints table")?;
    if exists.is_empty() {
        let create_cmd = Qail {
            action: Action::Make,
            table: "_qail_backfill_checkpoints".to_string(),
            columns: vec![
                Expr::Def {
                    name: "migration_version".to_string(),
                    data_type: "varchar".to_string(),
                    constraints: vec![Constraint::PrimaryKey],
                },
                Expr::Def {
                    name: "table_name".to_string(),
                    data_type: "varchar".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "pk_column".to_string(),
                    data_type: "varchar".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "last_pk".to_string(),
                    data_type: "bigint".to_string(),
                    constraints: vec![Constraint::Default("0".to_string())],
                },
                Expr::Def {
                    name: "last_pk_text".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
                Expr::Def {
                    name: "chunk_size".to_string(),
                    data_type: "int".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "rows_processed".to_string(),
                    data_type: "bigint".to_string(),
                    constraints: vec![Constraint::Default("0".to_string())],
                },
                Expr::Def {
                    name: "started_at".to_string(),
                    data_type: "timestamptz".to_string(),
                    constraints: vec![Constraint::Default("now()".to_string())],
                },
                Expr::Def {
                    name: "updated_at".to_string(),
                    data_type: "timestamptz".to_string(),
                    constraints: vec![Constraint::Default("now()".to_string())],
                },
                Expr::Def {
                    name: "finished_at".to_string(),
                    data_type: "timestamptz".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
            ],
            ..Default::default()
        };
        pg.execute(&create_cmd)
            .await
            .context("Failed to ensure _qail_backfill_checkpoints table")?;
    } else {
        // Cutover helper: add text checkpoint column for uuid/text PK runners.
        let col_exists_cmd = Qail::get("information_schema.columns")
            .column("1")
            .where_eq("table_schema", "public")
            .where_eq("table_name", "_qail_backfill_checkpoints")
            .where_eq("column_name", "last_pk_text")
            .limit(1);
        let col_rows = pg
            .fetch_all(&col_exists_cmd)
            .await
            .context("Failed to inspect _qail_backfill_checkpoints columns")?;
        if col_rows.is_empty() {
            let alter_cmd = Qail {
                action: Action::Mod,
                table: "_qail_backfill_checkpoints".to_string(),
                columns: vec![Expr::Def {
                    name: "last_pk_text".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![Constraint::Nullable],
                }],
                ..Default::default()
            };
            pg.execute(&alter_cmd)
                .await
                .context("Failed to add last_pk_text column for backfill checkpoints")?;
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackfillPkKind {
    Integer,
    TextComparable,
}

fn classify_backfill_pk_type(raw_type: &str) -> Result<BackfillPkKind> {
    let typ = raw_type.trim().to_ascii_lowercase();

    if ["smallint", "integer", "bigint"]
        .iter()
        .any(|t| typ.contains(t))
    {
        return Ok(BackfillPkKind::Integer);
    }

    if typ.contains("uuid")
        || typ.contains("text")
        || typ.contains("character varying")
        || typ.contains("varchar")
        || typ == "character"
        || typ.starts_with("character(")
        || typ.contains("bpchar")
    {
        return Ok(BackfillPkKind::TextComparable);
    }

    bail!(
        "Backfill checkpoint runner supports PK types: smallint/int/bigint/uuid/text/varchar/char. Found '{}'",
        typ
    )
}

async fn inspect_backfill_pk_kind(
    pg: &mut qail_pg::PgDriver,
    table: &str,
    pk_column: &str,
) -> Result<BackfillPkKind> {
    let (schema, table_name) = split_schema_table(table);
    let cmd = Qail::get("pg_attribute a")
        .column("format_type(a.atttypid, a.atttypmod) AS typ")
        .join(JoinKind::Inner, "pg_class c", "c.oid", "a.attrelid")
        .join(JoinKind::Inner, "pg_namespace n", "n.oid", "c.relnamespace")
        .where_eq("n.nspname", schema)
        .where_eq("c.relname", table_name)
        .where_eq("a.attname", pk_column)
        .gt("a.attnum", 0)
        .where_eq("a.attisdropped", false)
        .limit(1);

    let rows = pg.fetch_all(&cmd).await.map_err(|e| {
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

    let typ = row.get_string(0).unwrap_or_default();
    classify_backfill_pk_type(&typ).with_context(|| {
        format!(
            "Unsupported backfill PK '{}.{}' type '{}'",
            table, pk_column, typ
        )
    })
}

pub(super) async fn run_chunked_backfill(
    pg: &mut qail_pg::PgDriver,
    migration_version: &str,
    spec: &BackfillSpec,
) -> Result<BackfillRun> {
    ensure_backfill_checkpoint_table(pg).await?;
    let pk_kind = inspect_backfill_pk_kind(pg, &spec.table, &spec.pk_column).await?;

    let init_cmd = Qail::add("_qail_backfill_checkpoints")
        .set_value("migration_version", migration_version)
        .set_value("table_name", spec.table.as_str())
        .set_value("pk_column", spec.pk_column.as_str())
        .set_value("chunk_size", spec.chunk_size as i64)
        .on_conflict_nothing(&["migration_version"]);
    pg.execute(&init_cmd)
        .await
        .context("Failed to initialize backfill checkpoint")?;

    let status_cmd = Qail::get("_qail_backfill_checkpoints")
        .columns(["last_pk", "last_pk_text", "rows_processed", "finished_at"])
        .where_eq("migration_version", migration_version)
        .limit(1);
    let status_rows = pg
        .fetch_all(&status_cmd)
        .await
        .context("Failed to read backfill checkpoint")?;
    let Some(status_row) = status_rows.first() else {
        bail!(
            "Backfill checkpoint row missing after init for '{}'",
            migration_version
        );
    };

    let mut last_pk_int = status_row.get_i64(0).unwrap_or(0);
    let mut last_pk_text = status_row.get_string(1);
    let mut rows_updated = status_row.get_i64(2).unwrap_or(0);
    let already_finished = status_row.get_string(3).is_some();
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

    let resumed = match pk_kind {
        BackfillPkKind::Integer => last_pk_int > 0 || rows_updated > 0,
        BackfillPkKind::TextComparable => {
            last_pk_text.as_ref().is_some_and(|v| !v.is_empty()) || rows_updated > 0
        }
    };
    if resumed {
        let cursor = match pk_kind {
            BackfillPkKind::Integer => last_pk_int.to_string(),
            BackfillPkKind::TextComparable => {
                last_pk_text.clone().unwrap_or_else(|| "<none>".into())
            }
        };
        println!(
            "{}",
            format!(
                "↳ resuming checkpoint from last_pk={} rows_done={}",
                cursor, rows_updated
            )
            .dimmed()
        );
    }

    let set_expr = build_set_expr(spec);

    let mut chunks = 0i64;
    loop {
        let mut batch_cmd = Qail::get(spec.table.as_str())
            .column(spec.pk_column.as_str())
            .order_asc(spec.pk_column.as_str())
            .limit(spec.chunk_size as i64);
        match pk_kind {
            BackfillPkKind::Integer => {
                batch_cmd = batch_cmd.gt(spec.pk_column.as_str(), last_pk_int);
            }
            BackfillPkKind::TextComparable => {
                if let Some(cursor) = &last_pk_text {
                    batch_cmd = batch_cmd.gt(spec.pk_column.as_str(), cursor.as_str());
                }
            }
        }

        if let Some(where_null_col) = &spec.where_null_column {
            batch_cmd = batch_cmd.is_null(where_null_col.as_str());
        }

        let batch_rows = pg
            .fetch_all(&batch_cmd)
            .await
            .map_err(|e| anyhow!("Chunked backfill execution failed: {}", e))?;
        if batch_rows.is_empty() {
            break;
        }

        let mut batch_ids_int = Vec::<i64>::new();
        let mut batch_ids_text = Vec::<String>::new();
        match pk_kind {
            BackfillPkKind::Integer => {
                batch_ids_int.reserve(batch_rows.len());
                for row in &batch_rows {
                    if let Some(pk) = row.get_i64(0) {
                        batch_ids_int.push(pk);
                    }
                }
                if batch_ids_int.is_empty() {
                    bail!(
                        "Chunked backfill could not extract integer PK values for '{}.{}'",
                        spec.table,
                        spec.pk_column
                    );
                }
            }
            BackfillPkKind::TextComparable => {
                batch_ids_text.reserve(batch_rows.len());
                for row in &batch_rows {
                    if let Some(pk) = row.get_string(0) {
                        batch_ids_text.push(pk);
                    }
                }
                if batch_ids_text.is_empty() {
                    bail!(
                        "Chunked backfill could not extract text/uuid PK values for '{}.{}'",
                        spec.table,
                        spec.pk_column
                    );
                }
            }
        }

        let next_pk_int = batch_ids_int.last().copied().unwrap_or(last_pk_int);
        let next_pk_text = batch_ids_text.last().cloned().or(last_pk_text.clone());

        let mut update_cmd =
            Qail::set(spec.table.as_str()).set_value(spec.set_column.as_str(), set_expr.clone());
        update_cmd = match pk_kind {
            BackfillPkKind::Integer => update_cmd.in_vals(spec.pk_column.as_str(), batch_ids_int),
            BackfillPkKind::TextComparable => {
                update_cmd.in_vals(spec.pk_column.as_str(), batch_ids_text)
            }
        };
        if let Some(where_null_col) = &spec.where_null_column {
            update_cmd = update_cmd.is_null(where_null_col.as_str());
        }
        update_cmd = update_cmd.returning([spec.pk_column.as_str()]);

        pg.begin()
            .await
            .context("Failed to begin backfill chunk transaction")?;

        let updated_rows = pg
            .fetch_all(&update_cmd)
            .await
            .map_err(|e| anyhow!("Chunked backfill update failed: {}", e));
        let updated_rows = match updated_rows {
            Ok(rows) => rows,
            Err(err) => {
                let _ = pg.rollback().await;
                return Err(err);
            }
        };
        let updated = updated_rows.len() as i64;
        if updated <= 0 {
            let _ = pg.rollback().await;
            break;
        }

        if let Err(err) = maybe_failpoint("backfill.after_update_before_checkpoint") {
            let _ = pg.rollback().await;
            return Err(err);
        }

        let next_rows_updated = rows_updated.saturating_add(updated);

        let mut checkpoint_cmd = Qail::set("_qail_backfill_checkpoints")
            .set_value("rows_processed", next_rows_updated)
            .set_value("updated_at", qail_core::ast::builders::now())
            .where_eq("migration_version", migration_version);
        checkpoint_cmd = match pk_kind {
            BackfillPkKind::Integer => checkpoint_cmd
                .set_value("last_pk", next_pk_int)
                .set_value("last_pk_text", Option::<String>::None),
            BackfillPkKind::TextComparable => checkpoint_cmd
                .set_value("last_pk", 0i64)
                .set_value("last_pk_text", next_pk_text.clone()),
        };
        let checkpoint_res = pg
            .execute(&checkpoint_cmd)
            .await
            .context("Failed to update backfill checkpoint");
        if let Err(err) = checkpoint_res {
            let _ = pg.rollback().await;
            return Err(err);
        }

        pg.commit()
            .await
            .context("Failed to commit backfill chunk transaction")?;

        last_pk_int = next_pk_int;
        last_pk_text = next_pk_text;
        rows_updated = next_rows_updated;
        chunks += 1;
    }

    let mut finish_cmd = Qail::set("_qail_backfill_checkpoints")
        .set_value("finished_at", qail_core::ast::builders::now())
        .set_value("updated_at", qail_core::ast::builders::now())
        .set_value("rows_processed", rows_updated)
        .where_eq("migration_version", migration_version);
    finish_cmd = match pk_kind {
        BackfillPkKind::Integer => finish_cmd
            .set_value("last_pk", last_pk_int)
            .set_value("last_pk_text", Option::<String>::None),
        BackfillPkKind::TextComparable => finish_cmd
            .set_value("last_pk", 0i64)
            .set_value("last_pk_text", last_pk_text),
    };
    pg.execute(&finish_cmd)
        .await
        .context("Failed to finalize backfill checkpoint")?;

    Ok(BackfillRun {
        resumed,
        rows_updated,
        chunks,
    })
}

fn build_set_expr(spec: &BackfillSpec) -> Expr {
    let src = Expr::Named(spec.source_column.clone());
    match spec.transform {
        BackfillTransform::Identity => src,
        BackfillTransform::Lower => apply_transform_op(src, BackfillTransformOp::Lower),
        BackfillTransform::Upper => apply_transform_op(src, BackfillTransformOp::Upper),
        BackfillTransform::Trim => apply_transform_op(src, BackfillTransformOp::Trim),
        BackfillTransform::Initcap => apply_transform_op(src, BackfillTransformOp::Initcap),
        BackfillTransform::Pipeline(ref ops) => ops.iter().copied().fold(src, apply_transform_op),
    }
}

fn apply_transform_op(expr: Expr, op: BackfillTransformOp) -> Expr {
    let func = match op {
        BackfillTransformOp::Lower => "LOWER",
        BackfillTransformOp::Upper => "UPPER",
        BackfillTransformOp::Trim => "TRIM",
        BackfillTransformOp::Initcap => "INITCAP",
    };
    Expr::FunctionCall {
        name: func.to_string(),
        args: vec![expr],
        alias: None,
    }
}

#[cfg(test)]
mod pk_type_tests {
    use super::{BackfillPkKind, classify_backfill_pk_type};

    #[test]
    fn classify_integer_pk_types() {
        assert_eq!(
            classify_backfill_pk_type("bigint").expect("bigint should be supported"),
            BackfillPkKind::Integer
        );
        assert_eq!(
            classify_backfill_pk_type("integer").expect("integer should be supported"),
            BackfillPkKind::Integer
        );
        assert_eq!(
            classify_backfill_pk_type("smallint").expect("smallint should be supported"),
            BackfillPkKind::Integer
        );
    }

    #[test]
    fn classify_text_comparable_pk_types() {
        assert_eq!(
            classify_backfill_pk_type("uuid").expect("uuid should be supported"),
            BackfillPkKind::TextComparable
        );
        assert_eq!(
            classify_backfill_pk_type("character varying(255)")
                .expect("varchar should be supported"),
            BackfillPkKind::TextComparable
        );
        assert_eq!(
            classify_backfill_pk_type("text").expect("text should be supported"),
            BackfillPkKind::TextComparable
        );
    }

    #[test]
    fn reject_unsupported_pk_type() {
        let err = classify_backfill_pk_type("jsonb").expect_err("jsonb PK should be rejected");
        assert!(
            err.to_string().contains("supports PK types"),
            "error should explain supported PK types"
        );
    }
}

#[cfg(test)]
mod runtime_backfill_tests {
    use super::{BackfillSpec, BackfillTransform, BackfillTransformOp, run_chunked_backfill};
    use qail_core::ast::{Action, Constraint, Expr, Qail};

    fn test_suffix() -> String {
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{}_{}", std::process::id(), now_nanos)
    }

    #[tokio::test]
    async fn chunked_backfill_supports_uuid_pk_runtime() {
        let Some(url) = std::env::var("QAIL_TEST_DB_URL").ok() else {
            eprintln!("Skipping UUID backfill runtime test (set QAIL_TEST_DB_URL)");
            return;
        };

        let mut pg = qail_pg::PgDriver::connect_url(&url)
            .await
            .expect("connect QAIL_TEST_DB_URL");

        let suffix = test_suffix();
        let table = format!("bf_uuid_{}", suffix);
        let migration_version = format!("bf_uuid_{}.backfill.up.qail", suffix);

        let create_table = Qail {
            action: Action::Make,
            table: table.clone(),
            columns: vec![
                Expr::Def {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    constraints: vec![Constraint::PrimaryKey],
                },
                Expr::Def {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![],
                },
                Expr::Def {
                    name: "name_ci".to_string(),
                    data_type: "text".to_string(),
                    constraints: vec![Constraint::Nullable],
                },
            ],
            ..Default::default()
        };
        pg.execute(&create_table).await.expect("create uuid table");

        let rows = vec![
            ("00000000-0000-0000-0000-000000000001", "  Alice  ", "alice"),
            ("00000000-0000-0000-0000-000000000002", " Bob", "bob"),
            ("00000000-0000-0000-0000-000000000003", "CAROL ", "carol"),
        ];

        for (id, name, _) in &rows {
            let insert = Qail::add(table.as_str())
                .set_value("id", *id)
                .set_value("name", *name);
            pg.execute(&insert).await.expect("insert test row");
        }

        let spec = BackfillSpec {
            table: table.clone(),
            pk_column: "id".to_string(),
            set_column: "name_ci".to_string(),
            source_column: "name".to_string(),
            transform: BackfillTransform::Pipeline(vec![
                BackfillTransformOp::Lower,
                BackfillTransformOp::Trim,
            ]),
            where_null_column: Some("name_ci".to_string()),
            chunk_size: 2,
        };

        let run = run_chunked_backfill(&mut pg, &migration_version, &spec)
            .await
            .expect("run uuid backfill");
        assert_eq!(run.rows_updated, 3);
        assert!(
            run.chunks >= 2,
            "chunk size=2 with 3 rows should use >=2 chunks"
        );

        let verify_rows = pg
            .fetch_all(
                &Qail::get(table.as_str())
                    .columns(["id", "name_ci"])
                    .order_asc("id"),
            )
            .await
            .expect("query updated rows");
        assert_eq!(verify_rows.len(), rows.len());
        for (idx, row) in verify_rows.iter().enumerate() {
            assert_eq!(
                row.get_string(1).as_deref(),
                Some(rows[idx].2),
                "row {} should be normalized into name_ci",
                idx
            );
        }

        let checkpoint = pg
            .fetch_all(
                &Qail::get("_qail_backfill_checkpoints")
                    .columns(["last_pk", "last_pk_text", "rows_processed", "finished_at"])
                    .where_eq("migration_version", migration_version.as_str())
                    .limit(1),
            )
            .await
            .expect("query checkpoint");
        assert_eq!(checkpoint.len(), 1, "checkpoint row should exist");
        let cp = &checkpoint[0];
        assert_eq!(cp.get_i64(0), Some(0), "uuid cursor uses text checkpoint");
        assert_eq!(
            cp.get_i64(2),
            Some(3),
            "rows_processed should match updated rows"
        );
        assert!(
            cp.get_string(1).is_some(),
            "last_pk_text should be set for uuid/text PK backfill"
        );
        assert!(
            cp.get_string(3).is_some(),
            "finished_at should be set after successful backfill"
        );

        let _ = pg.execute(&Qail::del(table.as_str())).await;
        let _ = pg
            .execute(
                &Qail::del("_qail_backfill_checkpoints")
                    .where_eq("migration_version", migration_version.as_str()),
            )
            .await;
        let _ = pg
            .execute(&Qail {
                action: Action::Drop,
                table,
                ..Default::default()
            })
            .await;
    }
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
