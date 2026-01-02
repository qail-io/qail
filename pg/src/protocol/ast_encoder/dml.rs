//! DML (Data Manipulation Language) encoders.
//!
//! SELECT, INSERT, UPDATE, DELETE, EXPORT, and CTE statements.

use bytes::BytesMut;
use qail_core::ast::{CTEDef, CageKind, Expr, GroupByMode, JoinKind, Qail, SortOrder};

use super::helpers::write_usize;
use super::values::{encode_columns, encode_conditions, encode_expr, encode_join_value, encode_value};

/// Encode SELECT statement directly to bytes.
pub fn encode_select(cmd: &Qail, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) -> Result<(), crate::protocol::EncodeError> {
    // CTE prefix
    encode_cte_prefix(cmd, buf, params);

    buf.extend_from_slice(b"SELECT ");

    // DISTINCT ON (col1, col2, ...)
    if !cmd.distinct_on.is_empty() {
        buf.extend_from_slice(b"DISTINCT ON (");
        for (i, expr) in cmd.distinct_on.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            encode_expr(expr, buf);
        }
        buf.extend_from_slice(b") ");
    } else if cmd.distinct {
        // Regular DISTINCT (mutually exclusive with DISTINCT ON)
        buf.extend_from_slice(b"DISTINCT ");
    }

    encode_columns(&cmd.columns, buf);

    // FROM
    buf.extend_from_slice(b" FROM ");
    buf.extend_from_slice(cmd.table.as_bytes());

    // JOINs
    for join in &cmd.joins {
        match join.kind {
            JoinKind::Inner => buf.extend_from_slice(b" INNER JOIN "),
            JoinKind::Left => buf.extend_from_slice(b" LEFT JOIN "),
            JoinKind::Right => buf.extend_from_slice(b" RIGHT JOIN "),
            JoinKind::Full => buf.extend_from_slice(b" FULL OUTER JOIN "),
            JoinKind::Cross => buf.extend_from_slice(b" CROSS JOIN "),
            JoinKind::Lateral => buf.extend_from_slice(b" LEFT JOIN LATERAL "),
        }
        buf.extend_from_slice(join.table.as_bytes());

        if join.on_true {
            buf.extend_from_slice(b" ON TRUE");
        } else if let Some(conditions) = &join.on
            && !conditions.is_empty()
        {
            buf.extend_from_slice(b" ON ");
            for (i, cond) in conditions.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b" AND ");
                }
                encode_expr(&cond.left, buf);
                buf.extend_from_slice(b" = ");
                encode_join_value(&cond.value, buf);
            }
        }
    }

    // WHERE
    let filter_cage = cmd.cages.iter().find(|c| c.kind == CageKind::Filter);
    if let Some(cage) = filter_cage
        && !cage.conditions.is_empty()
    {
        buf.extend_from_slice(b" WHERE ");
        encode_conditions(&cage.conditions, buf, params)?;
    }

    // GROUP BY with ROLLUP/CUBE/GROUPING SETS
    let group_cols: Vec<&str> = cmd.columns.iter()
        .filter_map(|e| match e {
            Expr::Named(name) => Some(name.as_str()),
            Expr::Aliased { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    
    // Only output GROUP BY if we have aggregates and non-aggregate columns
    let has_aggregates = cmd.columns.iter().any(|e| matches!(e, Expr::Aggregate { .. }));
    if has_aggregates && !group_cols.is_empty() {
        buf.extend_from_slice(b" GROUP BY ");
        match &cmd.group_by_mode {
            GroupByMode::Simple => {
                for (i, col) in group_cols.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    buf.extend_from_slice(col.as_bytes());
                }
            }
            GroupByMode::Rollup => {
                buf.extend_from_slice(b"ROLLUP(");
                for (i, col) in group_cols.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    buf.extend_from_slice(col.as_bytes());
                }
                buf.extend_from_slice(b")");
            }
            GroupByMode::Cube => {
                buf.extend_from_slice(b"CUBE(");
                for (i, col) in group_cols.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    buf.extend_from_slice(col.as_bytes());
                }
                buf.extend_from_slice(b")");
            }
            GroupByMode::GroupingSets(sets) => {
                buf.extend_from_slice(b"GROUPING SETS (");
                for (i, set) in sets.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    buf.extend_from_slice(b"(");
                    for (j, col) in set.iter().enumerate() {
                        if j > 0 {
                            buf.extend_from_slice(b", ");
                        }
                        buf.extend_from_slice(col.as_bytes());
                    }
                    buf.extend_from_slice(b")");
                }
                buf.extend_from_slice(b")");
            }
        }
    }

    // ORDER BY
    for cage in &cmd.cages {
        if let CageKind::Sort(order) = &cage.kind {
            if !cage.conditions.is_empty() {
                buf.extend_from_slice(b" ORDER BY ");
                for (i, cond) in cage.conditions.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    encode_expr(&cond.left, buf);
                    match order {
                        SortOrder::Desc | SortOrder::DescNullsFirst | SortOrder::DescNullsLast => {
                            buf.extend_from_slice(b" DESC");
                        }
                        SortOrder::Asc | SortOrder::AscNullsFirst | SortOrder::AscNullsLast => {}
                    }
                }
            }
            break;
        }
    }

    // LIMIT
    for cage in &cmd.cages {
        if let CageKind::Limit(n) = cage.kind {
            buf.extend_from_slice(b" LIMIT ");
            write_usize(buf, n);
            break;
        }
    }

    // OFFSET
    for cage in &cmd.cages {
        if let CageKind::Offset(n) = cage.kind {
            buf.extend_from_slice(b" OFFSET ");
            write_usize(buf, n);
            break;
        }
    }
    Ok(())
}

/// Encode CTE prefix: WITH [RECURSIVE] cte1 AS (...), cte2 AS (...)
pub fn encode_cte_prefix(cmd: &Qail, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) {
    if cmd.ctes.is_empty() {
        return;
    }

    buf.extend_from_slice(b"WITH ");

    let has_recursive = cmd.ctes.iter().any(|c| c.recursive);
    if has_recursive {
        buf.extend_from_slice(b"RECURSIVE ");
    }

    for (i, cte) in cmd.ctes.iter().enumerate() {
        if i > 0 {
            buf.extend_from_slice(b", ");
        }
        encode_single_cte(cte, buf, params);
    }

    buf.extend_from_slice(b" ");
}

/// Encode a single CTE definition.
fn encode_single_cte(cte: &CTEDef, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) {
    buf.extend_from_slice(cte.name.as_bytes());

    // Optional column list
    if !cte.columns.is_empty() {
        buf.extend_from_slice(b"(");
        for (i, col) in cte.columns.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            buf.extend_from_slice(col.as_bytes());
        }
        buf.extend_from_slice(b")");
    }

    buf.extend_from_slice(b" AS (");

    // Encode base query recursively
    encode_select(&cte.base_query, buf, params).ok();

    // Recursive part (UNION ALL)
    if cte.recursive
        && let Some(ref recursive_query) = cte.recursive_query
    {
        buf.extend_from_slice(b" UNION ALL ");
        encode_select(recursive_query, buf, params).ok();
    }

    buf.extend_from_slice(b")");
}

/// Encode INSERT statement.
pub fn encode_insert(cmd: &Qail, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) -> Result<(), crate::protocol::EncodeError> {
    buf.extend_from_slice(b"INSERT INTO ");
    buf.extend_from_slice(cmd.table.as_bytes());

    // Find payload cage
    let payload_cage = cmd.cages.iter().find(|c| c.kind == CageKind::Payload);
    
    // Column list - prefer cmd.columns, but extract from conditions if empty (set_value pattern)
    if !cmd.columns.is_empty() {
        buf.extend_from_slice(b" (");
        encode_columns(&cmd.columns, buf);
        buf.extend_from_slice(b")");
    } else if let Some(cage) = payload_cage {
        // Extract column names from condition.left (set_value pattern)
        buf.extend_from_slice(b" (");
        for (i, cond) in cage.conditions.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            encode_expr(&cond.left, buf);
        }
        buf.extend_from_slice(b")");
    }

    // VALUES
    if let Some(cage) = payload_cage {
        buf.extend_from_slice(b" VALUES (");
        for (i, cond) in cage.conditions.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            encode_value(&cond.value, buf, params)?;
        }
        buf.extend_from_slice(b")");
    }
    
    // RETURNING clause
    if let Some(ref ret_cols) = cmd.returning {
        buf.extend_from_slice(b" RETURNING ");
        encode_columns(ret_cols, buf);
    }
    
    Ok(())
}

/// Encode UPDATE statement.
pub fn encode_update(cmd: &Qail, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) -> Result<(), crate::protocol::EncodeError> {
    buf.extend_from_slice(b"UPDATE ");
    buf.extend_from_slice(cmd.table.as_bytes());
    buf.extend_from_slice(b" SET ");

    // SET clause - pair columns with payload values
    if let Some(cage) = cmd.cages.iter().find(|c| c.kind == CageKind::Payload) {
        // Use cmd.columns if available (from .columns([...]).values([...]) pattern)
        // Otherwise use cage.conditions.left (from .set("col", value) pattern)
        if !cmd.columns.is_empty() {
            // Zip columns with values
            for (i, (col, cond)) in cmd.columns.iter().zip(cage.conditions.iter()).enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                // Column name from cmd.columns
                encode_expr(col, buf);
                buf.extend_from_slice(b" = ");
                // Value from payload condition
                encode_value(&cond.value, buf, params)?;
            }
        } else {
            // Fallback to old behavior (direct set)
            for (i, cond) in cage.conditions.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_expr(&cond.left, buf);
                buf.extend_from_slice(b" = ");
                encode_value(&cond.value, buf, params)?;
            }
        }
    }

    // WHERE
    if let Some(cage) = cmd.cages.iter().find(|c| c.kind == CageKind::Filter)
        && !cage.conditions.is_empty()
    {
        buf.extend_from_slice(b" WHERE ");
        encode_conditions(&cage.conditions, buf, params)?;
    }
    Ok(())
}

/// Encode DELETE statement.
pub fn encode_delete(cmd: &Qail, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) -> Result<(), crate::protocol::EncodeError> {
    buf.extend_from_slice(b"DELETE FROM ");
    buf.extend_from_slice(cmd.table.as_bytes());

    // WHERE
    if let Some(cage) = cmd.cages.iter().find(|c| c.kind == CageKind::Filter)
        && !cage.conditions.is_empty()
    {
        buf.extend_from_slice(b" WHERE ");
        encode_conditions(&cage.conditions, buf, params)?;
    }
    Ok(())
}

/// Encode EXPORT command as COPY (SELECT ...) TO STDOUT.
pub fn encode_export(cmd: &Qail, buf: &mut BytesMut, params: &mut Vec<Option<Vec<u8>>>) -> Result<(), crate::protocol::EncodeError> {
    buf.extend_from_slice(b"COPY (");
    encode_select(cmd, buf, params)?;
    buf.extend_from_slice(b") TO STDOUT");
    Ok(())
}
