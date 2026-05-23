//! DML (Data Manipulation Language) encoders.
//!
//! SELECT, INSERT, UPDATE, DELETE, EXPORT, and CTE statements.

use bytes::BytesMut;
use qail_core::ast::{
    CTEDef, CageKind, Expr, GroupByMode, JoinKind, LogicalOp, Merge, MergeAction, MergeMatchKind,
    MergeSource, Qail, SetOp, SortOrder,
};

use super::helpers::write_usize;
use super::values::{
    encode_columns, encode_columns_with_params, encode_conditions, encode_expr, encode_join_value,
    encode_operator, encode_value,
};

/// Encode a SELECT statement directly to bytes.
///
/// # Arguments
///
/// * `cmd` — Qail AST command with `Action::Get`.
/// * `buf` — Output buffer to append the SQL bytes to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_select(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    encode_select_with_columns(cmd, &cmd.columns, buf, params)
}

/// Encode a COUNT statement using the original query shape with a COUNT(*)
/// projection, without cloning the full AST.
pub fn encode_count(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    let count_columns = [Expr::Aggregate {
        col: "*".to_string(),
        func: qail_core::ast::AggregateFunc::Count,
        distinct: false,
        filter: None,
        alias: None,
    }];
    encode_select_with_columns(cmd, &count_columns, buf, params)
}

fn encode_select_with_columns(
    cmd: &Qail,
    columns: &[Expr],
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    if try_encode_simple_select_fast(cmd, columns, buf, params)? {
        return Ok(());
    }

    let select_start = buf.len();

    // CTE prefix
    encode_cte_prefix(cmd, buf, params)?;

    buf.extend_from_slice(b"SELECT ");

    // DISTINCT ON (col1, col2, ...)
    if !cmd.distinct_on.is_empty() {
        buf.extend_from_slice(b"DISTINCT ON (");
        for (i, expr) in cmd.distinct_on.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            encode_expr(expr, buf)?;
        }
        buf.extend_from_slice(b") ");
    } else if cmd.distinct {
        // Regular DISTINCT (mutually exclusive with DISTINCT ON)
        buf.extend_from_slice(b"DISTINCT ");
    }

    encode_columns_with_params(columns, buf, Some(params))?;

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
                encode_expr(&cond.left, buf)?;
                buf.extend_from_slice(b" ");
                encode_operator(&cond.op, buf);
                buf.extend_from_slice(b" ");
                encode_join_value(&cond.value, buf);
            }
        }
    }

    // WHERE (supports AND + OR filter cages)
    encode_where(cmd, buf, params)?;

    // GROUP BY - prefer explicit Partition cage, fall back to auto-extraction from columns
    let partition_cage = cmd.cages.iter().find(|c| c.kind == CageKind::Partition);

    if let Some(cage) = partition_cage {
        // Explicit GROUP BY from .group_by() or .group_by_expr()
        if !cage.conditions.is_empty() {
            buf.extend_from_slice(b" GROUP BY ");
            for (i, cond) in cage.conditions.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_expr(&cond.left, buf)?;
            }
        }
    } else {
        // Auto-generate GROUP BY from columns when aggregates are present
        let group_cols: Vec<&str> = columns
            .iter()
            .filter_map(|e| match e {
                Expr::Named(name) => Some(name.as_str()),
                Expr::Aliased { name, .. } => Some(name.as_str()),
                _ => None,
            })
            .collect();

        let has_aggregates = columns.iter().any(|e| matches!(e, Expr::Aggregate { .. }));
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
    }

    // ORDER BY - collect ALL sort cages and output them together
    let sort_cages: Vec<_> = cmd
        .cages
        .iter()
        .filter_map(|cage| {
            if let CageKind::Sort(order) = &cage.kind {
                Some((cage, *order))
            } else {
                None
            }
        })
        .collect();

    if !sort_cages.is_empty() {
        buf.extend_from_slice(b" ORDER BY ");
        let mut first = true;
        for (cage, order) in &sort_cages {
            for cond in &cage.conditions {
                if !first {
                    buf.extend_from_slice(b", ");
                }
                first = false;
                encode_expr(&cond.left, buf)?;
                match order {
                    SortOrder::Desc | SortOrder::DescNullsFirst | SortOrder::DescNullsLast => {
                        buf.extend_from_slice(b" DESC");
                    }
                    SortOrder::Asc | SortOrder::AscNullsFirst | SortOrder::AscNullsLast => {}
                }
            }
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

    append_fetch_clause(cmd, buf);

    if !cmd.set_ops.is_empty() && set_operand_has_branch_clauses(cmd) {
        wrap_sql_range_in_parens(buf, select_start);
    }

    // SET OPERATIONS (UNION, INTERSECT, EXCEPT)
    for (set_op, other_cmd) in &cmd.set_ops {
        match set_op {
            SetOp::Union => buf.extend_from_slice(b" UNION "),
            SetOp::UnionAll => buf.extend_from_slice(b" UNION ALL "),
            SetOp::Intersect => buf.extend_from_slice(b" INTERSECT "),
            SetOp::Except => buf.extend_from_slice(b" EXCEPT "),
        }
        encode_set_operand(other_cmd, buf, params)?;
    }

    Ok(())
}

fn encode_set_operand(
    query: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    let wrap = set_operand_needs_wrapper(query);
    if wrap {
        buf.extend_from_slice(b"(");
    }

    encode_select(query, buf, params)?;

    if wrap {
        buf.extend_from_slice(b")");
    }

    Ok(())
}

fn set_operand_needs_wrapper(query: &Qail) -> bool {
    !query.set_ops.is_empty() || set_operand_has_branch_clauses(query)
}

fn set_operand_has_branch_clauses(query: &Qail) -> bool {
    query.fetch.is_some()
        || query.cages.iter().any(|cage| {
            matches!(
                cage.kind,
                CageKind::Sort(_) | CageKind::Limit(_) | CageKind::Offset(_)
            )
        })
}

fn wrap_sql_range_in_parens(buf: &mut BytesMut, start: usize) {
    let suffix = buf.split_off(start);
    buf.extend_from_slice(b"(");
    buf.extend_from_slice(&suffix);
    buf.extend_from_slice(b")");
}

fn append_fetch_clause(cmd: &Qail, buf: &mut BytesMut) {
    if let Some((count, with_ties)) = cmd.fetch {
        buf.extend_from_slice(b" FETCH FIRST ");
        buf.extend_from_slice(count.to_string().as_bytes());
        if with_ties {
            buf.extend_from_slice(b" ROWS WITH TIES");
        } else {
            buf.extend_from_slice(b" ROWS ONLY");
        }
    }
}

/// Fast path for the dominant read shape:
/// `SELECT <columns> FROM <table> [LIMIT n] [OFFSET n]`
///
/// This bypasses the generic cage scans and grouping/order machinery when the
/// AST shape is simple enough, reducing branch and allocation overhead.
#[inline]
fn try_encode_simple_select_fast(
    cmd: &Qail,
    columns: &[Expr],
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<bool, crate::protocol::EncodeError> {
    if !cmd.ctes.is_empty()
        || cmd.distinct
        || !cmd.distinct_on.is_empty()
        || !cmd.joins.is_empty()
        || !cmd.set_ops.is_empty()
        || !cmd.having.is_empty()
        || cmd.fetch.is_some()
        || !matches!(cmd.group_by_mode, GroupByMode::Simple)
    {
        return Ok(false);
    }

    if columns
        .iter()
        .any(|expr| matches!(expr, Expr::Aggregate { .. }))
    {
        return Ok(false);
    }

    let mut limit: Option<usize> = None;
    let mut offset: Option<usize> = None;

    for cage in &cmd.cages {
        if !cage.conditions.is_empty() {
            return Ok(false);
        }

        match cage.kind {
            CageKind::Limit(n) => {
                if limit.is_none() {
                    limit = Some(n);
                }
            }
            CageKind::Offset(n) => {
                if offset.is_none() {
                    offset = Some(n);
                }
            }
            _ => return Ok(false),
        }
    }

    buf.extend_from_slice(b"SELECT ");
    encode_columns_with_params(columns, buf, Some(params))?;
    buf.extend_from_slice(b" FROM ");
    buf.extend_from_slice(cmd.table.as_bytes());

    if let Some(n) = limit {
        buf.extend_from_slice(b" LIMIT ");
        write_usize(buf, n);
    }

    if let Some(n) = offset {
        buf.extend_from_slice(b" OFFSET ");
        write_usize(buf, n);
    }

    Ok(true)
}

/// Encode the CTE prefix (`WITH [RECURSIVE] cte1 AS (...), cte2 AS (...)`).
///
/// # Arguments
///
/// * `cmd` — Qail AST command containing CTE definitions.
/// * `buf` — Output buffer to append the WITH clause to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_cte_prefix(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), super::super::EncodeError> {
    if cmd.ctes.is_empty() {
        return Ok(());
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
        encode_single_cte(cte, buf, params)?;
    }

    buf.extend_from_slice(b" ");
    Ok(())
}

/// Encode a single CTE definition.
fn encode_single_cte(
    cte: &CTEDef,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), super::super::EncodeError> {
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

    encode_recursive_cte_arm(&cte.base_query, buf, params)?;

    // Recursive part (UNION ALL)
    if cte.recursive
        && let Some(ref recursive_query) = cte.recursive_query
    {
        buf.extend_from_slice(b" UNION ALL ");
        encode_recursive_cte_arm(recursive_query, buf, params)?;
    }

    buf.extend_from_slice(b")");
    Ok(())
}

fn encode_recursive_cte_arm(
    query: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), super::super::EncodeError> {
    let wrap_set_ops = set_operand_needs_wrapper(query);
    if wrap_set_ops {
        buf.extend_from_slice(b"(");
    }

    encode_select(query, buf, params)?;

    if wrap_set_ops {
        buf.extend_from_slice(b")");
    }

    Ok(())
}

/// Encode an INSERT statement.
///
/// # Arguments
///
/// * `cmd` — Qail AST command with `Action::Add`.
/// * `buf` — Output buffer to append the SQL bytes to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_insert(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    buf.extend_from_slice(b"INSERT INTO ");
    buf.extend_from_slice(cmd.table.as_bytes());

    // Find payload cage
    let payload_cage = cmd.cages.iter().find(|c| c.kind == CageKind::Payload);

    // Column list - prefer cmd.columns, but extract from conditions if empty (set_value pattern)
    if !cmd.columns.is_empty() {
        buf.extend_from_slice(b" (");
        encode_columns(&cmd.columns, buf)?;
        buf.extend_from_slice(b")");
    } else if let Some(cage) = payload_cage {
        // Extract column names from condition.left (set_value pattern)
        buf.extend_from_slice(b" (");
        for (i, cond) in cage.conditions.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            encode_expr(&cond.left, buf)?;
        }
        buf.extend_from_slice(b")");
    }

    // INSERT ... SELECT source query takes the place of VALUES.
    if let Some(source_query) = &cmd.source_query {
        buf.extend_from_slice(b" ");
        encode_select(source_query, buf, params)?;
    } else if let Some(cage) = payload_cage {
        buf.extend_from_slice(b" VALUES (");
        for (i, cond) in cage.conditions.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            encode_value(&cond.value, buf, params)?;
        }
        buf.extend_from_slice(b")");
    }

    // ON CONFLICT clause (UPSERT support)
    if let Some(ref on_conflict) = cmd.on_conflict {
        use qail_core::ast::ConflictAction;

        buf.extend_from_slice(b" ON CONFLICT ");

        // Conflict target columns
        if !on_conflict.columns.is_empty() {
            buf.extend_from_slice(b"(");
            for (i, col) in on_conflict.columns.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                buf.extend_from_slice(col.as_bytes());
            }
            buf.extend_from_slice(b") ");
        }

        // Conflict action
        match &on_conflict.action {
            ConflictAction::DoNothing => {
                buf.extend_from_slice(b"DO NOTHING");
            }
            ConflictAction::DoUpdate { assignments } => {
                buf.extend_from_slice(b"DO UPDATE SET ");
                for (i, (col, expr)) in assignments.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    buf.extend_from_slice(col.as_bytes());
                    buf.extend_from_slice(b" = ");
                    encode_expr(expr, buf)?;
                }
                encode_where(cmd, buf, params)?;
            }
        }
    }

    // RETURNING clause
    if let Some(ref ret_cols) = cmd.returning {
        buf.extend_from_slice(b" RETURNING ");
        encode_columns(ret_cols, buf)?;
    }

    Ok(())
}

/// Encode an UPDATE statement.
///
/// # Arguments
///
/// * `cmd` — Qail AST command with `Action::Set`.
/// * `buf` — Output buffer to append the SQL bytes to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_update(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
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
                encode_expr(col, buf)?;
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
                encode_expr(&cond.left, buf)?;
                buf.extend_from_slice(b" = ");
                encode_value(&cond.value, buf, params)?;
            }
        }
    }

    if !cmd.from_tables.is_empty() {
        buf.extend_from_slice(b" FROM ");
        for (i, table) in cmd.from_tables.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            buf.extend_from_slice(table.as_bytes());
        }
    }

    // WHERE (supports AND + OR filter cages)
    encode_where(cmd, buf, params)?;

    // RETURNING clause
    if let Some(ref ret_cols) = cmd.returning {
        buf.extend_from_slice(b" RETURNING ");
        encode_columns(ret_cols, buf)?;
    }

    Ok(())
}

/// Encode a DELETE statement.
///
/// # Arguments
///
/// * `cmd` — Qail AST command with `Action::Del`.
/// * `buf` — Output buffer to append the SQL bytes to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_delete(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    buf.extend_from_slice(b"DELETE FROM ");
    buf.extend_from_slice(cmd.table.as_bytes());

    if !cmd.using_tables.is_empty() {
        buf.extend_from_slice(b" USING ");
        for (i, table) in cmd.using_tables.iter().enumerate() {
            if i > 0 {
                buf.extend_from_slice(b", ");
            }
            buf.extend_from_slice(table.as_bytes());
        }
    }

    // WHERE (supports AND + OR filter cages)
    encode_where(cmd, buf, params)?;

    // RETURNING clause
    if let Some(ref ret_cols) = cmd.returning {
        buf.extend_from_slice(b" RETURNING ");
        encode_columns(ret_cols, buf)?;
    }

    Ok(())
}

/// Encode a PostgreSQL MERGE statement.
pub fn encode_merge(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    let merge = cmd
        .merge
        .as_ref()
        .ok_or(crate::protocol::EncodeError::InvalidAst(
            "MERGE requires merge specification".to_string(),
        ))?;
    validate_merge_shape(merge)?;

    encode_cte_prefix(cmd, buf, params)?;
    buf.extend_from_slice(b"MERGE INTO ");
    buf.extend_from_slice(cmd.table.as_bytes());
    if let Some(alias) = &merge.target_alias {
        buf.extend_from_slice(b" AS ");
        buf.extend_from_slice(alias.as_bytes());
    }

    buf.extend_from_slice(b" USING ");
    encode_merge_source(&merge.source, buf, params)?;

    buf.extend_from_slice(b" ON ");
    encode_conditions(&merge.on, buf, params)?;

    for clause in &merge.clauses {
        buf.extend_from_slice(b" WHEN ");
        match clause.match_kind {
            MergeMatchKind::Matched => buf.extend_from_slice(b"MATCHED"),
            MergeMatchKind::NotMatchedByTarget => buf.extend_from_slice(b"NOT MATCHED BY TARGET"),
            MergeMatchKind::NotMatchedBySource => buf.extend_from_slice(b"NOT MATCHED BY SOURCE"),
        }
        if !clause.condition.is_empty() {
            buf.extend_from_slice(b" AND ");
            encode_conditions(&clause.condition, buf, params)?;
        }
        buf.extend_from_slice(b" THEN ");
        encode_merge_action(&clause.action, buf)?;
    }

    if let Some(ref ret_cols) = cmd.returning
        && !ret_cols.is_empty()
    {
        buf.extend_from_slice(b" RETURNING ");
        encode_columns(ret_cols, buf)?;
    }

    Ok(())
}

fn validate_merge_shape(merge: &Merge) -> Result<(), crate::protocol::EncodeError> {
    if let MergeSource::Table { name, .. } = &merge.source
        && name.trim().is_empty()
    {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "MERGE requires a USING source table or query".to_string(),
        ));
    }
    if merge.on.is_empty() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "MERGE requires at least one ON condition".to_string(),
        ));
    }
    if merge.clauses.is_empty() {
        return Err(crate::protocol::EncodeError::InvalidAst(
            "MERGE requires at least one WHEN clause".to_string(),
        ));
    }

    for clause in &merge.clauses {
        match (&clause.match_kind, &clause.action) {
            (MergeMatchKind::Matched, MergeAction::Insert { .. }) => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "WHEN MATCHED cannot INSERT".to_string(),
                ));
            }
            (MergeMatchKind::NotMatchedByTarget, MergeAction::Update { .. })
            | (MergeMatchKind::NotMatchedByTarget, MergeAction::Delete) => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "WHEN NOT MATCHED BY TARGET can only INSERT or DO NOTHING".to_string(),
                ));
            }
            (MergeMatchKind::NotMatchedBySource, MergeAction::Insert { .. }) => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "WHEN NOT MATCHED BY SOURCE cannot INSERT".to_string(),
                ));
            }
            (_, MergeAction::Update { assignments }) if assignments.is_empty() => {
                return Err(crate::protocol::EncodeError::InvalidAst(
                    "MERGE UPDATE requires at least one assignment".to_string(),
                ));
            }
            (_, MergeAction::Insert { columns, values }) => {
                if values.is_empty() {
                    return Err(crate::protocol::EncodeError::InvalidAst(
                        "MERGE INSERT requires at least one value".to_string(),
                    ));
                }
                if !columns.is_empty() && columns.len() != values.len() {
                    return Err(crate::protocol::EncodeError::InvalidAst(
                        "MERGE INSERT column count must match value count".to_string(),
                    ));
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn encode_merge_source(
    source: &MergeSource,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    match source {
        MergeSource::Table { name, alias } => {
            buf.extend_from_slice(name.as_bytes());
            if let Some(alias) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(alias.as_bytes());
            }
        }
        MergeSource::Query { query, alias } => {
            buf.extend_from_slice(b"(");
            encode_select(query, buf, params)?;
            buf.extend_from_slice(b")");
            if let Some(alias) = alias {
                buf.extend_from_slice(b" AS ");
                buf.extend_from_slice(alias.as_bytes());
            }
        }
    }
    Ok(())
}

fn encode_merge_action(
    action: &MergeAction,
    buf: &mut BytesMut,
) -> Result<(), crate::protocol::EncodeError> {
    match action {
        MergeAction::Update { assignments } => {
            buf.extend_from_slice(b"UPDATE SET ");
            for (i, (col, expr)) in assignments.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                buf.extend_from_slice(col.as_bytes());
                buf.extend_from_slice(b" = ");
                encode_expr(expr, buf)?;
            }
        }
        MergeAction::Insert { columns, values } => {
            buf.extend_from_slice(b"INSERT");
            if !columns.is_empty() {
                buf.extend_from_slice(b" (");
                for (i, col) in columns.iter().enumerate() {
                    if i > 0 {
                        buf.extend_from_slice(b", ");
                    }
                    buf.extend_from_slice(col.as_bytes());
                }
                buf.extend_from_slice(b")");
            }
            buf.extend_from_slice(b" VALUES (");
            for (i, value) in values.iter().enumerate() {
                if i > 0 {
                    buf.extend_from_slice(b", ");
                }
                encode_expr(value, buf)?;
            }
            buf.extend_from_slice(b")");
        }
        MergeAction::Delete => buf.extend_from_slice(b"DELETE"),
        MergeAction::DoNothing => buf.extend_from_slice(b"DO NOTHING"),
    }
    Ok(())
}

/// Encode an EXPORT command as `COPY (SELECT ...) TO STDOUT`.
///
/// # Arguments
///
/// * `cmd` — Qail AST command with `Action::Export`.
/// * `buf` — Output buffer to append the SQL bytes to.
/// * `params` — Accumulator for parameterized bind values.
pub fn encode_export(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    buf.extend_from_slice(b"COPY (");
    encode_select(cmd, buf, params)?;
    buf.extend_from_slice(b") TO STDOUT");
    Ok(())
}

/// Encode a WHERE clause that supports both AND and OR filter cages.
///
/// - AND conditions (from `.eq()`, `.filter()`, etc.)
///   are joined with `AND`.
/// - OR conditions (from `.or_filter()`) are grouped into a single
///   parenthesized `(c1 OR c2 OR ... OR cN)` block, appended with `AND`.
///
/// Example output: `WHERE is_active = $1 AND (topic ILIKE $2 OR question ILIKE $3)`
fn encode_where(
    cmd: &Qail,
    buf: &mut BytesMut,
    params: &mut Vec<Option<Vec<u8>>>,
) -> Result<(), crate::protocol::EncodeError> {
    // Fast pre-scan: detect whether any AND/OR filter cages exist without
    // allocating temporary vectors.
    let mut has_and = false;
    let mut has_or = false;
    for cage in &cmd.cages {
        if cage.kind != CageKind::Filter || cage.conditions.is_empty() {
            continue;
        }
        match cage.logical_op {
            LogicalOp::And => has_and = true,
            LogicalOp::Or => has_or = true,
        }
    }

    if !has_and && !has_or {
        return Ok(());
    }

    buf.extend_from_slice(b" WHERE ");

    let mut wrote_clause = false;

    if has_and {
        for cage in &cmd.cages {
            if cage.kind != CageKind::Filter
                || cage.logical_op != LogicalOp::And
                || cage.conditions.is_empty()
            {
                continue;
            }
            if wrote_clause {
                buf.extend_from_slice(b" AND ");
            }
            encode_conditions(&cage.conditions, buf, params)?;
            wrote_clause = true;
        }
    }

    if has_or {
        if wrote_clause {
            buf.extend_from_slice(b" AND ");
        }
        buf.extend_from_slice(b"(");
        let mut first = true;
        for cage in &cmd.cages {
            if cage.kind != CageKind::Filter
                || cage.logical_op != LogicalOp::Or
                || cage.conditions.is_empty()
            {
                continue;
            }
            for cond in &cage.conditions {
                if !first {
                    buf.extend_from_slice(b" OR ");
                }
                first = false;
                encode_conditions(std::slice::from_ref(cond), buf, params)?;
            }
        }
        buf.extend_from_slice(b")");
    }

    Ok(())
}
