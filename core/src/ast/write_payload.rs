//! INSERT/UPDATE payload rules shared by the SQL transpiler and the native
//! PostgreSQL encoder, so a preview names the same columns and values that
//! execution binds.
//!
//! Values come from the first `Payload` cage, never from whichever cage is
//! first. Target columns come from `Qail::columns` when present; otherwise a
//! named payload (`set_value`) supplies them. A positional payload (`values`)
//! carries `$N` placeholder names that are never column names.

use std::collections::HashSet;

use crate::ast::{Cage, CageKind, Condition, Expr, Qail};

/// How a payload cage names its values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadShape {
    /// No values.
    Empty,
    /// Every value is a `$N` placeholder from `values([...])`.
    Positional,
    /// Every value is named by its target column (`set_value`).
    Named,
    /// Both kinds; never valid.
    Mixed,
}

/// Whether `expr` is a `$N` positional payload name.
pub fn is_positional_placeholder(expr: &Expr) -> bool {
    let Expr::Named(name) = expr else {
        return false;
    };
    name.strip_prefix('$')
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

/// Classify the payload conditions of a write.
pub fn payload_shape(conditions: &[Condition]) -> PayloadShape {
    let mut saw_positional = false;
    let mut saw_named = false;

    for condition in conditions {
        if is_positional_placeholder(&condition.left) {
            saw_positional = true;
        } else {
            saw_named = true;
        }
    }

    match (saw_positional, saw_named) {
        (false, false) => PayloadShape::Empty,
        (true, false) => PayloadShape::Positional,
        (false, true) => PayloadShape::Named,
        (true, true) => PayloadShape::Mixed,
    }
}

/// The payload cage a write reads its values from.
pub fn payload_cage(cmd: &Qail) -> Option<&Cage> {
    cmd.cages.iter().find(|cage| cage.kind == CageKind::Payload)
}

/// Explicit write target columns. The text parser fills `columns` with a lone
/// `*` when no `fields` clause is given (`set users values a = 1`); that is a
/// default projection, not a target list.
pub fn explicit_write_columns(cmd: &Qail) -> &[Expr] {
    match cmd.columns.as_slice() {
        [Expr::Star] => &[],
        columns => columns,
    }
}

/// INSERT target columns. Empty means no column list: PostgreSQL then fills
/// columns in table order.
pub fn insert_columns(cmd: &Qail) -> Vec<&Expr> {
    let explicit = explicit_write_columns(cmd);
    if !explicit.is_empty() {
        return explicit.iter().collect();
    }
    match payload_cage(cmd) {
        Some(cage) if payload_shape(&cage.conditions) == PayloadShape::Named => cage
            .conditions
            .iter()
            .map(|condition| &condition.left)
            .collect(),
        _ => Vec::new(),
    }
}

/// The INSERT VALUES row; each condition's `value` is one column value.
pub fn insert_values(cmd: &Qail) -> &[Condition] {
    payload_cage(cmd).map_or(&[], |cage| cage.conditions.as_slice())
}

/// UPDATE SET pairs: explicit columns pair with payload values by position,
/// otherwise each named payload condition assigns its own column.
pub fn update_assignments(cmd: &Qail) -> Vec<(&Expr, &Condition)> {
    let Some(cage) = payload_cage(cmd) else {
        return Vec::new();
    };
    let explicit = explicit_write_columns(cmd);
    if explicit.is_empty() {
        cage.conditions
            .iter()
            .map(|condition| (&condition.left, condition))
            .collect()
    } else {
        explicit.iter().zip(cage.conditions.iter()).collect()
    }
}

/// Column-name check without identifier syntax rules: a write target must be
/// `Expr::Named`. Returns the folded name used for duplicate detection.
pub fn simple_write_column(field: &str, expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Named(name) => Ok(name.to_ascii_lowercase()),
        _ => Err(format!("{field} must be a simple column identifier")),
    }
}

/// UPDATE SET target check: a column, optionally followed by `[integer or
/// column]` subscripts and `.field` selections. Accepts exactly what the
/// preview's target renderer can write and folds to the lowercased target
/// text, as the native encoder's `validate_update_target` does, so `col[1]`
/// and `col[2]` are distinct assignments.
pub fn update_write_target(field: &str, expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Named(name) => Ok(name.to_ascii_lowercase()),
        _ => update_target_selection(field, expr),
    }
}

fn update_target_selection(field: &str, expr: &Expr) -> Result<String, String> {
    let invalid = || {
        format!(
            "{field} must be a column with optional [integer or column] subscripts \
             and .field selections"
        )
    };
    match expr {
        Expr::Subscript {
            expr,
            index,
            alias: None,
        } => {
            let base = update_target_base(field, expr)?;
            let index = match index.as_ref() {
                Expr::Literal(crate::ast::Value::Int(n)) => n.to_string(),
                Expr::Named(column) if column.split('.').all(is_update_target_atom) => {
                    column.to_ascii_lowercase()
                }
                _ => return Err(invalid()),
            };
            Ok(format!("{base}[{index}]"))
        }
        Expr::FieldAccess {
            expr,
            field: name,
            alias: None,
        } if is_update_target_atom(name) => {
            let base = update_target_base(field, expr)?;
            Ok(format!("{base}.{}", name.to_ascii_lowercase()))
        }
        _ => Err(invalid()),
    }
}

/// A dotted base would read as `column.field`, not `table.column`.
fn update_target_base(field: &str, expr: &Expr) -> Result<String, String> {
    match expr {
        Expr::Named(name) if is_update_target_atom(name) => Ok(name.to_ascii_lowercase()),
        Expr::Named(_) => Err(format!(
            "{field} must be a column with optional [integer or column] subscripts \
             and .field selections"
        )),
        other => update_target_selection(field, other),
    }
}

/// Letters, digits and underscore, at most 63 bytes: the rule the preview's
/// target renderer and the native encoder apply to target parts.
fn is_update_target_atom(name: &str) -> bool {
    !name.is_empty() && name.len() <= 63 && name.chars().all(|c| c.is_alphanumeric() || c == '_')
}

fn check_unique_columns<E>(
    statement: &str,
    columns: Vec<String>,
    invalid: &impl Fn(String) -> E,
) -> Result<(), E> {
    let mut seen = HashSet::new();
    for column in columns {
        if !seen.insert(column.clone()) {
            return Err(invalid(format!(
                "{statement} assigns column more than once: {column}"
            )));
        }
    }
    Ok(())
}

/// Explicit columns pair with payload values by position. A named payload that
/// names other columns, or the same ones in another order, would bind values
/// to the wrong columns, so it is rejected.
fn check_named_payload_order<E>(
    statement: &str,
    explicit: &[String],
    payload: &[Condition],
    column: &mut impl FnMut(&str, &Expr) -> Result<String, E>,
    invalid: &impl Fn(String) -> E,
) -> Result<(), E> {
    if payload_shape(payload) != PayloadShape::Named {
        return Ok(());
    }
    let field = format!("{}.payload.column", statement.to_ascii_lowercase());
    for (expected, condition) in explicit.iter().zip(payload) {
        let named = column(&field, &condition.left)?;
        if &named != expected {
            return Err(invalid(format!(
                "{statement} payload column {named} does not match column list entry {expected}"
            )));
        }
    }
    Ok(())
}

/// Validate INSERT payload shape. `column` checks one target column and
/// returns its folded name; `invalid` wraps a shape error message.
pub fn check_insert_shape<E>(
    cmd: &Qail,
    mut column: impl FnMut(&str, &Expr) -> Result<String, E>,
    invalid: impl Fn(String) -> E,
) -> Result<(), E> {
    let payload = payload_cage(cmd);
    let payload_len = payload.map_or(0, |cage| cage.conditions.len());
    let shape = payload.map_or(PayloadShape::Empty, |cage| payload_shape(&cage.conditions));

    if shape == PayloadShape::Mixed {
        return Err(invalid(
            "INSERT payload cannot mix positional values with named column assignments".to_string(),
        ));
    }

    if cmd.default_values {
        if cmd.source_query.is_some() || payload_len > 0 {
            return Err(invalid(
                "INSERT DEFAULT VALUES cannot combine source query or VALUES payload".to_string(),
            ));
        }
    } else if cmd.source_query.is_some() {
        if payload_len > 0 {
            return Err(invalid(
                "INSERT cannot combine source query with VALUES payload".to_string(),
            ));
        }
    } else if payload_len == 0 {
        return Err(invalid(
            "INSERT requires VALUES, source query, or DEFAULT VALUES".to_string(),
        ));
    }

    let explicit = explicit_write_columns(cmd);
    if !explicit.is_empty() {
        let columns = explicit
            .iter()
            .map(|expr| column("insert.column", expr))
            .collect::<Result<Vec<_>, _>>()?;
        check_unique_columns("INSERT", columns.clone(), &invalid)?;

        if !cmd.default_values && cmd.source_query.is_none() && explicit.len() != payload_len {
            return Err(invalid(
                "INSERT column count must match value count".to_string(),
            ));
        }
        if let Some(payload) = payload {
            check_named_payload_order(
                "INSERT",
                &columns,
                &payload.conditions,
                &mut column,
                &invalid,
            )?;
        }
    } else if let Some(payload) = payload
        && shape == PayloadShape::Named
    {
        let columns = payload
            .conditions
            .iter()
            .map(|condition| column("insert.payload.column", &condition.left))
            .collect::<Result<Vec<_>, _>>()?;
        check_unique_columns("INSERT", columns, &invalid)?;
    }

    Ok(())
}

/// Validate UPDATE payload shape; arguments as in [`check_insert_shape`].
pub fn check_update_shape<E>(
    cmd: &Qail,
    mut column: impl FnMut(&str, &Expr) -> Result<String, E>,
    invalid: impl Fn(String) -> E,
) -> Result<(), E> {
    let Some(payload) = payload_cage(cmd).filter(|cage| !cage.conditions.is_empty()) else {
        return Err(invalid(
            "UPDATE requires at least one assignment".to_string(),
        ));
    };

    let shape = payload_shape(&payload.conditions);
    if shape == PayloadShape::Mixed {
        return Err(invalid(
            "UPDATE payload cannot mix positional values with named column assignments".to_string(),
        ));
    }

    let explicit = explicit_write_columns(cmd);
    if !explicit.is_empty() {
        let columns = explicit
            .iter()
            .map(|expr| column("update.column", expr))
            .collect::<Result<Vec<_>, _>>()?;
        check_unique_columns("UPDATE", columns.clone(), &invalid)?;
        if explicit.len() != payload.conditions.len() {
            return Err(invalid(
                "UPDATE column count must match value count".to_string(),
            ));
        }
        check_named_payload_order(
            "UPDATE",
            &columns,
            &payload.conditions,
            &mut column,
            &invalid,
        )?;
    } else {
        if shape == PayloadShape::Positional {
            return Err(invalid(
                "UPDATE positional values require explicit target columns".to_string(),
            ));
        }
        let columns = payload
            .conditions
            .iter()
            .map(|condition| column("update.payload.column", &condition.left))
            .collect::<Result<Vec<_>, _>>()?;
        check_unique_columns("UPDATE", columns, &invalid)?;
    }

    Ok(())
}
