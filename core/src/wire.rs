//! QAIL wire codecs for command transport.
//!
//! - Text codecs (`QAIL-CMD/1`, `QAIL-CMDS/1`) round-trip through canonical text.
//! - Binary codec (`QWB2`) transports AST bytes directly (parser-free decode).

use crate::ast::Qail;

const CMD_TEXT_MAGIC: &str = "QAIL-CMD/1";
const CMDS_TEXT_MAGIC: &str = "QAIL-CMDS/1";
const CMD_BIN_MAGIC: [u8; 4] = *b"QWB2";
const CMD_BIN_LEGACY_MAGIC: [u8; 4] = *b"QWB1";

const MAX_BIN_AST_PAYLOAD_BYTES: usize = 64 * 1024;
const MAX_AST_DEPTH: usize = 64;
const MAX_AST_NODES: usize = 16_384;
const MAX_AST_COLLECTION_LEN: usize = 2_048;
const MAX_AST_STRING_LEN: usize = 32 * 1024;
const MAX_AST_VECTOR_LEN: usize = 8_192;
const MAX_AST_BINARY_VALUE_LEN: usize = 32 * 1024;

/// Encode one command into versioned text wire format.
pub fn encode_cmd_text(cmd: &Qail) -> String {
    let payload = cmd.to_string();
    let mut out = String::with_capacity(CMD_TEXT_MAGIC.len() + payload.len() + 32);
    out.push_str(CMD_TEXT_MAGIC);
    out.push('\n');
    out.push_str(&payload.len().to_string());
    out.push('\n');
    out.push_str(&payload);
    out
}

/// Decode one command from text wire format.
///
/// Also accepts raw QAIL query text as fallback for convenience.
pub fn decode_cmd_text(input: &str) -> Result<Qail, String> {
    let bytes = input.as_bytes();
    let mut idx = 0usize;

    let Ok(magic) = read_line(bytes, &mut idx) else {
        return crate::parse(input).map_err(|e| e.to_string());
    };

    if magic != CMD_TEXT_MAGIC {
        return crate::parse(input).map_err(|e| e.to_string());
    }

    let len_line = read_line(bytes, &mut idx)?;
    let payload_len = parse_usize("payload length", len_line)?;
    let payload = read_exact_utf8(bytes, &mut idx, payload_len)?;
    if idx != bytes.len() {
        return Err("trailing bytes after command payload".to_string());
    }

    crate::parse(payload).map_err(|e| e.to_string())
}

/// Encode multiple commands into versioned text wire format.
pub fn encode_cmds_text(cmds: &[Qail]) -> String {
    let mut out = String::new();
    out.push_str(CMDS_TEXT_MAGIC);
    out.push('\n');
    out.push_str(&cmds.len().to_string());
    out.push('\n');

    for cmd in cmds {
        let payload = cmd.to_string();
        out.push_str(&payload.len().to_string());
        out.push('\n');
        out.push_str(&payload);
    }

    out
}

/// Decode multiple commands from text wire format.
pub fn decode_cmds_text(input: &str) -> Result<Vec<Qail>, String> {
    let bytes = input.as_bytes();
    let mut idx = 0usize;

    let magic = read_line(bytes, &mut idx)?;
    if magic != CMDS_TEXT_MAGIC {
        return Err(format!(
            "invalid wire magic: expected {CMDS_TEXT_MAGIC}, got {magic}"
        ));
    }

    let count_line = read_line(bytes, &mut idx)?;
    let count = parse_usize("command count", count_line)?;
    let mut out = Vec::with_capacity(count);

    for _ in 0..count {
        let len_line = read_line(bytes, &mut idx)?;
        let payload_len = parse_usize("payload length", len_line)?;
        let payload = read_exact_utf8(bytes, &mut idx, payload_len)?;
        let cmd = crate::parse(payload).map_err(|e| e.to_string())?;
        out.push(cmd);
    }

    if idx != bytes.len() {
        return Err("trailing bytes after batch payload".to_string());
    }

    Ok(out)
}

/// Encode one command into compact binary wire format (QWB2 AST binary).
pub fn encode_cmd_binary(cmd: &Qail) -> Vec<u8> {
    try_encode_cmd_binary(cmd).expect("QWB2 AST binary encoding must succeed for valid Qail")
}

/// Fallible QWB2 AST-binary encoder.
pub fn try_encode_cmd_binary(cmd: &Qail) -> Result<Vec<u8>, String> {
    let payload = bincode::serde::encode_to_vec(cmd, bincode::config::standard())
        .map_err(|e| format!("binary AST encode failed: {e}"))?;
    if payload.len() > MAX_BIN_AST_PAYLOAD_BYTES {
        return Err(format!(
            "binary AST payload too large: {} bytes (max {})",
            payload.len(),
            MAX_BIN_AST_PAYLOAD_BYTES
        ));
    }

    let payload_len = u32::try_from(payload.len())
        .map_err(|_| format!("binary AST payload exceeds u32 length: {}", payload.len()))?;
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(&CMD_BIN_MAGIC);
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode one command from strict QWB2 AST-binary wire format.
///
/// This path is parser-free and rejects legacy QWB1/raw-text payloads.
pub fn decode_cmd_binary(input: &[u8]) -> Result<Qail, String> {
    let payload = decode_cmd_binary_payload(input)?;
    let (cmd, consumed): (Qail, usize) =
        bincode::serde::decode_from_slice(payload, bincode::config::standard())
            .map_err(|e| format!("binary AST decode failed: {e}"))?;
    if consumed != payload.len() {
        return Err("trailing bytes after AST payload".to_string());
    }
    validate_binary_ast_limits(&cmd)?;
    Ok(cmd)
}

/// Decode and validate strict QWB2-framed payload bytes.
///
/// This validates framing and payload-size limits only.
pub fn decode_cmd_binary_payload(input: &[u8]) -> Result<&[u8], String> {
    if input.len() < 8 {
        return Err("invalid wire header".to_string());
    }
    if input[0..4] != CMD_BIN_MAGIC {
        if input[0..4] == CMD_BIN_LEGACY_MAGIC {
            return Err(
                "legacy QWB1 text payload is not supported on parse-free binary path".to_string(),
            );
        }
        return Err("invalid wire header".to_string());
    }

    let len = u32::from_be_bytes([input[4], input[5], input[6], input[7]]) as usize;
    if len > MAX_BIN_AST_PAYLOAD_BYTES {
        return Err(format!(
            "binary AST payload too large: header={len}, max={MAX_BIN_AST_PAYLOAD_BYTES}"
        ));
    }
    if input.len() != 8 + len {
        return Err(format!(
            "invalid payload length: header={len}, actual={}",
            input.len().saturating_sub(8)
        ));
    }
    Ok(&input[8..])
}

#[derive(Default)]
struct AstLimitState {
    nodes: usize,
}

impl AstLimitState {
    fn bump(&mut self, kind: &str) -> Result<(), String> {
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or_else(|| "AST node counter overflow".to_string())?;
        if self.nodes > MAX_AST_NODES {
            return Err(format!(
                "AST node limit exceeded while walking {kind}: {} > {}",
                self.nodes, MAX_AST_NODES
            ));
        }
        Ok(())
    }
}

fn ensure_depth(depth: usize, kind: &str) -> Result<(), String> {
    if depth > MAX_AST_DEPTH {
        return Err(format!(
            "AST depth limit exceeded while walking {kind}: {depth} > {MAX_AST_DEPTH}"
        ));
    }
    Ok(())
}

fn ensure_len(kind: &str, len: usize, max: usize) -> Result<(), String> {
    if len > max {
        return Err(format!("{kind} exceeds limit: {len} > {max}"));
    }
    Ok(())
}

fn ensure_str(kind: &str, value: &str) -> Result<(), String> {
    ensure_len(kind, value.len(), MAX_AST_STRING_LEN)
}

fn validate_binary_ast_limits(cmd: &Qail) -> Result<(), String> {
    let mut state = AstLimitState::default();
    validate_qail_limits(cmd, 0, &mut state)
}

fn validate_qail_limits(cmd: &Qail, depth: usize, state: &mut AstLimitState) -> Result<(), String> {
    use crate::ast::GroupByMode;

    ensure_depth(depth, "Qail")?;
    state.bump("Qail")?;

    ensure_str("qail.table", &cmd.table)?;
    ensure_len("qail.columns", cmd.columns.len(), MAX_AST_COLLECTION_LEN)?;
    for expr in &cmd.columns {
        validate_expr_limits(expr, depth + 1, state)?;
    }

    ensure_len("qail.joins", cmd.joins.len(), MAX_AST_COLLECTION_LEN)?;
    for join in &cmd.joins {
        validate_join_limits(join, depth + 1, state)?;
    }

    ensure_len("qail.cages", cmd.cages.len(), MAX_AST_COLLECTION_LEN)?;
    for cage in &cmd.cages {
        validate_cage_limits(cage, depth + 1, state)?;
    }

    if let Some(index_def) = &cmd.index_def {
        validate_index_def_limits(index_def)?;
    }

    ensure_len(
        "qail.table_constraints",
        cmd.table_constraints.len(),
        MAX_AST_COLLECTION_LEN,
    )?;
    for constraint in &cmd.table_constraints {
        match constraint {
            crate::ast::TableConstraint::Unique(cols)
            | crate::ast::TableConstraint::PrimaryKey(cols) => {
                ensure_len(
                    "qail.table_constraint.columns",
                    cols.len(),
                    MAX_AST_COLLECTION_LEN,
                )?;
                for col in cols {
                    ensure_str("qail.table_constraint.column", col)?;
                }
            }
        }
    }

    ensure_len("qail.set_ops", cmd.set_ops.len(), MAX_AST_COLLECTION_LEN)?;
    for (_, rhs) in &cmd.set_ops {
        validate_qail_limits(rhs, depth + 1, state)?;
    }

    ensure_len("qail.having", cmd.having.len(), MAX_AST_COLLECTION_LEN)?;
    for cond in &cmd.having {
        validate_condition_limits(cond, depth + 1, state)?;
    }

    if let GroupByMode::GroupingSets(groups) = &cmd.group_by_mode {
        ensure_len("qail.grouping_sets", groups.len(), MAX_AST_COLLECTION_LEN)?;
        for group in groups {
            ensure_len("qail.grouping_set", group.len(), MAX_AST_COLLECTION_LEN)?;
            for col in group {
                ensure_str("qail.grouping_set.column", col)?;
            }
        }
    }

    ensure_len("qail.ctes", cmd.ctes.len(), MAX_AST_COLLECTION_LEN)?;
    for cte in &cmd.ctes {
        ensure_str("qail.cte.name", &cte.name)?;
        ensure_len(
            "qail.cte.columns",
            cte.columns.len(),
            MAX_AST_COLLECTION_LEN,
        )?;
        for col in &cte.columns {
            ensure_str("qail.cte.column", col)?;
        }
        validate_qail_limits(&cte.base_query, depth + 1, state)?;
        if let Some(recursive) = &cte.recursive_query {
            validate_qail_limits(recursive, depth + 1, state)?;
        }
        if let Some(source_table) = &cte.source_table {
            ensure_str("qail.cte.source_table", source_table)?;
        }
    }

    ensure_len(
        "qail.distinct_on",
        cmd.distinct_on.len(),
        MAX_AST_COLLECTION_LEN,
    )?;
    for expr in &cmd.distinct_on {
        validate_expr_limits(expr, depth + 1, state)?;
    }

    if let Some(returning) = &cmd.returning {
        ensure_len("qail.returning", returning.len(), MAX_AST_COLLECTION_LEN)?;
        for expr in returning {
            validate_expr_limits(expr, depth + 1, state)?;
        }
    }

    if let Some(on_conflict) = &cmd.on_conflict {
        ensure_len(
            "qail.on_conflict.columns",
            on_conflict.columns.len(),
            MAX_AST_COLLECTION_LEN,
        )?;
        for col in &on_conflict.columns {
            ensure_str("qail.on_conflict.column", col)?;
        }
        if let Some(assignments) = on_conflict.action.update_assignments() {
            ensure_len(
                "qail.on_conflict.assignments",
                assignments.len(),
                MAX_AST_COLLECTION_LEN,
            )?;
            for (col, expr) in assignments {
                ensure_str("qail.on_conflict.assignment.column", col)?;
                validate_expr_limits(expr, depth + 1, state)?;
            }
        }
    }

    if let Some(source_query) = &cmd.source_query {
        validate_qail_limits(source_query, depth + 1, state)?;
    }

    if let Some(channel) = &cmd.channel {
        ensure_str("qail.channel", channel)?;
    }
    if let Some(payload) = &cmd.payload {
        ensure_str("qail.payload", payload)?;
    }
    if let Some(savepoint_name) = &cmd.savepoint_name {
        ensure_str("qail.savepoint_name", savepoint_name)?;
    }

    ensure_len(
        "qail.from_tables",
        cmd.from_tables.len(),
        MAX_AST_COLLECTION_LEN,
    )?;
    for table in &cmd.from_tables {
        ensure_str("qail.from_table", table)?;
    }

    ensure_len(
        "qail.using_tables",
        cmd.using_tables.len(),
        MAX_AST_COLLECTION_LEN,
    )?;
    for table in &cmd.using_tables {
        ensure_str("qail.using_table", table)?;
    }

    if let Some((_, percent, _seed)) = cmd.sample
        && !percent.is_finite()
    {
        return Err("qail.sample.percent must be finite".to_string());
    }

    if let Some(vector) = &cmd.vector {
        ensure_len("qail.vector", vector.len(), MAX_AST_VECTOR_LEN)?;
    }
    if let Some(vector_name) = &cmd.vector_name {
        ensure_str("qail.vector_name", vector_name)?;
    }
    if let Some(function_def) = &cmd.function_def {
        validate_function_def_limits(function_def)?;
    }
    if let Some(trigger_def) = &cmd.trigger_def {
        validate_trigger_def_limits(trigger_def)?;
    }
    if let Some(policy_def) = &cmd.policy_def {
        validate_policy_def_limits(policy_def, depth + 1, state)?;
    }

    Ok(())
}

fn validate_join_limits(
    join: &crate::ast::Join,
    depth: usize,
    state: &mut AstLimitState,
) -> Result<(), String> {
    ensure_depth(depth, "Join")?;
    state.bump("Join")?;
    ensure_str("join.table", &join.table)?;
    if let Some(on) = &join.on {
        ensure_len("join.on", on.len(), MAX_AST_COLLECTION_LEN)?;
        for cond in on {
            validate_condition_limits(cond, depth + 1, state)?;
        }
    }
    Ok(())
}

fn validate_cage_limits(
    cage: &crate::ast::Cage,
    depth: usize,
    state: &mut AstLimitState,
) -> Result<(), String> {
    use crate::ast::CageKind;

    ensure_depth(depth, "Cage")?;
    state.bump("Cage")?;
    ensure_len(
        "cage.conditions",
        cage.conditions.len(),
        MAX_AST_COLLECTION_LEN,
    )?;
    for cond in &cage.conditions {
        validate_condition_limits(cond, depth + 1, state)?;
    }
    match cage.kind {
        CageKind::Limit(v) | CageKind::Offset(v) | CageKind::Sample(v) => {
            ensure_len("cage.numeric", v, usize::MAX)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_condition_limits(
    cond: &crate::ast::Condition,
    depth: usize,
    state: &mut AstLimitState,
) -> Result<(), String> {
    ensure_depth(depth, "Condition")?;
    state.bump("Condition")?;
    validate_expr_limits(&cond.left, depth + 1, state)?;
    validate_value_limits(&cond.value, depth + 1, state)
}

fn validate_expr_limits(
    expr: &crate::ast::Expr,
    depth: usize,
    state: &mut AstLimitState,
) -> Result<(), String> {
    use crate::ast::{ColumnGeneration, Constraint, Expr, WindowFrame};

    ensure_depth(depth, "Expr")?;
    state.bump("Expr")?;

    match expr {
        Expr::Star => {}
        Expr::Named(name) => ensure_str("expr.named", name)?,
        Expr::Aliased { name, alias } => {
            ensure_str("expr.aliased.name", name)?;
            ensure_str("expr.aliased.alias", alias)?;
        }
        Expr::Aggregate {
            col, filter, alias, ..
        } => {
            ensure_str("expr.aggregate.col", col)?;
            if let Some(filters) = filter {
                ensure_len(
                    "expr.aggregate.filter",
                    filters.len(),
                    MAX_AST_COLLECTION_LEN,
                )?;
                for cond in filters {
                    validate_condition_limits(cond, depth + 1, state)?;
                }
            }
            if let Some(alias) = alias {
                ensure_str("expr.aggregate.alias", alias)?;
            }
        }
        Expr::Cast {
            expr,
            target_type,
            alias,
        } => {
            validate_expr_limits(expr, depth + 1, state)?;
            ensure_str("expr.cast.target_type", target_type)?;
            if let Some(alias) = alias {
                ensure_str("expr.cast.alias", alias)?;
            }
        }
        Expr::Def {
            name,
            data_type,
            constraints,
        } => {
            ensure_str("expr.def.name", name)?;
            ensure_str("expr.def.data_type", data_type)?;
            ensure_len(
                "expr.def.constraints",
                constraints.len(),
                MAX_AST_COLLECTION_LEN,
            )?;
            for constraint in constraints {
                match constraint {
                    Constraint::PrimaryKey | Constraint::Unique | Constraint::Nullable => {}
                    Constraint::Default(v) => ensure_str("expr.def.default", v)?,
                    Constraint::Check(values) => {
                        ensure_len("expr.def.check", values.len(), MAX_AST_COLLECTION_LEN)?;
                        for value in values {
                            ensure_str("expr.def.check.value", value)?;
                        }
                    }
                    Constraint::Comment(v) | Constraint::References(v) => {
                        ensure_str("expr.def.constraint", v)?;
                    }
                    Constraint::Generated(ColumnGeneration::Stored(v))
                    | Constraint::Generated(ColumnGeneration::Virtual(v)) => {
                        ensure_str("expr.def.generated", v)?;
                    }
                }
            }
        }
        Expr::Mod { col, .. } => validate_expr_limits(col, depth + 1, state)?,
        Expr::Window {
            name,
            func,
            params,
            partition,
            order,
            frame,
        } => {
            ensure_str("expr.window.name", name)?;
            ensure_str("expr.window.func", func)?;
            ensure_len("expr.window.params", params.len(), MAX_AST_COLLECTION_LEN)?;
            for param in params {
                validate_expr_limits(param, depth + 1, state)?;
            }
            ensure_len(
                "expr.window.partition",
                partition.len(),
                MAX_AST_COLLECTION_LEN,
            )?;
            for col in partition {
                ensure_str("expr.window.partition.column", col)?;
            }
            ensure_len("expr.window.order", order.len(), MAX_AST_COLLECTION_LEN)?;
            for cage in order {
                validate_cage_limits(cage, depth + 1, state)?;
            }
            if let Some(frame) = frame {
                match frame {
                    WindowFrame::Rows { .. } | WindowFrame::Range { .. } => {}
                }
            }
        }
        Expr::Case {
            when_clauses,
            else_value,
            alias,
        } => {
            ensure_len("expr.case.when", when_clauses.len(), MAX_AST_COLLECTION_LEN)?;
            for (cond, then_expr) in when_clauses {
                validate_condition_limits(cond, depth + 1, state)?;
                validate_expr_limits(then_expr, depth + 1, state)?;
            }
            if let Some(else_expr) = else_value {
                validate_expr_limits(else_expr, depth + 1, state)?;
            }
            if let Some(alias) = alias {
                ensure_str("expr.case.alias", alias)?;
            }
        }
        Expr::JsonAccess {
            column,
            path_segments,
            alias,
        } => {
            ensure_str("expr.json_access.column", column)?;
            ensure_len(
                "expr.json_access.path_segments",
                path_segments.len(),
                MAX_AST_COLLECTION_LEN,
            )?;
            for (segment, _) in path_segments {
                ensure_str("expr.json_access.segment", segment)?;
            }
            if let Some(alias) = alias {
                ensure_str("expr.json_access.alias", alias)?;
            }
        }
        Expr::FunctionCall { name, args, alias } => {
            ensure_str("expr.function_call.name", name)?;
            ensure_len(
                "expr.function_call.args",
                args.len(),
                MAX_AST_COLLECTION_LEN,
            )?;
            for arg in args {
                validate_expr_limits(arg, depth + 1, state)?;
            }
            if let Some(alias) = alias {
                ensure_str("expr.function_call.alias", alias)?;
            }
        }
        Expr::SpecialFunction { name, args, alias } => {
            ensure_str("expr.special_function.name", name)?;
            ensure_len(
                "expr.special_function.args",
                args.len(),
                MAX_AST_COLLECTION_LEN,
            )?;
            for (keyword, arg) in args {
                if let Some(keyword) = keyword {
                    ensure_str("expr.special_function.keyword", keyword)?;
                }
                validate_expr_limits(arg, depth + 1, state)?;
            }
            if let Some(alias) = alias {
                ensure_str("expr.special_function.alias", alias)?;
            }
        }
        Expr::Binary {
            left, right, alias, ..
        } => {
            validate_expr_limits(left, depth + 1, state)?;
            validate_expr_limits(right, depth + 1, state)?;
            if let Some(alias) = alias {
                ensure_str("expr.binary.alias", alias)?;
            }
        }
        Expr::Literal(v) => validate_value_limits(v, depth + 1, state)?,
        Expr::ArrayConstructor { elements, alias } | Expr::RowConstructor { elements, alias } => {
            ensure_len("expr.elements", elements.len(), MAX_AST_COLLECTION_LEN)?;
            for el in elements {
                validate_expr_limits(el, depth + 1, state)?;
            }
            if let Some(alias) = alias {
                ensure_str("expr.elements.alias", alias)?;
            }
        }
        Expr::Subscript { expr, index, alias } => {
            validate_expr_limits(expr, depth + 1, state)?;
            validate_expr_limits(index, depth + 1, state)?;
            if let Some(alias) = alias {
                ensure_str("expr.subscript.alias", alias)?;
            }
        }
        Expr::Collate {
            expr,
            collation,
            alias,
        } => {
            validate_expr_limits(expr, depth + 1, state)?;
            ensure_str("expr.collate.collation", collation)?;
            if let Some(alias) = alias {
                ensure_str("expr.collate.alias", alias)?;
            }
        }
        Expr::FieldAccess { expr, field, alias } => {
            validate_expr_limits(expr, depth + 1, state)?;
            ensure_str("expr.field_access.field", field)?;
            if let Some(alias) = alias {
                ensure_str("expr.field_access.alias", alias)?;
            }
        }
        Expr::Subquery { query, alias } => {
            validate_qail_limits(query, depth + 1, state)?;
            if let Some(alias) = alias {
                ensure_str("expr.subquery.alias", alias)?;
            }
        }
        Expr::Exists { query, alias, .. } => {
            validate_qail_limits(query, depth + 1, state)?;
            if let Some(alias) = alias {
                ensure_str("expr.exists.alias", alias)?;
            }
        }
    }

    Ok(())
}

fn validate_value_limits(
    value: &crate::ast::Value,
    depth: usize,
    state: &mut AstLimitState,
) -> Result<(), String> {
    use crate::ast::Value;

    ensure_depth(depth, "Value")?;
    state.bump("Value")?;

    match value {
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::Param(_) => {}
        Value::String(v)
        | Value::NamedParam(v)
        | Value::Function(v)
        | Value::Column(v)
        | Value::Timestamp(v)
        | Value::Json(v) => ensure_str("value.string", v)?,
        Value::Array(values) => {
            ensure_len("value.array", values.len(), MAX_AST_COLLECTION_LEN)?;
            for v in values {
                validate_value_limits(v, depth + 1, state)?;
            }
        }
        Value::Subquery(q) => validate_qail_limits(q, depth + 1, state)?,
        Value::Uuid(_) | Value::NullUuid | Value::Interval { .. } => {}
        Value::Bytes(bytes) => ensure_len("value.bytes", bytes.len(), MAX_AST_BINARY_VALUE_LEN)?,
        Value::Expr(expr) => validate_expr_limits(expr, depth + 1, state)?,
        Value::Vector(values) => ensure_len("value.vector", values.len(), MAX_AST_VECTOR_LEN)?,
    }

    Ok(())
}

fn validate_index_def_limits(index_def: &crate::ast::IndexDef) -> Result<(), String> {
    ensure_str("index_def.name", &index_def.name)?;
    ensure_str("index_def.table", &index_def.table)?;
    ensure_len(
        "index_def.columns",
        index_def.columns.len(),
        MAX_AST_COLLECTION_LEN,
    )?;
    for col in &index_def.columns {
        ensure_str("index_def.column", col)?;
    }
    if let Some(index_type) = &index_def.index_type {
        ensure_str("index_def.index_type", index_type)?;
    }
    if let Some(where_clause) = &index_def.where_clause {
        ensure_str("index_def.where_clause", where_clause)?;
    }
    Ok(())
}

fn validate_function_def_limits(function_def: &crate::ast::FunctionDef) -> Result<(), String> {
    ensure_str("function_def.name", &function_def.name)?;
    ensure_len(
        "function_def.args",
        function_def.args.len(),
        MAX_AST_COLLECTION_LEN,
    )?;
    for arg in &function_def.args {
        ensure_str("function_def.arg", arg)?;
    }
    ensure_str("function_def.returns", &function_def.returns)?;
    ensure_str("function_def.body", &function_def.body)?;
    if let Some(language) = &function_def.language {
        ensure_str("function_def.language", language)?;
    }
    if let Some(volatility) = &function_def.volatility {
        ensure_str("function_def.volatility", volatility)?;
    }
    Ok(())
}

fn validate_trigger_def_limits(trigger_def: &crate::ast::TriggerDef) -> Result<(), String> {
    ensure_str("trigger_def.name", &trigger_def.name)?;
    ensure_str("trigger_def.table", &trigger_def.table)?;
    ensure_len(
        "trigger_def.events",
        trigger_def.events.len(),
        MAX_AST_COLLECTION_LEN,
    )?;
    ensure_len(
        "trigger_def.update_columns",
        trigger_def.update_columns.len(),
        MAX_AST_COLLECTION_LEN,
    )?;
    for col in &trigger_def.update_columns {
        ensure_str("trigger_def.update_column", col)?;
    }
    ensure_str(
        "trigger_def.execute_function",
        &trigger_def.execute_function,
    )?;
    Ok(())
}

fn validate_policy_def_limits(
    policy_def: &crate::migrate::policy::RlsPolicy,
    depth: usize,
    state: &mut AstLimitState,
) -> Result<(), String> {
    ensure_str("policy_def.name", &policy_def.name)?;
    ensure_str("policy_def.table", &policy_def.table)?;
    if let Some(using_expr) = &policy_def.using {
        validate_expr_limits(using_expr, depth + 1, state)?;
    }
    if let Some(with_check_expr) = &policy_def.with_check {
        validate_expr_limits(with_check_expr, depth + 1, state)?;
    }
    if let Some(role) = &policy_def.role {
        ensure_str("policy_def.role", role)?;
    }
    Ok(())
}

fn read_line<'a>(bytes: &'a [u8], idx: &mut usize) -> Result<&'a str, String> {
    if *idx >= bytes.len() {
        return Err("unexpected EOF".to_string());
    }

    let start = *idx;
    while *idx < bytes.len() && bytes[*idx] != b'\n' {
        *idx += 1;
    }

    if *idx >= bytes.len() {
        return Err("unterminated header line".to_string());
    }

    let line =
        std::str::from_utf8(&bytes[start..*idx]).map_err(|_| "header is not UTF-8".to_string())?;
    *idx += 1; // consume '\n'
    Ok(line)
}

fn parse_usize(field: &str, line: &str) -> Result<usize, String> {
    line.parse::<usize>()
        .map_err(|_| format!("invalid {field}: {line}"))
}

fn read_exact_utf8<'a>(bytes: &'a [u8], idx: &mut usize, len: usize) -> Result<&'a str, String> {
    if *idx + len > bytes.len() {
        return Err("payload truncated".to_string());
    }
    let start = *idx;
    *idx += len;
    std::str::from_utf8(&bytes[start..start + len]).map_err(|_| "payload is not UTF-8".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn cmd_text_roundtrip() {
        let cmd = crate::ast::Qail::get("users")
            .columns(["id", "email"])
            .where_eq("active", true)
            .limit(10);

        let encoded = encode_cmd_text(&cmd);
        let decoded = decode_cmd_text(&encoded).unwrap();
        assert_eq!(decoded.to_string(), cmd.to_string());
    }

    #[test]
    fn cmd_binary_roundtrip() {
        let cmd = crate::ast::Qail::set("users")
            .set_value("active", true)
            .where_eq("id", 7);

        let encoded = encode_cmd_binary(&cmd);
        let decoded = decode_cmd_binary(&encoded).unwrap();
        assert_eq!(decoded.to_string(), cmd.to_string());
    }

    #[test]
    fn cmd_binary_payload_roundtrip() {
        let cmd = crate::ast::Qail::get("users").limit(3);
        let encoded = encode_cmd_binary(&cmd);
        let payload = decode_cmd_binary_payload(&encoded).unwrap();
        let (decoded, consumed): (crate::ast::Qail, usize) =
            bincode::serde::decode_from_slice(payload, bincode::config::standard()).unwrap();
        assert_eq!(consumed, payload.len());
        assert_eq!(decoded.to_string(), cmd.to_string());
    }

    #[test]
    fn cmd_binary_payload_rejects_legacy_qwb1() {
        let legacy_text = b"get users limit 1";
        let mut payload = Vec::new();
        payload.extend_from_slice(&CMD_BIN_LEGACY_MAGIC);
        payload.extend_from_slice(&(legacy_text.len() as u32).to_be_bytes());
        payload.extend_from_slice(legacy_text);

        let err = decode_cmd_binary_payload(&payload).unwrap_err();
        assert!(err.contains("legacy QWB1"));
    }

    #[test]
    fn cmd_binary_decode_rejects_raw_text_without_qwb2_header() {
        let err = decode_cmd_binary(b"get users limit 1").unwrap_err();
        assert!(err.contains("invalid wire header"));
    }

    #[test]
    fn cmd_binary_decode_rejects_trailing_bytes() {
        let cmd = crate::ast::Qail::get("users").limit(1);
        let mut encoded = encode_cmd_binary(&cmd);
        encoded.extend_from_slice(&[0xAA, 0xBB]);
        let err = decode_cmd_binary(&encoded).unwrap_err();
        assert!(err.contains("invalid payload length"));
    }

    #[test]
    fn cmd_binary_decode_enforces_depth_limits() {
        let mut nested = crate::ast::Qail::get("users").limit(1);
        for _ in 0..(MAX_AST_DEPTH + 2) {
            nested = crate::ast::Qail {
                action: crate::ast::Action::Get,
                table: "users".to_string(),
                columns: vec![crate::ast::Expr::Subquery {
                    query: Box::new(nested),
                    alias: None,
                }],
                ..crate::ast::Qail::default()
            };
        }

        let encoded = encode_cmd_binary(&nested);
        let err = decode_cmd_binary(&encoded).unwrap_err();
        assert!(err.contains("AST depth limit exceeded"));
    }

    #[test]
    fn cmd_binary_decode_bitflip_corpus_no_panic() {
        let seeds = vec![
            encode_cmd_binary(&crate::ast::Qail::get("users").limit(1)),
            encode_cmd_binary(&crate::ast::Qail::set("users").set_value("active", true)),
            vec![],
            b"QWB2garbage".to_vec(),
            vec![0u8; 32],
        ];

        for seed in seeds {
            for i in 0..seed.len().min(128) {
                for bit in 0..8u8 {
                    let mut mutated = seed.clone();
                    mutated[i] ^= 1 << bit;
                    let _ = decode_cmd_binary(&mutated);
                }
            }
            let _ = decode_cmd_binary(&seed);
        }
    }

    proptest! {
        #[test]
        fn cmd_binary_decode_fuzz_never_panics(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
            let _ = decode_cmd_binary(&data);
        }
    }

    #[test]
    fn cmds_text_roundtrip() {
        let cmds = vec![
            crate::ast::Qail::get("users").columns(["id", "email"]),
            crate::ast::Qail::get("users").limit(1),
            crate::ast::Qail::del("users").where_eq("id", 99),
        ];

        let encoded = encode_cmds_text(&cmds);
        let decoded = decode_cmds_text(&encoded).unwrap();
        assert_eq!(decoded.len(), cmds.len());
        for (lhs, rhs) in decoded.iter().zip(cmds.iter()) {
            assert_eq!(lhs.to_string(), rhs.to_string());
        }
    }

    #[test]
    fn decode_cmd_text_falls_back_to_raw_qail() {
        let decoded = decode_cmd_text("get users limit 1").unwrap();
        assert_eq!(decoded.action, crate::ast::Action::Get);
        assert_eq!(decoded.table, "users");
        assert!(
            decoded
                .cages
                .iter()
                .any(|c| matches!(c.kind, crate::ast::CageKind::Limit(1)))
        );
    }
}
