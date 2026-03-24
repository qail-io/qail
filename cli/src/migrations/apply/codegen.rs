//! .qail → SQL code generation.

use anyhow::{Result, anyhow, bail};
use qail_core::ast::{
    Action, Constraint, Expr, FunctionDef, IndexDef, Qail, TriggerDef, TriggerEvent, TriggerTiming,
};
use qail_core::migrate::parse_qail;
use qail_core::migrate::policy::RlsPolicy;
#[cfg(test)]
use qail_core::migrate::schema::FkAction;
#[cfg(test)]
use qail_core::migrate::schema::GrantAction;
use qail_core::migrate::schema::{
    Comment, CommentTarget, EnumType, Extension, Grant, MigrationHint, ResourceDef,
    SchemaFunctionDef, SchemaTriggerDef, Sequence, ViewDef,
};
use qail_core::parser::schema::Schema;
use qail_core::transpiler::ToSql;

/// Parse a .qail schema file and generate SQL DDL.
///
/// Detects whether the content uses brace-based (`table foo { ... }`) or
/// paren-based (`table foo ( ... )`) format and routes to the appropriate parser.
///
/// - Brace-based: handled by `parse_qail()` + `migrate_schema_to_sql()` —
///   supports tables, indexes, functions, triggers, grants, `$$` blocks.
/// - Paren-based: handled by `Schema::parse()` + `schema.to_sql()` —
///   the established "schema.qail" format with `enable_rls` annotations.
/// - Fallback: `parse_functions_and_triggers()` for raw function/trigger blocks.
#[cfg(test)]
pub(super) fn parse_qail_to_sql(content: &str) -> Result<String> {
    // Detect format: look for `table <name> {` vs `table <name> (`
    let uses_braces = content.lines().any(|line| {
        let trimmed = line.trim();
        trimmed.starts_with("table ") && trimmed.ends_with('{')
    });

    if uses_braces {
        // 1. Brace-based format: use the full migrate parser
        if let Ok(schema) = parse_qail(content) {
            let sql = migrate_schema_to_sql(&schema);
            if !sql.is_empty() {
                return Ok(sql);
            }
        }
    }

    // 2. Paren-based format (or brace parser failed): use Schema::parse
    match Schema::parse(content) {
        Ok(schema) => {
            if schema.tables.is_empty() && schema.policies.is_empty() && schema.indexes.is_empty() {
                return parse_functions_and_triggers(content);
            }
            Ok(schema.to_sql())
        }
        Err(_) => {
            // 3. Last resort: try brace parser even without brace detection
            //    (for files with only functions/triggers/grants)
            if !uses_braces && let Ok(schema) = parse_qail(content) {
                let sql = migrate_schema_to_sql(&schema);
                if !sql.is_empty() {
                    return Ok(sql);
                }
            }
            parse_functions_and_triggers(content)
        }
    }
}

/// Parse a `.qail` migration into strictly AST-executable commands.
///
/// This compiler intentionally fails on constructs that do not yet have
/// first-class AST/wire support in the migration executor, rather than
/// silently falling back to raw SQL execution.
pub(crate) fn parse_qail_to_commands_strict(content: &str) -> Result<Vec<Qail>> {
    let uses_braces = content.lines().any(|line| {
        let trimmed = line.trim();
        trimmed.starts_with("table ") && trimmed.ends_with('{')
    });

    if uses_braces {
        let schema = parse_qail(content).map_err(|e| anyhow!(e))?;
        return compile_migrate_schema_strict(&schema);
    }

    if let Ok(schema) = Schema::parse(content) {
        return compile_parser_schema_strict(&schema);
    }

    if let Ok(schema) = parse_qail(content) {
        return compile_migrate_schema_strict(&schema);
    }

    bail!("Could not parse migration into strict AST commands")
}

/// Render compiled commands back to SQL text (for receipts/checksums/guards).
pub(crate) fn commands_to_sql(cmds: &[Qail]) -> String {
    cmds.iter()
        .map(|cmd| cmd.to_sql())
        .collect::<Vec<_>>()
        .join(";\n")
}

fn compile_migrate_schema_strict(schema: &qail_core::migrate::schema::Schema) -> Result<Vec<Qail>> {
    let (hint_cmds, hint_unsupported) = compile_migration_hints_strict(&schema.migrations)?;
    let (early_hint_cmds, late_hint_cmds): (Vec<Qail>, Vec<Qail>) = hint_cmds
        .into_iter()
        .partition(|cmd| is_early_hint_action(cmd.action));

    if !schema.resources.is_empty() {
        bail!(
            "Strict AST migration compiler rejects infrastructure resources in migration apply: {}. \
             Resources (bucket/queue/topic) are declarative infra objects, not executable database AST commands. \
             Move them to schema/deploy tooling and keep delta migrations database-only.",
            format_resource_summary(&schema.resources)
        );
    }

    let mut unsupported = Vec::new();
    unsupported.extend(hint_unsupported);
    if schema
        .tables
        .values()
        .any(|t| !t.multi_column_fks.is_empty())
    {
        unsupported.push("multi-column foreign keys");
    }
    if !unsupported.is_empty() {
        bail!(
            "Strict AST migration compiler does not support: {}",
            unsupported.join(", ")
        );
    }

    let mut cmds = Vec::new();
    let mut early_functions = Vec::new();
    let mut late_functions = Vec::new();
    for func in &schema.functions {
        if function_used_by_table_columns(schema, &func.name) {
            early_functions.push(func.clone());
        } else {
            late_functions.push(func.clone());
        }
    }

    // Order matters for dependency correctness:
    // explicit drop/rename hints -> extensions/types/sequences + table-default
    // functions -> tables/indexes -> views + remaining functions ->
    // triggers/policies/comments -> late hints.
    cmds.extend(early_hint_cmds);
    cmds.extend(compile_extensions_strict(&schema.extensions)?);
    cmds.extend(compile_enums_strict(&schema.enums)?);
    cmds.extend(compile_sequences_strict(&schema.sequences)?);
    cmds.extend(compile_functions_strict(&early_functions)?);
    cmds.extend(qail_core::migrate::schema::schema_to_commands(schema));

    // Preserve schema-level RLS toggles for CREATE TABLE blocks.
    let mut table_names: Vec<&String> = schema.tables.keys().collect();
    table_names.sort();
    for table_name in table_names {
        if let Some(table) = schema.tables.get(table_name) {
            if table.enable_rls {
                cmds.push(Qail {
                    action: Action::AlterEnableRls,
                    table: table_name.clone(),
                    ..Default::default()
                });
            }
            if table.force_rls {
                cmds.push(Qail {
                    action: Action::AlterForceRls,
                    table: table_name.clone(),
                    ..Default::default()
                });
            }
        }
    }

    cmds.extend(compile_views_strict(&schema.views)?);
    cmds.extend(compile_functions_strict(&late_functions)?);
    cmds.extend(compile_triggers_strict(&schema.triggers)?);
    cmds.extend(compile_policies_strict(&schema.policies)?);
    cmds.extend(compile_grants_strict(&schema.grants)?);
    cmds.extend(compile_comments_strict(&schema.comments)?);
    cmds.extend(late_hint_cmds);

    if cmds.is_empty() {
        bail!("No executable AST commands found in migration");
    }

    Ok(cmds)
}

fn is_early_hint_action(action: Action) -> bool {
    matches!(
        action,
        Action::Drop
            | Action::DropIndex
            | Action::AlterDrop
            | Action::DropView
            | Action::DropMaterializedView
            | Action::DropExtension
            | Action::DropSequence
            | Action::DropEnum
            | Action::DropFunction
            | Action::DropTrigger
            | Action::DropPolicy
            | Action::Mod
    )
}

fn compile_migration_hints_strict(
    hints: &[MigrationHint],
) -> Result<(Vec<Qail>, Vec<&'static str>)> {
    let mut cmds = Vec::new();
    let mut unsupported = Vec::new();

    for hint in hints {
        match hint {
            // In migration files, drop directives are explicit execution steps.
            // They are compiled directly to AST commands for strict runtime execution.
            MigrationHint::Drop { target, .. } => {
                cmds.push(compile_drop_hint_strict(target)?);
            }
            MigrationHint::Rename { from, to } => {
                cmds.push(compile_rename_hint_strict(from, to)?);
            }
            MigrationHint::Transform { .. } => add_unsupported(&mut unsupported, "transform hints"),
        }
    }

    Ok((cmds, unsupported))
}

fn add_unsupported(unsupported: &mut Vec<&'static str>, item: &'static str) {
    if !unsupported.contains(&item) {
        unsupported.push(item);
    }
}

fn format_resource_summary(resources: &[ResourceDef]) -> String {
    resources
        .iter()
        .map(|r| format!("{} {}", r.kind, r.name))
        .collect::<Vec<_>>()
        .join(", ")
}

fn function_used_by_table_columns(
    schema: &qail_core::migrate::schema::Schema,
    func_name: &str,
) -> bool {
    for table in schema.tables.values() {
        for col in &table.columns {
            if let Some(default) = &col.default
                && contains_function_call(default, func_name)
            {
                return true;
            }
            if let Some(generated) = &col.generated {
                let expr = match generated {
                    qail_core::migrate::schema::Generated::AlwaysStored(expr) => expr.as_str(),
                    qail_core::migrate::schema::Generated::AlwaysIdentity
                    | qail_core::migrate::schema::Generated::ByDefaultIdentity => "",
                };
                if !expr.is_empty() && contains_function_call(expr, func_name) {
                    return true;
                }
            }
        }
    }
    false
}

fn contains_function_call(expr: &str, func_name: &str) -> bool {
    let expr_l = expr.to_lowercase();
    let name_l = func_name.to_lowercase();
    expr_l.contains(&format!("{name_l}(")) || expr_l.contains(&format!("{name_l} ("))
}

fn compile_extensions_strict(extensions: &[Extension]) -> Result<Vec<Qail>> {
    let mut cmds = Vec::with_capacity(extensions.len());

    for ext in extensions {
        let name = ext.name.trim();
        if name.is_empty() {
            bail!("Strict AST migration compiler rejects extension with empty name");
        }

        let mut columns = Vec::new();
        if let Some(schema) = &ext.schema {
            if !is_valid_ident_path(schema) {
                bail!(
                    "Strict AST migration compiler rejects invalid extension schema '{}'",
                    schema
                );
            }
            columns.push(Expr::Named(format!("SCHEMA {}", schema)));
        }
        if let Some(version) = &ext.version {
            columns.push(Expr::Named(format!(
                "VERSION '{}'",
                escape_sql_literal(version)
            )));
        }

        cmds.push(Qail {
            action: Action::CreateExtension,
            table: name.to_string(),
            columns,
            ..Default::default()
        });
    }

    Ok(cmds)
}

fn compile_comments_strict(comments: &[Comment]) -> Result<Vec<Qail>> {
    let mut cmds = Vec::with_capacity(comments.len());
    for comment in comments {
        let target = match &comment.target {
            CommentTarget::Table(table) => {
                if !is_valid_ident_path(table) {
                    bail!(
                        "Strict AST migration compiler rejects invalid table comment target '{}'",
                        table
                    );
                }
                table.clone()
            }
            CommentTarget::Column { table, column } => {
                if !is_valid_ident_path(table) || !is_valid_ident(column) {
                    bail!(
                        "Strict AST migration compiler rejects invalid column comment target '{}.{}'",
                        table,
                        column
                    );
                }
                format!("{}.{}", table, column)
            }
            CommentTarget::Raw(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() || trimmed.contains('\n') || trimmed.contains(';') {
                    bail!(
                        "Strict AST migration compiler rejects unsafe raw comment target '{}'",
                        raw
                    );
                }
                trimmed.to_string()
            }
        };
        cmds.push(Qail {
            action: Action::CommentOn,
            table: target,
            columns: vec![Expr::Named(comment.text.clone())],
            ..Default::default()
        });
    }
    Ok(cmds)
}

fn compile_grants_strict(grants: &[Grant]) -> Result<Vec<Qail>> {
    let mut cmds = Vec::with_capacity(grants.len());
    for grant in grants {
        let object = grant.on_object.trim();
        if object.is_empty() {
            bail!("Strict AST migration compiler rejects GRANT/REVOKE with empty target object");
        }
        if object.contains(';') || object.contains('\n') {
            bail!(
                "Strict AST migration compiler rejects unsafe GRANT/REVOKE object '{}'",
                grant.on_object
            );
        }

        let role = grant.to_role.trim();
        if !is_valid_ident_path(role) {
            bail!(
                "Strict AST migration compiler rejects invalid GRANT/REVOKE role '{}'",
                grant.to_role
            );
        }

        let privileges: Vec<Expr> = grant
            .privileges
            .iter()
            .map(|p| Expr::Named(p.to_string()))
            .collect();
        if privileges.is_empty() {
            bail!("Strict AST migration compiler rejects GRANT/REVOKE with empty privileges");
        }

        cmds.push(Qail {
            action: match grant.action {
                qail_core::migrate::schema::GrantAction::Grant => Action::Grant,
                qail_core::migrate::schema::GrantAction::Revoke => Action::Revoke,
            },
            table: object.to_string(),
            columns: privileges,
            payload: Some(role.to_string()),
            ..Default::default()
        });
    }
    Ok(cmds)
}

fn compile_policies_strict(policies: &[RlsPolicy]) -> Result<Vec<Qail>> {
    let mut cmds = Vec::with_capacity(policies.len());
    for policy in policies {
        if !is_valid_ident(&policy.name) {
            bail!(
                "Strict AST migration compiler rejects invalid policy name '{}'",
                policy.name
            );
        }
        if !is_valid_ident_path(&policy.table) {
            bail!(
                "Strict AST migration compiler rejects invalid policy table '{}'",
                policy.table
            );
        }
        if let Some(role) = &policy.role
            && !is_valid_ident_path(role)
        {
            bail!(
                "Strict AST migration compiler rejects invalid policy role '{}'",
                role
            );
        }

        cmds.push(Qail {
            action: Action::CreatePolicy,
            policy_def: Some(policy.clone()),
            ..Default::default()
        });
    }
    Ok(cmds)
}

fn compile_sequences_strict(sequences: &[Sequence]) -> Result<Vec<Qail>> {
    let mut cmds = Vec::with_capacity(sequences.len());
    for seq in sequences {
        if !is_valid_ident_path(&seq.name) {
            bail!(
                "Strict AST migration compiler rejects invalid sequence identifier '{}'",
                seq.name
            );
        }

        let mut opts = Vec::new();
        if let Some(data_type) = &seq.data_type {
            opts.push(Expr::Named(format!("AS {}", data_type)));
        }
        if let Some(start) = seq.start {
            opts.push(Expr::Named(format!("START WITH {}", start)));
        }
        if let Some(increment) = seq.increment {
            opts.push(Expr::Named(format!("INCREMENT BY {}", increment)));
        }
        if let Some(min_value) = seq.min_value {
            opts.push(Expr::Named(format!("MINVALUE {}", min_value)));
        }
        if let Some(max_value) = seq.max_value {
            opts.push(Expr::Named(format!("MAXVALUE {}", max_value)));
        }
        if let Some(cache) = seq.cache {
            opts.push(Expr::Named(format!("CACHE {}", cache)));
        }
        if seq.cycle {
            opts.push(Expr::Named("CYCLE".to_string()));
        }
        if let Some(owned_by) = &seq.owned_by {
            if !is_valid_ident_path(owned_by) {
                bail!(
                    "Strict AST migration compiler rejects invalid sequence OWNED BY target '{}'",
                    owned_by
                );
            }
            opts.push(Expr::Named(format!("OWNED BY {}", owned_by)));
        }

        cmds.push(Qail {
            action: Action::CreateSequence,
            table: seq.name.clone(),
            columns: opts,
            ..Default::default()
        });
    }
    Ok(cmds)
}

fn compile_enums_strict(enums: &[EnumType]) -> Result<Vec<Qail>> {
    let mut cmds = Vec::with_capacity(enums.len());
    for enum_type in enums {
        if !is_valid_ident_path(&enum_type.name) {
            bail!(
                "Strict AST migration compiler rejects invalid enum type identifier '{}'",
                enum_type.name
            );
        }
        if enum_type.values.is_empty() {
            bail!(
                "Strict AST migration compiler rejects enum '{}' with no values",
                enum_type.name
            );
        }

        cmds.push(Qail {
            action: Action::CreateEnum,
            table: enum_type.name.clone(),
            columns: enum_type
                .values
                .iter()
                .map(|v| Expr::Named(v.clone()))
                .collect(),
            ..Default::default()
        });
    }
    Ok(cmds)
}

fn compile_views_strict(views: &[ViewDef]) -> Result<Vec<Qail>> {
    let mut cmds = Vec::with_capacity(views.len());
    for view in views {
        if !is_valid_ident_path(&view.name) {
            bail!(
                "Strict AST migration compiler rejects invalid view identifier '{}'",
                view.name
            );
        }
        let query = view.query.trim();
        if query.is_empty() {
            bail!(
                "Strict AST migration compiler rejects view '{}' with empty query body",
                view.name
            );
        }

        cmds.push(Qail {
            action: if view.materialized {
                Action::CreateMaterializedView
            } else {
                Action::CreateView
            },
            table: view.name.clone(),
            payload: Some(query.to_string()),
            ..Default::default()
        });
    }
    Ok(cmds)
}

fn compile_functions_strict(functions: &[SchemaFunctionDef]) -> Result<Vec<Qail>> {
    let mut cmds = Vec::with_capacity(functions.len());
    for func in functions {
        if !is_valid_ident_path(&func.name) {
            bail!(
                "Strict AST migration compiler rejects invalid function name '{}'",
                func.name
            );
        }
        if func.name.trim().is_empty() || func.returns.trim().is_empty() {
            bail!(
                "Strict AST migration compiler rejects invalid function definition '{}'",
                func.name
            );
        }
        if func
            .args
            .iter()
            .any(|arg| arg.contains(';') || arg.contains('\n'))
        {
            bail!(
                "Strict AST migration compiler rejects unsafe function arguments in '{}'",
                func.name
            );
        }

        cmds.push(Qail {
            action: Action::CreateFunction,
            function_def: Some(FunctionDef {
                name: func.name.clone(),
                args: func.args.clone(),
                returns: func.returns.clone(),
                body: func.body.clone(),
                language: Some(func.language.clone()),
                volatility: func.volatility.clone(),
            }),
            ..Default::default()
        });
    }
    Ok(cmds)
}

fn compile_triggers_strict(triggers: &[SchemaTriggerDef]) -> Result<Vec<Qail>> {
    let mut cmds = Vec::with_capacity(triggers.len());
    for trigger in triggers {
        if !is_valid_ident(&trigger.name) {
            bail!(
                "Strict AST migration compiler rejects invalid trigger name '{}'",
                trigger.name
            );
        }
        if !is_valid_ident_path(&trigger.table) {
            bail!(
                "Strict AST migration compiler rejects invalid trigger table '{}'",
                trigger.table
            );
        }
        if !is_valid_ident_path(&trigger.execute_function) {
            bail!(
                "Strict AST migration compiler rejects invalid trigger execute function '{}'",
                trigger.execute_function
            );
        }
        if trigger.condition.is_some() {
            bail!(
                "Strict AST migration compiler does not support trigger WHEN conditions yet (trigger '{}')",
                trigger.name
            );
        }

        let timing = parse_trigger_timing(&trigger.timing, &trigger.name)?;
        let events = parse_trigger_events(&trigger.events, &trigger.name)?;
        let mut update_columns = Vec::new();
        for col in &trigger.update_columns {
            if !is_valid_ident(col) {
                bail!(
                    "Strict AST migration compiler rejects invalid trigger UPDATE OF column '{}' on '{}'",
                    col,
                    trigger.name
                );
            }
            update_columns.push(col.clone());
        }

        cmds.push(Qail {
            action: Action::CreateTrigger,
            trigger_def: Some(TriggerDef {
                name: trigger.name.clone(),
                table: trigger.table.clone(),
                timing,
                events,
                update_columns,
                for_each_row: trigger.for_each_row,
                execute_function: trigger.execute_function.clone(),
            }),
            ..Default::default()
        });
    }
    Ok(cmds)
}

fn parse_trigger_timing(timing: &str, trigger_name: &str) -> Result<TriggerTiming> {
    match timing.trim().to_ascii_uppercase().as_str() {
        "BEFORE" => Ok(TriggerTiming::Before),
        "AFTER" => Ok(TriggerTiming::After),
        "INSTEAD" | "INSTEAD OF" => Ok(TriggerTiming::InsteadOf),
        other => bail!(
            "Strict AST migration compiler rejects unsupported trigger timing '{}' on '{}'",
            other,
            trigger_name
        ),
    }
}

fn parse_trigger_events(events: &[String], trigger_name: &str) -> Result<Vec<TriggerEvent>> {
    let mut out = Vec::new();
    for event in events {
        match event.trim().to_ascii_uppercase().as_str() {
            "INSERT" => out.push(TriggerEvent::Insert),
            "UPDATE" => out.push(TriggerEvent::Update),
            "DELETE" => out.push(TriggerEvent::Delete),
            "TRUNCATE" => out.push(TriggerEvent::Truncate),
            other => bail!(
                "Strict AST migration compiler rejects unsupported trigger event '{}' on '{}'",
                other,
                trigger_name
            ),
        }
    }
    if out.is_empty() {
        bail!(
            "Strict AST migration compiler rejects trigger '{}' with no events",
            trigger_name
        );
    }
    Ok(out)
}

fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn compile_rename_hint_strict(from: &str, to: &str) -> Result<Qail> {
    let Some((from_table, from_col)) = split_table_column_target(from) else {
        bail!(
            "Strict AST migration compiler expected rename source in '<table>.<column>' form, got '{}'",
            from
        );
    };
    let Some((to_table, to_col)) = split_table_column_target(to) else {
        bail!(
            "Strict AST migration compiler expected rename target in '<table>.<column>' form, got '{}'",
            to
        );
    };

    if from_table != to_table {
        bail!(
            "Strict AST migration compiler only supports same-table column rename hints (got '{} -> {}')",
            from,
            to
        );
    }
    if from_col == to_col {
        bail!(
            "Strict AST migration compiler rejects no-op rename hint '{} -> {}'",
            from,
            to
        );
    }

    if !is_valid_ident_path(from_table) || !is_valid_ident(from_col) || !is_valid_ident(to_col) {
        bail!(
            "Strict AST migration compiler rejects invalid rename identifier in hint '{} -> {}'",
            from,
            to
        );
    }

    Ok(Qail {
        action: Action::Mod,
        table: from_table.to_string(),
        columns: vec![Expr::Named(format!("{from_col} -> {to_col}"))],
        ..Default::default()
    })
}

fn compile_drop_hint_strict(target: &str) -> Result<Qail> {
    let target = target.trim();
    if target.is_empty() {
        bail!("Strict AST migration compiler got empty drop target");
    }

    if let Some(view_name) = target.strip_prefix("materialized view ").map(str::trim) {
        let view_name = normalize_optional_if_exists_prefix(view_name);
        if !is_valid_ident_path(&view_name) {
            bail!(
                "Strict AST migration compiler rejects invalid materialized view identifier in drop hint: '{}'",
                target
            );
        }
        return Ok(Qail {
            action: Action::DropMaterializedView,
            table: view_name.to_string(),
            ..Default::default()
        });
    }

    if let Some(view_name) = target.strip_prefix("view ").map(str::trim) {
        let view_name = normalize_optional_if_exists_prefix(view_name);
        if !is_valid_ident_path(&view_name) {
            bail!(
                "Strict AST migration compiler rejects invalid view identifier in drop hint: '{}'",
                target
            );
        }
        return Ok(Qail {
            action: Action::DropView,
            table: view_name.to_string(),
            ..Default::default()
        });
    }

    if let Some(ext_name) = target.strip_prefix("extension ").map(str::trim) {
        let ext_name = normalize_optional_if_exists_prefix(ext_name);
        if ext_name.is_empty() {
            bail!(
                "Strict AST migration compiler rejects empty extension identifier in drop hint: '{}'",
                target
            );
        }
        return Ok(Qail {
            action: Action::DropExtension,
            table: ext_name.to_string(),
            ..Default::default()
        });
    }

    if let Some(seq_name) = target.strip_prefix("sequence ").map(str::trim) {
        let seq_name = normalize_optional_if_exists_prefix(seq_name);
        if !is_valid_ident_path(&seq_name) {
            bail!(
                "Strict AST migration compiler rejects invalid sequence identifier in drop hint: '{}'",
                target
            );
        }
        return Ok(Qail {
            action: Action::DropSequence,
            table: seq_name.to_string(),
            ..Default::default()
        });
    }

    if let Some(enum_name) = target
        .strip_prefix("enum ")
        .or_else(|| target.strip_prefix("type "))
        .map(str::trim)
    {
        let enum_name = normalize_optional_if_exists_prefix(enum_name);
        if !is_valid_ident_path(&enum_name) {
            bail!(
                "Strict AST migration compiler rejects invalid enum/type identifier in drop hint: '{}'",
                target
            );
        }
        return Ok(Qail {
            action: Action::DropEnum,
            table: enum_name.to_string(),
            ..Default::default()
        });
    }

    if let Some(function_target) = target.strip_prefix("function ").map(str::trim) {
        let function_target = normalize_optional_if_exists_prefix(function_target);

        if let Some((fn_name, _arg_sig)) = split_function_signature(&function_target) {
            if !is_valid_ident_path(fn_name) || !is_valid_function_signature(&function_target) {
                bail!(
                    "Strict AST migration compiler rejects invalid function signature in drop hint: '{}'",
                    target
                );
            }
            return Ok(Qail {
                action: Action::DropFunction,
                table: fn_name.to_string(),
                payload: Some(function_target),
                ..Default::default()
            });
        }

        if !is_valid_ident_path(&function_target) {
            bail!(
                "Strict AST migration compiler rejects invalid function identifier in drop hint: '{}'",
                target
            );
        }
        return Ok(Qail {
            action: Action::DropFunction,
            table: function_target.to_string(),
            ..Default::default()
        });
    }

    if let Some(policy_target) = target.strip_prefix("policy ").map(str::trim) {
        let policy_target = normalize_optional_if_exists_prefix(policy_target);
        return compile_drop_policy_target(&policy_target, target);
    }

    if let Some(trigger_target) = target.strip_prefix("trigger ").map(str::trim) {
        let trigger_target = normalize_optional_if_exists_prefix(trigger_target);
        if !is_valid_ident_path(&trigger_target) || !trigger_target.contains('.') {
            bail!(
                "Strict AST migration compiler expects trigger drop hint as 'trigger <table>.<trigger>' (got '{}')",
                target
            );
        }
        return Ok(Qail {
            action: Action::DropTrigger,
            table: trigger_target.to_string(),
            ..Default::default()
        });
    }

    if let Some(index_name) = target.strip_prefix("index ").map(str::trim) {
        let index_name = normalize_optional_if_exists_prefix(index_name);
        if !is_valid_ident_path(&index_name) {
            bail!(
                "Strict AST migration compiler rejects invalid index identifier in drop hint: '{}'",
                target
            );
        }
        return Ok(Qail {
            action: Action::DropIndex,
            // qail-pg AST encoder emits "DROP INDEX IF EXISTS ..." already.
            // Keep only the raw index identifier here to avoid "IF EXISTS IF EXISTS ...".
            table: index_name.to_string(),
            ..Default::default()
        });
    }

    if let Some(table_name) = target.strip_prefix("table ").map(str::trim) {
        let table_name = normalize_optional_if_exists_prefix(table_name);
        if !is_valid_ident_path(&table_name) {
            bail!(
                "Strict AST migration compiler rejects invalid table identifier in drop hint: '{}'",
                target
            );
        }
        return Ok(Qail {
            action: Action::Drop,
            table: table_name.to_string(),
            ..Default::default()
        });
    }

    if let Some(column_target) = target.strip_prefix("column ").map(str::trim) {
        let column_target = normalize_optional_if_exists_prefix(column_target);
        return compile_drop_column_target(&column_target, target);
    }

    // Legacy hint style: `drop table_name` or `drop table.column`
    let normalized_target = normalize_optional_if_exists_prefix(target);
    if normalized_target.contains('.') {
        return compile_drop_column_target(&normalized_target, target);
    }

    if !is_valid_ident_path(&normalized_target) {
        bail!(
            "Strict AST migration compiler rejects invalid drop target identifier: '{}'",
            target
        );
    }
    Ok(Qail {
        action: Action::Drop,
        table: normalized_target,
        ..Default::default()
    })
}

fn compile_drop_column_target(column_target: &str, original_target: &str) -> Result<Qail> {
    let Some((table, column)) = split_table_column_target(column_target) else {
        bail!(
            "Strict AST migration compiler expected '<table>.<column>' in drop hint, got '{}'",
            original_target
        );
    };
    if !is_valid_ident_path(table) || !is_valid_ident(column) {
        bail!(
            "Strict AST migration compiler rejects invalid column drop hint: '{}'",
            original_target
        );
    }
    Ok(Qail {
        action: Action::AlterDrop,
        table: table.to_string(),
        columns: vec![Expr::Named(column.to_string())],
        ..Default::default()
    })
}

fn compile_drop_policy_target(policy_target: &str, original_target: &str) -> Result<Qail> {
    let Some((policy_name, table_name)) = split_policy_target(policy_target) else {
        bail!(
            "Strict AST migration compiler expects policy drop hint as 'policy <name> on <table>' or 'policy <table>.<name>' (got '{}')",
            original_target
        );
    };
    if !is_valid_ident(policy_name) || !is_valid_ident_path(table_name) {
        bail!(
            "Strict AST migration compiler rejects invalid policy drop hint: '{}'",
            original_target
        );
    }
    Ok(Qail {
        action: Action::DropPolicy,
        table: table_name.to_string(),
        payload: Some(policy_name.to_string()),
        ..Default::default()
    })
}

fn split_policy_target(target: &str) -> Option<(&str, &str)> {
    if let Some((name, table)) = target.split_once(" on ") {
        let name = name.trim();
        let table = table.trim();
        if !name.is_empty() && !table.is_empty() {
            return Some((name, table));
        }
    }

    let (table, policy) = split_table_column_target(target)?;
    Some((policy, table))
}

fn split_table_column_target(target: &str) -> Option<(&str, &str)> {
    let (table, column) = target.rsplit_once('.')?;
    let table = table.trim();
    let column = column.trim();
    if table.is_empty() || column.is_empty() {
        return None;
    }
    Some((table, column))
}

fn normalize_optional_if_exists_prefix(target: &str) -> String {
    let tokens: Vec<&str> = target.split_whitespace().collect();
    if tokens.len() >= 3
        && tokens[0].eq_ignore_ascii_case("if")
        && tokens[1].eq_ignore_ascii_case("exists")
    {
        tokens[2..].join(" ")
    } else {
        target.trim().to_string()
    }
}

fn split_function_signature(target: &str) -> Option<(&str, &str)> {
    let open = target.find('(')?;
    if !target.ends_with(')') || open == 0 {
        return None;
    }
    let name = target[..open].trim();
    let args = &target[open + 1..target.len() - 1];
    if name.is_empty() {
        return None;
    }
    Some((name, args))
}

fn is_valid_function_signature(target: &str) -> bool {
    let Some((name, args)) = split_function_signature(target) else {
        return false;
    };
    if !is_valid_ident_path(name) {
        return false;
    }
    if args.contains(';') || args.contains('\n') || args.contains('\r') {
        return false;
    }
    args.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == ',' || c == ' ')
}

fn is_valid_ident_path(path: &str) -> bool {
    let mut seen = false;
    for part in path.split('.') {
        seen = true;
        if !is_valid_ident(part.trim()) {
            return false;
        }
    }
    seen
}

fn is_valid_ident(ident: &str) -> bool {
    let mut chars = ident.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn compile_parser_schema_strict(schema: &Schema) -> Result<Vec<Qail>> {
    let mut cmds = Vec::<Qail>::new();

    for table in &schema.tables {
        let mut cols = Vec::<Expr>::new();
        for col in &table.columns {
            if col.type_params.is_some() {
                bail!(
                    "Strict AST migration compiler does not support parameterized type '{}({:?})' on {}.{} yet",
                    col.typ,
                    col.type_params,
                    table.name,
                    col.name
                );
            }
            if col.is_array {
                bail!(
                    "Strict AST migration compiler does not support array type '{}[]' on {}.{} yet",
                    col.typ,
                    table.name,
                    col.name
                );
            }
            if col.check.is_some() {
                bail!(
                    "Strict AST migration compiler does not support CHECK constraints on {}.{} yet",
                    table.name,
                    col.name
                );
            }

            let mut constraints = Vec::new();
            if col.primary_key {
                constraints.push(Constraint::PrimaryKey);
            }
            if col.nullable {
                constraints.push(Constraint::Nullable);
            }
            if col.unique {
                constraints.push(Constraint::Unique);
            }
            if let Some(default) = &col.default_value {
                constraints.push(Constraint::Default(default.clone()));
            }
            if let Some(reference) = &col.references {
                constraints.push(Constraint::References(reference.clone()));
            }

            cols.push(Expr::Def {
                name: col.name.clone(),
                data_type: col.typ.clone(),
                constraints,
            });
        }

        cmds.push(Qail {
            action: Action::Make,
            table: table.name.clone(),
            columns: cols,
            ..Default::default()
        });

        if table.enable_rls {
            cmds.push(Qail {
                action: Action::AlterEnableRls,
                table: table.name.clone(),
                ..Default::default()
            });
            // Parser-schema `enable_rls` historically expands to ENABLE + FORCE.
            cmds.push(Qail {
                action: Action::AlterForceRls,
                table: table.name.clone(),
                ..Default::default()
            });
        }
    }

    for idx in &schema.indexes {
        cmds.push(Qail {
            action: Action::Index,
            index_def: Some(IndexDef {
                name: idx.name.clone(),
                table: idx.table.clone(),
                columns: idx.columns.clone(),
                unique: idx.unique,
                index_type: None,
                where_clause: None,
            }),
            ..Default::default()
        });
    }

    cmds.extend(compile_policies_strict(&schema.policies)?);

    if cmds.is_empty() {
        bail!("No executable AST commands found in migration");
    }
    Ok(cmds)
}

/// Generate SQL DDL from a fully-parsed migrate Schema.
#[cfg(test)]
fn migrate_schema_to_sql(schema: &qail_core::migrate::schema::Schema) -> String {
    let mut parts: Vec<String> = Vec::new();

    // Extensions first
    for ext in &schema.extensions {
        parts.push(format!("CREATE EXTENSION IF NOT EXISTS \"{}\";", ext.name));
    }

    // Enum types
    for en in &schema.enums {
        let values: Vec<String> = en.values.iter().map(|v| format!("'{}'", v)).collect();
        parts.push(format!(
            "DO $$ BEGIN CREATE TYPE {} AS ENUM ({}); EXCEPTION WHEN duplicate_object THEN null; END $$;",
            en.name, values.join(", ")
        ));
    }

    // Sequences
    for seq in &schema.sequences {
        parts.push(format!("CREATE SEQUENCE IF NOT EXISTS {};", seq.name));
    }

    // Tables: CREATE without FK references (avoids dependency ordering issues)
    // FK constraints are added separately via ALTER TABLE afterward.
    let mut fk_alters: Vec<String> = Vec::new();
    let mut table_names: Vec<&String> = schema.tables.keys().collect();
    table_names.sort();
    for name in &table_names {
        let table = &schema.tables[*name];
        let mut col_defs = Vec::new();
        for col in &table.columns {
            let mut line = format!("    {} {}", col.name, col.data_type);
            if col.primary_key {
                line.push_str(" PRIMARY KEY");
            }
            if !col.nullable && !col.primary_key {
                line.push_str(" NOT NULL");
            }
            if col.unique && !col.primary_key {
                line.push_str(" UNIQUE");
            }
            if let Some(ref default) = col.default {
                line.push_str(&format!(" DEFAULT {}", default));
            }
            // Collect FK constraints for deferred ALTER TABLE
            if let Some(ref fk) = col.foreign_key {
                let mut alter = format!(
                    "ALTER TABLE {} ADD CONSTRAINT fk_{}_{} FOREIGN KEY ({}) REFERENCES {}({})",
                    name, name, col.name, col.name, fk.table, fk.column
                );
                if fk.on_delete != FkAction::NoAction {
                    alter.push_str(&format!(" ON DELETE {}", fk_action_sql(&fk.on_delete)));
                }
                alter.push(';');
                fk_alters.push(alter);
            }
            col_defs.push(line);
        }
        parts.push(format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n);",
            name,
            col_defs.join(",\n")
        ));

        // RLS: ENABLE and FORCE row-level security
        if table.enable_rls {
            parts.push(format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY;", name));
        }
        if table.force_rls {
            parts.push(format!("ALTER TABLE {} FORCE ROW LEVEL SECURITY;", name));
        }
    }

    // Deferred FK constraints (after all tables exist)
    parts.extend(fk_alters);

    // Indexes
    for idx in &schema.indexes {
        let unique = if idx.unique { " UNIQUE" } else { "" };
        parts.push(format!(
            "CREATE{} INDEX IF NOT EXISTS {} ON {} ({});",
            unique,
            idx.name,
            idx.table,
            idx.columns.join(", ")
        ));
    }

    // Functions
    for func in &schema.functions {
        let args = func.args.join(", ");
        parts.push(format!(
            "CREATE OR REPLACE FUNCTION {}({}) RETURNS {} AS $$\n{}\n$$ LANGUAGE {};",
            func.name, args, func.returns, func.body, func.language
        ));
    }

    // Triggers
    for trigger in &schema.triggers {
        let events = trigger.events.join(" OR ");
        let for_each = if trigger.for_each_row {
            "FOR EACH ROW "
        } else {
            ""
        };
        // Drop + recreate for idempotency
        parts.push(format!(
            "DROP TRIGGER IF EXISTS {} ON {};\nCREATE TRIGGER {} {} {} ON {} {}EXECUTE FUNCTION {};",
            trigger.name, trigger.table,
            trigger.name, trigger.timing, events, trigger.table, for_each, trigger.execute_function
        ));
    }

    // Grants
    for grant in &schema.grants {
        let privs: Vec<String> = grant.privileges.iter().map(|p| p.to_string()).collect();
        let action = match grant.action {
            GrantAction::Grant => "GRANT",
            GrantAction::Revoke => "REVOKE",
        };
        let prep = match grant.action {
            GrantAction::Grant => "TO",
            GrantAction::Revoke => "FROM",
        };
        parts.push(format!(
            "{} {} ON {} {} {};",
            action,
            privs.join(", "),
            grant.on_object,
            prep,
            grant.to_role
        ));
    }

    // Comments
    for comment in &schema.comments {
        use qail_core::migrate::schema::CommentTarget;
        let target_sql = match &comment.target {
            CommentTarget::Table(name) => format!("TABLE {}", name),
            CommentTarget::Column { table, column } => format!("COLUMN {}.{}", table, column),
            CommentTarget::Raw(raw) => raw.clone(),
        };
        parts.push(format!(
            "COMMENT ON {} IS '{}';",
            target_sql,
            comment.text.replace('\'', "''")
        ));
    }

    parts.join("\n\n")
}

/// Convert FkAction to SQL string
#[cfg(test)]
fn fk_action_sql(action: &FkAction) -> &'static str {
    match action {
        FkAction::NoAction => "NO ACTION",
        FkAction::Cascade => "CASCADE",
        FkAction::SetNull => "SET NULL",
        FkAction::SetDefault => "SET DEFAULT",
        FkAction::Restrict => "RESTRICT",
    }
}

/// Parse function and trigger definitions from .qail format
#[cfg(test)]
fn parse_functions_and_triggers(content: &str) -> Result<String> {
    let mut sql_parts = Vec::new();
    let mut current_block = String::new();
    let mut in_function = false;
    let mut in_trigger = false;
    let mut brace_depth = 0;

    for line in content.lines() {
        let trimmed = line.trim();

        // Skip comments
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }

        // Detect function start
        if trimmed.starts_with("function ") {
            in_function = true;
            current_block = line.to_string();
            if trimmed.contains('{') {
                brace_depth = 1;
            }
            continue;
        }

        // Detect trigger start
        if trimmed.starts_with("trigger ") {
            in_trigger = true;
            current_block = line.to_string();
            continue;
        }

        // Detect table start (for index definitions)
        if trimmed.starts_with("index ") {
            sql_parts.push(parse_index_line(trimmed)?);
            continue;
        }

        // Detect table block
        if trimmed.starts_with("table ") {
            in_function = false;
            in_trigger = false;
            // Re-parse as schema
            let table_content = extract_table_block(content, trimmed)?;
            if let Ok(schema) = Schema::parse(&table_content) {
                for table in &schema.tables {
                    sql_parts.push(table.to_ddl());
                }
            }
            continue;
        }

        // Handle function body
        if in_function {
            current_block.push('\n');
            current_block.push_str(line);

            brace_depth += line.matches('{').count();
            brace_depth -= line.matches('}').count();

            if brace_depth == 0 && trimmed.ends_with('}') {
                sql_parts.push(translate_function(&current_block)?);
                in_function = false;
                current_block.clear();
            }
            continue;
        }

        // Handle trigger line
        if in_trigger {
            current_block.push('\n');
            current_block.push_str(line);

            if trimmed.contains("execute ") {
                sql_parts.push(translate_trigger(&current_block)?);
                in_trigger = false;
                current_block.clear();
            }
            continue;
        }
    }

    if sql_parts.is_empty() {
        anyhow::bail!("Could not parse any valid QAIL statements");
    }

    Ok(sql_parts.join("\n\n"))
}

/// Parse an index line: index idx_name on table (col1, col2)
#[cfg(test)]
fn parse_index_line(line: &str) -> Result<String> {
    // index idx_qail_queue_poll on _qail_queue (status, id)
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 5 {
        anyhow::bail!("Invalid index syntax: {}", line);
    }

    let idx_name = parts[1];
    let table_name = parts[3];

    // Extract columns between ( and )
    if let (Some(start), Some(end)) = (line.find('('), line.find(')')) {
        let columns = &line[start..=end];
        return Ok(format!(
            "CREATE INDEX IF NOT EXISTS {} ON {}{};",
            idx_name, table_name, columns
        ));
    }

    anyhow::bail!("Invalid index syntax: {}", line)
}

/// Extract a complete table block from content
#[cfg(test)]
fn extract_table_block(content: &str, start_line: &str) -> Result<String> {
    let mut result = String::new();
    let mut found = false;
    let mut brace_depth = 0;

    for line in content.lines() {
        if line.trim() == start_line || (found && brace_depth > 0) {
            found = true;
            result.push_str(line);
            result.push('\n');

            brace_depth += line.matches('{').count();
            brace_depth -= line.matches('}').count();

            if brace_depth == 0 && found {
                break;
            }
        }
    }

    Ok(result)
}

/// Translate a QAIL function block to PL/pgSQL
#[cfg(test)]
fn translate_function(block: &str) -> Result<String> {
    // function _qail_products_notify() returns trigger { ... }
    let mut sql = String::new();

    // Extract function name and return type
    let first_line = block.lines().next().unwrap_or("");
    let func_match = first_line
        .trim()
        .strip_prefix("function ")
        .ok_or_else(|| anyhow::anyhow!("Invalid function definition"))?;

    // Parse: name() returns type
    if let Some(returns_idx) = func_match.find(" returns ") {
        let name_part = &func_match[..returns_idx];
        let returns_part = func_match[returns_idx + 9..].trim();
        let return_type = returns_part.split_whitespace().next().unwrap_or("void");

        sql.push_str(&format!(
            "CREATE OR REPLACE FUNCTION {} RETURNS {} AS $$\n",
            name_part.trim(),
            return_type
        ));
        sql.push_str("BEGIN\n");

        // Extract body (between { and })
        if let (Some(body_start), Some(body_end)) = (block.find('{'), block.rfind('}')) {
            let body = &block[body_start + 1..body_end];
            sql.push_str(&translate_function_body(body));
        }

        sql.push_str("END;\n");
        sql.push_str("$$ LANGUAGE plpgsql;");

        return Ok(sql);
    }

    anyhow::bail!("Invalid function syntax: {}", first_line)
}

/// Translate QAIL function body to PL/pgSQL
#[cfg(test)]
fn translate_function_body(body: &str) -> String {
    let mut sql = String::new();

    for line in body.lines() {
        let trimmed = line.trim();

        // Skip comments
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }

        // Translate if statements
        if trimmed.starts_with("if ") {
            let condition = trimmed.strip_prefix("if ").unwrap_or("");
            let condition = condition.trim_end_matches('{').trim();
            // Replace 'and' with 'AND' for SQL
            let condition = condition.replace(" and ", " AND ");
            sql.push_str(&format!("  IF {} THEN\n", condition));
            continue;
        }

        // Handle closing brace
        if trimmed == "}" {
            sql.push_str("  END IF;\n");
            continue;
        }

        // Regular statements - indent and add
        if !trimmed.is_empty() {
            sql.push_str(&format!("    {};\n", trimmed.trim_end_matches(';')));
        }
    }

    // Add RETURN statement for trigger functions
    sql.push_str("  RETURN COALESCE(NEW, OLD);\n");

    sql
}

/// Translate a QAIL trigger definition to SQL
#[cfg(test)]
fn translate_trigger(block: &str) -> Result<String> {
    // trigger qail_sync_products
    //   after insert or update or delete on products
    //   for each row execute _qail_products_notify()

    let lines: Vec<&str> = block.lines().collect();
    if lines.is_empty() {
        anyhow::bail!("Empty trigger definition");
    }

    let first_line = lines[0].trim();
    let trigger_name = first_line
        .strip_prefix("trigger ")
        .ok_or_else(|| anyhow::anyhow!("Invalid trigger definition"))?
        .trim();

    // Find timing and events line
    let mut timing = "";
    let mut table = "";
    let mut function = "";

    for line in &lines[1..] {
        let trimmed = line.trim();

        if trimmed.starts_with("after ") || trimmed.starts_with("before ") {
            let parts: Vec<&str> = trimmed.split(" on ").collect();
            if parts.len() >= 2 {
                timing = parts[0];
                table = parts[1].trim();
            }
        }

        if trimmed.contains("execute ")
            && let Some(func_start) = trimmed.find("execute ")
        {
            function = &trimmed[func_start + 8..];
        }
    }

    // Build SQL with DROP IF EXISTS for idempotency
    let mut sql = format!("DROP TRIGGER IF EXISTS {} ON {};\n", trigger_name, table);
    sql.push_str(&format!(
        "CREATE TRIGGER {}\n  {} ON {}\n  FOR EACH ROW EXECUTE FUNCTION {};",
        trigger_name,
        timing.to_uppercase(),
        table,
        function.trim()
    ));

    Ok(sql)
}
