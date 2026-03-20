//! .qail → SQL code generation.

use anyhow::{Result, anyhow, bail};
use qail_core::ast::{Action, Constraint, Expr, IndexDef, Qail};
use qail_core::migrate::parse_qail;
#[cfg(test)]
use qail_core::migrate::schema::GrantAction;
use qail_core::migrate::schema::{FkAction, MigrationHint};
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

    let mut unsupported = Vec::new();
    unsupported.extend(hint_unsupported);
    if !schema.extensions.is_empty() {
        unsupported.push("extensions");
    }
    if !schema.comments.is_empty() {
        unsupported.push("comments");
    }
    if !schema.sequences.is_empty() {
        unsupported.push("sequences");
    }
    if !schema.enums.is_empty() {
        unsupported.push("enums");
    }
    if !schema.views.is_empty() {
        unsupported.push("views");
    }
    if !schema.functions.is_empty() {
        unsupported.push("functions");
    }
    if !schema.triggers.is_empty() {
        unsupported.push("triggers");
    }
    if !schema.grants.is_empty() {
        unsupported.push("grants/revokes");
    }
    if !schema.policies.is_empty() {
        unsupported.push("rls policies");
    }
    if !schema.resources.is_empty() {
        unsupported.push("resources");
    }
    if schema
        .tables
        .values()
        .any(|t| !t.multi_column_fks.is_empty())
    {
        unsupported.push("multi-column foreign keys");
    }
    if schema.tables.values().any(|t| {
        t.columns.iter().any(|c| {
            c.check.is_some()
                || c.generated.is_some()
                || c.foreign_key.as_ref().is_some_and(|fk| {
                    fk.on_delete != FkAction::NoAction
                        || fk.on_update != FkAction::NoAction
                        || !matches!(
                            fk.deferrable,
                            qail_core::migrate::schema::Deferrable::NotDeferrable
                        )
                })
        })
    }) {
        unsupported.push("advanced column constraints");
    }

    if !unsupported.is_empty() {
        bail!(
            "Strict AST migration compiler does not support: {}",
            unsupported.join(", ")
        );
    }

    let mut cmds = qail_core::migrate::schema::schema_to_commands(schema);

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

    cmds.extend(hint_cmds);

    if cmds.is_empty() {
        bail!("No executable AST commands found in migration");
    }

    Ok(cmds)
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

    if let Some(index_name) = target.strip_prefix("index ").map(str::trim) {
        if !is_valid_ident_path(index_name) {
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
        if !is_valid_ident_path(table_name) {
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
        return compile_drop_column_target(column_target, target);
    }

    // Legacy hint style: `drop table_name` or `drop table.column`
    if target.contains('.') {
        return compile_drop_column_target(target, target);
    }

    if !is_valid_ident_path(target) {
        bail!(
            "Strict AST migration compiler rejects invalid drop target identifier: '{}'",
            target
        );
    }
    Ok(Qail {
        action: Action::Drop,
        table: target.to_string(),
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

fn split_table_column_target(target: &str) -> Option<(&str, &str)> {
    let (table, column) = target.rsplit_once('.')?;
    let table = table.trim();
    let column = column.trim();
    if table.is_empty() || column.is_empty() {
        return None;
    }
    Some((table, column))
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
    if !schema.policies.is_empty() {
        bail!("Strict AST migration compiler does not support parser-schema policies yet");
    }

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
            }),
            ..Default::default()
        });
    }

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
