//! .qail → SQL code generation.

use anyhow::Result;
use qail_core::migrate::parse_qail;
use qail_core::migrate::schema::{FkAction, GrantAction};
use qail_core::parser::schema::Schema;

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

/// Generate SQL DDL from a fully-parsed migrate Schema.
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
