//! SQL Transpiler for QAIL AST.
//!

/// Condition-to-SQL conversion.
pub mod conditions;
/// DDL statement transpilation (CREATE TABLE, ALTER TABLE, etc.).
pub mod ddl;
/// SQL dialect selection (PostgreSQL primary; SQLite compatibility retained).
pub mod dialect;
/// DML statement transpilation (INSERT, UPDATE, DELETE).
pub mod dml;
pub(crate) mod identifier;
/// RLS policy transpilation (CREATE POLICY).
pub mod policy;
/// Core SQL generation utilities.
pub mod sql;
/// Transpiler traits (SqlGenerator, escape_identifier).
pub mod traits;

/// NoSQL/vector transpilers.
pub mod nosql;
pub use nosql::dynamo::ToDynamo;
pub use nosql::mongo::ToMongo;
pub use nosql::qdrant::ToQdrant;

#[cfg(test)]
mod tests;

use crate::ast::*;
pub use conditions::ConditionToSql;
pub use dialect::Dialect;
pub use traits::SqlGenerator;
pub use traits::{escape_identifier, escape_sql_string_literal};

/// Result of transpilation with extracted parameters.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TranspileResult {
    /// The SQL template with placeholders (e.g., $1, $2 or ?, ?)
    pub sql: String,
    /// The extracted parameter values in order
    pub params: Vec<Value>,
    /// Names of named parameters in order they appear (for :name → $n mapping)
    pub named_params: Vec<String>,
}

impl TranspileResult {
    /// Create a new TranspileResult.
    pub fn new(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
            named_params: vec![],
        }
    }

    /// Create a result with no parameters.
    pub fn sql_only(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
            named_params: Vec::new(),
        }
    }
}

/// Trait for converting AST nodes to parameterized SQL.
pub trait ToSqlParameterized {
    /// Convert to SQL with extracted parameters (default dialect).
    fn to_sql_parameterized(&self) -> TranspileResult {
        self.to_sql_parameterized_with_dialect(Dialect::default())
    }
    /// Convert to SQL with extracted parameters for specific dialect.
    fn to_sql_parameterized_with_dialect(&self, dialect: Dialect) -> TranspileResult;
}

/// Trait for converting AST nodes to SQL.
pub trait ToSql {
    /// Convert this node to a SQL string using default dialect.
    fn to_sql(&self) -> String {
        self.to_sql_with_dialect(Dialect::default())
    }
    /// Convert this node to a SQL string with specific dialect.
    fn to_sql_with_dialect(&self, dialect: Dialect) -> String;
}

impl ToSql for Qail {
    fn to_sql_with_dialect(&self, dialect: Dialect) -> String {
        match self.action {
            Action::Get => dml::select::build_select(self, dialect),
            Action::Cnt => {
                // Build a count query: SELECT COUNT(*) FROM table WHERE ...
                let mut count_ast = self.clone();
                count_ast.action = Action::Get;
                count_ast.columns = vec![Expr::Aggregate {
                    col: "*".to_string(),
                    func: AggregateFunc::Count,
                    distinct: false,
                    filter: None,
                    alias: None,
                }];
                dml::select::build_select(&count_ast, dialect)
            }
            Action::Set => dml::update::build_update(self, dialect),
            Action::Del => dml::delete::build_delete(self, dialect),
            Action::Add => dml::insert::build_insert(self, dialect),
            Action::Merge => dml::merge::build_merge(self, dialect),
            Action::Gen => format!("-- gen::{}  (generates Rust struct, not SQL)", self.table),
            Action::Make => ddl::build_create_table(self, dialect),
            Action::Mod => ddl::build_alter_table(self, dialect),
            Action::Over => dml::window::build_window(self, dialect),
            Action::With => dml::cte::build_cte(self, dialect),
            Action::Index => ddl::build_create_index(self, dialect),
            Action::DropIndex => format!("DROP INDEX IF EXISTS {}", escape_identifier(&self.table)),
            Action::Alter => ddl::build_alter_add_column(self, dialect),
            Action::AlterDrop => ddl::build_alter_drop_column(self, dialect),
            Action::AlterType => ddl::build_alter_column_type(self, dialect),
            // Stubs
            Action::TxnStart => "BEGIN TRANSACTION;".to_string(), // Default stub
            Action::TxnCommit => "COMMIT;".to_string(),
            Action::TxnRollback => "ROLLBACK;".to_string(),
            Action::Put => dml::upsert::build_upsert(self, dialect),
            Action::Drop => format!("DROP TABLE {}", escape_identifier(&self.table)),
            Action::DropCol | Action::RenameCol => ddl::build_alter_column(self, dialect),
            // JSON features
            Action::JsonTable => dml::json_table::build_json_table(self, dialect),
            // COPY protocol (AST-native in qail-pg, generates SELECT for fallback)
            Action::Export => dml::select::build_select(self, dialect),
            // TRUNCATE TABLE
            Action::Truncate => format!("TRUNCATE TABLE {}", escape_identifier(&self.table)),
            // EXPLAIN - wrap SELECT query
            Action::Explain => format!("EXPLAIN {}", dml::select::build_select(self, dialect)),
            // EXPLAIN ANALYZE - execute and analyze query
            Action::ExplainAnalyze => format!(
                "EXPLAIN ANALYZE {}",
                dml::select::build_select(self, dialect)
            ),
            // LOCK TABLE
            Action::Lock => format!(
                "LOCK TABLE {} IN ACCESS EXCLUSIVE MODE",
                escape_identifier(&self.table)
            ),
            // CREATE MATERIALIZED VIEW - uses source_query for the view definition
            Action::CreateMaterializedView => {
                if let Some(source) = &self.source_query {
                    format!(
                        "CREATE MATERIALIZED VIEW {} AS {}",
                        escape_identifier(&self.table),
                        source.to_sql_with_dialect(dialect)
                    )
                } else if let Some(query) = &self.payload {
                    match checked_sql_query_fragment(query, "materialized view query") {
                        Ok(query) => format!(
                            "CREATE MATERIALIZED VIEW {} AS {}",
                            escape_identifier(&self.table),
                            query
                        ),
                        Err(err) => err,
                    }
                } else {
                    format!(
                        "CREATE MATERIALIZED VIEW {} AS {}",
                        escape_identifier(&self.table),
                        dml::select::build_select(self, dialect)
                    )
                }
            }
            // REFRESH MATERIALIZED VIEW
            Action::RefreshMaterializedView => {
                format!(
                    "REFRESH MATERIALIZED VIEW {}",
                    escape_identifier(&self.table)
                )
            }
            // DROP MATERIALIZED VIEW
            Action::DropMaterializedView => {
                format!(
                    "DROP MATERIALIZED VIEW IF EXISTS {}",
                    escape_identifier(&self.table)
                )
            }
            // LISTEN/NOTIFY (Pub/Sub)
            Action::Listen => {
                if let Some(ch) = &self.channel {
                    format!("LISTEN {}", quote_single_identifier(ch))
                } else {
                    "LISTEN".to_string()
                }
            }
            Action::Notify => {
                if let Some(ch) = &self.channel {
                    if let Some(msg) = &self.payload {
                        format!(
                            "NOTIFY {}, '{}'",
                            quote_single_identifier(ch),
                            escape_sql_string_literal(msg)
                        )
                    } else {
                        format!("NOTIFY {}", quote_single_identifier(ch))
                    }
                } else {
                    "NOTIFY".to_string()
                }
            }
            Action::Unlisten => {
                if let Some(ch) = &self.channel {
                    format!("UNLISTEN {}", quote_single_identifier(ch))
                } else {
                    "UNLISTEN *".to_string()
                }
            }
            // Savepoints
            Action::Savepoint => {
                if let Some(name) = &self.savepoint_name {
                    format!("SAVEPOINT {}", quote_single_identifier(name))
                } else {
                    "SAVEPOINT".to_string()
                }
            }
            Action::ReleaseSavepoint => {
                if let Some(name) = &self.savepoint_name {
                    format!("RELEASE SAVEPOINT {}", quote_single_identifier(name))
                } else {
                    "RELEASE SAVEPOINT".to_string()
                }
            }
            Action::RollbackToSavepoint => {
                if let Some(name) = &self.savepoint_name {
                    format!("ROLLBACK TO SAVEPOINT {}", quote_single_identifier(name))
                } else {
                    "ROLLBACK TO SAVEPOINT".to_string()
                }
            }
            // Views
            Action::CreateView => {
                if let Some(source) = &self.source_query {
                    format!(
                        "CREATE VIEW {} AS {}",
                        escape_identifier(&self.table),
                        source.to_sql_with_dialect(dialect)
                    )
                } else if let Some(query) = &self.payload {
                    match checked_sql_query_fragment(query, "view query") {
                        Ok(query) => {
                            format!(
                                "CREATE VIEW {} AS {}",
                                escape_identifier(&self.table),
                                query
                            )
                        }
                        Err(err) => err,
                    }
                } else {
                    format!(
                        "CREATE VIEW {} AS {}",
                        escape_identifier(&self.table),
                        dml::select::build_select(self, dialect)
                    )
                }
            }
            Action::DropView => format!("DROP VIEW IF EXISTS {}", escape_identifier(&self.table)),
            // Vector database operations - use qail-qdrant driver instead
            operators::Action::Search | operators::Action::Upsert | operators::Action::Scroll => {
                format!(
                    "-- Vector operation {:?} not supported in SQL. Use qail-qdrant driver.",
                    self.action
                )
            }
            operators::Action::CreateCollection | operators::Action::DeleteCollection => {
                format!(
                    "-- Vector DDL {:?} not supported in SQL. Use qail-qdrant driver.",
                    self.action
                )
            }
            // Function and Trigger operations
            operators::Action::CreateFunction => {
                if let Some(func) = &self.function_def {
                    let Some(args) = function_args_to_sql(&func.args) else {
                        return "/* ERROR: Invalid function arguments */".to_string();
                    };
                    if !is_safe_sql_type_fragment(&func.returns) {
                        return "/* ERROR: Invalid function return type */".to_string();
                    }
                    let lang = func.language.as_deref().unwrap_or("plpgsql");
                    let volatility = if let Some(volatility) = func.volatility.as_deref() {
                        if volatility.trim().is_empty() {
                            String::new()
                        } else if let Some(volatility) = volatility_to_sql(volatility) {
                            format!(" {volatility}")
                        } else {
                            return "/* ERROR: Invalid function volatility */".to_string();
                        }
                    } else {
                        String::new()
                    };
                    let body = dollar_quote_block(&func.body);
                    format!(
                        "CREATE OR REPLACE FUNCTION {}({}) RETURNS {} LANGUAGE {}{} AS {}",
                        escape_identifier(&func.name),
                        args,
                        func.returns.trim(),
                        escape_identifier(lang),
                        volatility,
                        body
                    )
                } else {
                    "-- CreateFunction requires function_def".to_string()
                }
            }
            operators::Action::DropFunction => {
                if let Some(signature) = &self.payload {
                    format!(
                        "DROP FUNCTION IF EXISTS {}",
                        function_signature_to_sql(signature)
                    )
                } else {
                    format!(
                        "DROP FUNCTION IF EXISTS {}()",
                        escape_identifier(&self.table)
                    )
                }
            }
            operators::Action::CreateTrigger => {
                if let Some(trig) = &self.trigger_def {
                    let timing = match trig.timing {
                        crate::ast::TriggerTiming::Before => "BEFORE",
                        crate::ast::TriggerTiming::After => "AFTER",
                        crate::ast::TriggerTiming::InsteadOf => "INSTEAD OF",
                    };
                    let events: Vec<String> = trig
                        .events
                        .iter()
                        .map(|e| match e {
                            crate::ast::TriggerEvent::Insert => "INSERT".to_string(),
                            crate::ast::TriggerEvent::Update if !trig.update_columns.is_empty() => {
                                format!(
                                    "UPDATE OF {}",
                                    trig.update_columns
                                        .iter()
                                        .map(|column| escape_identifier(column))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )
                            }
                            crate::ast::TriggerEvent::Update => "UPDATE".to_string(),
                            crate::ast::TriggerEvent::Delete => "DELETE".to_string(),
                            crate::ast::TriggerEvent::Truncate => "TRUNCATE".to_string(),
                        })
                        .collect();
                    let for_each = if trig.for_each_row {
                        "FOR EACH ROW"
                    } else {
                        "FOR EACH STATEMENT"
                    };
                    format!(
                        "CREATE TRIGGER {} {} {} ON {} {} EXECUTE FUNCTION {}()",
                        escape_identifier(&trig.name),
                        timing,
                        events.join(" OR "),
                        escape_identifier(&trig.table),
                        for_each,
                        escape_identifier(&trig.execute_function)
                    )
                } else {
                    "-- CreateTrigger requires trigger_def".to_string()
                }
            }
            operators::Action::DropTrigger => {
                if let Some((table, trigger)) = self.table.rsplit_once('.') {
                    format!(
                        "DROP TRIGGER IF EXISTS {} ON {}",
                        escape_identifier(trigger),
                        escape_identifier(table)
                    )
                } else {
                    format!("DROP TRIGGER IF EXISTS {}", escape_identifier(&self.table))
                }
            }
            // Phase 7: Extensions, Comments, Sequences
            Action::CreateExtension => ddl::build_create_extension(self, dialect),
            Action::DropExtension => ddl::build_drop_extension(self, dialect),
            Action::CommentOn => ddl::build_comment_on(self, dialect),
            Action::CreateSequence => ddl::build_create_sequence(self, dialect),
            Action::DropSequence => ddl::build_drop_sequence(self, dialect),
            Action::CreateEnum => ddl::build_create_enum(self, dialect),
            Action::DropEnum => ddl::build_drop_enum(self, dialect),
            Action::AlterEnumAddValue => ddl::build_alter_enum_add_value(self, dialect),
            // ALTER TABLE property operations (from diff engine)
            Action::AlterSetNotNull => {
                let [Expr::Named(col)] = self.columns.as_slice() else {
                    return "/* ERROR: ALTER SET NOT NULL requires exactly one named column */"
                        .to_string();
                };
                if col.trim().is_empty() {
                    return "/* ERROR: ALTER SET NOT NULL column cannot be empty */".to_string();
                }
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                    escape_identifier(&self.table),
                    escape_identifier(col)
                )
            }
            Action::AlterDropNotNull => {
                let [Expr::Named(col)] = self.columns.as_slice() else {
                    return "/* ERROR: ALTER DROP NOT NULL requires exactly one named column */"
                        .to_string();
                };
                if col.trim().is_empty() {
                    return "/* ERROR: ALTER DROP NOT NULL column cannot be empty */".to_string();
                }
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                    escape_identifier(&self.table),
                    escape_identifier(col)
                )
            }
            Action::AlterSetDefault => {
                let [Expr::Named(col)] = self.columns.as_slice() else {
                    return "/* ERROR: ALTER SET DEFAULT requires exactly one named column */"
                        .to_string();
                };
                if col.trim().is_empty() {
                    return "/* ERROR: ALTER SET DEFAULT column cannot be empty */".to_string();
                }
                let Some(default_expr) = self.payload.as_deref() else {
                    return "/* ERROR: ALTER SET DEFAULT requires a default expression */"
                        .to_string();
                };
                if default_expr.trim().is_empty()
                    || default_expr.contains('\0')
                    || contains_unquoted_statement_delimiter(default_expr)
                {
                    return "/* ERROR: Invalid default expression */".to_string();
                }
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                    escape_identifier(&self.table),
                    escape_identifier(col),
                    default_expr.trim()
                )
            }
            Action::AlterDropDefault => {
                let [Expr::Named(col)] = self.columns.as_slice() else {
                    return "/* ERROR: ALTER DROP DEFAULT requires exactly one named column */"
                        .to_string();
                };
                if col.trim().is_empty() {
                    return "/* ERROR: ALTER DROP DEFAULT column cannot be empty */".to_string();
                }
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                    escape_identifier(&self.table),
                    escape_identifier(col)
                )
            }
            Action::AlterEnableRls => {
                format!(
                    "ALTER TABLE {} ENABLE ROW LEVEL SECURITY",
                    escape_identifier(&self.table)
                )
            }
            Action::AlterDisableRls => {
                format!(
                    "ALTER TABLE {} DISABLE ROW LEVEL SECURITY",
                    escape_identifier(&self.table)
                )
            }
            Action::AlterForceRls => {
                format!(
                    "ALTER TABLE {} FORCE ROW LEVEL SECURITY",
                    escape_identifier(&self.table)
                )
            }
            Action::AlterNoForceRls => {
                format!(
                    "ALTER TABLE {} NO FORCE ROW LEVEL SECURITY",
                    escape_identifier(&self.table)
                )
            }
            // Session & procedural commands
            Action::Call => {
                format!("CALL {}", call_target_to_sql(&self.table))
            }
            Action::Do => {
                let body = self.payload.as_deref().unwrap_or("");
                let lang = if self.table.is_empty() {
                    "plpgsql"
                } else {
                    &self.table
                };
                format!(
                    "DO {} LANGUAGE {}",
                    dollar_quote_block(body),
                    escape_identifier(lang)
                )
            }
            Action::SessionSet => {
                let value = self.payload.as_deref().unwrap_or("");
                format!(
                    "SET {} = '{}'",
                    session_setting_name_to_sql(&self.table),
                    escape_sql_string_literal(value)
                )
            }
            Action::SessionShow => {
                format!("SHOW {}", session_setting_name_to_sql(&self.table))
            }
            Action::SessionReset => {
                format!("RESET {}", session_setting_name_to_sql(&self.table))
            }
            Action::CreateDatabase => {
                format!("CREATE DATABASE {}", escape_identifier(&self.table))
            }
            Action::DropDatabase => {
                format!("DROP DATABASE IF EXISTS {}", escape_identifier(&self.table))
            }
            Action::Grant => {
                let role = self.payload.as_deref().unwrap_or("");
                if let Some(privs) = privileges_to_sql(&self.columns) {
                    format!(
                        "GRANT {} ON {} TO {}",
                        privs,
                        escape_identifier(&self.table),
                        escape_identifier(role)
                    )
                } else {
                    "/* ERROR: Invalid privileges */".to_string()
                }
            }
            Action::Revoke => {
                let role = self.payload.as_deref().unwrap_or("");
                if let Some(privs) = privileges_to_sql(&self.columns) {
                    format!(
                        "REVOKE {} ON {} FROM {}",
                        privs,
                        escape_identifier(&self.table),
                        escape_identifier(role)
                    )
                } else {
                    "/* ERROR: Invalid privileges */".to_string()
                }
            }
            Action::CreatePolicy => {
                if let Some(policy) = &self.policy_def {
                    policy::create_policy_sql(policy)
                } else {
                    "-- CreatePolicy requires policy_def".to_string()
                }
            }
            Action::DropPolicy => {
                if let Some(policy) = &self.policy_def {
                    policy::drop_policy_sql(&policy.name, &policy.table)
                } else if let Some(policy_name) = &self.payload {
                    policy::drop_policy_sql(policy_name, &self.table)
                } else {
                    "-- DropPolicy requires policy name + table".to_string()
                }
            }
        }
    }
}

fn session_setting_name_to_sql(name: &str) -> String {
    if is_valid_session_setting_name(name) {
        name.to_string()
    } else {
        escape_identifier(name)
    }
}

fn quote_single_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('\0', "").replace('"', "\"\""))
}

fn dollar_quote_block(body: &str) -> String {
    let body = body.replace('\0', "");
    for idx in 0..=body.len() {
        let tag = if idx == 0 {
            String::new()
        } else {
            format!("qail_body_{idx}")
        };
        let delimiter = format!("${tag}$");
        if !body.contains(&delimiter) {
            return format!("{delimiter} {body} {delimiter}");
        }
    }

    format!("'{}'", escape_sql_string_literal(&body))
}

fn call_target_to_sql(target: &str) -> String {
    let target = target.trim().trim_end_matches(';').trim();
    if target.is_empty()
        || target.contains('\0')
        || target.contains(';')
        || target.contains("--")
        || target.contains("/*")
        || target.contains("*/")
    {
        return escape_identifier(target);
    }

    match target.split_once('(') {
        Some((name, args)) if args.ends_with(')') && !args[..args.len() - 1].contains('(') => {
            format!("{}({}", escape_identifier(name.trim()), args)
        }
        None => escape_identifier(target),
        _ => escape_identifier(target),
    }
}

fn contains_unquoted_statement_delimiter(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut i = 0;
    let mut in_single = false;
    let mut in_double = false;

    while i < bytes.len() {
        let b = bytes[i];
        if b == 0 {
            return true;
        }

        if in_single {
            if b == b'\'' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single = false;
            }
            i += 1;
            continue;
        }

        if in_double {
            if b == b'"' {
                if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double = false;
            }
            i += 1;
            continue;
        }

        match b {
            b'\'' => in_single = true,
            b'"' => in_double = true,
            b';' => return true,
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => return true,
            b'/' if i + 1 < bytes.len() && bytes[i + 1] == b'*' => return true,
            _ => {}
        }
        i += 1;
    }

    false
}

fn checked_sql_query_fragment(query: &str, context: &str) -> Result<String, String> {
    let query = query.trim();
    if query.is_empty() || query.contains('\0') || contains_unquoted_statement_delimiter(query) {
        return Err(format!("/* ERROR: Invalid {context} */"));
    }
    Ok(query.to_string())
}

fn privilege_to_sql(privilege: &str) -> Option<&'static str> {
    match privilege.trim().to_ascii_uppercase().as_str() {
        "SELECT" => Some("SELECT"),
        "INSERT" => Some("INSERT"),
        "UPDATE" => Some("UPDATE"),
        "DELETE" => Some("DELETE"),
        "TRUNCATE" => Some("TRUNCATE"),
        "REFERENCES" => Some("REFERENCES"),
        "TRIGGER" => Some("TRIGGER"),
        "USAGE" => Some("USAGE"),
        "CREATE" => Some("CREATE"),
        "CONNECT" => Some("CONNECT"),
        "TEMP" | "TEMPORARY" => Some("TEMPORARY"),
        "EXECUTE" => Some("EXECUTE"),
        "ALL" | "ALL PRIVILEGES" => Some("ALL PRIVILEGES"),
        _ => None,
    }
}

fn privileges_to_sql(columns: &[Expr]) -> Option<String> {
    if columns.is_empty() {
        None
    } else {
        let mut privileges = Vec::with_capacity(columns.len());
        for column in columns {
            let Expr::Named(privilege) = column else {
                return None;
            };
            let sql = privilege_to_sql(privilege)?;
            privileges.push(sql);
        }
        Some(privileges.join(", "))
    }
}

fn is_safe_sql_type_fragment(fragment: &str) -> bool {
    let fragment = fragment.trim();
    !fragment.is_empty()
        && !fragment.contains('\0')
        && !fragment.contains(';')
        && !fragment.contains('\'')
        && !fragment.contains('"')
        && !fragment.contains("--")
        && !fragment.contains("/*")
        && !fragment.contains("*/")
        && fragment.bytes().all(|b| {
            b.is_ascii_alphanumeric()
                || matches!(
                    b,
                    b'_' | b'.' | b' ' | b'(' | b')' | b',' | b'[' | b']' | b'%' | b'+' | b'-'
                )
        })
}

fn volatility_to_sql(volatility: &str) -> Option<&'static str> {
    match volatility.trim().to_ascii_uppercase().as_str() {
        "VOLATILE" => Some("VOLATILE"),
        "STABLE" => Some("STABLE"),
        "IMMUTABLE" => Some("IMMUTABLE"),
        _ => None,
    }
}

fn function_arg_to_sql(arg: &str) -> Option<String> {
    let arg = arg.trim();
    if !is_safe_sql_type_fragment(arg) {
        return None;
    }

    let mut parts = arg.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }
    if parts.len() == 1 {
        return Some(parts[0].to_string());
    }

    let mode = match parts[0].to_ascii_uppercase().as_str() {
        "IN" | "OUT" | "INOUT" | "VARIADIC" => Some(parts.remove(0).to_ascii_uppercase()),
        _ => None,
    };
    if parts.len() < 2 {
        return None;
    }

    let name = escape_identifier(parts.remove(0));
    let type_fragment = parts.join(" ");
    if !is_safe_sql_type_fragment(&type_fragment) {
        return None;
    }

    let mut rendered = String::new();
    if let Some(mode) = mode {
        rendered.push_str(&mode);
        rendered.push(' ');
    }
    rendered.push_str(&name);
    rendered.push(' ');
    rendered.push_str(type_fragment.trim());
    Some(rendered)
}

fn function_args_to_sql(args: &[String]) -> Option<String> {
    let mut rendered = Vec::with_capacity(args.len());
    for arg in args {
        rendered.push(function_arg_to_sql(arg)?);
    }
    Some(rendered.join(", "))
}

fn split_top_level_args(args: &str) -> Option<Vec<&str>> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    for (idx, ch) in args.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.checked_sub(1)?,
            ',' if depth == 0 => {
                result.push(args[start..idx].trim());
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    if depth != 0 {
        return None;
    }
    let tail = args[start..].trim();
    if !tail.is_empty() {
        result.push(tail);
    }
    Some(result)
}

fn function_signature_to_sql(signature: &str) -> String {
    let signature = signature.trim().trim_end_matches(';').trim();
    if signature.is_empty()
        || signature.contains('\0')
        || signature.contains(';')
        || signature.contains("--")
        || signature.contains("/*")
        || signature.contains("*/")
    {
        return escape_identifier(signature);
    }

    match signature.split_once('(') {
        Some((name, args)) if args.ends_with(')') => {
            let args = &args[..args.len() - 1];
            let Some(parts) = split_top_level_args(args) else {
                return escape_identifier(signature);
            };
            let mut rendered_args = Vec::new();
            for part in parts {
                if part.is_empty() {
                    continue;
                }
                if !is_safe_sql_type_fragment(part) {
                    return escape_identifier(signature);
                }
                rendered_args.push(part.trim().to_string());
            }
            format!(
                "{}({})",
                escape_identifier(name.trim()),
                rendered_args.join(", ")
            )
        }
        None => escape_identifier(signature),
        _ => escape_identifier(signature),
    }
}

fn is_valid_session_setting_name(name: &str) -> bool {
    !name.is_empty()
        && name.split('.').all(|part| {
            let mut chars = part.chars();
            matches!(chars.next(), Some(ch) if ch.is_ascii_alphabetic() || ch == '_')
                && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        })
}

impl ToSqlParameterized for Qail {
    fn to_sql_parameterized_with_dialect(&self, dialect: Dialect) -> TranspileResult {
        // Use the full ToSql implementation which handles CTEs, JOINs, etc.
        // Then post-process to extract named parameters for binding
        let full_sql = self.to_sql_with_dialect(dialect);
        let (sql, named_params) = replace_named_params_outside_sql_literals(&full_sql);

        TranspileResult {
            sql,
            params: Vec::new(), // Positional params not used, named_params provides mapping
            named_params,
        }
    }
}

fn replace_named_params_outside_sql_literals(sql: &str) -> (String, Vec<String>) {
    let mut named_params: Vec<String> = Vec::new();
    let mut seen_params: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut result = String::with_capacity(sql.len());
    let mut param_index = 1;
    let mut i = 0;
    let mut state = SqlScanState::Normal;

    while i < sql.len() {
        match &state {
            SqlScanState::Normal => {
                if sql[i..].starts_with("--") {
                    result.push_str("--");
                    i += 2;
                    state = SqlScanState::LineComment;
                    continue;
                }
                if sql[i..].starts_with("/*") {
                    result.push_str("/*");
                    i += 2;
                    state = SqlScanState::BlockComment;
                    continue;
                }
                if sql[i..].starts_with("::") {
                    result.push_str("::");
                    i += 2;
                    continue;
                }
                if let Some(delimiter) = sql_dollar_quote_delimiter_at(sql, i) {
                    result.push_str(&delimiter);
                    i += delimiter.len();
                    state = SqlScanState::DollarQuoted(delimiter);
                    continue;
                }

                let Some((ch, next_i)) = next_sql_char(sql, i) else {
                    break;
                };
                match ch {
                    '\'' => {
                        result.push(ch);
                        i = next_i;
                        state = SqlScanState::SingleQuoted;
                    }
                    '"' => {
                        result.push(ch);
                        i = next_i;
                        state = SqlScanState::DoubleQuoted;
                    }
                    ':' => {
                        let Some((next, mut cursor)) = next_sql_char(sql, next_i) else {
                            result.push(ch);
                            i = next_i;
                            continue;
                        };
                        if is_named_param_start(next) {
                            let mut param_name = String::new();
                            param_name.push(next);
                            while let Some((candidate, candidate_next)) = next_sql_char(sql, cursor)
                            {
                                if is_named_param_continue(candidate) {
                                    param_name.push(candidate);
                                    cursor = candidate_next;
                                } else {
                                    break;
                                }
                            }

                            let idx = if let Some(&existing) = seen_params.get(&param_name) {
                                existing
                            } else {
                                let idx = param_index;
                                seen_params.insert(param_name.clone(), idx);
                                named_params.push(param_name);
                                param_index += 1;
                                idx
                            };
                            result.push('$');
                            result.push_str(&idx.to_string());
                            i = cursor;
                        } else {
                            result.push(ch);
                            i = next_i;
                        }
                    }
                    _ => {
                        result.push(ch);
                        i = next_i;
                    }
                }
            }
            SqlScanState::SingleQuoted => {
                let Some((ch, next_i)) = next_sql_char(sql, i) else {
                    break;
                };
                result.push(ch);
                i = next_i;
                if ch == '\'' {
                    if sql[i..].starts_with('\'') {
                        result.push('\'');
                        i += 1;
                    } else {
                        state = SqlScanState::Normal;
                    }
                }
            }
            SqlScanState::DoubleQuoted => {
                let Some((ch, next_i)) = next_sql_char(sql, i) else {
                    break;
                };
                result.push(ch);
                i = next_i;
                if ch == '"' {
                    if sql[i..].starts_with('"') {
                        result.push('"');
                        i += 1;
                    } else {
                        state = SqlScanState::Normal;
                    }
                }
            }
            SqlScanState::LineComment => {
                let Some((ch, next_i)) = next_sql_char(sql, i) else {
                    break;
                };
                result.push(ch);
                i = next_i;
                if ch == '\n' {
                    state = SqlScanState::Normal;
                }
            }
            SqlScanState::BlockComment => {
                if sql[i..].starts_with("*/") {
                    result.push_str("*/");
                    i += 2;
                    state = SqlScanState::Normal;
                    continue;
                }
                let Some((ch, next_i)) = next_sql_char(sql, i) else {
                    break;
                };
                result.push(ch);
                i = next_i;
            }
            SqlScanState::DollarQuoted(delimiter) => {
                if sql[i..].starts_with(delimiter) {
                    result.push_str(delimiter);
                    i += delimiter.len();
                    state = SqlScanState::Normal;
                    continue;
                }
                let Some((ch, next_i)) = next_sql_char(sql, i) else {
                    break;
                };
                result.push(ch);
                i = next_i;
            }
        }
    }

    (result, named_params)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SqlScanState {
    Normal,
    SingleQuoted,
    DoubleQuoted,
    LineComment,
    BlockComment,
    DollarQuoted(String),
}

fn next_sql_char(sql: &str, idx: usize) -> Option<(char, usize)> {
    let ch = sql.get(idx..)?.chars().next()?;
    Some((ch, idx + ch.len_utf8()))
}

fn is_named_param_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_named_param_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn sql_dollar_quote_delimiter_at(sql: &str, idx: usize) -> Option<String> {
    if !sql.get(idx..)?.starts_with('$') {
        return None;
    }
    let rest = sql.get(idx + 1..)?;
    for (offset, ch) in rest.char_indices() {
        if ch == '$' {
            let tag = &rest[..offset];
            if tag.is_empty()
                || (is_named_param_start(tag.chars().next()?)
                    && tag.chars().all(is_named_param_continue))
            {
                return Some(sql[idx..idx + offset + 2].to_string());
            }
            return None;
        }
        if !is_named_param_continue(ch) {
            return None;
        }
    }
    None
}
