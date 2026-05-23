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
                    format!(
                        "CREATE MATERIALIZED VIEW {} AS {}",
                        escape_identifier(&self.table),
                        query
                    )
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
                    format!(
                        "CREATE VIEW {} AS {}",
                        escape_identifier(&self.table),
                        query
                    )
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
                    let lang = func.language.as_deref().unwrap_or("plpgsql");
                    let args = func.args.join(", ");
                    let volatility = func
                        .volatility
                        .as_deref()
                        .map(|v| format!(" {}", v.to_uppercase()))
                        .unwrap_or_default();
                    let body = dollar_quote_block(&func.body);
                    format!(
                        "CREATE OR REPLACE FUNCTION {}({}) RETURNS {} LANGUAGE {}{} AS {}",
                        escape_identifier(&func.name),
                        args,
                        func.returns,
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
                    format!("DROP FUNCTION IF EXISTS {}", signature)
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
                    let events: Vec<&str> = trig
                        .events
                        .iter()
                        .map(|e| match e {
                            crate::ast::TriggerEvent::Insert => "INSERT",
                            crate::ast::TriggerEvent::Update => "UPDATE",
                            crate::ast::TriggerEvent::Delete => "DELETE",
                            crate::ast::TriggerEvent::Truncate => "TRUNCATE",
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
                if let Some(Expr::Named(col)) = self.columns.first() {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                        escape_identifier(&self.table),
                        escape_identifier(col)
                    )
                } else {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN ... SET NOT NULL",
                        escape_identifier(&self.table)
                    )
                }
            }
            Action::AlterDropNotNull => {
                if let Some(Expr::Named(col)) = self.columns.first() {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                        escape_identifier(&self.table),
                        escape_identifier(col)
                    )
                } else {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN ... DROP NOT NULL",
                        escape_identifier(&self.table)
                    )
                }
            }
            Action::AlterSetDefault => {
                if let Some(Expr::Named(col)) = self.columns.first() {
                    let default_expr = self.payload.as_deref().unwrap_or("NULL");
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                        escape_identifier(&self.table),
                        escape_identifier(col),
                        default_expr
                    )
                } else {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN ... SET DEFAULT ...",
                        escape_identifier(&self.table)
                    )
                }
            }
            Action::AlterDropDefault => {
                if let Some(Expr::Named(col)) = self.columns.first() {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                        escape_identifier(&self.table),
                        escape_identifier(col)
                    )
                } else {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN ... DROP DEFAULT",
                        escape_identifier(&self.table)
                    )
                }
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
                let privs: Vec<String> = self
                    .columns
                    .iter()
                    .filter_map(|c| match c {
                        Expr::Named(p) => Some(p.clone()),
                        _ => None,
                    })
                    .collect();
                format!(
                    "GRANT {} ON {} TO {}",
                    privs.join(", "),
                    escape_identifier(&self.table),
                    escape_identifier(role)
                )
            }
            Action::Revoke => {
                let role = self.payload.as_deref().unwrap_or("");
                let privs: Vec<String> = self
                    .columns
                    .iter()
                    .filter_map(|c| match c {
                        Expr::Named(p) => Some(p.clone()),
                        _ => None,
                    })
                    .collect();
                format!(
                    "REVOKE {} ON {} FROM {}",
                    privs.join(", "),
                    escape_identifier(&self.table),
                    escape_identifier(role)
                )
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
