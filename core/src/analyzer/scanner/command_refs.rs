use std::collections::HashSet;
use std::path::Path;

#[cfg(test)]
use crate::ast::CageKind;
use crate::ast::{Action, Cage, Condition, ConflictAction, Expr, MergeAction, Value};

use super::{CodeReference, QueryType};

#[derive(Clone, Copy)]
struct ColumnScope<'a> {
    table: &'a str,
    alias: Option<&'a str>,
    include_unqualified: bool,
}

impl<'a> ColumnScope<'a> {
    fn target(table: &'a str, alias: Option<&'a str>) -> Self {
        Self {
            table,
            alias,
            include_unqualified: true,
        }
    }

    fn related(table: &'a str, alias: Option<&'a str>) -> Self {
        Self {
            table,
            alias,
            include_unqualified: false,
        }
    }

    fn matches_qualifier(&self, qualifier: &str) -> bool {
        self.alias.is_some_and(|alias| ident_eq(alias, qualifier))
            || ident_eq(self.table, qualifier)
            || ident_eq(bare_ident(self.table), qualifier)
    }
}

fn command_to_reference(path: &Path, line: usize, cmd: &crate::Qail) -> Option<CodeReference> {
    if cmd.table.trim().is_empty() {
        return None;
    }

    let snippet = match cmd.action {
        Action::Get => format!("get {} fields ...", cmd.table),
        Action::Set => format!("set {} values ...", cmd.table),
        Action::Del => format!("del {}", cmd.table),
        Action::Add => format!("add {} fields ...", cmd.table),
        Action::Merge => format!("merge {} ...", cmd.table),
        _ => return None,
    };
    let columns = collect_reference_columns(cmd);

    Some(CodeReference {
        file: path.to_path_buf(),
        line,
        table: cmd.table.clone(),
        columns,
        query_type: QueryType::Qail,
        snippet,
    })
}

pub(super) fn command_to_references(
    path: &Path,
    line: usize,
    cmd: &crate::Qail,
) -> Vec<CodeReference> {
    command_to_references_with_cte_aliases(path, line, cmd, &[])
}

fn command_to_references_with_cte_aliases(
    path: &Path,
    line: usize,
    cmd: &crate::Qail,
    inherited_cte_aliases: &[String],
) -> Vec<CodeReference> {
    let mut refs = Vec::new();
    let mut local_cte_aliases = inherited_cte_aliases.to_vec();
    local_cte_aliases.extend(cmd.ctes.iter().map(|cte| cte.name.clone()));

    if !is_cte_alias(&cmd.table, &local_cte_aliases)
        && let Some(reference) = command_to_reference(path, line, cmd)
    {
        refs.push(reference);
    }
    collect_related_table_references(path, line, cmd, &local_cte_aliases, &mut refs);
    collect_subquery_references(path, line, cmd, &local_cte_aliases, &mut refs);
    refs
}

fn collect_related_table_references(
    path: &Path,
    line: usize,
    cmd: &crate::Qail,
    cte_aliases: &[String],
    refs: &mut Vec<CodeReference>,
) {
    for join in &cmd.joins {
        if is_cte_alias(&join.table, cte_aliases) {
            continue;
        }
        push_scoped_reference(
            path,
            line,
            cmd,
            &join.table,
            None,
            format!("join {} ...", join.table),
            refs,
        );
    }
    for table in &cmd.from_tables {
        if is_cte_alias(table, cte_aliases) {
            continue;
        }
        push_scoped_reference(
            path,
            line,
            cmd,
            table,
            None,
            format!("from {} ...", table),
            refs,
        );
    }
    for table in &cmd.using_tables {
        if is_cte_alias(table, cte_aliases) {
            continue;
        }
        push_scoped_reference(
            path,
            line,
            cmd,
            table,
            None,
            format!("using {} ...", table),
            refs,
        );
    }
    if let Some(merge) = &cmd.merge
        && let crate::ast::MergeSource::Table { name, alias } = &merge.source
        && !is_cte_alias(name, cte_aliases)
    {
        push_scoped_reference(
            path,
            line,
            cmd,
            name,
            alias.as_deref(),
            format!("merge source {} ...", name),
            refs,
        );
    }
}

fn push_scoped_reference(
    path: &Path,
    line: usize,
    cmd: &crate::Qail,
    table: &str,
    alias: Option<&str>,
    snippet: String,
    refs: &mut Vec<CodeReference>,
) {
    if table.trim().is_empty() {
        return;
    }

    let scope = ColumnScope::related(table, alias);
    refs.push(CodeReference {
        file: path.to_path_buf(),
        line,
        table: table.to_string(),
        columns: collect_reference_columns_for_scope(cmd, scope),
        query_type: QueryType::Qail,
        snippet,
    });
}

fn collect_subquery_references(
    path: &Path,
    line: usize,
    cmd: &crate::Qail,
    cte_aliases: &[String],
    refs: &mut Vec<CodeReference>,
) {
    for expr in &cmd.columns {
        collect_expr_subquery_references(path, line, expr, cte_aliases, refs);
    }
    for cage in &cmd.cages {
        collect_cage_subquery_references(path, line, cage, cte_aliases, refs);
    }
    for join in &cmd.joins {
        if let Some(conditions) = &join.on {
            collect_conditions_subquery_references(path, line, conditions, cte_aliases, refs);
        }
    }
    collect_conditions_subquery_references(path, line, &cmd.having, cte_aliases, refs);
    for expr in &cmd.distinct_on {
        collect_expr_subquery_references(path, line, expr, cte_aliases, refs);
    }
    if let Some(returning) = &cmd.returning {
        for expr in returning {
            collect_expr_subquery_references(path, line, expr, cte_aliases, refs);
        }
    }
    if let Some(on_conflict) = &cmd.on_conflict
        && let ConflictAction::DoUpdate { assignments } = &on_conflict.action
    {
        for (_, expr) in assignments {
            collect_expr_subquery_references(path, line, expr, cte_aliases, refs);
        }
    }
    if let Some(merge) = &cmd.merge {
        match &merge.source {
            crate::ast::MergeSource::Query { query, .. } => {
                refs.extend(command_to_references_with_cte_aliases(
                    path,
                    line,
                    query,
                    cte_aliases,
                ));
            }
            crate::ast::MergeSource::Table { .. } => {}
        }
        collect_conditions_subquery_references(path, line, &merge.on, cte_aliases, refs);
        for clause in &merge.clauses {
            collect_conditions_subquery_references(
                path,
                line,
                &clause.condition,
                cte_aliases,
                refs,
            );
            match &clause.action {
                MergeAction::Update { assignments } => {
                    for (_, expr) in assignments {
                        collect_expr_subquery_references(path, line, expr, cte_aliases, refs);
                    }
                }
                MergeAction::Insert { values, .. } => {
                    for expr in values {
                        collect_expr_subquery_references(path, line, expr, cte_aliases, refs);
                    }
                }
                MergeAction::Delete | MergeAction::DoNothing => {}
            }
        }
    }
    if let Some(source_query) = &cmd.source_query {
        refs.extend(command_to_references_with_cte_aliases(
            path,
            line,
            source_query,
            cte_aliases,
        ));
    }
    for (_, set_query) in &cmd.set_ops {
        refs.extend(command_to_references_with_cte_aliases(
            path,
            line,
            set_query,
            cte_aliases,
        ));
    }
    let mut known_aliases = cte_aliases.to_vec();
    for cte in &cmd.ctes {
        if !known_aliases.iter().any(|alias| ident_eq(alias, &cte.name)) {
            known_aliases.push(cte.name.clone());
        }
        refs.extend(command_to_references_with_cte_aliases(
            path,
            line,
            &cte.base_query,
            &known_aliases,
        ));
        if let Some(recursive_query) = &cte.recursive_query {
            refs.extend(command_to_references_with_cte_aliases(
                path,
                line,
                recursive_query,
                &known_aliases,
            ));
        }
    }
}

fn collect_expr_subquery_references(
    path: &Path,
    line: usize,
    expr: &Expr,
    cte_aliases: &[String],
    refs: &mut Vec<CodeReference>,
) {
    match expr {
        Expr::Aggregate { filter, .. } => {
            if let Some(conditions) = filter {
                collect_conditions_subquery_references(path, line, conditions, cte_aliases, refs);
            }
        }
        Expr::Cast { expr, .. }
        | Expr::Mod { col: expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::FieldAccess { expr, .. } => {
            collect_expr_subquery_references(path, line, expr, cte_aliases, refs)
        }
        Expr::Subscript { expr, index, .. } => {
            collect_expr_subquery_references(path, line, expr, cte_aliases, refs);
            collect_expr_subquery_references(path, line, index, cte_aliases, refs);
        }
        Expr::FunctionCall { args, .. } | Expr::ArrayConstructor { elements: args, .. } => {
            for arg in args {
                collect_expr_subquery_references(path, line, arg, cte_aliases, refs);
            }
        }
        Expr::SpecialFunction { args, .. } => {
            for (_, arg) in args {
                collect_expr_subquery_references(path, line, arg, cte_aliases, refs);
            }
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_subquery_references(path, line, left, cte_aliases, refs);
            collect_expr_subquery_references(path, line, right, cte_aliases, refs);
        }
        Expr::Literal(value) => {
            collect_value_subquery_references(path, line, value, cte_aliases, refs)
        }
        Expr::RowConstructor { elements, .. } => {
            for expr in elements {
                collect_expr_subquery_references(path, line, expr, cte_aliases, refs);
            }
        }
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            for (condition, value) in when_clauses {
                collect_condition_subquery_references(path, line, condition, cte_aliases, refs);
                collect_expr_subquery_references(path, line, value, cte_aliases, refs);
            }
            if let Some(value) = else_value {
                collect_expr_subquery_references(path, line, value, cte_aliases, refs);
            }
        }
        Expr::Window { params, order, .. } => {
            for param in params {
                collect_expr_subquery_references(path, line, param, cte_aliases, refs);
            }
            for cage in order {
                collect_cage_subquery_references(path, line, cage, cte_aliases, refs);
            }
        }
        Expr::Subquery { query, .. } | Expr::Exists { query, .. } => {
            refs.extend(command_to_references_with_cte_aliases(
                path,
                line,
                query,
                cte_aliases,
            ));
        }
        Expr::Star
        | Expr::Named(_)
        | Expr::Aliased { .. }
        | Expr::JsonAccess { .. }
        | Expr::Def { .. } => {}
    }
}

fn collect_cage_subquery_references(
    path: &Path,
    line: usize,
    cage: &Cage,
    cte_aliases: &[String],
    refs: &mut Vec<CodeReference>,
) {
    collect_conditions_subquery_references(path, line, &cage.conditions, cte_aliases, refs);
}

fn collect_conditions_subquery_references(
    path: &Path,
    line: usize,
    conditions: &[Condition],
    cte_aliases: &[String],
    refs: &mut Vec<CodeReference>,
) {
    for condition in conditions {
        collect_condition_subquery_references(path, line, condition, cte_aliases, refs);
    }
}

fn collect_condition_subquery_references(
    path: &Path,
    line: usize,
    condition: &Condition,
    cte_aliases: &[String],
    refs: &mut Vec<CodeReference>,
) {
    collect_expr_subquery_references(path, line, &condition.left, cte_aliases, refs);
    collect_value_subquery_references(path, line, &condition.value, cte_aliases, refs);
}

fn collect_value_subquery_references(
    path: &Path,
    line: usize,
    value: &Value,
    cte_aliases: &[String],
    refs: &mut Vec<CodeReference>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_value_subquery_references(path, line, value, cte_aliases, refs);
            }
        }
        Value::Subquery(query) => refs.extend(command_to_references_with_cte_aliases(
            path,
            line,
            query,
            cte_aliases,
        )),
        Value::Expr(expr) => collect_expr_subquery_references(path, line, expr, cte_aliases, refs),
        _ => {}
    }
}

fn is_cte_alias(table: &str, cte_aliases: &[String]) -> bool {
    cte_aliases.iter().any(|alias| ident_eq(alias, table))
}

fn collect_reference_columns(cmd: &crate::Qail) -> Vec<String> {
    let target_alias = cmd
        .merge
        .as_ref()
        .and_then(|merge| merge.target_alias.as_deref());
    collect_reference_columns_for_scope(cmd, ColumnScope::target(&cmd.table, target_alias))
}

fn collect_reference_columns_for_scope(cmd: &crate::Qail, scope: ColumnScope<'_>) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();

    collect_exprs_columns(&cmd.columns, scope, &mut cols, &mut seen);
    for cage in &cmd.cages {
        collect_cage_columns(cage, scope, &mut cols, &mut seen);
    }
    for join in &cmd.joins {
        if let Some(conditions) = &join.on {
            collect_conditions_columns(conditions, scope, &mut cols, &mut seen);
        }
    }
    collect_conditions_columns(&cmd.having, scope, &mut cols, &mut seen);
    collect_exprs_columns(&cmd.distinct_on, scope, &mut cols, &mut seen);
    if let Some(returning) = &cmd.returning {
        collect_exprs_columns(returning, scope, &mut cols, &mut seen);
    }
    if let Some(on_conflict) = &cmd.on_conflict {
        for column in &on_conflict.columns {
            push_column_ref(column, scope, &mut cols, &mut seen);
        }
        if let ConflictAction::DoUpdate { assignments } = &on_conflict.action {
            for (column, expr) in assignments {
                push_column_ref(column, scope, &mut cols, &mut seen);
                collect_expr_columns(expr, scope, &mut cols, &mut seen);
            }
        }
    }
    if let Some(merge) = &cmd.merge {
        collect_conditions_columns(&merge.on, scope, &mut cols, &mut seen);
        for clause in &merge.clauses {
            collect_conditions_columns(&clause.condition, scope, &mut cols, &mut seen);
            match &clause.action {
                MergeAction::Update { assignments } => {
                    for (column, expr) in assignments {
                        push_column_ref(column, scope, &mut cols, &mut seen);
                        collect_expr_columns(expr, scope, &mut cols, &mut seen);
                    }
                }
                MergeAction::Insert { columns, values } => {
                    for column in columns {
                        push_column_ref(column, scope, &mut cols, &mut seen);
                    }
                    collect_exprs_columns(values, scope, &mut cols, &mut seen);
                }
                MergeAction::Delete | MergeAction::DoNothing => {}
            }
        }
    }

    cols
}

fn collect_exprs_columns(
    exprs: &[Expr],
    scope: ColumnScope<'_>,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    for expr in exprs {
        collect_expr_columns(expr, scope, cols, seen);
    }
}

fn collect_cage_columns(
    cage: &Cage,
    scope: ColumnScope<'_>,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    collect_conditions_columns(&cage.conditions, scope, cols, seen);
}

fn collect_conditions_columns(
    conditions: &[Condition],
    scope: ColumnScope<'_>,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    for condition in conditions {
        collect_condition_columns(condition, scope, cols, seen);
    }
}

fn collect_condition_columns(
    condition: &Condition,
    scope: ColumnScope<'_>,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    collect_expr_columns(&condition.left, scope, cols, seen);
    collect_value_columns(&condition.value, scope, cols, seen);
}

fn collect_expr_columns(
    expr: &Expr,
    scope: ColumnScope<'_>,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    match expr {
        Expr::Star => push_column_ref("*", scope, cols, seen),
        Expr::Named(name) | Expr::Aliased { name, .. } => push_column_ref(name, scope, cols, seen),
        Expr::Aggregate { col, filter, .. } => {
            if is_column_like_aggregate_arg(col) {
                push_column_ref(col, scope, cols, seen);
            }
            if let Some(conditions) = filter {
                collect_conditions_columns(conditions, scope, cols, seen);
            }
        }
        Expr::JsonAccess { column, .. } => push_column_ref(column, scope, cols, seen),
        Expr::Cast { expr, .. }
        | Expr::Mod { col: expr, .. }
        | Expr::Collate { expr, .. }
        | Expr::FieldAccess { expr, .. } => collect_expr_columns(expr, scope, cols, seen),
        Expr::Subscript { expr, index, .. } => {
            collect_expr_columns(expr, scope, cols, seen);
            collect_expr_columns(index, scope, cols, seen);
        }
        Expr::FunctionCall { args, .. } | Expr::ArrayConstructor { elements: args, .. } => {
            collect_exprs_columns(args, scope, cols, seen);
        }
        Expr::SpecialFunction { args, .. } => {
            for (_, arg) in args {
                collect_expr_columns(arg, scope, cols, seen);
            }
        }
        Expr::Binary { left, right, .. } => {
            collect_expr_columns(left, scope, cols, seen);
            collect_expr_columns(right, scope, cols, seen);
        }
        Expr::Literal(value) => collect_value_columns(value, scope, cols, seen),
        Expr::RowConstructor { elements, .. } => collect_exprs_columns(elements, scope, cols, seen),
        Expr::Case {
            when_clauses,
            else_value,
            ..
        } => {
            for (condition, value) in when_clauses {
                collect_condition_columns(condition, scope, cols, seen);
                collect_expr_columns(value, scope, cols, seen);
            }
            if let Some(value) = else_value {
                collect_expr_columns(value, scope, cols, seen);
            }
        }
        Expr::Window {
            params,
            partition,
            order,
            ..
        } => {
            collect_exprs_columns(params, scope, cols, seen);
            for column in partition {
                push_column_ref(column, scope, cols, seen);
            }
            for cage in order {
                collect_cage_columns(cage, scope, cols, seen);
            }
        }
        Expr::Def { .. } | Expr::Subquery { .. } | Expr::Exists { .. } => {}
    }
}

fn collect_value_columns(
    value: &Value,
    scope: ColumnScope<'_>,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    match value {
        Value::Column(column) => push_column_ref(column, scope, cols, seen),
        Value::Expr(expr) => collect_expr_columns(expr, scope, cols, seen),
        Value::Array(values) => {
            for value in values {
                collect_value_columns(value, scope, cols, seen);
            }
        }
        _ => {}
    }
}

fn push_column_ref(
    name: &str,
    scope: ColumnScope<'_>,
    cols: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    let name = name.trim();
    if name.is_empty() {
        return;
    }

    let (qualifier, column) = split_column_ref(name);
    if qualifier
        .map(|qualifier| scope.matches_qualifier(qualifier))
        .unwrap_or(scope.include_unqualified)
    {
        push_plain_column_ref(column, cols, seen);
    }
}

fn push_plain_column_ref(name: &str, cols: &mut Vec<String>, seen: &mut HashSet<String>) {
    let name = name.trim();
    if !name.is_empty() && seen.insert(name.to_string()) {
        cols.push(name.to_string());
    }
}

fn split_column_ref(name: &str) -> (Option<&str>, &str) {
    let mut parts = name.rsplitn(3, '.');
    let column = parts.next().unwrap_or(name).trim();
    let qualifier = parts.next().map(str::trim).filter(|part| !part.is_empty());
    (qualifier, column)
}

fn ident_eq(left: &str, right: &str) -> bool {
    left.trim_matches('"')
        .eq_ignore_ascii_case(right.trim_matches('"'))
}

fn is_column_like_aggregate_arg(arg: &str) -> bool {
    let arg = arg.trim();
    !arg.is_empty()
        && arg != "*"
        && !arg.chars().all(|ch| ch.is_ascii_digit())
        && arg
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '"'))
}

fn bare_ident(name: &str) -> &str {
    name.trim_matches('"')
        .rsplit_once('.')
        .map_or(name.trim_matches('"'), |(_, bare)| bare.trim_matches('"'))
}

#[cfg(test)]
fn extract_payload_columns(cmd: &crate::Qail) -> Vec<String> {
    let mut cols = Vec::new();
    let mut seen = HashSet::new();

    for cage in &cmd.cages {
        if !matches!(cage.kind, CageKind::Payload) {
            continue;
        }

        for cond in &cage.conditions {
            if let Expr::Named(name) = &cond.left {
                push_plain_column_ref(name, &mut cols, &mut seen);
            }
        }
    }

    cols
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::ast::{Action, AggregateFunc, CTEDef, Join, JoinKind, Qail};
    use crate::parse;

    use super::*;

    #[test]
    fn test_set_payload_column_extraction() {
        let cmd = parse("set users values name = \"Alice\", status = \"active\" where id = $1")
            .expect("set parse");
        let columns = extract_payload_columns(&cmd);
        assert_eq!(columns, vec!["name", "status"]);
    }

    #[test]
    fn test_command_reference_tracks_filter_columns() {
        let cmd = parse("get users fields id where email = $1 order by created_at desc")
            .expect("get parse");
        let reference =
            command_to_reference(Path::new("src/users.ts"), 1, &cmd).expect("reference");

        assert_eq!(reference.table, "users");
        assert_eq!(reference.columns, vec!["id", "email", "created_at"]);
    }

    #[test]
    fn test_command_references_track_native_qail_subqueries() {
        let cmd =
            parse("get users fields id where exists (get orders fields user_id where total > $1)")
                .expect("get parse");
        let refs = command_to_references(Path::new("src/users.ts"), 1, &cmd);

        assert_eq!(refs.len(), 2, "{refs:?}");

        let users = refs
            .iter()
            .find(|reference| reference.table == "users")
            .expect("users reference");
        assert_eq!(users.columns, vec!["id"]);

        let orders = refs
            .iter()
            .find(|reference| reference.table == "orders")
            .expect("orders reference");
        assert_eq!(orders.columns, vec!["user_id", "total"]);
    }

    #[test]
    fn test_command_references_track_join_tables_by_scope() {
        let cmd = parse(
            "get users join posts on users.id = posts.user_id fields users.id, posts.title where posts.status = $1",
        )
        .expect("get parse");
        let refs = command_to_references(Path::new("src/users.ts"), 1, &cmd);

        assert_eq!(refs.len(), 2, "{refs:?}");

        let users = refs
            .iter()
            .find(|reference| reference.table == "users")
            .expect("users reference");
        assert_eq!(users.columns, vec!["id"]);

        let posts = refs
            .iter()
            .find(|reference| reference.table == "posts")
            .expect("posts reference");
        assert_eq!(posts.columns, vec!["title", "status", "user_id"]);
    }

    #[test]
    fn test_command_references_track_merge_target_and_source_by_scope() {
        let cmd = parse(
            "merge users as u using staging_users as s on u.id = s.id \
             when matched and u.name != s.name then update set name = s.name, email = s.email \
             when not matched then insert (id, name, email) values (s.id, s.name, s.email)",
        )
        .expect("merge parse");
        let refs = command_to_references(Path::new("src/users.ts"), 1, &cmd);

        assert_eq!(refs.len(), 2, "{refs:?}");

        let users = refs
            .iter()
            .find(|reference| reference.table == "users")
            .expect("users reference");
        assert_eq!(users.columns, vec!["id", "name", "email"]);

        let staging_users = refs
            .iter()
            .find(|reference| reference.table == "staging_users")
            .expect("staging users reference");
        assert_eq!(staging_users.columns, vec!["id", "name", "email"]);
    }

    #[test]
    fn test_command_references_skip_native_qail_cte_aliases() {
        let cmd = Qail {
            table: "summary".to_string(),
            columns: vec![Expr::Named("id".to_string())],
            ctes: vec![CTEDef {
                name: "summary".to_string(),
                recursive: false,
                columns: vec![],
                base_query: Box::new(Qail {
                    table: "orders".to_string(),
                    columns: vec![
                        Expr::Named("id".to_string()),
                        Expr::Named("total".to_string()),
                    ],
                    ..Default::default()
                }),
                recursive_query: None,
                source_table: None,
            }],
            ..Default::default()
        };

        let refs = command_to_references(Path::new("src/reporting.ts"), 1, &cmd);

        assert_eq!(refs.len(), 1, "{refs:?}");
        assert_eq!(refs[0].table, "orders");
        assert_eq!(refs[0].columns, vec!["id", "total"]);
    }

    #[test]
    fn test_command_references_skip_recursive_cte_self_join_alias() {
        let cmd = Qail {
            table: "tree".to_string(),
            ctes: vec![CTEDef {
                name: "tree".to_string(),
                recursive: true,
                columns: vec![],
                base_query: Box::new(Qail {
                    table: "nodes".to_string(),
                    columns: vec![Expr::Named("id".to_string())],
                    ..Default::default()
                }),
                recursive_query: Some(Box::new(Qail {
                    table: "nodes".to_string(),
                    columns: vec![Expr::Named("id".to_string())],
                    joins: vec![Join {
                        table: "tree".to_string(),
                        kind: JoinKind::Left,
                        on: None,
                        on_true: true,
                    }],
                    ..Default::default()
                })),
                source_table: None,
            }],
            ..Default::default()
        };

        let refs = command_to_references(Path::new("src/tree.ts"), 1, &cmd);

        assert_eq!(refs.len(), 2, "{refs:?}");
        assert!(refs.iter().all(|reference| reference.table == "nodes"));
    }

    #[test]
    fn test_command_reference_skips_aggregate_constant_columns() {
        let cmd = Qail {
            action: Action::Get,
            table: "users".to_string(),
            columns: vec![
                Expr::Aggregate {
                    col: "*".to_string(),
                    func: AggregateFunc::Count,
                    distinct: false,
                    filter: None,
                    alias: Some("total".to_string()),
                },
                Expr::Aggregate {
                    col: "1".to_string(),
                    func: AggregateFunc::Count,
                    distinct: false,
                    filter: None,
                    alias: Some("total_one".to_string()),
                },
            ],
            ..Default::default()
        };

        let reference =
            command_to_reference(Path::new("src/users.ts"), 1, &cmd).expect("reference");

        assert_eq!(reference.table, "users");
        assert!(reference.columns.is_empty(), "{reference:?}");
    }
}
