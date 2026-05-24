//! Policy Transpiler — converts AST-native RLS policies to SQL.
//!
//! Generates `CREATE POLICY`, `DROP POLICY`, `ALTER TABLE ... ENABLE ROW LEVEL SECURITY`,
//! and `ALTER TABLE ... FORCE ROW LEVEL SECURITY` from typed AST structures.

use crate::migrate::alter::{AlterOp, AlterTable};
use crate::migrate::policy::RlsPolicy;
use crate::migrate::schema::CheckExpr;
use crate::transpiler::traits::{escape_identifier, escape_sql_string_literal};

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

fn sql_expr_fragment_to_sql(expr: &str, fallback: &str) -> String {
    let expr = expr.trim();
    if expr.is_empty() || contains_unquoted_statement_delimiter(expr) {
        fallback.to_string()
    } else {
        expr.replace('\0', "")
    }
}

fn policy_expr_to_sql(expr: &impl std::fmt::Display) -> String {
    sql_expr_fragment_to_sql(&expr.to_string(), "FALSE")
}

/// Transpile an `RlsPolicy` to a `CREATE POLICY` SQL statement.
///
/// Expression nodes are transpiled via their `Display` impl —
/// QAIL speaks AST, the transpiler speaks SQL.
///
/// # Example
/// ```
/// use qail_core::migrate::policy::{RlsPolicy, tenant_check};
/// use qail_core::transpiler::policy::create_policy_sql;
///
/// let policy = RlsPolicy::create("orders_isolation", "orders")
///     .for_all()
///     .using(tenant_check("operator_id", "app.current_operator_id", "uuid"))
///     .with_check(tenant_check("operator_id", "app.current_operator_id", "uuid"));
///
/// let sql = create_policy_sql(&policy);
/// assert!(sql.contains("CREATE POLICY"));
/// assert!(sql.contains("operator_id"));
/// ```
pub fn create_policy_sql(policy: &RlsPolicy) -> String {
    let mut sql = format!(
        "CREATE POLICY {} ON {}",
        escape_identifier(&policy.name),
        escape_identifier(&policy.table),
    );

    // AS PERMISSIVE / RESTRICTIVE (only emit if restrictive, permissive is default)
    if policy.permissiveness == crate::migrate::policy::PolicyPermissiveness::Restrictive {
        sql.push_str(" AS RESTRICTIVE");
    }

    // FOR ALL / SELECT / INSERT / UPDATE / DELETE
    sql.push_str(&format!(" FOR {}", policy.target));

    // TO role
    if let Some(role) = &policy.role {
        sql.push_str(&format!(" TO {}", escape_identifier(role)));
    }

    // USING (expr)
    if let Some(expr) = &policy.using {
        sql.push_str(&format!(" USING ({})", policy_expr_to_sql(expr)));
    }

    // WITH CHECK (expr)
    if let Some(expr) = &policy.with_check {
        sql.push_str(&format!(" WITH CHECK ({})", policy_expr_to_sql(expr)));
    }

    sql
}

/// Transpile an `RlsPolicy` to a `DROP POLICY` SQL statement.
pub fn drop_policy_sql(policy_name: &str, table: &str) -> String {
    format!(
        "DROP POLICY IF EXISTS {} ON {}",
        escape_identifier(policy_name),
        escape_identifier(table),
    )
}

/// Convert a `CheckExpr` AST node to SQL.
fn check_expr_to_sql(expr: &CheckExpr) -> String {
    match expr {
        CheckExpr::GreaterThan { column, value } => {
            format!("{} > {}", escape_identifier(column), value)
        }
        CheckExpr::GreaterOrEqual { column, value } => {
            format!("{} >= {}", escape_identifier(column), value)
        }
        CheckExpr::LessThan { column, value } => {
            format!("{} < {}", escape_identifier(column), value)
        }
        CheckExpr::LessOrEqual { column, value } => {
            format!("{} <= {}", escape_identifier(column), value)
        }
        CheckExpr::Between { column, low, high } => {
            format!("{} BETWEEN {} AND {}", escape_identifier(column), low, high)
        }
        CheckExpr::In { column, values } => {
            let vals: Vec<String> = values
                .iter()
                .map(|v| format!("'{}'", escape_sql_string_literal(v)))
                .collect();
            format!("{} IN ({})", escape_identifier(column), vals.join(", "))
        }
        CheckExpr::Regex { column, pattern } => {
            format!(
                "{} ~ '{}'",
                escape_identifier(column),
                escape_sql_string_literal(pattern)
            )
        }
        CheckExpr::MaxLength { column, max } => {
            format!("LENGTH({}) <= {}", escape_identifier(column), max)
        }
        CheckExpr::MinLength { column, min } => {
            format!("LENGTH({}) >= {}", escape_identifier(column), min)
        }
        CheckExpr::NotNull { column } => format!("{} IS NOT NULL", escape_identifier(column)),
        CheckExpr::And(left, right) => format!(
            "({} AND {})",
            check_expr_to_sql(left),
            check_expr_to_sql(right)
        ),
        CheckExpr::Or(left, right) => format!(
            "({} OR {})",
            check_expr_to_sql(left),
            check_expr_to_sql(right)
        ),
        CheckExpr::Not(inner) => format!("NOT ({})", check_expr_to_sql(inner)),
        CheckExpr::Sql(sql) => sql_expr_fragment_to_sql(sql, "FALSE"),
    }
}

fn quoted_column_list(cols: &[String]) -> String {
    cols.iter()
        .map(|col| escape_identifier(col))
        .collect::<Vec<_>>()
        .join(", ")
}

fn exclude_method_to_sql(method: &str) -> &'static str {
    match method.trim().to_ascii_lowercase().as_str() {
        "btree" => "btree",
        "hash" => "hash",
        "gin" => "gin",
        "gist" => "gist",
        "brin" => "brin",
        "spgist" | "sp-gist" => "spgist",
        _ => "btree",
    }
}

fn exclude_element_to_sql(element: &str) -> String {
    let element = element.trim();
    if element.is_empty() || contains_unquoted_statement_delimiter(element) {
        format!("{} WITH =", escape_identifier(element))
    } else {
        element.to_string()
    }
}

/// Transpile an `AlterTable` to SQL statements.
///
/// Handles all `AlterOp` variants including:
/// - `SetRowLevelSecurity` → `ENABLE/DISABLE ROW LEVEL SECURITY`
/// - `ForceRowLevelSecurity` → `FORCE/NO FORCE ROW LEVEL SECURITY`
/// - Standard ops: ADD COLUMN, DROP COLUMN, RENAME, etc.
pub fn alter_table_sql(alter: &AlterTable) -> Vec<String> {
    let table = escape_identifier(&alter.table);
    let mut stmts = Vec::new();

    for op in &alter.ops {
        let sql = match op {
            AlterOp::SetRowLevelSecurity(true) => {
                format!("ALTER TABLE {} ENABLE ROW LEVEL SECURITY", table)
            }
            AlterOp::SetRowLevelSecurity(false) => {
                format!("ALTER TABLE {} DISABLE ROW LEVEL SECURITY", table)
            }
            AlterOp::ForceRowLevelSecurity(true) => {
                format!("ALTER TABLE {} FORCE ROW LEVEL SECURITY", table)
            }
            AlterOp::ForceRowLevelSecurity(false) => {
                format!("ALTER TABLE {} NO FORCE ROW LEVEL SECURITY", table)
            }
            AlterOp::AddColumn(col) => {
                format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    table,
                    escape_identifier(&col.name),
                    col.data_type.to_pg_type()
                )
            }
            AlterOp::DropColumn { name, cascade } => {
                let cascade_str = if *cascade { " CASCADE" } else { "" };
                format!(
                    "ALTER TABLE {} DROP COLUMN {}{}",
                    table,
                    escape_identifier(name),
                    cascade_str
                )
            }
            AlterOp::RenameColumn { from, to } => {
                format!(
                    "ALTER TABLE {} RENAME COLUMN {} TO {}",
                    table,
                    escape_identifier(from),
                    escape_identifier(to)
                )
            }
            AlterOp::RenameTable(new_name) => {
                format!(
                    "ALTER TABLE {} RENAME TO {}",
                    table,
                    escape_identifier(new_name)
                )
            }
            AlterOp::SetSchema(schema) => {
                format!(
                    "ALTER TABLE {} SET SCHEMA {}",
                    table,
                    escape_identifier(schema)
                )
            }
            AlterOp::AlterType {
                column,
                new_type,
                using,
            } => {
                let mut s = format!(
                    "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                    table,
                    escape_identifier(column),
                    new_type
                );
                if let Some(using_expr) = using {
                    s.push_str(&format!(
                        " USING {}",
                        sql_expr_fragment_to_sql(using_expr, "NULL")
                    ));
                }
                s
            }
            AlterOp::SetNotNull(col) => {
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET NOT NULL",
                    table,
                    escape_identifier(col)
                )
            }
            AlterOp::DropNotNull(col) => {
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                    table,
                    escape_identifier(col)
                )
            }
            AlterOp::SetDefault { column, expr } => {
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} SET DEFAULT {}",
                    table,
                    escape_identifier(column),
                    sql_expr_fragment_to_sql(expr, "NULL")
                )
            }
            AlterOp::DropDefault(col) => {
                format!(
                    "ALTER TABLE {} ALTER COLUMN {} DROP DEFAULT",
                    table,
                    escape_identifier(col)
                )
            }
            AlterOp::AddConstraint { name, constraint } => {
                let constraint_sql = match constraint {
                    crate::migrate::alter::TableConstraint::PrimaryKey(cols) => {
                        format!("PRIMARY KEY ({})", quoted_column_list(cols))
                    }
                    crate::migrate::alter::TableConstraint::Unique(cols) => {
                        format!("UNIQUE ({})", quoted_column_list(cols))
                    }
                    crate::migrate::alter::TableConstraint::Check(expr) => {
                        format!("CHECK ({})", check_expr_to_sql(expr))
                    }
                    crate::migrate::alter::TableConstraint::ForeignKey {
                        columns,
                        ref_table,
                        ref_columns,
                    } => {
                        format!(
                            "FOREIGN KEY ({}) REFERENCES {}({})",
                            quoted_column_list(columns),
                            escape_identifier(ref_table),
                            quoted_column_list(ref_columns)
                        )
                    }
                    crate::migrate::alter::TableConstraint::Exclude { method, elements } => {
                        format!(
                            "EXCLUDE USING {} ({})",
                            exclude_method_to_sql(method),
                            elements
                                .iter()
                                .map(|element| exclude_element_to_sql(element))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    }
                };
                format!(
                    "ALTER TABLE {} ADD CONSTRAINT {} {}",
                    table,
                    escape_identifier(name),
                    constraint_sql
                )
            }
            AlterOp::DropConstraint { name, cascade } => {
                let cascade_str = if *cascade { " CASCADE" } else { "" };
                format!(
                    "ALTER TABLE {} DROP CONSTRAINT {}{}",
                    table,
                    escape_identifier(name),
                    cascade_str
                )
            }
        };
        stmts.push(sql);
    }

    stmts
}

/// Generate a complete RLS setup for a table: enable RLS + force + create policy.
///
/// This is the common pattern for multi-tenant tables:
/// 1. `ALTER TABLE t ENABLE ROW LEVEL SECURITY`
/// 2. `ALTER TABLE t FORCE ROW LEVEL SECURITY`
/// 3. `CREATE POLICY ... USING (...) WITH CHECK (...)`
pub fn rls_setup_sql(table: &str, policy: &RlsPolicy) -> Vec<String> {
    let alter = AlterTable::new(table).enable_rls().force_rls();
    let mut stmts = alter_table_sql(&alter);
    stmts.push(create_policy_sql(policy));
    stmts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::policy::{PolicyTarget, RlsPolicy, or, session_bool_check, tenant_check};

    #[test]
    fn test_create_policy_basic() {
        let policy = RlsPolicy::create("orders_isolation", "orders")
            .for_all()
            .using(tenant_check(
                "operator_id",
                "app.current_operator_id",
                "uuid",
            ))
            .with_check(tenant_check(
                "operator_id",
                "app.current_operator_id",
                "uuid",
            ));

        let sql = create_policy_sql(&policy);
        assert!(sql.contains("CREATE POLICY"));
        assert!(sql.contains("orders_isolation"));
        assert!(sql.contains("orders"));
        assert!(sql.contains("FOR ALL"));
        assert!(sql.contains("USING"));
        assert!(sql.contains("WITH CHECK"));
        assert!(sql.contains("operator_id"));
        // Expr::FunctionCall::Display uppercases the function name
        assert!(sql.contains("CURRENT_SETTING"));
    }

    #[test]
    fn test_create_policy_restrictive() {
        let policy = RlsPolicy::create("admin_only", "secrets")
            .for_select()
            .restrictive()
            .to_role("app_user")
            .using(session_bool_check("app.is_super_admin"));

        let sql = create_policy_sql(&policy);
        assert!(sql.contains("AS RESTRICTIVE"));
        assert!(sql.contains("FOR SELECT"));
        assert!(sql.contains("TO"));
        assert!(sql.contains("app_user"));
    }

    #[test]
    fn test_create_policy_expression_fragments_are_sanitized() {
        let policy = RlsPolicy::create("unsafe_policy", "users")
            .for_all()
            .using(crate::ast::Expr::Named(
                "tenant_id = current_setting('app.tenant')::uuid; DROP TABLE users; --".to_string(),
            ))
            .with_check(crate::ast::Expr::Named("note = 'semi;inside'".to_string()));

        let sql = create_policy_sql(&policy);
        assert!(sql.contains("USING (FALSE)"));
        assert!(sql.contains("WITH CHECK (note = 'semi;inside')"));
    }

    #[test]
    fn test_create_policy_with_or() {
        let expr = or(
            tenant_check("operator_id", "app.current_operator_id", "uuid"),
            session_bool_check("app.is_super_admin"),
        );

        let policy = RlsPolicy::create("tenant_or_admin", "orders")
            .for_all()
            .using(expr);

        let sql = create_policy_sql(&policy);
        assert!(sql.contains("OR"));
        assert!(sql.contains("operator_id"));
        assert!(sql.contains("is_super_admin"));
    }

    #[test]
    fn test_drop_policy() {
        let sql = drop_policy_sql("orders_isolation", "orders");
        assert!(sql.contains("DROP POLICY IF EXISTS"));
        assert!(sql.contains("orders_isolation"));
        assert!(sql.contains("orders"));
    }

    #[test]
    fn test_alter_table_enable_rls() {
        let alter = AlterTable::new("orders").enable_rls().force_rls();
        let stmts = alter_table_sql(&alter);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("ENABLE ROW LEVEL SECURITY"));
        assert!(stmts[0].contains("orders"));
        assert!(stmts[1].contains("FORCE ROW LEVEL SECURITY"));
    }

    #[test]
    fn test_alter_table_disable_rls() {
        let alter = AlterTable::new("orders").disable_rls().no_force_rls();
        let stmts = alter_table_sql(&alter);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("DISABLE ROW LEVEL SECURITY"));
        assert!(stmts[1].contains("NO FORCE ROW LEVEL SECURITY"));
    }

    #[test]
    fn test_alter_table_expression_fragments_are_sanitized() {
        use crate::migrate::alter::TableConstraint;
        use crate::migrate::schema::CheckExpr;
        use crate::migrate::types::ColumnType;

        let alter = AlterTable::new("events")
            .set_default("score", "0; DROP TABLE events; --")
            .set_type_using(
                "score",
                ColumnType::Text,
                "score::text; DROP TABLE events; --",
            )
            .add_constraint(
                "raw_check",
                TableConstraint::Check(CheckExpr::Sql(
                    "score > 0; DROP TABLE events; --".to_string(),
                )),
            )
            .add_constraint(
                "kind_check",
                TableConstraint::Check(CheckExpr::In {
                    column: "kind".to_string(),
                    values: vec!["O'Brien".to_string()],
                }),
            )
            .add_constraint(
                "pattern_check",
                TableConstraint::Check(CheckExpr::Regex {
                    column: "kind".to_string(),
                    pattern: "^[a-z']+$".to_string(),
                }),
            );

        let stmts = alter_table_sql(&alter);
        assert_eq!(
            stmts[0],
            "ALTER TABLE events ALTER COLUMN score SET DEFAULT NULL"
        );
        assert_eq!(
            stmts[1],
            "ALTER TABLE events ALTER COLUMN score TYPE TEXT USING NULL"
        );
        assert_eq!(
            stmts[2],
            "ALTER TABLE events ADD CONSTRAINT raw_check CHECK (FALSE)"
        );
        assert_eq!(
            stmts[3],
            "ALTER TABLE events ADD CONSTRAINT kind_check CHECK (kind IN ('O''Brien'))"
        );
        assert_eq!(
            stmts[4],
            "ALTER TABLE events ADD CONSTRAINT pattern_check CHECK (kind ~ '^[a-z'']+$')"
        );
    }

    #[test]
    fn test_alter_table_constraint_fragments_are_sanitized() {
        use crate::migrate::alter::TableConstraint;

        let alter = AlterTable::new("orders")
            .add_constraint(
                "pk_bad",
                TableConstraint::PrimaryKey(vec!["id); DROP TABLE orders; --".to_string()]),
            )
            .add_constraint(
                "uq_bad",
                TableConstraint::Unique(vec!["email); DROP TABLE orders; --".to_string()]),
            )
            .add_constraint(
                "fk_bad",
                TableConstraint::ForeignKey {
                    columns: vec!["user_id); DROP TABLE orders; --".to_string()],
                    ref_table: "users; DROP TABLE orders; --".to_string(),
                    ref_columns: vec!["id); DROP TABLE orders; --".to_string()],
                },
            )
            .add_constraint(
                "exclude_bad",
                TableConstraint::Exclude {
                    method: "gist; DROP TABLE orders; --".to_string(),
                    elements: vec!["room_id WITH =; DROP TABLE orders; --".to_string()],
                },
            );

        let stmts = alter_table_sql(&alter);
        assert_eq!(
            stmts[0],
            "ALTER TABLE orders ADD CONSTRAINT pk_bad PRIMARY KEY (\"id); DROP TABLE orders; --\")"
        );
        assert_eq!(
            stmts[1],
            "ALTER TABLE orders ADD CONSTRAINT uq_bad UNIQUE (\"email); DROP TABLE orders; --\")"
        );
        assert_eq!(
            stmts[2],
            "ALTER TABLE orders ADD CONSTRAINT fk_bad FOREIGN KEY (\"user_id); DROP TABLE orders; --\") REFERENCES \"users; DROP TABLE orders; --\"(\"id); DROP TABLE orders; --\")"
        );
        assert_eq!(
            stmts[3],
            "ALTER TABLE orders ADD CONSTRAINT exclude_bad EXCLUDE USING btree (\"room_id WITH =; DROP TABLE orders; --\" WITH =)"
        );
    }

    #[test]
    fn test_rls_setup_sql() {
        let policy = RlsPolicy::create("orders_tenant", "orders")
            .for_all()
            .using(tenant_check(
                "operator_id",
                "app.current_operator_id",
                "uuid",
            ))
            .with_check(tenant_check(
                "operator_id",
                "app.current_operator_id",
                "uuid",
            ));

        let stmts = rls_setup_sql("orders", &policy);
        assert_eq!(stmts.len(), 3);
        assert!(stmts[0].contains("ENABLE ROW LEVEL SECURITY"));
        assert!(stmts[1].contains("FORCE ROW LEVEL SECURITY"));
        assert!(stmts[2].contains("CREATE POLICY"));
    }

    #[test]
    fn test_policy_target_variants() {
        for (target, expected) in [
            (PolicyTarget::All, "FOR ALL"),
            (PolicyTarget::Select, "FOR SELECT"),
            (PolicyTarget::Insert, "FOR INSERT"),
            (PolicyTarget::Update, "FOR UPDATE"),
            (PolicyTarget::Delete, "FOR DELETE"),
        ] {
            let policy = RlsPolicy::create("test", "t").using(tenant_check("id", "app.id", "uuid"));
            // Apply target
            let policy = match target {
                PolicyTarget::All => policy.for_all(),
                PolicyTarget::Select => policy.for_select(),
                PolicyTarget::Insert => policy.for_insert(),
                PolicyTarget::Update => policy.for_update(),
                PolicyTarget::Delete => policy.for_delete(),
            };
            let sql = create_policy_sql(&policy);
            assert!(
                sql.contains(expected),
                "Expected '{}' in '{}'",
                expected,
                sql
            );
        }
    }
}
