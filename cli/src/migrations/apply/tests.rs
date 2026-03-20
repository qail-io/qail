//! Apply module tests.

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::super::backfill::{parse_backfill_spec, split_schema_table};
    use super::super::codegen::{parse_qail_to_commands_strict, parse_qail_to_sql};
    use super::super::discovery::{
        detect_phase, discover_migrations, normalize_group_key, parse_drop_targets,
    };
    use super::super::types::{BackfillTransform, MigrateDirection, MigrationPhase};
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_parse_booking_to_sql() {
        let input = r#"
table booking_orders {
  id                    uuid primary_key default gen_random_uuid()
  hold_id               uuid nullable
  connection_id         uuid nullable
  voyage_id             uuid nullable
  operator_id           uuid not_null
  status                text not_null default 'Draft'
  total_fare            bigint not_null
  currency              text not_null default 'IDR'
  nationality           text not_null default 'indo'
  pax_breakdown         jsonb not_null default '{}'
  contact_info          jsonb not_null default '{}'
  pricing_breakdown     jsonb nullable
  passenger_details     jsonb nullable default '[]'
  connection_snapshot   jsonb nullable
  invoice_number        text nullable unique
  booking_number        text nullable
  metadata              jsonb nullable
  user_id               uuid nullable
  agent_id              uuid nullable
  created_at            timestamptz not_null default now()
  updated_at            timestamptz not_null default now()

  enable_rls
  force_rls
}

index idx_booking_orders_operator on booking_orders (operator_id)
index idx_booking_orders_status on booking_orders (status)
index idx_booking_orders_user on booking_orders (user_id)
"#;
        let sql = parse_qail_to_sql(input).expect("parse_qail_to_sql should succeed");
        assert!(
            sql.contains("CREATE TABLE IF NOT EXISTS booking_orders"),
            "SQL should contain CREATE TABLE"
        );
        assert!(
            sql.contains("ALTER TABLE booking_orders ENABLE ROW LEVEL SECURITY"),
            "SQL should enable RLS"
        );
        assert!(
            sql.contains("ALTER TABLE booking_orders FORCE ROW LEVEL SECURITY"),
            "SQL should force RLS"
        );
        assert!(
            sql.contains("CREATE INDEX IF NOT EXISTS idx_booking_orders_operator"),
            "SQL should create indexes"
        );
        assert!(
            sql.contains("CREATE INDEX IF NOT EXISTS idx_booking_orders_status"),
            "SQL should create status index"
        );
        assert!(
            sql.contains("CREATE INDEX IF NOT EXISTS idx_booking_orders_user"),
            "SQL should create user index"
        );
    }

    #[test]
    fn test_parse_paren_based_booking() {
        let input = r#"
table orders (
    id                    uuid primary_key default gen_random_uuid(),
    operator_id           uuid,
    status                varchar not_null default 'Draft',
    total_fare            bigint not_null,
    currency              varchar not_null default 'IDR',
    pax_breakdown         jsonb not_null default '{}',
    contact_info          jsonb not_null default '{}',
    created_at            timestamptz not_null default now(),
    updated_at            timestamptz not_null default now()
) enable_rls

index idx_orders_operator on orders (operator_id)
index idx_orders_status on orders (status)
"#;
        let sql = parse_qail_to_sql(input).expect("paren-based parse should succeed");
        assert!(!sql.contains("( ("), "SQL should not have double parens");
        assert!(
            sql.contains("CREATE TABLE IF NOT EXISTS orders"),
            "SQL should contain CREATE TABLE"
        );
        assert!(
            sql.contains("ALTER TABLE orders ENABLE ROW LEVEL SECURITY"),
            "SQL should enable RLS"
        );
        assert!(
            sql.contains("CREATE INDEX IF NOT EXISTS idx_orders_operator"),
            "SQL should create indexes"
        );
    }

    #[test]
    fn test_parse_qail_to_commands_strict_basic() {
        let input = r#"
table users (
    id uuid primary_key,
    name text not null
) enable_rls

index idx_users_name on users (name)
"#;
        let cmds = parse_qail_to_commands_strict(input).expect("strict compile should succeed");
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::Make)),
            "should include CREATE TABLE"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::Index)),
            "should include CREATE INDEX"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::AlterEnableRls)),
            "should include ENABLE RLS"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::AlterForceRls)),
            "should include FORCE RLS"
        );
    }

    #[test]
    fn test_parse_qail_to_commands_strict_supports_policies() {
        let input = r#"
table users (
    id uuid primary_key,
    tenant_id uuid not null
) enable_rls

policy users_isolation on users
    for all
    using (tenant_id = current_setting('app.current_tenant_id')::uuid)
"#;

        let cmds = parse_qail_to_commands_strict(input).expect("policies should compile");
        let policy_cmd = cmds
            .iter()
            .find(|c| matches!(c.action, qail_core::ast::Action::CreatePolicy))
            .expect("expected CREATE POLICY command");
        let policy = policy_cmd
            .policy_def
            .as_ref()
            .expect("policy_def should be present");
        assert_eq!(policy.name, "users_isolation");
        assert_eq!(policy.table, "users");
        assert_eq!(policy.target, qail_core::migrate::policy::PolicyTarget::All);
    }

    #[test]
    fn test_parse_qail_to_commands_strict_supports_drop_hints() {
        let input = r#"
drop index idx_qail_queue_ref
drop index idx_qail_queue_poll
drop table _qail_queue
"#;

        let cmds = parse_qail_to_commands_strict(input).expect("drop hints should compile");
        assert_eq!(cmds.len(), 3);
        assert!(matches!(cmds[0].action, qail_core::ast::Action::DropIndex));
        assert_eq!(cmds[0].table, "idx_qail_queue_ref");
        assert!(matches!(cmds[1].action, qail_core::ast::Action::DropIndex));
        assert_eq!(cmds[1].table, "idx_qail_queue_poll");
        assert!(matches!(cmds[2].action, qail_core::ast::Action::Drop));
        assert_eq!(cmds[2].table, "_qail_queue");
    }

    #[test]
    fn test_parse_qail_to_commands_strict_supports_rename_hints() {
        let input = "rename users.old_name -> users.new_name";
        let cmds = parse_qail_to_commands_strict(input).expect("rename hints should compile");
        assert_eq!(cmds.len(), 1);
        assert!(matches!(cmds[0].action, qail_core::ast::Action::Mod));
        assert_eq!(cmds[0].table, "users");
        assert!(
            cmds[0].columns.iter().any(
                |c| matches!(c, qail_core::ast::Expr::Named(n) if n == "old_name -> new_name")
            ),
            "rename command should encode 'old_name -> new_name'"
        );
    }

    #[test]
    fn test_parse_qail_to_commands_strict_rejects_cross_table_rename_hints() {
        let input = "rename users.name -> profiles.name";
        let err = parse_qail_to_commands_strict(input).expect_err("cross-table rename must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("same-table"),
            "error should mention same-table constraint, got: {}",
            msg
        );
    }

    #[test]
    fn test_parse_qail_to_commands_strict_supports_schema_objects() {
        let input = r#"
extension "uuid-ossp"
enum status { active, inactive }
sequence order_seq { start 1000 increment 1 }

table users {
  id uuid primary_key
  tenant_id uuid not_null
  status status not_null
}

view active_users $$ SELECT id FROM users WHERE status = 'active' $$
function set_updated_at() returns trigger language plpgsql $$ BEGIN RETURN NEW; END; $$
trigger trg_users_updated on users before update execute set_updated_at
policy users_isolation on users for select
  using $$ tenant_id = current_setting('app.current_tenant_id')::uuid $$
grant select on users to app_role
revoke insert on users from app_role
comment on users "User accounts"
"#;

        let cmds =
            parse_qail_to_commands_strict(input).expect("schema objects should compile strictly");

        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::CreateExtension)),
            "should include CREATE EXTENSION"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::CreateEnum)),
            "should include CREATE ENUM"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::CreateSequence)),
            "should include CREATE SEQUENCE"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::CreateView)),
            "should include CREATE VIEW"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::CreateFunction)),
            "should include CREATE FUNCTION"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::CreateTrigger)),
            "should include CREATE TRIGGER"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::CreatePolicy)),
            "should include CREATE POLICY"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::CommentOn)),
            "should include COMMENT ON"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::Grant)),
            "should include GRANT"
        );
        assert!(
            cmds.iter()
                .any(|c| matches!(c.action, qail_core::ast::Action::Revoke)),
            "should include REVOKE"
        );
    }

    #[test]
    fn test_parse_qail_to_commands_strict_supports_extended_drop_hints() {
        let input = r#"
drop view active_users
drop materialized view booking_stats
drop extension pgcrypto
drop sequence order_seq
drop enum status
drop function set_updated_at
drop trigger users.trg_users_updated
drop policy users_isolation on users
"#;

        let cmds =
            parse_qail_to_commands_strict(input).expect("extended drop hints should compile");
        assert_eq!(cmds.len(), 8);
        assert!(matches!(cmds[0].action, qail_core::ast::Action::DropView));
        assert!(matches!(
            cmds[1].action,
            qail_core::ast::Action::DropMaterializedView
        ));
        assert!(matches!(
            cmds[2].action,
            qail_core::ast::Action::DropExtension
        ));
        assert!(matches!(
            cmds[3].action,
            qail_core::ast::Action::DropSequence
        ));
        assert!(matches!(cmds[4].action, qail_core::ast::Action::DropEnum));
        assert!(matches!(
            cmds[5].action,
            qail_core::ast::Action::DropFunction
        ));
        assert!(matches!(
            cmds[6].action,
            qail_core::ast::Action::DropTrigger
        ));
        assert_eq!(cmds[6].table, "users.trg_users_updated");
        assert!(matches!(cmds[7].action, qail_core::ast::Action::DropPolicy));
        assert_eq!(cmds[7].table, "users");
        assert_eq!(cmds[7].payload.as_deref(), Some("users_isolation"));
    }

    #[test]
    fn test_parse_qail_to_commands_strict_supports_function_args() {
        let input = r#"
function sum_one(v int) returns int language plpgsql $$ BEGIN RETURN v + 1; END; $$
"#;
        let cmds =
            parse_qail_to_commands_strict(input).expect("function args should compile strictly");
        let func = cmds
            .iter()
            .find(|c| matches!(c.action, qail_core::ast::Action::CreateFunction))
            .expect("expected CREATE FUNCTION command");
        let args = func
            .function_def
            .as_ref()
            .expect("function_def should be present")
            .args
            .clone();
        assert_eq!(args, vec!["v int".to_string()]);
    }

    #[test]
    fn test_detect_phase_from_name() {
        assert_eq!(
            detect_phase("20260101010101_add_users.expand.up.qail"),
            MigrationPhase::Expand
        );
        assert_eq!(
            detect_phase("20260101010101_users_backfill.up.qail"),
            MigrationPhase::Backfill
        );
        assert_eq!(
            detect_phase("20260101010101_contract_cleanup.up.qail"),
            MigrationPhase::Contract
        );
    }

    #[test]
    fn test_parse_drop_targets_from_sql() {
        let sql = r#"
            ALTER TABLE users DROP COLUMN old_email;
            DROP TABLE IF EXISTS audit_logs;
        "#;
        let (tables, columns) = parse_drop_targets(sql);
        assert_eq!(tables, vec!["audit_logs".to_string()]);
        assert_eq!(
            columns,
            vec![("users".to_string(), "old_email".to_string())]
        );
    }

    #[test]
    fn test_parse_backfill_spec_directives() {
        let content = r#"
-- @backfill.table: users
-- @backfill.pk: id
-- @backfill.set: name_ci = lower(name)
-- @backfill.where: name_ci IS NULL
-- @backfill.chunk_size: 2048
"#;
        let spec = parse_backfill_spec(content, 5000)
            .expect("spec parse should work")
            .expect("spec should exist");
        assert_eq!(spec.table, "users");
        assert_eq!(spec.pk_column, "id");
        assert_eq!(spec.set_column, "name_ci");
        assert_eq!(spec.source_column, "name");
        assert!(matches!(spec.transform, BackfillTransform::Lower));
        assert_eq!(spec.chunk_size, 2048);
        assert_eq!(spec.where_null_column.as_deref(), Some("name_ci"));
    }

    #[test]
    fn test_parse_backfill_spec_none_when_absent() {
        let content = "table users (id serial primary_key)";
        let spec = parse_backfill_spec(content, 5000).expect("parse should succeed");
        assert!(spec.is_none());
    }

    #[test]
    fn test_backfill_directive_rejects_sql_body() {
        let content = r#"
-- @backfill.table: users
-- @backfill.pk: id
-- @backfill.set: name_ci = lower(name)

ALTER TABLE users ADD COLUMN name_ci text;
"#;
        let result = parse_backfill_spec(content, 5000);
        assert!(
            result.is_err(),
            "Should reject files mixing directives and SQL body"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("non-directive body"),
            "Error should mention non-directive body, got: {}",
            msg
        );
    }

    #[test]
    fn test_backfill_directive_allows_comments_only() {
        let content = r#"
-- Backfill name_ci for existing users
-- @backfill.table: users
-- @backfill.pk: id
-- @backfill.set: name_ci = lower(name)
-- @backfill.chunk_size: 1000
"#;
        let spec = parse_backfill_spec(content, 5000)
            .expect("should parse ok")
            .expect("should have a spec");
        assert_eq!(spec.table, "users");
        assert_eq!(spec.chunk_size, 1000);
    }

    #[test]
    fn test_normalize_group_key_underscore_variants() {
        assert_eq!(normalize_group_key("001_users_expand"), "001_users");
        assert_eq!(normalize_group_key("001_users_backfill"), "001_users");
        assert_eq!(normalize_group_key("001_users_contract"), "001_users");
    }

    #[test]
    fn test_normalize_group_key_hyphen_variants() {
        assert_eq!(normalize_group_key("001_users-expand"), "001_users");
        assert_eq!(normalize_group_key("001_users-backfill"), "001_users");
        assert_eq!(normalize_group_key("001_users-contract"), "001_users");
    }

    #[test]
    fn test_normalize_group_key_dot_variants() {
        assert_eq!(normalize_group_key("001_users.expand"), "001_users");
        assert_eq!(normalize_group_key("001_users.backfill"), "001_users");
        assert_eq!(normalize_group_key("001_users.contract"), "001_users");
    }

    #[test]
    fn test_normalize_group_key_no_phase_suffix() {
        assert_eq!(normalize_group_key("001_add_users"), "001_add_users");
        assert_eq!(normalize_group_key("002_orders"), "002_orders");
    }

    #[test]
    fn test_split_schema_table_qualified() {
        let (schema, table) = split_schema_table("analytics.events");
        assert_eq!(schema, "analytics");
        assert_eq!(table, "events");
    }

    #[test]
    fn test_split_schema_table_unqualified() {
        let (schema, table) = split_schema_table("users");
        assert_eq!(schema, "public");
        assert_eq!(table, "users");
    }

    #[test]
    fn test_backfill_directive_allows_hash_comments() {
        let content = r#"
# This is a hash-style comment
-- @backfill.table: users
-- @backfill.pk: id
-- @backfill.set: email_lower = lower(email)
# Another hash comment
"#;
        let spec = parse_backfill_spec(content, 5000)
            .expect("should parse ok with # comments")
            .expect("should have a spec");
        assert_eq!(spec.table, "users");
        assert_eq!(spec.set_column, "email_lower");
        assert_eq!(spec.source_column, "email");
        assert!(matches!(spec.transform, BackfillTransform::Lower));
    }

    #[test]
    fn test_discover_migrations_down_runs_newest_first() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let root = std::env::temp_dir().join(format!(
            "qail_apply_discovery_down_{}_{}",
            std::process::id(),
            nanos
        ));
        fs::create_dir_all(&root).expect("create temp migration dir");

        fs::write(root.join("001_init.down.qail"), "drop table init_table\n").expect("write 001");
        fs::write(root.join("002_users.down.qail"), "drop table users\n").expect("write 002");
        fs::write(root.join("003_orders.down.qail"), "drop table orders\n").expect("write 003");

        let discovered = discover_migrations(&root, MigrateDirection::Down).expect("discover down");
        let names: Vec<String> = discovered.iter().map(|m| m.display_name.clone()).collect();
        assert_eq!(
            names,
            vec![
                "003_orders.down.qail".to_string(),
                "002_users.down.qail".to_string(),
                "001_init.down.qail".to_string()
            ]
        );

        let _ = fs::remove_dir_all(&root);
    }
}
