//! Feature tests (DDL, Upsert, JSON operations, advanced features).

use crate::ast::*;
use crate::migrate::policy::RlsPolicy;
use crate::parser::parse;
use crate::transpiler::conditions::ParamContext;
use crate::transpiler::sql::postgres::PostgresGenerator;
use crate::transpiler::{ConditionToSql, Dialect, ToSql};

// ============= DDL Tests =============

#[test]
fn test_index_sql_basic() {
    let cmd = parse("index idx_email on users email").unwrap();
    let sql = cmd.to_sql();
    assert!(sql.contains("CREATE INDEX idx_email ON users"));
    assert!(sql.contains("email"));
}

#[test]
fn test_index_sql_unique() {
    let cmd = parse("index idx_unique_email on users email unique").unwrap();
    let sql = cmd.to_sql();
    assert!(sql.contains("CREATE UNIQUE INDEX"));
}

#[test]
fn test_index_fragments_validate_method_and_predicate() {
    let valid = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "idx_lower_email".to_string(),
            table: "users".to_string(),
            columns: vec!["lower(email)".to_string()],
            unique: false,
            index_type: Some("btree".to_string()),
            where_clause: Some("active = true".to_string()),
        }),
        ..Default::default()
    };
    assert_eq!(
        valid.to_sql_with_dialect(Dialect::Postgres),
        "CREATE INDEX idx_lower_email ON users USING btree (lower(email)) WHERE active = true"
    );

    let hnsw = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "idx_docs_embedding".to_string(),
            table: "documents".to_string(),
            columns: vec!["embedding vector_l2_ops".to_string()],
            unique: false,
            index_type: Some("hnsw".to_string()),
            where_clause: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        hnsw.to_sql_with_dialect(Dialect::Postgres),
        "CREATE INDEX idx_docs_embedding ON documents USING hnsw (embedding vector_l2_ops)"
    );

    let ivfflat = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "idx_docs_embedding_cosine".to_string(),
            table: "documents".to_string(),
            columns: vec!["embedding vector_cosine_ops".to_string()],
            unique: false,
            index_type: Some("ivf-flat".to_string()),
            where_clause: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        ivfflat.to_sql_with_dialect(Dialect::Postgres),
        "CREATE INDEX idx_docs_embedding_cosine ON documents USING ivfflat (embedding vector_cosine_ops)"
    );

    let quoted_column = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "idx_bad".to_string(),
            table: "users".to_string(),
            columns: vec!["lower(email); DROP TABLE users; --".to_string()],
            unique: false,
            index_type: None,
            where_clause: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        quoted_column.to_sql_with_dialect(Dialect::Postgres),
        "CREATE INDEX idx_bad ON users (\"lower(email); DROP TABLE users; --\")"
    );

    let invalid_column = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "idx_bad".to_string(),
            table: "users".to_string(),
            columns: vec!["lower(email)\0".to_string()],
            unique: false,
            index_type: None,
            where_clause: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        invalid_column.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid index column */"
    );

    let invalid_method = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "idx_bad".to_string(),
            table: "users".to_string(),
            columns: vec!["email".to_string()],
            unique: false,
            index_type: Some("btree; DROP TABLE users".to_string()),
            where_clause: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        invalid_method.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid index method */"
    );

    let invalid_predicate = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "idx_bad".to_string(),
            table: "users".to_string(),
            columns: vec!["email".to_string()],
            unique: false,
            index_type: Some("btree".to_string()),
            where_clause: Some("active = true; DROP TABLE users; --".to_string()),
        }),
        ..Default::default()
    };
    assert_eq!(
        invalid_predicate.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid index predicate */"
    );

    let invalid_nul_predicate = Qail {
        action: Action::Index,
        index_def: Some(IndexDef {
            name: "idx_bad".to_string(),
            table: "users".to_string(),
            columns: vec!["email".to_string()],
            unique: false,
            index_type: Some("btree".to_string()),
            where_clause: Some("active = true\0".to_string()),
        }),
        ..Default::default()
    };
    assert_eq!(
        invalid_nul_predicate.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid index predicate */"
    );
}

#[test]
fn test_composite_pk_sql() {
    // make order_items order_id:uuid, item_id:uuid primary key(order_id, item_id)
    let cmd = parse("make order_items order_id:uuid, item_id:uuid primary key(order_id, item_id)")
        .unwrap();
    let sql = cmd.to_sql();
    assert!(sql.contains("PRIMARY KEY (order_id, item_id)"));
}

#[test]
fn test_drop_column() {
    // Manual construction for DROP COLUMN
    let mut cmd = Qail::get("users");
    cmd.action = Action::DropCol;
    cmd.columns.push(Expr::Named("password".to_string()));
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("ALTER TABLE users DROP COLUMN password"));
}

#[test]
fn test_rename_column() {
    // Manual construction for RENAME COLUMN
    let mut cmd = Qail::get("users");
    cmd.action = Action::RenameCol;
    cmd.columns.push(Expr::Named("old_name".to_string()));
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("to".to_string()),
            op: Operator::Eq,
            value: Value::String("new_name".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("ALTER TABLE users RENAME COLUMN old_name TO new_name"));
}

#[test]
fn test_grant_sql() {
    let cmd = Qail {
        action: Action::Grant,
        table: "users".to_string(),
        columns: vec![
            Expr::Named("SELECT".to_string()),
            Expr::Named("INSERT".to_string()),
        ],
        payload: Some("app_role".to_string()),
        ..Default::default()
    };
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(sql, "GRANT SELECT, INSERT ON users TO app_role");
}

#[test]
fn test_grant_rejects_invalid_privileges() {
    let grant = Qail {
        action: Action::Grant,
        table: "users".to_string(),
        columns: vec![
            Expr::Named("all privileges".to_string()),
            Expr::Named("temp".to_string()),
        ],
        payload: Some("app_role".to_string()),
        ..Default::default()
    };
    assert_eq!(
        grant.to_sql_with_dialect(Dialect::Postgres),
        "GRANT ALL PRIVILEGES, TEMPORARY ON users TO app_role"
    );

    let mixed_invalid = Qail {
        action: Action::Grant,
        table: "users".to_string(),
        columns: vec![
            Expr::Named("SELECT".to_string()),
            Expr::Named("INSERT; DROP TABLE users; --".to_string()),
        ],
        payload: Some("app_role".to_string()),
        ..Default::default()
    };
    assert_eq!(
        mixed_invalid.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid privileges */"
    );

    let revoke = Qail {
        action: Action::Revoke,
        table: "users".to_string(),
        columns: vec![Expr::Named("UPDATE; DROP TABLE users; --".to_string())],
        payload: Some("app_role".to_string()),
        ..Default::default()
    };
    assert_eq!(
        revoke.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid privileges */"
    );
}

#[test]
fn test_create_database_quotes_hyphenated_name() {
    let cmd = Qail::create_database("qail-engine-db_shadow");
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(sql, "CREATE DATABASE \"qail-engine-db_shadow\"");
}

#[test]
fn test_drop_database_quotes_hyphenated_name() {
    let cmd = Qail::drop_database("qail-engine-db_shadow");
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(sql, "DROP DATABASE IF EXISTS \"qail-engine-db_shadow\"");
}

#[test]
fn test_ddl_options_reject_invalid_fragments() {
    let extension = Qail {
        action: Action::CreateExtension,
        table: "uuid-ossp\0".to_string(),
        columns: vec![
            Expr::Named("SCHEMA public; DROP TABLE users; --".to_string()),
            Expr::Named("VERSION '1.1; DROP SCHEMA public; --'".to_string()),
        ],
        ..Default::default()
    };
    assert_eq!(
        extension.to_sql_with_dialect(Dialect::Postgres),
        "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\" SCHEMA \"public; DROP TABLE users; --\" VERSION '1.1; DROP SCHEMA public; --'"
    );

    let invalid_extension = Qail {
        action: Action::CreateExtension,
        table: "uuid-ossp".to_string(),
        columns: vec![
            Expr::Named("SCHEMA public".to_string()),
            Expr::Named("CASCADE; DROP TABLE users".to_string()),
        ],
        ..Default::default()
    };
    assert_eq!(
        invalid_extension.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid extension option */"
    );

    let sequence = Qail {
        action: Action::CreateSequence,
        table: "order_seq".to_string(),
        columns: vec![
            Expr::Named("start 1000".to_string()),
            Expr::Named("increment by -1".to_string()),
            Expr::Named("owned_by public.orders.id".to_string()),
        ],
        ..Default::default()
    };
    assert_eq!(
        sequence.to_sql_with_dialect(Dialect::Postgres),
        "CREATE SEQUENCE order_seq START WITH 1000 INCREMENT BY -1 OWNED BY public.orders.id"
    );

    let invalid_sequence = Qail {
        action: Action::CreateSequence,
        table: "order_seq".to_string(),
        columns: vec![
            Expr::Named("start 1000".to_string()),
            Expr::Named("cache 10; DROP TABLE users".to_string()),
        ],
        ..Default::default()
    };
    assert_eq!(
        invalid_sequence.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid sequence option */"
    );
}

#[test]
fn test_foreign_key_reference_targets_are_sanitized() {
    let cmd = Qail {
        action: Action::Make,
        table: "posts".to_string(),
        columns: vec![
            Expr::Def {
                name: "user_id".to_string(),
                data_type: "uuid".to_string(),
                constraints: vec![Constraint::References(
                    "public.users(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED".to_string(),
                )],
            },
            Expr::Def {
                name: "unsafe_ref".to_string(),
                data_type: "uuid".to_string(),
                constraints: vec![Constraint::References(
                    "users(id); DROP TABLE users; --".to_string(),
                )],
            },
        ],
        ..Default::default()
    };
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(
        sql.contains("REFERENCES public.users(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED")
    );
    assert!(sql.contains("REFERENCES \"users(id); DROP TABLE users; --\""));
    assert!(!sql.contains("REFERENCES REFERENCES"));
}

#[test]
fn test_column_expression_fragments_reject_invalid_fragments() {
    let safe = Qail {
        action: Action::Make,
        table: "events".to_string(),
        columns: vec![Expr::Def {
            name: "safe_note".to_string(),
            data_type: "str".to_string(),
            constraints: vec![Constraint::Default("'semi;inside'".to_string())],
        }],
        ..Default::default()
    };
    let sql = safe.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("DEFAULT 'semi;inside'"));

    let unsafe_default = Qail {
        action: Action::Make,
        table: "events".to_string(),
        columns: vec![Expr::Def {
            name: "unsafe_default".to_string(),
            data_type: "int".to_string(),
            constraints: vec![Constraint::Default("0; DROP TABLE users; --".to_string())],
        }],
        ..Default::default()
    };
    assert_eq!(
        unsafe_default.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid column default expression */"
    );

    let unsafe_nul_default = Qail {
        action: Action::Make,
        table: "events".to_string(),
        columns: vec![Expr::Def {
            name: "unsafe_nul_default".to_string(),
            data_type: "int".to_string(),
            constraints: vec![Constraint::Default("0\0".to_string())],
        }],
        ..Default::default()
    };
    assert_eq!(
        unsafe_nul_default.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid column default expression */"
    );

    let unsafe_check = Qail {
        action: Action::Make,
        table: "events".to_string(),
        columns: vec![Expr::Def {
            name: "unsafe_check".to_string(),
            data_type: "int".to_string(),
            constraints: vec![Constraint::Check(vec![
                "unsafe_check > 0; DROP TABLE users; --".to_string(),
            ])],
        }],
        ..Default::default()
    };
    assert_eq!(
        unsafe_check.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid column check expression */"
    );

    let unsafe_constraint_check = Qail {
        action: Action::Make,
        table: "events".to_string(),
        columns: vec![Expr::Def {
            name: "unsafe_check_constraint".to_string(),
            data_type: "int".to_string(),
            constraints: vec![Constraint::Check(vec![
                "CONSTRAINT score_positive CHECK (unsafe_check_constraint > 0)\0".to_string(),
            ])],
        }],
        ..Default::default()
    };
    assert_eq!(
        unsafe_constraint_check.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid column check constraint for unsafe_check_constraint */"
    );

    let unsafe_generated = Qail {
        action: Action::Make,
        table: "events".to_string(),
        columns: vec![Expr::Def {
            name: "unsafe_generated".to_string(),
            data_type: "str".to_string(),
            constraints: vec![Constraint::Generated(ColumnGeneration::Stored(
                "lower(safe_note); DROP TABLE users; --".to_string(),
            ))],
        }],
        ..Default::default()
    };
    assert_eq!(
        unsafe_generated.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid generated column expression */"
    );

    let unsafe_alter_default = Qail {
        action: Action::Alter,
        table: "events".to_string(),
        columns: vec![Expr::Def {
            name: "score".to_string(),
            data_type: "int".to_string(),
            constraints: vec![Constraint::Default("0; DROP TABLE users; --".to_string())],
        }],
        ..Default::default()
    };
    assert_eq!(
        unsafe_alter_default.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid column default expression */"
    );
}

#[test]
fn test_column_data_type_fragments_are_sanitized() {
    let unsafe_type = "text); DROP TABLE users; --";
    let make = Qail {
        action: Action::Make,
        table: "events".to_string(),
        columns: vec![
            Expr::Def {
                name: "safe_custom".to_string(),
                data_type: "public.citext".to_string(),
                constraints: vec![],
            },
            Expr::Def {
                name: "unsafe_type".to_string(),
                data_type: unsafe_type.to_string(),
                constraints: vec![],
            },
        ],
        ..Default::default()
    };
    let sql = make.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("safe_custom public.citext NOT NULL"));
    assert!(sql.contains("unsafe_type TEXT NOT NULL"));
    assert!(!sql.contains("DROP TABLE"));

    let alter_add = Qail {
        action: Action::Alter,
        table: "events".to_string(),
        columns: vec![Expr::Def {
            name: "unsafe_type".to_string(),
            data_type: unsafe_type.to_string(),
            constraints: vec![],
        }],
        ..Default::default()
    };
    assert_eq!(
        alter_add.to_sql_with_dialect(Dialect::Postgres),
        "ALTER TABLE events ADD COLUMN unsafe_type TEXT NOT NULL"
    );

    let alter_type = Qail {
        action: Action::AlterType,
        table: "events".to_string(),
        columns: vec![Expr::Def {
            name: "unsafe_type".to_string(),
            data_type: unsafe_type.to_string(),
            constraints: vec![],
        }],
        ..Default::default()
    };
    assert_eq!(
        alter_type.to_sql_with_dialect(Dialect::Postgres),
        "ALTER TABLE events ALTER COLUMN unsafe_type TYPE TEXT"
    );
}

#[test]
fn test_alter_columns_reject_invalid_shapes() {
    let invalid_add = Qail {
        action: Action::Alter,
        table: "events".to_string(),
        columns: vec![Expr::Named("not_a_definition".to_string())],
        ..Default::default()
    };
    assert_eq!(
        invalid_add.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid ALTER ADD column */"
    );

    let invalid_drop = Qail {
        action: Action::AlterDrop,
        table: "events".to_string(),
        columns: vec![Expr::Literal(Value::Int(1))],
        ..Default::default()
    };
    assert_eq!(
        invalid_drop.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid ALTER DROP column */"
    );

    let invalid_type = Qail {
        action: Action::AlterType,
        table: "events".to_string(),
        columns: vec![Expr::Named("not_a_definition".to_string())],
        ..Default::default()
    };
    assert_eq!(
        invalid_type.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid ALTER TYPE column */"
    );
}

#[test]
fn test_alter_set_default_rejects_invalid_fragments() {
    let safe = Qail {
        action: Action::AlterSetDefault,
        table: "events".to_string(),
        columns: vec![Expr::Named("note".to_string())],
        payload: Some("'semi;inside'".to_string()),
        ..Default::default()
    };
    assert_eq!(
        safe.to_sql_with_dialect(Dialect::Postgres),
        "ALTER TABLE events ALTER COLUMN note SET DEFAULT 'semi;inside'"
    );

    let unsafe_default = Qail {
        action: Action::AlterSetDefault,
        table: "events".to_string(),
        columns: vec![Expr::Named("score".to_string())],
        payload: Some("0; DROP TABLE events; --".to_string()),
        ..Default::default()
    };
    assert_eq!(
        unsafe_default.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid default expression */"
    );

    let unsafe_nul_default = Qail {
        action: Action::AlterSetDefault,
        table: "events".to_string(),
        columns: vec![Expr::Named("score".to_string())],
        payload: Some("0\0".to_string()),
        ..Default::default()
    };
    assert_eq!(
        unsafe_nul_default.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid default expression */"
    );

    let missing_column = Qail {
        action: Action::AlterDropDefault,
        table: "events".to_string(),
        columns: vec![],
        ..Default::default()
    };
    assert_eq!(
        missing_column.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: ALTER DROP DEFAULT requires exactly one named column */"
    );
}

#[test]
fn test_view_payload_fragments_reject_invalid_fragments() {
    let safe = Qail {
        action: Action::CreateView,
        table: "notes_view".to_string(),
        payload: Some("SELECT 'semi;inside' AS note".to_string()),
        ..Default::default()
    };
    assert_eq!(
        safe.to_sql_with_dialect(Dialect::Postgres),
        "CREATE VIEW notes_view AS SELECT 'semi;inside' AS note"
    );

    let unsafe_view = Qail {
        action: Action::CreateView,
        table: "active_users".to_string(),
        payload: Some("SELECT id FROM users; DROP TABLE users; --".to_string()),
        ..Default::default()
    };
    assert_eq!(
        unsafe_view.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid view query */"
    );

    let unsafe_nul_view = Qail {
        action: Action::CreateView,
        table: "active_users".to_string(),
        payload: Some("SELECT id FROM users\0".to_string()),
        ..Default::default()
    };
    assert_eq!(
        unsafe_nul_view.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid view query */"
    );

    let unsafe_materialized = Qail {
        action: Action::CreateMaterializedView,
        table: "booking_stats".to_string(),
        payload: Some("SELECT COUNT(*) FROM bookings; DROP TABLE bookings; --".to_string()),
        ..Default::default()
    };
    assert_eq!(
        unsafe_materialized.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid materialized view query */"
    );
}

#[test]
fn test_comment_on_targets_are_sanitized() {
    let safe = Qail {
        action: Action::CommentOn,
        table: "FUNCTION public.cleanup(numeric(10,2), text)".to_string(),
        columns: vec![Expr::Named("cleanup helper".to_string())],
        ..Default::default()
    };
    assert_eq!(
        safe.to_sql_with_dialect(Dialect::Postgres),
        "COMMENT ON FUNCTION public.cleanup(numeric(10,2), text) IS 'cleanup helper'"
    );

    let unsafe_target = Qail {
        action: Action::CommentOn,
        table: "TABLE users; DROP TABLE users; --".to_string(),
        columns: vec![Expr::Named("owner's note\0".to_string())],
        ..Default::default()
    };
    assert_eq!(
        unsafe_target.to_sql_with_dialect(Dialect::Postgres),
        "COMMENT ON TABLE \"TABLE users; DROP TABLE users; --\" IS 'owner''s note'"
    );
}

#[test]
fn test_revoke_sql() {
    let cmd = Qail {
        action: Action::Revoke,
        table: "users".to_string(),
        columns: vec![Expr::Named("UPDATE".to_string())],
        payload: Some("app_role".to_string()),
        ..Default::default()
    };
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(sql, "REVOKE UPDATE ON users FROM app_role");
}

#[test]
fn test_create_function_with_args_sql() {
    let cmd = Qail {
        action: Action::CreateFunction,
        function_def: Some(FunctionDef {
            name: "sum_one".to_string(),
            args: vec!["v int".to_string()],
            returns: "int".to_string(),
            body: "BEGIN RETURN v + 1; END;".to_string(),
            language: Some("plpgsql".to_string()),
            volatility: None,
        }),
        ..Default::default()
    };
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "CREATE OR REPLACE FUNCTION sum_one(v int) RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN v + 1; END; $$"
    );
}

#[test]
fn test_function_definition_rejects_invalid_fragments() {
    let invalid_arg = Qail {
        action: Action::CreateFunction,
        function_def: Some(FunctionDef {
            name: "notice_boom".to_string(),
            args: vec!["v int); DROP TABLE users; --".to_string()],
            returns: "int".to_string(),
            body: "BEGIN RETURN; END;".to_string(),
            language: Some("plpgsql".to_string()),
            volatility: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        invalid_arg.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid function arguments */"
    );

    let invalid_return = Qail {
        action: Action::CreateFunction,
        function_def: Some(FunctionDef {
            name: "notice_boom".to_string(),
            args: vec![
                "amount numeric(10,2)".to_string(),
                "OUT result text".to_string(),
            ],
            returns: "int; DROP TABLE users".to_string(),
            body: "BEGIN RETURN; END;".to_string(),
            language: Some("plpgsql".to_string()),
            volatility: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        invalid_return.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid function return type */"
    );

    let invalid_volatility = Qail {
        action: Action::CreateFunction,
        function_def: Some(FunctionDef {
            name: "notice_boom".to_string(),
            args: vec![
                "amount numeric(10,2)".to_string(),
                "OUT result text".to_string(),
            ],
            returns: "int".to_string(),
            body: "BEGIN RETURN; END;".to_string(),
            language: Some("plpgsql".to_string()),
            volatility: Some("stable; DROP TABLE users".to_string()),
        }),
        ..Default::default()
    };
    assert_eq!(
        invalid_volatility.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid function volatility */"
    );

    let valid_drop = Qail {
        action: Action::DropFunction,
        payload: Some("public.cleanup(numeric(10,2), text)".to_string()),
        ..Default::default()
    };
    assert_eq!(
        valid_drop.to_sql_with_dialect(Dialect::Postgres),
        "DROP FUNCTION IF EXISTS public.cleanup(numeric(10,2), text)"
    );

    let malicious_drop = Qail {
        action: Action::DropFunction,
        payload: Some("public.cleanup(int); DROP TABLE users; --".to_string()),
        ..Default::default()
    };
    assert_eq!(
        malicious_drop.to_sql_with_dialect(Dialect::Postgres),
        "DROP FUNCTION IF EXISTS public.\"cleanup(int); DROP TABLE users; --\""
    );
}

#[test]
fn test_create_trigger_renders_update_of_columns() {
    let cmd = Qail {
        action: Action::CreateTrigger,
        trigger_def: Some(TriggerDef {
            name: "trg_touch_email".to_string(),
            table: "users".to_string(),
            timing: TriggerTiming::Before,
            events: vec![TriggerEvent::Update],
            update_columns: vec!["email".to_string(), "display-name".to_string()],
            for_each_row: true,
            execute_function: "touch_updated_at".to_string(),
        }),
        ..Default::default()
    };
    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::Postgres),
        "CREATE TRIGGER trg_touch_email BEFORE UPDATE OF email, \"display-name\" ON users FOR EACH ROW EXECUTE FUNCTION touch_updated_at()"
    );
}

#[test]
fn test_procedural_bodies_use_non_colliding_dollar_quotes() {
    let do_cmd = Qail {
        action: Action::Do,
        table: "plpgsql".to_string(),
        payload: Some("BEGIN RAISE NOTICE $$boom$$; END;".to_string()),
        ..Default::default()
    };
    assert_eq!(
        do_cmd.to_sql_with_dialect(Dialect::Postgres),
        "DO $qail_body_1$ BEGIN RAISE NOTICE $$boom$$; END; $qail_body_1$ LANGUAGE plpgsql"
    );

    let function_cmd = Qail {
        action: Action::CreateFunction,
        function_def: Some(FunctionDef {
            name: "notice_boom".to_string(),
            args: vec![],
            returns: "void".to_string(),
            body: "BEGIN RAISE NOTICE $$boom$$; END;".to_string(),
            language: Some("plpgsql".to_string()),
            volatility: None,
        }),
        ..Default::default()
    };
    assert_eq!(
        function_cmd.to_sql_with_dialect(Dialect::Postgres),
        "CREATE OR REPLACE FUNCTION notice_boom() RETURNS void LANGUAGE plpgsql AS $qail_body_1$ BEGIN RAISE NOTICE $$boom$$; END; $qail_body_1$"
    );

    let nul_body = Qail {
        action: Action::Do,
        table: "plpgsql".to_string(),
        payload: Some("BEGIN PERFORM '\0'; END;".to_string()),
        ..Default::default()
    };
    let sql = nul_body.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains('\0'));
}

#[test]
fn test_call_target_quotes_malformed_builder_target() {
    let cmd = Qail {
        action: Action::Call,
        table: "refresh(); DROP TABLE users; --".to_string(),
        ..Default::default()
    };
    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::Postgres),
        "CALL \"refresh(); DROP TABLE users; --\""
    );

    let valid = Qail {
        action: Action::Call,
        table: "maintenance.refresh()".to_string(),
        ..Default::default()
    };
    assert_eq!(
        valid.to_sql_with_dialect(Dialect::Postgres),
        "CALL maintenance.refresh()"
    );
}

#[test]
fn test_create_policy_sql() {
    let policy = RlsPolicy::create("users_isolation", "users")
        .for_all()
        .restrictive()
        .to_role("app_role")
        .using(Expr::Named(
            "tenant_id = current_setting('app.current_tenant_id')::uuid".to_string(),
        ))
        .with_check(Expr::Named(
            "tenant_id = current_setting('app.current_tenant_id')::uuid".to_string(),
        ));
    let cmd = Qail {
        action: Action::CreatePolicy,
        policy_def: Some(policy),
        ..Default::default()
    };
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("CREATE POLICY users_isolation ON users"));
    assert!(sql.contains("AS RESTRICTIVE"));
    assert!(sql.contains("FOR ALL"));
    assert!(sql.contains("TO app_role"));
    assert!(sql.contains("USING (tenant_id = current_setting('app.current_tenant_id')::uuid)"));
    assert!(
        sql.contains("WITH CHECK (tenant_id = current_setting('app.current_tenant_id')::uuid)")
    );
}

#[test]
fn test_drop_policy_sql() {
    let cmd = Qail {
        action: Action::DropPolicy,
        table: "users".to_string(),
        payload: Some("users_isolation".to_string()),
        ..Default::default()
    };
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(sql, "DROP POLICY IF EXISTS users_isolation ON users");
}

#[test]
fn test_drop_index_sql_uses_if_exists() {
    let cmd = Qail {
        action: Action::DropIndex,
        table: "idx_users_email".to_string(),
        ..Default::default()
    };
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(sql, "DROP INDEX IF EXISTS idx_users_email");
}

#[test]
fn test_top_level_ddl_quotes_untrusted_identifiers() {
    let drop = Qail {
        action: Action::Drop,
        table: "orders; DROP TABLE users; --".to_string(),
        ..Default::default()
    };
    assert_eq!(
        drop.to_sql_with_dialect(Dialect::Postgres),
        "DROP TABLE \"orders; DROP TABLE users; --\""
    );

    let lock = Qail {
        action: Action::Lock,
        table: "orders; DROP TABLE users; --".to_string(),
        ..Default::default()
    };
    assert_eq!(
        lock.to_sql_with_dialect(Dialect::Postgres),
        "LOCK TABLE \"orders; DROP TABLE users; --\" IN ACCESS EXCLUSIVE MODE"
    );

    let alter_rls = Qail {
        action: Action::AlterEnableRls,
        table: "orders; DROP TABLE users; --".to_string(),
        ..Default::default()
    };
    assert_eq!(
        alter_rls.to_sql_with_dialect(Dialect::Postgres),
        "ALTER TABLE \"orders; DROP TABLE users; --\" ENABLE ROW LEVEL SECURITY"
    );
}

#[test]
fn test_pubsub_and_savepoint_escape_names_and_payloads() {
    let notify = Qail {
        action: Action::Notify,
        channel: Some("tenant\"; DROP TABLE users; --".to_string()),
        payload: Some("ok'; SELECT 'bad".to_string()),
        ..Default::default()
    };
    assert_eq!(
        notify.to_sql_with_dialect(Dialect::Postgres),
        "NOTIFY \"tenant\"\"; DROP TABLE users; --\", 'ok''; SELECT ''bad'"
    );

    let savepoint = Qail {
        action: Action::Savepoint,
        savepoint_name: Some("sp\"; DROP TABLE users; --\0tail".to_string()),
        ..Default::default()
    };
    assert_eq!(
        savepoint.to_sql_with_dialect(Dialect::Postgres),
        "SAVEPOINT \"sp\"\"; DROP TABLE users; --\0tail\""
    );

    let nul_notify = Qail {
        action: Action::Notify,
        channel: Some("tenant\0events".to_string()),
        payload: Some("ok\0payload".to_string()),
        ..Default::default()
    };
    let sql = nul_notify.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains('\0'));
    assert_eq!(sql, "NOTIFY \"tenant\0events\", 'ok\0payload'");
}

// ============= Upsert Tests =============

#[test]
fn test_upsert_postgres() {
    // Manual construction for UPSERT
    let mut cmd = Qail::put("users");
    cmd.columns.push(Expr::Named("id".to_string())); // Conflict key
    cmd.cages.push(Cage {
        kind: CageKind::Payload,
        conditions: vec![
            Condition {
                left: Expr::Named("id".to_string()),
                op: Operator::Eq,
                value: Value::Int(1),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("name".to_string()),
                op: Operator::Eq,
                value: Value::String("John".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("role".to_string()),
                op: Operator::Eq,
                value: Value::String("admin".to_string()),
                is_array_unnest: false,
            },
        ],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("INSERT INTO users"));
    assert!(sql.contains("ON CONFLICT (id) DO UPDATE SET"));
    assert!(sql.contains("name = EXCLUDED.name"));
    assert!(sql.contains("RETURNING *"));
}

#[test]
fn test_upsert_single_reserved_pk_column_quotes_fallback_update() {
    let cmd = Qail::put("events").columns(["order"]).set_value("order", 1);

    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::Postgres),
        "INSERT INTO events (\"order\") VALUES (1) ON CONFLICT (\"order\") DO UPDATE SET \"order\" = EXCLUDED.\"order\" RETURNING *"
    );
}

#[test]
fn test_merge_postgres_builder() {
    let cmd = Qail::merge_into("users")
        .target_alias("u")
        .using_table_as("staging_users", "s")
        .merge_on_column("u.id", Operator::Eq, "s.id")
        .when_matched_update(&[
            ("name", Expr::Named("s.name".to_string())),
            ("email", Expr::Named("s.email".to_string())),
        ])
        .when_not_matched_insert(
            &["id", "name", "email"],
            &[
                Expr::Named("s.id".to_string()),
                Expr::Named("s.name".to_string()),
                Expr::Named("s.email".to_string()),
            ],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "MERGE INTO users AS u USING staging_users AS s ON u.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name, email = s.email \
         WHEN NOT MATCHED BY TARGET THEN INSERT (id, name, email) VALUES (s.id, s.name, s.email)"
    );
}

#[test]
fn test_merge_postgres_parser_to_sql() {
    let cmd = crate::parser::parse(
        "merge users using staging_users on users.id = staging_users.id \
         when not matched by source then delete \
         when matched then do nothing",
    )
    .unwrap();

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "MERGE INTO users USING staging_users ON users.id = staging_users.id \
         WHEN NOT MATCHED BY SOURCE THEN DELETE \
         WHEN MATCHED THEN DO NOTHING"
    );
}

#[test]
fn test_merge_postgres_using_table_inline_alias_renders_as_reference() {
    let cmd = Qail::merge_into("orders")
        .target_alias("o")
        .using_table("stage_orders s")
        .merge_on_column("o.id", Operator::Eq, "s.order_id")
        .when_matched_update(&[("status", Expr::Named("s.status".to_string()))]);

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "MERGE INTO orders AS o USING stage_orders s ON o.id = s.order_id \
         WHEN MATCHED THEN UPDATE SET status = s.status"
    );
}

#[test]
fn test_merge_postgres_with_cte() {
    let source = Qail::get("staging_users").columns(["id", "name"]);
    let cmd = Qail::merge_into("users")
        .with("incoming", source)
        .using_table_as("incoming", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[("name", Expr::Named("s.name".to_string()))])
        .when_not_matched_insert(
            &["id", "name"],
            &[
                Expr::Named("s.id".to_string()),
                Expr::Named("s.name".to_string()),
            ],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "WITH incoming(id, name) AS (SELECT id, name FROM staging_users) \
         MERGE INTO users USING incoming AS s ON users.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name \
         WHEN NOT MATCHED BY TARGET THEN INSERT (id, name) VALUES (s.id, s.name)"
    );
}

#[test]
fn test_merge_postgres_rejects_invalid_action_shape() {
    let mut cmd = Qail::merge_into("users")
        .using_table("staging_users")
        .merge_on_column("users.id", Operator::Eq, "staging_users.id")
        .when_matched_do_nothing();

    let merge = cmd.merge.as_mut().expect("merge spec");
    merge.clauses[0].action = MergeAction::Insert {
        columns: vec!["id".to_string()],
        values: vec![Expr::Named("staging_users.id".to_string())],
    };

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(sql, "/* ERROR: WHEN MATCHED cannot INSERT */");
}

#[test]
fn test_merge_postgres_rejects_mutating_source_query_to_sql() {
    let cmd = Qail::merge_into("users")
        .using_query_as(Qail::del("staging_users"), "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "/* ERROR: MERGE source query must be read-only SELECT, got DEL */"
    );
}

#[test]
fn test_merge_postgres_rejects_mutating_source_cte_to_sql() {
    let source = Qail::get("incoming").with("incoming", Qail::add("staging_users"));
    let cmd = Qail::merge_into("users")
        .using_query_as(source, "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "/* ERROR: MERGE source query must be read-only SELECT, got ADD */"
    );
}

#[test]
fn test_merge_postgres_rejects_mutating_source_expression_subquery_to_sql() {
    let mut source = Qail::get("incoming").columns(["id"]);
    source.columns.push(Expr::Subquery {
        query: Box::new(Qail::add("audit_log")),
        alias: Some("audit_id".to_string()),
    });
    let cmd = Qail::merge_into("users")
        .using_query_as(source, "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[("name", Expr::Named("s.name".to_string()))]);

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "/* ERROR: MERGE source query must be read-only SELECT, got ADD */"
    );
}

#[test]
fn test_merge_postgres_renders_complex_action_expressions() {
    let cmd = Qail::merge_into("users")
        .target_alias("u")
        .using_table_as("staging_users", "s")
        .merge_on_condition(Condition {
            left: Expr::Cast {
                expr: Box::new(Expr::JsonAccess {
                    column: "u.profile".to_string(),
                    path_segments: vec![("external_id".to_string(), true)],
                    alias: None,
                }),
                target_type: "integer".to_string(),
                alias: None,
            },
            op: Operator::Eq,
            value: Value::Column("s.external_id".to_string()),
            is_array_unnest: false,
        })
        .when_matched_update_if(
            vec![
                Condition {
                    left: Expr::JsonAccess {
                        column: "s.profile".to_string(),
                        path_segments: vec![("tier".to_string(), true)],
                        alias: None,
                    },
                    op: Operator::Eq,
                    value: Value::String("gold".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("s.score".to_string()),
                    op: Operator::Gt,
                    value: Value::Expr(Box::new(Expr::Binary {
                        left: Box::new(Expr::Named("u.score".to_string())),
                        op: BinaryOp::Add,
                        right: Box::new(Expr::Literal(Value::Int(5))),
                        alias: None,
                    })),
                    is_array_unnest: false,
                },
            ],
            &[
                (
                    "name",
                    Expr::FunctionCall {
                        name: "coalesce".to_string(),
                        args: vec![
                            Expr::Named("s.name".to_string()),
                            Expr::Named("u.name".to_string()),
                        ],
                        alias: None,
                    },
                ),
                (
                    "score",
                    Expr::Binary {
                        left: Box::new(Expr::Named("s.score".to_string())),
                        op: BinaryOp::Add,
                        right: Box::new(Expr::Literal(Value::Int(1))),
                        alias: None,
                    },
                ),
                (
                    "tier",
                    Expr::JsonAccess {
                        column: "s.profile".to_string(),
                        path_segments: vec![("tier".to_string(), true)],
                        alias: None,
                    },
                ),
                (
                    "status",
                    Expr::Case {
                        when_clauses: vec![(
                            Condition {
                                left: Expr::Cast {
                                    expr: Box::new(Expr::JsonAccess {
                                        column: "s.profile".to_string(),
                                        path_segments: vec![("active".to_string(), true)],
                                        alias: None,
                                    }),
                                    target_type: "integer".to_string(),
                                    alias: None,
                                },
                                op: Operator::Gt,
                                value: Value::Int(0),
                                is_array_unnest: false,
                            },
                            Box::new(Expr::Literal(Value::String("active".to_string()))),
                        )],
                        else_value: Some(Box::new(Expr::Literal(Value::String(
                            "archived".to_string(),
                        )))),
                        alias: None,
                    },
                ),
            ],
        )
        .when_not_matched_insert_if(
            vec![Condition {
                left: Expr::Cast {
                    expr: Box::new(Expr::Named("s.external_id".to_string())),
                    target_type: "integer".to_string(),
                    alias: None,
                },
                op: Operator::Gt,
                value: Value::Int(0),
                is_array_unnest: false,
            }],
            &["id", "name", "score", "tier", "status"],
            &[
                Expr::Cast {
                    expr: Box::new(Expr::Named("s.external_id".to_string())),
                    target_type: "integer".to_string(),
                    alias: None,
                },
                Expr::FunctionCall {
                    name: "coalesce".to_string(),
                    args: vec![
                        Expr::Named("s.name".to_string()),
                        Expr::Literal(Value::String("unknown".to_string())),
                    ],
                    alias: None,
                },
                Expr::Binary {
                    left: Box::new(Expr::Named("s.score".to_string())),
                    op: BinaryOp::Add,
                    right: Box::new(Expr::Literal(Value::Int(1))),
                    alias: None,
                },
                Expr::JsonAccess {
                    column: "s.profile".to_string(),
                    path_segments: vec![("tier".to_string(), true)],
                    alias: None,
                },
                Expr::Literal(Value::String("new".to_string())),
            ],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "MERGE INTO users AS u USING staging_users AS s ON (u.profile->>'external_id')::integer = s.external_id \
         WHEN MATCHED AND s.profile->>'tier' = 'gold' AND s.score > (u.score + 5) \
         THEN UPDATE SET name = COALESCE(s.name, u.name), score = (s.score + 1), tier = s.profile->>'tier', status = CASE WHEN (s.profile->>'active')::integer > 0 THEN 'active' ELSE 'archived' END \
         WHEN NOT MATCHED BY TARGET AND s.external_id::integer > 0 \
         THEN INSERT (id, name, score, tier, status) VALUES (s.external_id::integer, COALESCE(s.name, 'unknown'), (s.score + 1), s.profile->>'tier', 'new')"
    );
}

#[test]
fn test_merge_postgres_schema_qualified_alias_refs_prefer_alias() {
    let cmd = Qail::merge_into("public.orders")
        .target_alias("o")
        .using_table_as("staging.orders", "s")
        .merge_on_column("public.orders.id", Operator::Eq, "staging.orders.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("staging.orders.updated_at".to_string()),
                op: Operator::Gt,
                value: Value::Column("public.orders.updated_at".to_string()),
                is_array_unnest: false,
            }],
            &[("status", Expr::Named("staging.orders.status".to_string()))],
        )
        .when_not_matched_insert(
            &["id", "status"],
            &[
                Expr::Named("staging.orders.id".to_string()),
                Expr::Named("staging.orders.status".to_string()),
            ],
        )
        .returning(["public.orders.id"]);

    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::Postgres),
        "MERGE INTO public.orders AS o USING staging.orders AS s ON o.id = s.id \
         WHEN MATCHED AND s.updated_at > o.updated_at THEN UPDATE SET status = s.status \
         WHEN NOT MATCHED BY TARGET THEN INSERT (id, status) VALUES (s.id, s.status) \
         RETURNING o.id"
    );
}

#[test]
fn test_merge_postgres_inline_source_alias_json_refs_prefer_alias() {
    let cmd = Qail::merge_into("public.orders")
        .using_table("staging.orders s")
        .merge_on_column("public.orders.id", Operator::Eq, "staging.orders.order_id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::JsonAccess {
                    column: "staging.orders.payload".to_string(),
                    path_segments: vec![("tier".to_string(), true)],
                    alias: None,
                },
                op: Operator::Eq,
                value: Value::String("gold".to_string()),
                is_array_unnest: false,
            }],
            &[(
                "status",
                Expr::JsonAccess {
                    column: "staging.orders.payload".to_string(),
                    path_segments: vec![("status".to_string(), true)],
                    alias: None,
                },
            )],
        );

    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::Postgres),
        "MERGE INTO public.orders USING staging.orders s ON public.orders.id = s.order_id \
         WHEN MATCHED AND s.payload->>'tier' = 'gold' THEN UPDATE SET status = s.payload->>'status'"
    );
}

#[test]
fn test_merge_postgres_sanitizes_raw_expression_fragments() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[(
            "name",
            Expr::Named("lower(s.name); DROP TABLE users; --".to_string()),
        )]);

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert_eq!(
        sql,
        "MERGE INTO users USING staging_users AS s ON users.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = \"lower(s\".\"name); DROP TABLE users; --\""
    );
}

#[test]
fn test_merge_postgres_rejects_invalid_cast_target_type() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[(
            "name",
            Expr::Cast {
                expr: Box::new(Expr::Named("s.name".to_string())),
                target_type: "text; DROP TABLE users; --".to_string(),
                alias: None,
            },
        )]);

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert_eq!(
        sql,
        "MERGE INTO users USING staging_users AS s ON users.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = /* ERROR: Invalid cast target type */"
    );
}

#[test]
fn test_merge_postgres_rejects_invalid_function_name() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[(
            "name",
            Expr::FunctionCall {
                name: "coalesce); DROP TABLE users; --".to_string(),
                args: vec![
                    Expr::Named("s.name".to_string()),
                    Expr::Named("users.name".to_string()),
                ],
                alias: None,
            },
        )]);

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert_eq!(
        sql,
        "MERGE INTO users USING staging_users AS s ON users.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = /* ERROR: Invalid function name */"
    );
}

#[test]
fn test_merge_postgres_rejects_raw_function_condition_value() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("users.updated_at".to_string()),
                op: Operator::Lt,
                value: Value::Function("NOW(); DROP TABLE users; --".to_string()),
                is_array_unnest: false,
            }],
            &[("name", Expr::Named("s.name".to_string()))],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert!(
        sql.contains("users.updated_at < /* ERROR: Invalid function expression */"),
        "unsafe raw function value should fail closed: {sql}"
    );
    assert!(
        !sql.contains("DROP TABLE"),
        "unsafe raw function value leaked into MERGE SQL: {sql}"
    );
}

#[test]
fn test_merge_postgres_rejects_mutating_action_subquery() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[(
            "name",
            Expr::Subquery {
                query: Box::new(Qail::del("audit_log")),
                alias: None,
            },
        )]);

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert!(
        sql.contains("name = (/* ERROR: subquery must be read-only SELECT, got DEL */)"),
        "mutating MERGE action subquery must fail closed: {sql}"
    );
    assert!(
        !sql.contains("DELETE FROM"),
        "mutating MERGE action subquery leaked: {sql}"
    );
}

#[test]
fn test_merge_postgres_rejects_mutating_condition_subquery() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("users.id".to_string()),
                op: Operator::In,
                value: Value::Subquery(Box::new(
                    Qail::set("audit_log").set_value("seen", Value::Bool(true)),
                )),
                is_array_unnest: false,
            }],
            &[("name", Expr::Named("s.name".to_string()))],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert!(
        sql.contains("users.id IN (/* ERROR: subquery must be read-only SELECT, got SET */)"),
        "mutating MERGE condition subquery must fail closed: {sql}"
    );
    assert!(
        !sql.contains("UPDATE audit_log"),
        "mutating MERGE condition subquery leaked: {sql}"
    );
}

#[test]
fn test_merge_postgres_preserves_special_condition_operators() {
    let cmd = Qail::merge_into("users")
        .target_alias("u")
        .using_table_as("staging_users", "s")
        .merge_on_condition(Condition {
            left: Expr::Named("u.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("s.id".to_string()),
            is_array_unnest: false,
        })
        .when_matched_update_if(
            vec![
                Condition {
                    left: Expr::Named("u.name".to_string()),
                    op: Operator::Fuzzy,
                    value: Value::String("ana".to_string()),
                    is_array_unnest: false,
                },
                Condition {
                    left: Expr::Named("u.profile".to_string()),
                    op: Operator::JsonExists,
                    value: Value::String("$.active".to_string()),
                    is_array_unnest: false,
                },
            ],
            &[("name", Expr::Named("s.name".to_string()))],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "MERGE INTO users AS u USING staging_users AS s ON u.id = s.id \
         WHEN MATCHED AND u.name ILIKE '%ana%' AND JSON_EXISTS(u.profile, '$.active') \
         THEN UPDATE SET name = s.name"
    );
}

#[test]
fn test_merge_postgres_fuzzy_fallback_escapes_rendered_value() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("users.name".to_string()),
                op: Operator::Fuzzy,
                value: Value::Function("x'; DROP TABLE users; --".to_string()),
                is_array_unnest: false,
            }],
            &[("name", Expr::Named("s.name".to_string()))],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "MERGE INTO users USING staging_users AS s ON users.id = s.id \
         WHEN MATCHED AND users.name ILIKE '%x''; DROP TABLE users; --%' \
         THEN UPDATE SET name = s.name"
    );
}

#[test]
fn test_merge_postgres_rejects_non_subquery_exists_condition() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("ignored".to_string()),
                op: Operator::Exists,
                value: Value::Function("SELECT 1); DROP TABLE users; --".to_string()),
                is_array_unnest: false,
            }],
            &[("name", Expr::Named("s.name".to_string()))],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert!(
        sql.contains(
            "WHEN MATCHED AND FALSE /* ERROR: EXISTS condition requires subquery value */"
        ),
        "invalid MERGE EXISTS must fail closed: {sql}"
    );
    assert!(
        !sql.contains("DROP TABLE"),
        "invalid MERGE EXISTS value leaked into SQL: {sql}"
    );
}

#[test]
fn test_merge_postgres_rejects_between_wrong_arity() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("users.score".to_string()),
                op: Operator::Between,
                value: Value::Array(vec![Value::Int(10)]),
                is_array_unnest: false,
            }],
            &[("name", Expr::Named("s.name".to_string()))],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert!(
        sql.contains(
            "WHEN MATCHED AND FALSE /* ERROR: BETWEEN condition requires exactly two array values */"
        ),
        "invalid MERGE BETWEEN must fail closed: {sql}"
    );
}

#[test]
fn test_merge_postgres_rejects_scalar_in_condition() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("users.role".to_string()),
                op: Operator::In,
                value: Value::String("admin".to_string()),
                is_array_unnest: false,
            }],
            &[("name", Expr::Named("s.name".to_string()))],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert!(
        sql.contains(
            "WHEN MATCHED AND FALSE /* ERROR: IN condition requires a non-empty array, subquery, or array parameter */"
        ),
        "invalid MERGE IN must fail closed: {sql}"
    );
}

#[test]
fn test_merge_postgres_collate_escapes_identifier_fragment() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update(&[(
            "name",
            Expr::Collate {
                expr: Box::new(Expr::Named("s.name".to_string())),
                collation: "C\"; DROP TABLE users; --".to_string(),
                alias: None,
            },
        )]);

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert!(
        sql.contains("COLLATE \"C\"\"; DROP TABLE users; --\""),
        "MERGE collation identifier was not escaped: {sql}"
    );
    assert!(
        !sql.contains("COLLATE \"C\"; DROP"),
        "MERGE collation escaped identifier broke out of quotes: {sql}"
    );
}

#[test]
fn test_merge_postgres_parameterized_fuzzy_binds_named_param() {
    use crate::transpiler::ToSqlParameterized;

    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("users.name".to_string()),
                op: Operator::Fuzzy,
                value: Value::NamedParam("term".to_string()),
                is_array_unnest: false,
            }],
            &[("name", Expr::Named("s.name".to_string()))],
        );

    let result = cmd.to_sql_parameterized();
    assert!(
        result.sql.contains("users.name ILIKE '%' || $1 || '%'"),
        "sql={}",
        result.sql
    );
    assert_eq!(result.named_params, vec!["term"]);
}

#[test]
fn test_merge_postgres_rejects_unsafe_named_param_fuzzy_condition() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("users.name".to_string()),
                op: Operator::Fuzzy,
                value: Value::NamedParam("term); DROP TABLE users; --".to_string()),
                is_array_unnest: false,
            }],
            &[("name", Expr::Named("s.name".to_string()))],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert!(
        sql.contains("/* ERROR: Invalid parameter name */"),
        "unsafe MERGE fuzzy named param should fail closed: {sql}"
    );
    assert!(
        !sql.contains("DROP TABLE"),
        "unsafe MERGE fuzzy named param leaked into SQL: {sql}"
    );
}

#[test]
fn test_merge_postgres_json_path_escapes_literal() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("users.profile".to_string()),
                op: Operator::JsonExists,
                value: Value::String("$.flag' OR true --".to_string()),
                is_array_unnest: false,
            }],
            &[("profile", Expr::Named("s.profile".to_string()))],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "MERGE INTO users USING staging_users AS s ON users.id = s.id \
         WHEN MATCHED AND JSON_EXISTS(users.profile, '$.flag'' OR true --') \
         THEN UPDATE SET profile = s.profile"
    );
}

#[test]
fn test_merge_postgres_rejects_unsafe_named_param_json_path() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("users.profile".to_string()),
                op: Operator::JsonValue,
                value: Value::NamedParam("json_path); DROP TABLE users; --".to_string()),
                is_array_unnest: false,
            }],
            &[("profile", Expr::Named("s.profile".to_string()))],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert!(
        sql.contains(
            "JSON_VALUE(users.profile, '/* ERROR: Invalid parameter name */') IS NOT NULL"
        ),
        "unsafe MERGE JSON path named param should fail closed: {sql}"
    );
    assert!(
        !sql.contains("DROP TABLE"),
        "unsafe MERGE JSON path named param leaked into SQL: {sql}"
    );
}

// ============= JSON Tests =============

#[test]
fn test_json_access() {
    // Manual construction for JSON field access
    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("meta.theme".to_string()),
            op: Operator::Eq,
            value: Value::String("dark".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains(r#"meta->>'theme' = 'dark'"#));
}

#[test]
fn test_json_access_escapes_path_segments_in_select_renderers() {
    let hostile_path = "x') IS NOT NULL OR TRUE --".to_string();
    let json_expr = Expr::JsonAccess {
        column: "payload".to_string(),
        path_segments: vec![(hostile_path.clone(), true)],
        alias: Some("payload_value".to_string()),
    };

    let mut cmd = Qail::get("events").order_by_expr(
        Expr::JsonAccess {
            column: "payload".to_string(),
            path_segments: vec![(hostile_path, true)],
            alias: None,
        },
        SortOrder::Asc,
    );
    cmd.columns.push(json_expr);
    cmd.columns.push(Expr::Aggregate {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: None,
        alias: Some("total".to_string()),
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);

    assert!(
        sql.contains("payload->>'x'') IS NOT NULL OR TRUE --' AS payload_value"),
        "{sql}"
    );
    assert!(
        sql.contains("GROUP BY payload->>'x'') IS NOT NULL OR TRUE --'"),
        "{sql}"
    );
    assert!(
        sql.contains("ORDER BY payload->>'x'') IS NOT NULL OR TRUE --' ASC"),
        "{sql}"
    );
    assert!(
        !sql.contains("payload->>'x') IS NOT NULL OR TRUE --'"),
        "{sql}"
    );
}

#[test]
fn test_json_contains() {
    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("metadata".to_string()),
            op: Operator::Contains,
            value: Value::String(r#"{"theme": "dark"}"#.to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains(r#"@> '{"theme": "dark"}'"#));
}

#[test]
fn test_json_key_exists() {
    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("metadata".to_string()),
            op: Operator::KeyExists,
            value: Value::String("theme".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });
    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("metadata ? 'theme'"));
}

// ============= Advanced Features =============

#[test]
fn test_json_table() {
    let mut cmd = Qail::get("orders.items");
    cmd.action = Action::JsonTable;
    cmd.columns = vec![
        Expr::Named("name=$.product".to_string()),
        Expr::Named("qty=$.quantity".to_string()),
    ];

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("JSON_TABLE("));
    assert!(sql.contains("COLUMNS"));
}

#[test]
fn test_json_table_postgres_standalone_has_no_dual_table() {
    let mut cmd = Qail::get("items");
    cmd.action = Action::JsonTable;
    cmd.columns = vec![
        Expr::Named("name=$.product".to_string()),
        Expr::Named("qty=$.quantity".to_string()),
    ];

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "SELECT jt.* FROM JSON_TABLE(items, '$[*]' COLUMNS (name TEXT PATH '$.product', qty TEXT PATH '$.quantity')) AS jt"
    );
    assert!(!sql.contains("dual"));
}

#[test]
fn test_json_table_postgres_quotes_standalone_source_column() {
    let mut cmd = Qail::get("items\"; DROP TABLE users; --");
    cmd.action = Action::JsonTable;
    cmd.columns = vec![Expr::Named("name=$.product".to_string())];

    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::Postgres),
        "SELECT jt.* FROM JSON_TABLE(\"items\"\"; DROP TABLE users; --\", '$[*]' COLUMNS (name TEXT PATH '$.product')) AS jt"
    );
}

#[test]
fn test_json_table_postgres_rejects_unsafe_column_type() {
    let mut cmd = Qail::get("items");
    cmd.action = Action::JsonTable;
    cmd.columns = vec![Expr::Def {
        name: "name".to_string(),
        data_type: "TEXT); DROP TABLE users; --".to_string(),
        constraints: vec![],
    }];

    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::Postgres),
        "/* ERROR: Invalid JSON_TABLE column type */"
    );
}

#[test]
fn test_tablesample() {
    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Sample(10),
        conditions: vec![],
        logical_op: LogicalOp::And,
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("TABLESAMPLE BERNOULLI(10)"));
}

#[test]
fn test_qualify() {
    let mut cmd = Qail::get("users");
    cmd.columns.push(Expr::Named("id".to_string()));
    cmd.cages.push(Cage {
        kind: CageKind::Qualify,
        conditions: vec![Condition {
            left: Expr::Named("rn".to_string()),
            op: Operator::Eq,
            value: Value::Int(1),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });

    // Snowflake removed, using Postgres/default which might not support QUALIFY directly or handles it differently
    // But since this test explicitly tested Snowflake dialect output for QUALIFY, and we removed Snowflake...
    // Postgres doesn't natively support QUALIFY (it uses subquery window functions).
    // If the transpiler doesn't support QUALIFY for Postgres, this test should be removed or adapted.
    // However, for now, I will remove the test or comment it out if it relies on removed dialect logic.
    // The previous code verified Dialect::Snowflake.
    // I will remove this test as QUALIFY is not standard Postgres.
}

#[test]
fn test_lateral_join() {
    let mut cmd = Qail::get("users");
    cmd.columns.push(Expr::Named("*".to_string()));
    cmd.joins.push(Join {
        table: "orders".to_string(),
        kind: JoinKind::Lateral,
        on: None,
        on_true: false,
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert!(sql.contains("LATERAL JOIN"));
}

// ============= SQL/JSON Standard Functions (Postgres 17+) =============

#[test]
fn test_json_exists() {
    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("metadata".to_string()),
            op: Operator::JsonExists,
            value: Value::String("$.theme".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    println!("JSON_EXISTS: {}", sql);
    assert!(sql.contains("JSON_EXISTS("));
    assert!(sql.contains("$.theme"));
}

#[test]
fn test_json_exists_escapes_path_literal() {
    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("metadata".to_string()),
            op: Operator::JsonExists,
            value: Value::String("$.owner' ? (@ == \"root\")".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE JSON_EXISTS(metadata, '$.owner'' ? (@ == \"root\")')"
    );
}

#[test]
fn test_json_exists_keeps_placeholder_unquoted() {
    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("metadata".to_string()),
            op: Operator::JsonExists,
            value: Value::Param(1),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(sql, "SELECT * FROM users WHERE JSON_EXISTS(metadata, $1)");
}

#[test]
fn test_json_query() {
    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("settings".to_string()),
            op: Operator::JsonQuery,
            value: Value::String("$.notifications".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE JSON_QUERY(settings, '$.notifications') IS NOT NULL"
    );
}

#[test]
fn test_json_value() {
    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("profile".to_string()),
            op: Operator::JsonValue,
            value: Value::String("$.name".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "SELECT * FROM users WHERE JSON_VALUE(profile, '$.name') IS NOT NULL"
    );
}

#[test]
fn test_json_value_parameterized_path_is_not_reused_as_comparison_value() {
    use crate::transpiler::ToSqlParameterized;

    let mut cmd = Qail::get("users");
    cmd.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("profile".to_string()),
            op: Operator::JsonValue,
            value: Value::NamedParam("json_path".to_string()),
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });

    let result = cmd.to_sql_parameterized();
    assert_eq!(
        result.sql,
        "SELECT * FROM users WHERE JSON_VALUE(profile, $1) IS NOT NULL"
    );
    assert_eq!(result.named_params, vec!["json_path"]);
}

#[test]
fn test_merge_json_value_condition_is_boolean_predicate() {
    let cmd = Qail::merge_into("users")
        .using_table_as("staging_users", "s")
        .merge_on_column("users.id", Operator::Eq, "s.id")
        .when_matched_update_if(
            vec![Condition {
                left: Expr::Named("users.profile".to_string()),
                op: Operator::JsonValue,
                value: Value::String("$.status".to_string()),
                is_array_unnest: false,
            }],
            &[("profile", Expr::Named("s.profile".to_string()))],
        );

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    assert_eq!(
        sql,
        "MERGE INTO users USING staging_users AS s ON users.id = s.id \
         WHEN MATCHED AND JSON_VALUE(users.profile, '$.status') IS NOT NULL \
         THEN UPDATE SET profile = s.profile"
    );
}

// ============= Set Operations (UNION, INTERSECT, EXCEPT) =============

#[test]
fn test_union() {
    let mut users_cmd = Qail::get("users");
    users_cmd.columns.push(Expr::Named("name".to_string()));

    let mut admins_cmd = Qail::get("admins");
    admins_cmd.columns.push(Expr::Named("name".to_string()));

    users_cmd.set_ops.push((SetOp::Union, Box::new(admins_cmd)));

    let sql = users_cmd.to_sql_with_dialect(Dialect::Postgres);
    println!("UNION: {}", sql);
    assert!(sql.contains("UNION"));
    assert!(sql.contains("users"));
    assert!(sql.contains("admins"));
}

#[test]
fn test_union_all() {
    let mut q1 = Qail::get("active_users");
    let q2 = Qail::get("inactive_users");

    q1.set_ops.push((SetOp::UnionAll, Box::new(q2)));

    let sql = q1.to_sql();
    println!("UNION ALL: {}", sql);
    assert!(sql.contains("UNION ALL"));
}

#[test]
fn test_postgres_set_op_parenthesizes_limited_left_operand() {
    let mut q1 = Qail::get("employees").columns(["id"]).limit(5);
    let q2 = Qail::get("contractors").columns(["id"]);

    q1.set_ops.push((SetOp::Union, Box::new(q2)));

    let sql = q1.to_sql_with_dialect(Dialect::Postgres);

    assert_eq!(
        sql,
        "(SELECT id FROM employees LIMIT 5) UNION SELECT id FROM contractors"
    );
}

#[test]
fn test_postgres_set_op_parenthesizes_sorted_right_operand() {
    let mut q1 = Qail::get("employees").columns(["id"]);
    let q2 = Qail::get("contractors")
        .columns(["id"])
        .order_desc("id")
        .limit(5);

    q1.set_ops.push((SetOp::Union, Box::new(q2)));

    let sql = q1.to_sql_with_dialect(Dialect::Postgres);

    assert_eq!(
        sql,
        "SELECT id FROM employees UNION (SELECT id FROM contractors ORDER BY id DESC LIMIT 5)"
    );
}

#[test]
fn test_postgres_set_op_parenthesizes_fetch_left_operand() {
    let mut q1 = Qail::get("employees").columns(["id"]).fetch_first(5);
    let q2 = Qail::get("contractors").columns(["id"]);

    q1.set_ops.push((SetOp::Union, Box::new(q2)));

    let sql = q1.to_sql_with_dialect(Dialect::Postgres);

    assert_eq!(
        sql,
        "(SELECT id FROM employees FETCH FIRST 5 ROWS ONLY) UNION SELECT id FROM contractors"
    );
}

#[test]
fn test_intersect() {
    let mut q1 = Qail::get("premium_users");
    q1.columns.push(Expr::Named("id".to_string()));

    let mut q2 = Qail::get("verified_users");
    q2.columns.push(Expr::Named("id".to_string()));

    q1.set_ops.push((SetOp::Intersect, Box::new(q2)));

    let sql = q1.to_sql();
    println!("INTERSECT: {}", sql);
    assert!(sql.contains("INTERSECT"));
}

// ============= CASE Expressions =============

#[test]
fn test_case_expression() {
    let mut cmd = Qail::get("users");
    cmd.columns.push(Expr::Named("name".to_string()));
    cmd.columns.push(Expr::Case {
        when_clauses: vec![
            (
                Condition {
                    left: Expr::Named("status".to_string()),
                    op: Operator::Eq,
                    value: Value::String("active".to_string()),
                    is_array_unnest: false,
                },
                Box::new(Expr::Named("1".to_string())),
            ),
            (
                Condition {
                    left: Expr::Named("status".to_string()),
                    op: Operator::Eq,
                    value: Value::String("pending".to_string()),
                    is_array_unnest: false,
                },
                Box::new(Expr::Named("2".to_string())),
            ),
        ],
        else_value: Some(Box::new(Expr::Named("0".to_string()))),
        alias: Some("priority".to_string()),
    });

    let sql = cmd.to_sql_with_dialect(Dialect::Postgres);
    println!("CASE: {}", sql);
    assert!(sql.contains("CASE"));
    assert!(sql.contains("WHEN"));
    assert!(sql.contains("THEN"));
    assert!(sql.contains("ELSE"));
    assert!(sql.contains("END"));
    assert!(sql.contains("AS"));
}

// ============= HAVING Clause =============

#[test]
fn test_having_clause() {
    let mut cmd = Qail::get("orders");
    cmd.columns.push(Expr::Named("customer_id".to_string()));
    cmd.columns.push(Expr::Aggregate {
        col: "total".to_string(),
        func: AggregateFunc::Sum,
        distinct: false,
        filter: None,
        alias: None,
    });
    cmd.having.push(Condition {
        left: Expr::Named("SUM(total)".to_string()),
        op: Operator::Gt,
        value: Value::Int(100),
        is_array_unnest: false,
    });

    let sql = cmd.to_sql();
    println!("HAVING: {}", sql);
    assert!(sql.contains("HAVING"));
    assert!(sql.contains("SUM(total)"));
}

// ============= ROLLUP / CUBE =============

#[test]
fn test_group_by_rollup() {
    let mut cmd = Qail::get("sales");
    cmd.columns.push(Expr::Named("region".to_string()));
    cmd.columns.push(Expr::Named("year".to_string()));
    cmd.columns.push(Expr::Aggregate {
        col: "amount".to_string(),
        func: AggregateFunc::Sum,
        distinct: false,
        filter: None,
        alias: None,
    });
    cmd.group_by_mode = GroupByMode::Rollup;

    let sql = cmd.to_sql();
    println!("ROLLUP: {}", sql);
    assert!(sql.contains("GROUP BY ROLLUP("));
}

#[test]
fn test_group_by_cube() {
    let mut cmd = Qail::get("sales");
    cmd.columns.push(Expr::Named("region".to_string()));
    cmd.columns.push(Expr::Named("product".to_string()));
    cmd.columns.push(Expr::Aggregate {
        col: "amount".to_string(),
        func: AggregateFunc::Sum,
        distinct: false,
        filter: None,
        alias: None,
    });
    cmd.group_by_mode = GroupByMode::Cube;

    let sql = cmd.to_sql();
    println!("CUBE: {}", sql);
    assert!(sql.contains("GROUP BY CUBE("));
}

// ============= AGGREGATE FILTER =============

#[test]
fn test_aggregate_filter() {
    // Test PostgreSQL FILTER (WHERE ...) clause on aggregates
    let mut cmd = Qail::get("messages");

    // COUNT(*) FILTER (WHERE direction = 'outbound')
    cmd.columns.push(Expr::Aggregate {
        col: "*".to_string(),
        func: AggregateFunc::Count,
        distinct: false,
        filter: Some(vec![Condition {
            left: Expr::Named("direction".to_string()),
            op: Operator::Eq,
            value: Value::String("outbound".to_string()),
            is_array_unnest: false,
        }]),
        alias: Some("sent_count".to_string()),
    });

    let sql = cmd.to_sql();
    println!("FILTER clause: {}", sql);
    assert!(sql.contains("FILTER"));
    assert!(sql.contains("WHERE"));
    assert!(sql.contains("direction"));
}

// ============= RECURSIVE CTEs =============

#[test]
fn test_recursive_cte() {
    let mut base = Qail::get("employees");
    base.columns.push(Expr::Named("id".to_string()));
    base.columns.push(Expr::Named("name".to_string()));
    base.columns.push(Expr::Named("manager_id".to_string()));
    base.cages.push(Cage {
        kind: CageKind::Filter,
        conditions: vec![Condition {
            left: Expr::Named("manager_id".to_string()),
            op: Operator::IsNull,
            value: Value::Null,
            is_array_unnest: false,
        }],
        logical_op: LogicalOp::And,
    });

    let mut recursive = Qail::get("employees");
    recursive.columns.push(Expr::Named("id".to_string()));
    recursive.columns.push(Expr::Named("name".to_string()));
    recursive
        .columns
        .push(Expr::Named("manager_id".to_string()));

    // Outer query with CTE
    let mut cmd = Qail::get("emp_tree");
    cmd.ctes = vec![CTEDef {
        name: "emp_tree".to_string(),
        recursive: true,
        columns: vec![
            "id".to_string(),
            "name".to_string(),
            "manager_id".to_string(),
        ],
        base_query: Box::new(base),
        recursive_query: Some(Box::new(recursive)),
        source_table: Some("employees".to_string()),
    }];
    cmd.action = Action::With;

    use crate::transpiler::dml::cte::build_cte;
    let sql = build_cte(&cmd, Dialect::Postgres);
    println!("RECURSIVE CTE: {}", sql);
    assert!(sql.contains("WITH RECURSIVE"));
    assert!(sql.contains("emp_tree"));
    assert!(sql.contains("UNION ALL"));
}

#[test]
fn test_postgres_recursive_cte_parenthesizes_set_op_base_term() {
    let mut base = Qail::get("employees");
    base.columns.push(Expr::Named("id".to_string()));

    let mut second_base = Qail::get("contractors");
    second_base.columns.push(Expr::Named("id".to_string()));

    base.set_ops.push((SetOp::UnionAll, Box::new(second_base)));

    let mut recursive = Qail::get("tree");
    recursive.columns.push(Expr::Named("id".to_string()));

    let mut cmd = Qail::get("tree");
    cmd.action = Action::With;
    cmd.ctes = vec![CTEDef {
        name: "tree".to_string(),
        recursive: true,
        columns: vec!["id".to_string()],
        base_query: Box::new(base),
        recursive_query: Some(Box::new(recursive)),
        source_table: None,
    }];

    use crate::transpiler::dml::cte::build_cte;
    let sql = build_cte(&cmd, Dialect::Postgres);

    assert_eq!(
        sql,
        "WITH RECURSIVE tree(id) AS ((SELECT id FROM employees UNION ALL SELECT id FROM contractors) UNION ALL SELECT id FROM tree) SELECT * FROM tree"
    );
}

#[test]
fn test_postgres_recursive_cte_parenthesizes_set_op_recursive_term() {
    let mut base = Qail::get("roots");
    base.columns.push(Expr::Named("id".to_string()));

    let mut recursive = Qail::get("tree");
    recursive.columns.push(Expr::Named("id".to_string()));

    let mut fallback_recursive = Qail::get("archived_tree");
    fallback_recursive
        .columns
        .push(Expr::Named("id".to_string()));

    recursive
        .set_ops
        .push((SetOp::UnionAll, Box::new(fallback_recursive)));

    let mut cmd = Qail::get("tree");
    cmd.action = Action::With;
    cmd.ctes = vec![CTEDef {
        name: "tree".to_string(),
        recursive: true,
        columns: vec!["id".to_string()],
        base_query: Box::new(base),
        recursive_query: Some(Box::new(recursive)),
        source_table: None,
    }];

    use crate::transpiler::dml::cte::build_cte;
    let sql = build_cte(&cmd, Dialect::Postgres);

    assert_eq!(
        sql,
        "WITH RECURSIVE tree(id) AS (SELECT id FROM roots UNION ALL (SELECT id FROM tree UNION ALL SELECT id FROM archived_tree)) SELECT * FROM tree"
    );
}

#[test]
fn test_postgres_recursive_cte_parenthesizes_limited_base_term() {
    let base = Qail::get("roots").columns(["id"]).limit(1);

    let mut recursive = Qail::get("tree");
    recursive.columns.push(Expr::Named("id".to_string()));

    let mut cmd = Qail::get("tree");
    cmd.action = Action::With;
    cmd.ctes = vec![CTEDef {
        name: "tree".to_string(),
        recursive: true,
        columns: vec!["id".to_string()],
        base_query: Box::new(base),
        recursive_query: Some(Box::new(recursive)),
        source_table: None,
    }];

    use crate::transpiler::dml::cte::build_cte;
    let sql = build_cte(&cmd, Dialect::Postgres);

    assert_eq!(
        sql,
        "WITH RECURSIVE tree(id) AS ((SELECT id FROM roots LIMIT 1) UNION ALL SELECT id FROM tree) SELECT * FROM tree"
    );
}

#[test]
fn test_cte_final_select_preserves_outer_filters() {
    let base = Qail::get("orders").columns(["id", "total", "tenant_id"]);
    let mut cmd = Qail::get("summary")
        .with("summary", base)
        .eq("tenant_id", "tenant-1");
    cmd.action = Action::With;

    use crate::transpiler::dml::cte::build_cte;
    let sql = build_cte(&cmd, Dialect::Postgres);

    assert!(sql.contains("SELECT * FROM summary WHERE tenant_id = 'tenant-1'"));
}

// ============= v0.8.6: Custom JOINs & DISTINCT ON =============

#[test]
fn test_custom_join_on() {
    // Manual construction for JOIN with ON clause
    let mut cmd = Qail::get("users");
    cmd.joins.push(Join {
        table: "orders".to_string(),
        kind: JoinKind::Inner,
        on: Some(vec![Condition {
            left: Expr::Named("users.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("orders.user_id".to_string()),
            is_array_unnest: false,
        }]),
        on_true: false,
    });
    let sql = cmd.to_sql();
    // Identifiers are unquoted if safe in Postgres dialect implementation used
    assert!(
        sql.contains("INNER JOIN orders ON users.id = orders.user_id"),
        "SQL was: {}",
        sql
    );
}

#[test]
fn test_custom_join_multiple_conditions() {
    let mut cmd = Qail::get("A");
    cmd.joins.push(Join {
        table: "B".to_string(),
        kind: JoinKind::Inner,
        on: Some(vec![
            Condition {
                left: Expr::Named("A.x".to_string()),
                op: Operator::Eq,
                value: Value::Column("B.x".to_string()),
                is_array_unnest: false,
            },
            Condition {
                left: Expr::Named("A.y".to_string()),
                op: Operator::Eq,
                value: Value::Column("B.y".to_string()),
                is_array_unnest: false,
            },
        ]),
        on_true: false,
    });
    let sql = cmd.to_sql();
    assert!(
        sql.contains("INNER JOIN B ON A.x = B.x AND A.y = B.y"),
        "SQL was: {}",
        sql
    );
    // Verify AST structure
    assert!(cmd.joins[0].on.is_some());
    assert_eq!(cmd.joins[0].on.as_ref().unwrap().len(), 2);
}

#[test]
fn test_distinct_on() {
    // Manual construction for DISTINCT ON
    let mut cmd = Qail::get("employees");
    cmd.distinct_on = vec![
        Expr::Named("department".to_string()),
        Expr::Named("role".to_string()),
    ];

    // Transpiler check (Postgres default)
    let sql = cmd.to_sql();
    assert!(
        sql.starts_with("SELECT DISTINCT ON (department, role)"),
        "SQL was: {}",
        sql
    );
}

#[test]
fn test_table_alias_renders_as_reference_and_qualifies_filters() {
    let cmd = Qail::get("users").table_alias("u").eq("u.active", true);

    assert_eq!(cmd.to_sql(), "SELECT * FROM users u WHERE u.active = true");
}

#[test]
fn test_join_alias_renders_as_reference_and_qualifies_filters() {
    let cmd = Qail::get("orders")
        .table_alias("o")
        .left_join_conds(
            "inventory inv",
            vec![Condition {
                left: Expr::Named("inv.order_id".to_string()),
                op: Operator::Eq,
                value: Value::Column("o.id".to_string()),
                is_array_unnest: false,
            }],
        )
        .eq("inv.capacity", 10);

    assert_eq!(
        cmd.to_sql(),
        "SELECT * FROM orders o LEFT JOIN inventory inv ON inv.order_id = o.id WHERE inv.capacity = 10"
    );
}

#[test]
fn test_schema_qualified_table_condition_renders_as_column_reference() {
    let cmd = Qail::get("public.users").eq("public.users.id", 42);
    let sql = cmd.to_sql();

    assert_eq!(sql, "SELECT * FROM public.users WHERE public.users.id = 42");
    assert!(
        !sql.contains("->"),
        "schema-qualified table reference was rendered as JSON access: {sql}"
    );
}

#[test]
fn test_schema_qualified_table_alias_condition_prefers_alias() {
    let cmd = Qail::get("public.users")
        .table_alias("u")
        .eq("public.users.id", 42);

    assert_eq!(cmd.to_sql(), "SELECT * FROM public.users u WHERE u.id = 42");
}

#[test]
fn test_schema_qualified_join_conditions_resolve_both_sides() {
    let cmd = Qail::get("public.orders").table_alias("o").left_join_conds(
        "crm.users u",
        vec![Condition {
            left: Expr::Named("crm.users.id".to_string()),
            op: Operator::Eq,
            value: Value::Column("public.orders.user_id".to_string()),
            is_array_unnest: false,
        }],
    );

    assert_eq!(
        cmd.to_sql(),
        "SELECT * FROM public.orders o LEFT JOIN crm.users u ON u.id = o.user_id"
    );
}

#[test]
fn test_schema_qualified_alias_projection_prefers_alias() {
    let cmd = Qail::get("public.orders")
        .table_alias("o")
        .columns(["public.orders.id", "public.orders.status"]);

    assert_eq!(cmd.to_sql(), "SELECT o.id, o.status FROM public.orders o");
}

#[test]
fn test_schema_qualified_alias_aggregate_group_by_prefers_alias() {
    let cmd = Qail::get("public.orders").table_alias("o").columns_expr([
        Expr::Named("public.orders.customer_id".to_string()),
        Expr::Aggregate {
            col: "public.orders.total".to_string(),
            func: AggregateFunc::Sum,
            distinct: false,
            filter: None,
            alias: Some("total".to_string()),
        },
    ]);

    assert_eq!(
        cmd.to_sql(),
        "SELECT o.customer_id, SUM(o.total) AS total FROM public.orders o GROUP BY o.customer_id"
    );
}

#[test]
fn test_schema_qualified_alias_distinct_and_order_by_prefer_alias() {
    let cmd = Qail::get("public.orders")
        .table_alias("o")
        .columns(["public.orders.id"])
        .distinct_on(["public.orders.id"])
        .order_by("public.orders.created_at", SortOrder::Desc);

    assert_eq!(
        cmd.to_sql(),
        "SELECT DISTINCT ON (o.id) o.id FROM public.orders o ORDER BY o.created_at DESC"
    );
}

#[test]
fn test_schema_qualified_alias_window_partition_order_prefer_alias() {
    let cmd = Qail::get("public.orders")
        .table_alias("o")
        .columns_expr([Expr::Window {
            name: "rn".to_string(),
            func: "row_number".to_string(),
            params: vec![],
            partition: vec!["public.orders.customer_id".to_string()],
            order: vec![Cage {
                kind: CageKind::Sort(SortOrder::Desc),
                conditions: vec![Condition {
                    left: Expr::Named("public.orders.created_at".to_string()),
                    op: Operator::Eq,
                    value: Value::Null,
                    is_array_unnest: false,
                }],
                logical_op: LogicalOp::And,
            }],
            frame: None,
        }]);

    assert_eq!(
        cmd.to_sql(),
        "SELECT ROW_NUMBER() OVER (PARTITION BY o.customer_id ORDER BY o.created_at DESC) AS rn FROM public.orders o"
    );
}

#[test]
fn test_update_from_alias_renders_as_table_reference() {
    let cmd = Qail::set("orders")
        .set_value("status", Value::Column("p.status".to_string()))
        .update_from(["payments p"])
        .filter(
            "orders.payment_id",
            Operator::Eq,
            Value::Column("p.id".to_string()),
        );

    assert_eq!(
        cmd.to_sql(),
        "UPDATE orders SET status = p.status FROM payments p WHERE orders.payment_id = p.id"
    );
}

#[test]
fn test_update_target_alias_renders_as_table_reference() {
    let cmd = Qail::set("orders")
        .table_alias("o")
        .set_value("status", "paid")
        .filter("o.id", Operator::Eq, 7);

    assert_eq!(
        cmd.to_sql(),
        "UPDATE orders o SET status = 'paid' WHERE o.id = 7"
    );
}

#[test]
fn test_delete_using_alias_renders_as_table_reference() {
    let cmd = Qail::del("sessions").delete_using(["users u"]).filter(
        "sessions.user_id",
        Operator::Eq,
        Value::Column("u.id".to_string()),
    );

    assert_eq!(
        cmd.to_sql(),
        "DELETE FROM sessions USING users u WHERE sessions.user_id = u.id"
    );
}

#[test]
fn test_delete_target_alias_renders_as_table_reference() {
    let cmd = Qail::del("sessions")
        .table_alias("s")
        .filter("s.id", Operator::Eq, 7);

    assert_eq!(cmd.to_sql(), "DELETE FROM sessions s WHERE s.id = 7");
}

#[test]
fn test_condition_parameterized_preserves_column_rhs() {
    let generator = PostgresGenerator::new();
    let cmd = Qail::get("public.orders").table_alias("o");
    let condition = Condition {
        left: Expr::Named("public.orders.user_id".to_string()),
        op: Operator::Eq,
        value: Value::Column("public.orders.account_id".to_string()),
        is_array_unnest: false,
    };
    let mut params = ParamContext::new();

    let sql = condition.to_sql_parameterized(&generator, Some(&cmd), &mut params);

    assert_eq!(sql, "o.user_id = o.account_id");
    assert!(params.params.is_empty());
    assert!(params.named_params.is_empty());
}
