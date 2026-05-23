//! PostgreSQL MERGE integration tests for qail-pg.
//!
//! These are ignored by default because they require a live PostgreSQL server.
//! For local runs:
//! `QAIL_MERGE_DATABASE_URL=postgres://qail:qail@127.0.0.1:55432/qail_test \
//! cargo test -p qail-pg --test merge_integration -- --ignored --nocapture`

use qail_core::ast::{Expr, Operator, Qail};
use qail_core::parser::parse;
use qail_core::rls::RlsContext;
use qail_core::rls::tenant::register_tenant_table;
use qail_pg::{PgDriver, PgResult};
use uuid::Uuid;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

fn database_url() -> String {
    std::env::var("QAIL_MERGE_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://qail:qail@127.0.0.1:55432/qail_test".to_string())
}

fn test_table(prefix: &str) -> String {
    format!("{}_{}", prefix, Uuid::new_v4().simple())
}

async fn connect() -> PgResult<PgDriver> {
    PgDriver::connect_url(&database_url()).await
}

async fn create_merge_tables(driver: &mut PgDriver, target: &str, source: &str) -> PgResult<()> {
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {target}"))
        .await?;
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {source}"))
        .await?;
    driver
        .execute_simple(&format!(
            "CREATE TABLE {target} (
                id integer PRIMARY KEY,
                name text NOT NULL,
                status text NOT NULL,
                tenant_id text
            )"
        ))
        .await?;
    driver
        .execute_simple(&format!(
            "CREATE TABLE {source} (
                id integer PRIMARY KEY,
                name text NOT NULL,
                status text NOT NULL,
                tenant_id text
            )"
        ))
        .await?;
    Ok(())
}

async fn drop_merge_tables(driver: &mut PgDriver, target: &str, source: &str) -> PgResult<()> {
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {target}"))
        .await?;
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {source}"))
        .await?;
    Ok(())
}

async fn create_expression_merge_tables(
    driver: &mut PgDriver,
    target: &str,
    source: &str,
) -> PgResult<()> {
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {target}"))
        .await?;
    driver
        .execute_simple(&format!("DROP TABLE IF EXISTS {source}"))
        .await?;
    driver
        .execute_simple(&format!(
            "CREATE TABLE {target} (
                id integer PRIMARY KEY,
                name text NOT NULL,
                score integer NOT NULL,
                tier text NOT NULL,
                status text NOT NULL,
                profile jsonb NOT NULL,
                tenant_id text
            )"
        ))
        .await?;
    driver
        .execute_simple(&format!(
            "CREATE TABLE {source} (
                external_id text PRIMARY KEY,
                name text,
                score integer NOT NULL,
                profile jsonb NOT NULL,
                deleted_at timestamptz,
                tenant_id text
            )"
        ))
        .await?;
    Ok(())
}

async fn rows_for(
    driver: &mut PgDriver,
    table: &str,
) -> PgResult<Vec<(i32, String, String, String)>> {
    let rows = driver
        .fetch_all(
            &Qail::get(table)
                .columns(["id", "name", "status", "tenant_id"])
                .order_asc("id"),
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.get_i32(0).expect("id"),
                row.get_string(1).expect("name"),
                row.get_string(2).expect("status"),
                row.get_string(3).expect("tenant_id"),
            )
        })
        .collect())
}

async fn expression_rows_for(
    driver: &mut PgDriver,
    table: &str,
) -> PgResult<Vec<(i32, String, i32, String, String, String)>> {
    let rows = driver
        .fetch_all(
            &Qail::get(table)
                .columns(["id", "name", "score", "tier", "status", "tenant_id"])
                .order_asc("id"),
        )
        .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.get_i32(0).expect("id"),
                row.get_string(1).expect("name"),
                row.get_i32(2).expect("score"),
                row.get_string(3).expect("tier"),
                row.get_string(4).expect("status"),
                row.get_string(5).expect("tenant_id"),
            )
        })
        .collect())
}

#[tokio::test]
#[ignore = "Requires PostgreSQL 17+; run explicitly with QAIL_MERGE_DATABASE_URL"]
async fn test_merge_parser_ast_encoder_update_insert_delete_against_postgres() -> PgResult<()> {
    let mut driver = connect().await?;
    let target = test_table("qail_merge_target");
    let source = test_table("qail_merge_source");
    create_merge_tables(&mut driver, &target, &source).await?;

    driver
        .execute_simple(&format!(
            "INSERT INTO {target} (id, name, status, tenant_id) VALUES
                (1, 'old-a', 'old', '{TENANT_A}'),
                (2, 'keep-a', 'keep', '{TENANT_A}'),
                (5, 'stale-a', 'stale', '{TENANT_A}')"
        ))
        .await?;
    driver
        .execute_simple(&format!(
            "INSERT INTO {source} (id, name, status, tenant_id) VALUES
                (1, 'new-a', 'active', '{TENANT_A}'),
                (2, 'keep-a', 'keep', '{TENANT_A}'),
                (3, 'insert-a', 'active', '{TENANT_A}')"
        ))
        .await?;

    let cmd = parse(&format!(
        "merge {target} as t using {source} as s on t.id = s.id \
         when matched and t.name != s.name then update set name = s.name, status = s.status, tenant_id = s.tenant_id \
         when not matched by target then insert (id, name, status, tenant_id) values (s.id, s.name, s.status, s.tenant_id) \
         when not matched by source then delete"
    ))
    .expect("parser should accept MERGE");

    let affected = driver.execute(&cmd).await?;
    assert_eq!(affected, 3, "expected update + insert + delete");

    assert_eq!(
        rows_for(&mut driver, &target).await?,
        vec![
            (
                1,
                "new-a".to_string(),
                "active".to_string(),
                TENANT_A.to_string()
            ),
            (
                2,
                "keep-a".to_string(),
                "keep".to_string(),
                TENANT_A.to_string()
            ),
            (
                3,
                "insert-a".to_string(),
                "active".to_string(),
                TENANT_A.to_string()
            ),
        ]
    );

    drop_merge_tables(&mut driver, &target, &source).await
}

#[tokio::test]
#[ignore = "Requires PostgreSQL 17+; run explicitly with QAIL_MERGE_DATABASE_URL"]
async fn test_merge_with_rls_scopes_update_insert_and_by_source_delete() -> PgResult<()> {
    let mut driver = connect().await?;
    let target = test_table("qail_merge_rls_target");
    let source = test_table("qail_merge_rls_source");
    create_merge_tables(&mut driver, &target, &source).await?;
    register_tenant_table(&target, "tenant_id");
    register_tenant_table(&source, "tenant_id");

    driver
        .execute_simple(&format!(
            "INSERT INTO {target} (id, name, status, tenant_id) VALUES
                (1, 'old-a', 'old', '{TENANT_A}'),
                (2, 'stale-a', 'stale', '{TENANT_A}'),
                (10, 'old-b', 'old', '{TENANT_B}')"
        ))
        .await?;
    driver
        .execute_simple(&format!(
            "INSERT INTO {source} (id, name, status, tenant_id) VALUES
                (1, 'new-a', 'active', '{TENANT_A}'),
                (3, 'insert-a', 'active', '{TENANT_A}'),
                (10, 'new-b', 'active', '{TENANT_B}'),
                (11, 'insert-b', 'active', '{TENANT_B}')"
        ))
        .await?;

    let cmd = Qail::merge_into(&target)
        .target_alias("t")
        .using_table_as(&source, "s")
        .merge_on_column("t.id", Operator::Eq, "s.id")
        .when_matched_update(&[
            ("name", Expr::Named("s.name".to_string())),
            ("status", Expr::Named("s.status".to_string())),
        ])
        .when_not_matched_insert(
            &["id", "name", "status"],
            &[
                Expr::Named("s.id".to_string()),
                Expr::Named("s.name".to_string()),
                Expr::Named("s.status".to_string()),
            ],
        )
        .when_not_matched_by_source_delete()
        .with_rls(&RlsContext::tenant(TENANT_A))
        .expect("MERGE should accept explicit insert columns for RLS");

    let affected = driver.execute(&cmd).await?;
    assert_eq!(
        affected, 3,
        "tenant A should update, insert, and delete once"
    );

    assert_eq!(
        rows_for(&mut driver, &target).await?,
        vec![
            (
                1,
                "new-a".to_string(),
                "active".to_string(),
                TENANT_A.to_string()
            ),
            (
                3,
                "insert-a".to_string(),
                "active".to_string(),
                TENANT_A.to_string()
            ),
            (
                10,
                "old-b".to_string(),
                "old".to_string(),
                TENANT_B.to_string()
            ),
        ]
    );

    drop_merge_tables(&mut driver, &target, &source).await
}

#[tokio::test]
#[ignore = "Requires PostgreSQL 17+; run explicitly with QAIL_MERGE_DATABASE_URL"]
async fn test_merge_cte_query_source_and_returning_against_postgres() -> PgResult<()> {
    let mut driver = connect().await?;
    let target = test_table("qail_merge_returning_target");
    let source = test_table("qail_merge_returning_source");
    create_merge_tables(&mut driver, &target, &source).await?;

    driver
        .execute_simple(&format!(
            "INSERT INTO {target} (id, name, status, tenant_id) VALUES
                (1, 'old-a', 'old', '{TENANT_A}')"
        ))
        .await?;
    driver
        .execute_simple(&format!(
            "INSERT INTO {source} (id, name, status, tenant_id) VALUES
                (1, 'new-a', 'active', '{TENANT_A}'),
                (2, 'insert-a', 'active', '{TENANT_A}'),
                (3, 'ignored-a', 'active', '{TENANT_A}')"
        ))
        .await?;

    let incoming = Qail::get(&source)
        .columns(["id", "name", "status", "tenant_id"])
        .filter("id", Operator::Lt, 3);
    let source_query = Qail::get("incoming").columns(["id", "name", "status", "tenant_id"]);
    let mut cmd = Qail::merge_into(&target)
        .with("incoming", incoming)
        .using_query_as(source_query, "s")
        .merge_on_column(format!("{target}.id"), Operator::Eq, "s.id")
        .when_matched_update(&[
            ("name", Expr::Named("s.name".to_string())),
            ("status", Expr::Named("s.status".to_string())),
        ])
        .when_not_matched_insert(
            &["id", "name", "status", "tenant_id"],
            &[
                Expr::Named("s.id".to_string()),
                Expr::Named("s.name".to_string()),
                Expr::Named("s.status".to_string()),
                Expr::Named("s.tenant_id".to_string()),
            ],
        );
    cmd.returning = Some(vec![
        Expr::Named("merge_action()".to_string()),
        Expr::Named(format!("{target}.id")),
    ]);

    let result = driver.query_ast(&cmd).await?;
    let mut returned = result
        .rows
        .into_iter()
        .map(|row| {
            (
                row[0].as_deref().expect("merge action").to_string(),
                row[1]
                    .as_deref()
                    .expect("id")
                    .parse::<i32>()
                    .expect("int id"),
            )
        })
        .collect::<Vec<_>>();
    returned.sort_by_key(|(_, id)| *id);

    assert_eq!(
        returned,
        vec![("UPDATE".to_string(), 1), ("INSERT".to_string(), 2)]
    );
    assert_eq!(
        rows_for(&mut driver, &target).await?,
        vec![
            (
                1,
                "new-a".to_string(),
                "active".to_string(),
                TENANT_A.to_string()
            ),
            (
                2,
                "insert-a".to_string(),
                "active".to_string(),
                TENANT_A.to_string()
            ),
        ]
    );

    drop_merge_tables(&mut driver, &target, &source).await
}

#[tokio::test]
#[ignore = "Requires PostgreSQL 17+; run explicitly with QAIL_MERGE_DATABASE_URL"]
async fn test_merge_complex_expressions_against_postgres() -> PgResult<()> {
    let mut driver = connect().await?;
    let target = test_table("qail_merge_expr_target");
    let source = test_table("qail_merge_expr_source");
    create_expression_merge_tables(&mut driver, &target, &source).await?;

    driver
        .execute_simple(&format!(
            "INSERT INTO {target} (id, name, score, tier, status, profile, tenant_id) VALUES
                (1, 'old-a', 10, 'silver', 'old', '{{\"external_id\":\"1\",\"tier\":\"silver\"}}', '{TENANT_A}')"
        ))
        .await?;
    driver
        .execute_simple(&format!(
            "INSERT INTO {source} (external_id, name, score, profile, deleted_at, tenant_id) VALUES
                ('1', 'new-a', 20, '{{\"tier\":\"gold\",\"active\":\"1\"}}', NULL, '{TENANT_A}'),
                ('2', NULL, 5, '{{\"tier\":\"bronze\",\"active\":\"1\"}}', NULL, '{TENANT_A}'),
                ('-1', 'ignored-a', 99, '{{\"tier\":\"gold\",\"active\":\"1\"}}', NULL, '{TENANT_A}')"
        ))
        .await?;

    let cmd = Qail::merge_into(&target)
        .target_alias("t")
        .using_table_as(&source, "s")
        .merge_on_condition(qail_core::ast::Condition {
            left: Expr::Cast {
                expr: Box::new(Expr::JsonAccess {
                    column: "t.profile".to_string(),
                    path_segments: vec![("external_id".to_string(), true)],
                    alias: None,
                }),
                target_type: "integer".to_string(),
                alias: None,
            },
            op: Operator::Eq,
            value: qail_core::ast::Value::Expr(Box::new(Expr::Cast {
                expr: Box::new(Expr::Named("s.external_id".to_string())),
                target_type: "integer".to_string(),
                alias: None,
            })),
            is_array_unnest: false,
        })
        .when_matched_update_if(
            vec![
                qail_core::ast::Condition {
                    left: Expr::JsonAccess {
                        column: "s.profile".to_string(),
                        path_segments: vec![("tier".to_string(), true)],
                        alias: None,
                    },
                    op: Operator::Eq,
                    value: qail_core::ast::Value::String("gold".to_string()),
                    is_array_unnest: false,
                },
                qail_core::ast::Condition {
                    left: Expr::Named("s.score".to_string()),
                    op: Operator::Gt,
                    value: qail_core::ast::Value::Expr(Box::new(Expr::Binary {
                        left: Box::new(Expr::Named("t.score".to_string())),
                        op: qail_core::ast::BinaryOp::Add,
                        right: Box::new(Expr::Literal(qail_core::ast::Value::Int(5))),
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
                            Expr::Named("t.name".to_string()),
                        ],
                        alias: None,
                    },
                ),
                (
                    "score",
                    Expr::Binary {
                        left: Box::new(Expr::Named("s.score".to_string())),
                        op: qail_core::ast::BinaryOp::Add,
                        right: Box::new(Expr::Literal(qail_core::ast::Value::Int(1))),
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
                            qail_core::ast::Condition {
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
                                value: qail_core::ast::Value::Int(0),
                                is_array_unnest: false,
                            },
                            Box::new(Expr::Literal(qail_core::ast::Value::String(
                                "active".to_string(),
                            ))),
                        )],
                        else_value: Some(Box::new(Expr::Literal(qail_core::ast::Value::String(
                            "archived".to_string(),
                        )))),
                        alias: None,
                    },
                ),
            ],
        )
        .when_not_matched_insert_if(
            vec![qail_core::ast::Condition {
                left: Expr::Cast {
                    expr: Box::new(Expr::Named("s.external_id".to_string())),
                    target_type: "integer".to_string(),
                    alias: None,
                },
                op: Operator::Gt,
                value: qail_core::ast::Value::Int(0),
                is_array_unnest: false,
            }],
            &[
                "id",
                "name",
                "score",
                "tier",
                "status",
                "profile",
                "tenant_id",
            ],
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
                        Expr::Literal(qail_core::ast::Value::String("unknown".to_string())),
                    ],
                    alias: None,
                },
                Expr::Binary {
                    left: Box::new(Expr::Named("s.score".to_string())),
                    op: qail_core::ast::BinaryOp::Add,
                    right: Box::new(Expr::Literal(qail_core::ast::Value::Int(1))),
                    alias: None,
                },
                Expr::JsonAccess {
                    column: "s.profile".to_string(),
                    path_segments: vec![("tier".to_string(), true)],
                    alias: None,
                },
                Expr::Literal(qail_core::ast::Value::String("new".to_string())),
                Expr::Named("s.profile".to_string()),
                Expr::Named("s.tenant_id".to_string()),
            ],
        );

    let affected = driver.execute(&cmd).await?;
    assert_eq!(affected, 2, "expected expression update + insert");
    assert_eq!(
        expression_rows_for(&mut driver, &target).await?,
        vec![
            (
                1,
                "new-a".to_string(),
                21,
                "gold".to_string(),
                "active".to_string(),
                TENANT_A.to_string()
            ),
            (
                2,
                "unknown".to_string(),
                6,
                "bronze".to_string(),
                "new".to_string(),
                TENANT_A.to_string()
            ),
        ]
    );

    drop_merge_tables(&mut driver, &target, &source).await
}
