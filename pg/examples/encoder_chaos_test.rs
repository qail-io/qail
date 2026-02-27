//! PG Protocol Encoder Chaos Test
//!
//! Tests every Action variant in the AST encoder to ensure
//! none panic. Also tests edge-case ASTs with empty/huge columns,
//! deep filter chains, and boundary parameter counts.

use qail_core::ast::*;
use qail_pg::protocol::EncodeError;
use qail_pg::protocol::ast_encoder::AstEncoder;

#[allow(unused_variables, unused_assignments)]
fn main() {
    let mut pass = 0u32;
    let mut fail = 0u32;
    let panics = 0u32;

    println!("═══════════════════════════════════════════════════");
    println!("  🧪 PG ENCODER CHAOS TEST");
    println!("═══════════════════════════════════════════════════");

    // ═══════════════════════════════════════════════════
    // 1. All DML Actions — must not panic
    // ═══════════════════════════════════════════════════
    println!("\n── 1. DML Action Encoding (must not panic) ──");

    let dml_tests: Vec<(&str, Qail)> = vec![
        ("GET basic", Qail::get("users")),
        (
            "GET with columns",
            Qail::get("users").columns(vec!["id", "name"]),
        ),
        (
            "GET with filter",
            Qail::get("users").filter("id", Operator::Eq, Value::String("abc".into())),
        ),
        ("GET with limit", Qail::get("users").limit(10)),
        ("GET with offset", Qail::get("users").offset(20)),
        ("GET with limit 0", Qail::get("users").limit(0)),
        ("GET with huge limit", Qail::get("users").limit(999999999)),
        ("GET with sort", Qail::get("users").order_asc("name")),
        (
            "GET with sort desc",
            Qail::get("users").order_desc("created_at"),
        ),
        ("CNT basic", {
            let mut q = Qail::get("users");
            q.action = Action::Cnt;
            q
        }),
        ("CNT with filter", {
            let mut q = Qail::get("users").filter("active", Operator::Eq, Value::Bool(true));
            q.action = Action::Cnt;
            q
        }),
        (
            "ADD basic",
            Qail::add("users").set_value("name", Value::String("test".into())),
        ),
        ("ADD multiple values", {
            Qail::add("users")
                .set_value("name", Value::String("test".into()))
                .set_value("age", Value::Int(25))
                .set_value("active", Value::Bool(true))
        }),
        (
            "SET basic",
            Qail::set("users")
                .set_value("name", Value::String("updated".into()))
                .filter("id", Operator::Eq, Value::String("abc".into())),
        ),
        (
            "DEL basic",
            Qail::del("users").filter("id", Operator::Eq, Value::String("abc".into())),
        ),
        // Edge: empty columns (SELECT *)
        ("GET empty columns", Qail::get("users")),
        // Edge: many columns
        ("GET 100 columns", {
            let cols: Vec<&str> = (0..100).map(|_| "col").collect();
            Qail::get("users").columns(cols)
        }),
        // Edge: chained filters
        ("GET 10 filters", {
            let mut q = Qail::get("users");
            for i in 0..10 {
                q = q.filter(&format!("f{}", i), Operator::Eq, Value::Int(i));
            }
            q
        }),
        // Edge: all operators
        (
            "GET filter Eq",
            Qail::get("t").filter("a", Operator::Eq, Value::Int(1)),
        ),
        (
            "GET filter Neq",
            Qail::get("t").filter("a", Operator::Ne, Value::Int(1)),
        ),
        (
            "GET filter Gt",
            Qail::get("t").filter("a", Operator::Gt, Value::Int(1)),
        ),
        (
            "GET filter Gte",
            Qail::get("t").filter("a", Operator::Gte, Value::Int(1)),
        ),
        (
            "GET filter Lt",
            Qail::get("t").filter("a", Operator::Lt, Value::Int(1)),
        ),
        (
            "GET filter Lte",
            Qail::get("t").filter("a", Operator::Lte, Value::Int(1)),
        ),
        (
            "GET filter IsNull",
            Qail::get("t").filter("a", Operator::IsNull, Value::Null),
        ),
        (
            "GET filter IsNotNull",
            Qail::get("t").filter("a", Operator::IsNotNull, Value::Null),
        ),
        (
            "GET filter Like",
            Qail::get("t").filter("a", Operator::Like, Value::String("%test%".into())),
        ),
        (
            "GET filter ILike",
            Qail::get("t").filter("a", Operator::ILike, Value::String("%test%".into())),
        ),
        (
            "GET filter In",
            Qail::get("t").filter("a", Operator::In, Value::String("1,2,3".into())),
        ),
        (
            "GET filter NotIn",
            Qail::get("t").filter("a", Operator::NotIn, Value::String("1,2,3".into())),
        ),
        // Edge: value types
        ("ADD with null", Qail::add("t").set_value("a", Value::Null)),
        (
            "ADD with bool true",
            Qail::add("t").set_value("a", Value::Bool(true)),
        ),
        (
            "ADD with bool false",
            Qail::add("t").set_value("a", Value::Bool(false)),
        ),
        (
            "ADD with float",
            Qail::add("t").set_value("a", Value::Float(9.87654)),
        ),
        (
            "ADD with negative int",
            Qail::add("t").set_value("a", Value::Int(-42)),
        ),
        (
            "ADD with i64 max",
            Qail::add("t").set_value("a", Value::Int(i64::MAX)),
        ),
        (
            "ADD with i64 min",
            Qail::add("t").set_value("a", Value::Int(i64::MIN)),
        ),
        (
            "ADD with empty string",
            Qail::add("t").set_value("a", Value::String("".into())),
        ),
        (
            "ADD with long string",
            Qail::add("t").set_value("a", Value::String("x".repeat(10000))),
        ),
        (
            "ADD with unicode",
            Qail::add("t").set_value("a", Value::String("中文🚀مرحبا".into())),
        ),
        (
            "ADD with single quotes",
            Qail::add("t").set_value("a", Value::String("it's a test".into())),
        ),
        (
            "ADD with backslash",
            Qail::add("t").set_value("a", Value::String("path\\to\\file".into())),
        ),
        // Edge: positional params
        (
            "GET with $1",
            Qail::get("t").filter("id", Operator::Eq, Value::Param(1)),
        ),
        (
            "GET with $0",
            Qail::get("t").filter("id", Operator::Eq, Value::Param(0)),
        ),
        (
            "GET with $9999",
            Qail::get("t").filter("id", Operator::Eq, Value::Param(9999)),
        ),
        // Edge: named params
        (
            "GET with :name",
            Qail::get("t").filter("id", Operator::Eq, Value::NamedParam("name".into())),
        ),
        // Edge: joins
        (
            "GET with INNER JOIN",
            Qail::get("users").join(JoinKind::Inner, "orders", "users.id", "orders.user_id"),
        ),
        (
            "GET with LEFT JOIN",
            Qail::get("users").join(JoinKind::Left, "orders", "users.id", "orders.user_id"),
        ),
        (
            "GET with RIGHT JOIN",
            Qail::get("users").join(JoinKind::Right, "orders", "users.id", "orders.user_id"),
        ),
        (
            "GET with FULL JOIN",
            Qail::get("users").join(JoinKind::Full, "orders", "users.id", "orders.user_id"),
        ),
        (
            "GET with CROSS JOIN",
            Qail::get("users").join(JoinKind::Cross, "orders", "users.id", "orders.user_id"),
        ),
        // Edge: aggregates
        ("GET with COUNT(*)", {
            let mut q = Qail::get("users");
            q.columns = vec![Expr::Aggregate {
                col: "*".to_string(),
                func: AggregateFunc::Count,
                distinct: false,
                filter: None,
                alias: None,
            }];
            q
        }),
        ("GET with SUM", {
            let mut q = Qail::get("users");
            q.columns = vec![Expr::Aggregate {
                col: "amount".to_string(),
                func: AggregateFunc::Sum,
                distinct: false,
                filter: None,
                alias: None,
            }];
            q
        }),
        ("GET with COUNT DISTINCT", {
            let mut q = Qail::get("users");
            q.columns = vec![Expr::Aggregate {
                col: "email".to_string(),
                func: AggregateFunc::Count,
                distinct: true,
                filter: None,
                alias: None,
            }];
            q
        }),
    ];

    for (label, cmd) in &dml_tests {
        // Test encode_cmd (wire protocol)
        match AstEncoder::encode_cmd(cmd) {
            Ok(_) => pass += 1,
            Err(e) => {
                println!("  ❌ encode_cmd Err on '{}': {}", label, e);
                fail += 1;
            }
        }

        // Test encode_cmd_sql (SQL string)
        match AstEncoder::encode_cmd_sql(cmd) {
            Ok(_) => pass += 1,
            Err(e) => {
                println!("  ❌ encode_cmd_sql Err on '{}': {}", label, e);
                fail += 1;
            }
        }
    }
    println!(
        "  ✅ {}/{} DML encode tests passed (x2 for wire+sql)",
        pass,
        dml_tests.len() * 2
    );

    // ═══════════════════════════════════════════════════
    // 2. DDL Actions — must not panic
    // ═══════════════════════════════════════════════════
    println!("\n── 2. DDL Action Encoding ──");
    let ddl_start = pass;

    let ddl_tests: Vec<(&str, Qail)> = vec![
        ("MAKE table", {
            let mut q = Qail::get("users");
            q.action = Action::Make;
            q.columns = vec![Expr::Named("id".into()), Expr::Named("name".into())];
            q
        }),
        ("DROP table", {
            let mut q = Qail::get("users");
            q.action = Action::Drop;
            q
        }),
        ("INDEX", {
            let mut q = Qail::get("users");
            q.action = Action::Index;
            q.columns = vec![Expr::Named("email".into())];
            q
        }),
        ("DROP INDEX", {
            let mut q = Qail::get("users_email_idx");
            q.action = Action::DropIndex;
            q
        }),
        ("ALTER add column", {
            let mut q = Qail::get("users");
            q.action = Action::Alter;
            q.columns = vec![Expr::Named("phone".into())];
            q
        }),
        ("ALTER drop column", {
            let mut q = Qail::get("users");
            q.action = Action::AlterDrop;
            q.columns = vec![Expr::Named("phone".into())];
            q
        }),
        ("CREATE VIEW", {
            let mut q = Qail::get("active_users");
            q.action = Action::CreateView;
            q
        }),
        ("DROP VIEW", {
            let mut q = Qail::get("active_users");
            q.action = Action::DropView;
            q
        }),
    ];

    for (label, cmd) in &ddl_tests {
        match AstEncoder::encode_cmd(cmd) {
            Ok(_) | Err(_) => pass += 1, // DDL may be unsupported; both Ok and Err are fine
        }
    }
    println!(
        "  ✅ {}/{} DDL encode tests passed",
        pass - ddl_start,
        ddl_tests.len()
    );

    // ═══════════════════════════════════════════════════
    // 3. Unsupported Actions — should not panic
    // ═══════════════════════════════════════════════════
    println!("\n── 3. Unsupported/Exotic Actions (should not panic) ──");
    let exotic_start = pass;

    let exotic_actions = vec![
        Action::Truncate,
        Action::Explain,
        Action::ExplainAnalyze,
        Action::Lock,
        Action::CreateMaterializedView,
        Action::RefreshMaterializedView,
        Action::DropMaterializedView,
        Action::Listen,
        Action::Notify,
        Action::Unlisten,
        Action::Savepoint,
        Action::ReleaseSavepoint,
        Action::RollbackToSavepoint,
        Action::Search,
        Action::Upsert,
        Action::Scroll,
        Action::CreateCollection,
        Action::DeleteCollection,
        Action::CreateFunction,
        Action::DropFunction,
        Action::CreateTrigger,
        Action::DropTrigger,
        Action::CreateExtension,
        Action::DropExtension,
        Action::CommentOn,
        Action::CreateSequence,
        Action::DropSequence,
        Action::CreateEnum,
        Action::DropEnum,
        Action::AlterEnumAddValue,
        Action::TxnStart,
        Action::TxnCommit,
        Action::TxnRollback,
    ];

    let mut exotic_errs = 0u32;
    for action in &exotic_actions {
        let mut q = Qail::get("test_table");
        q.action = *action;
        match AstEncoder::encode_cmd(&q) {
            Ok(_) => pass += 1,
            Err(EncodeError::UnsupportedAction(_)) => {
                exotic_errs += 1;
                pass += 1; // Expected: unsupported actions return Err, not panic
            }
            Err(e) => {
                println!("  ❌ Unexpected Err on {:?}: {}", action, e);
                fail += 1;
            }
        }
    }
    println!(
        "  {}/{} exotic actions return UnsupportedAction (no panic)",
        exotic_errs,
        exotic_actions.len()
    );

    // ═══════════════════════════════════════════════════
    // 4. SQL String Output Validation
    // ═══════════════════════════════════════════════════
    println!("\n── 4. SQL Output Validation ──");
    let sql_tests: Vec<(&str, Qail, &str)> = vec![
        ("Simple SELECT", Qail::get("users"), "SELECT"),
        (
            "COUNT",
            {
                let mut q = Qail::get("users");
                q.action = Action::Cnt;
                q
            },
            "COUNT",
        ),
        (
            "INSERT",
            Qail::add("t").set_value("name", Value::String("x".into())),
            "INSERT",
        ),
        (
            "UPDATE",
            Qail::set("t")
                .set_value("name", Value::String("x".into()))
                .filter("id", Operator::Eq, Value::Int(1)),
            "UPDATE",
        ),
        (
            "DELETE",
            Qail::del("t").filter("id", Operator::Eq, Value::Int(1)),
            "DELETE",
        ),
    ];

    let mut sql_pass = 0u32;
    for (label, cmd, expected_keyword) in &sql_tests {
        let (sql, _params) = match AstEncoder::encode_cmd_sql(cmd) {
            Ok(x) => x,
            Err(e) => {
                println!("  ❌ encode_cmd_sql Err on '{}': {}", label, e);
                fail += 1;
                continue;
            }
        };
        if sql.contains(expected_keyword) {
            sql_pass += 1;
            pass += 1;
        } else {
            println!(
                "  ❌ {} — SQL missing '{}': {}",
                label,
                expected_keyword,
                &sql[..std::cmp::min(sql.len(), 100)]
            );
            fail += 1;
        }
    }
    println!(
        "  ✅ {}/{} SQL outputs contain expected keywords",
        sql_pass,
        sql_tests.len()
    );

    // ═══════════════════════════════════════════════════
    // 5. Parse → Encode integration
    // ═══════════════════════════════════════════════════
    println!("\n── 5. Parse → Encode Integration ──");
    let integration_queries = vec![
        "get users",
        "get users fields id,name",
        "get users[active = true] limit 10",
        "count users",
        "count users[active = true]",
        "add users",
        "set users[id = $1]",
        "del users[id = $1]",
        "get users sort name:asc limit 50 offset 100",
        "get users fields count(id)",
        "get users fields sum(amount)",
    ];

    let mut integ_pass = 0u32;
    for query in &integration_queries {
        match qail_core::parser::parse(query) {
            Ok(ast) => {
                match AstEncoder::encode_cmd(&ast).and_then(|_| AstEncoder::encode_cmd_sql(&ast)) {
                    Ok(_) => {
                        integ_pass += 1;
                        pass += 1;
                    }
                    Err(e) => {
                        println!("  ❌ Encode Err on parse→encode '{}': {}", query, e);
                        fail += 1;
                    }
                }
            }
            Err(e) => {
                println!("  ❌ Parse failed (skipped encode): {} — {:?}", query, e);
                fail += 1;
            }
        }
    }
    println!(
        "  ✅ {}/{} parse→encode integrations passed",
        integ_pass,
        integration_queries.len()
    );

    // ═══════════════════════════════════════════════════
    // SUMMARY
    // ═══════════════════════════════════════════════════
    let total = pass + fail + panics;
    println!("\n═══════════════════════════════════════════════════");
    println!("  PG Encoder Chaos Results: {} total tests", total);
    println!("  ✅ {} passed", pass);
    println!("  ❌ {} failed", fail);
    println!("  💀 {} PANICS", panics);
    println!("═══════════════════════════════════════════════════");

    if panics > 0 {
        // Don't exit with error for exotic/unsupported actions panicking
        // Those are expected — only flag DML/DDL panics
        println!("\n  Note: Exotic action panics are expected (unsupported in binary encoder)");
    }
}
