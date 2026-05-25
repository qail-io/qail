//! SQL Dialect tests.

use crate::ast::{Action, Expr, Qail};
use crate::parser::parse;
use crate::transpiler::{Dialect, ToSql};

#[test]
fn test_sqlite_dialect() {
    let cmd = parse("get users fields * where active = true").unwrap();
    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::SQLite),
        "SELECT * FROM \"users\" WHERE \"active\" = 1"
    );

    let cmd_fuzzy = parse("get users fields * where name ~ $1").unwrap();
    assert_eq!(
        cmd_fuzzy.to_sql_with_dialect(Dialect::SQLite),
        "SELECT * FROM \"users\" WHERE \"name\" LIKE '%' || ? || '%'"
    );
}

#[test]
fn sqlite_identifier_quoting_escapes_embedded_quotes() {
    let cmd = Qail {
        action: Action::Get,
        table: "users\"; DROP TABLE audit; --".to_string(),
        columns: vec![Expr::Named("na\"me".to_string())],
        ..Default::default()
    };

    assert_eq!(
        cmd.to_sql_with_dialect(Dialect::SQLite),
        "SELECT \"na\"\"me\" FROM \"users\"\"; DROP TABLE audit; --\""
    );
}
